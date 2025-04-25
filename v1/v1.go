// Filename: search_index.go
package v1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/goccy/go-reflect"
	"github.com/oarkflow/json"

	"github.com/oarkflow/search/v1/utils"
)

type GenericRecord map[string]any

func (rec GenericRecord) String() string {
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		switch val := rec[k].(type) {
		case string:
			parts = append(parts, val)
		case json.Number:
			if i, err := val.Int64(); err == nil {
				parts = append(parts, strconv.FormatInt(i, 10))
			} else if f, err := val.Float64(); err == nil {
				parts = append(parts, strconv.FormatFloat(f, 'f', -1, 64))
			}
		case float64:
			parts = append(parts, strconv.FormatFloat(val, 'f', -1, 64))
		case int:
			parts = append(parts, strconv.Itoa(val))
		default:
		}
	}
	return strings.Join(parts, " ")
}

func (rec GenericRecord) getFrequency() map[string]int {
	combined := rec.String()
	tokens := utils.Tokenize(combined)
	freq := make(map[string]int, len(tokens))
	for _, t := range tokens {
		freq[t]++
	}
	return freq
}

type job struct {
	id  int
	rec GenericRecord
}
type result struct {
	id   int
	rec  GenericRecord
	freq map[string]int
}

type Posting struct {
	DocID     int
	Frequency int
}

type ScoredDoc struct {
	DocID int
	Score float64
}

type Index struct {
	sync.RWMutex
	ID                 string
	Index              map[string][]Posting
	DocLengths         map[int]int
	Documents          map[int]GenericRecord
	TotalDocs          int
	AvgDocLength       float64
	indexingInProgress bool
	numWorkers         int
}

type IndexRequest struct {
	Path string          `json:"path"`
	Data []GenericRecord `json:"data"`
}

type Options func(*Index)

func WithNumOfWorkers(numOfWorkers int) Options {
	return func(index *Index) {
		index.numWorkers = numOfWorkers
	}
}

func NewIndex(id string, opts ...Options) *Index {
	index := &Index{
		ID:         id,
		numWorkers: runtime.NumCPU(),
		Index:      make(map[string][]Posting),
		DocLengths: make(map[int]int),
		Documents:  make(map[int]GenericRecord),
	}
	for _, opt := range opts {
		opt(index)
	}
	return index
}

func (index *Index) FuzzySearch(term string, threshold int) []string {
	index.RLock()
	defer index.RUnlock()
	var results []string
	for token := range index.Index {
		if utils.BoundedLevenshtein(term, token, threshold) <= threshold {
			results = append(results, token)
		}
	}
	return results
}

func (index *Index) BuildFromReader(ctx context.Context, r io.Reader, callbacks ...func(v GenericRecord) error) error {
	index.Lock()
	if index.indexingInProgress {
		index.Unlock()
		return fmt.Errorf("indexing already in progress")
	}
	index.indexingInProgress = true
	index.Unlock()
	defer func() {
		index.Lock()
		index.indexingInProgress = false
		index.Unlock()
	}()
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return fmt.Errorf("invalid JSON array")
	}
	jobs := make(chan job, 100)
	results := make(chan result, 100)
	var wg sync.WaitGroup
	for i := 0; i < index.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
					freq := j.rec.getFrequency()
					results <- result{id: j.id, rec: j.rec, freq: freq}
				}
			}
		}()
	}
	go func() {
		docID := 0
		for decoder.More() {
			select {
			case <-ctx.Done():
				break
			default:
				var rec GenericRecord
				if err := decoder.Decode(&rec); err != nil {
					log.Printf("Skipping invalid record: %v", err)
					continue
				}
				docID++
				jobs <- job{id: docID, rec: rec}
			}
		}
		close(jobs)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	for res := range results {
		index.Lock()
		index.indexDoc(job{id: res.id, rec: res.rec}, res.freq)
		index.TotalDocs++
		index.Unlock()
		for _, cb := range callbacks {
			if err := cb(res.rec); err != nil {
				log.Printf("callback error: %v", err)
			}
		}
	}
	index.update()
	return nil
}

func (index *Index) Build(ctx context.Context, input any, callbacks ...func(v GenericRecord) error) error {
	switch v := input.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if strings.HasPrefix(trimmed, "[") {
			return index.BuildFromReader(ctx, strings.NewReader(v), callbacks...)
		}
		return index.BuildFromFile(ctx, v, callbacks...)
	case []byte:
		return index.BuildFromReader(ctx, bytes.NewReader(v), callbacks...)
	case io.Reader:
		return index.BuildFromReader(ctx, v, callbacks...)
	case []GenericRecord:
		return index.BuildFromRecords(ctx, v, callbacks...)
	case IndexRequest:
		if v.Path != "" {
			return index.BuildFromFile(ctx, v.Path, callbacks...)
		}
		if len(v.Data) > 0 {
			return index.BuildFromRecords(ctx, v.Data, callbacks...)
		}
		return fmt.Errorf("no data or path provided")
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			return index.BuildFromStruct(ctx, v, callbacks...)
		}
	}
	return fmt.Errorf("unsupported input type: %T", input)
}

func (index *Index) BuildFromFile(ctx context.Context, path string, callbacks ...func(v GenericRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return index.BuildFromReader(ctx, file, callbacks...)
}

func (index *Index) BuildFromRecords(ctx context.Context, records []GenericRecord, callbacks ...func(v GenericRecord) error) error {
	index.Lock()
	if index.indexingInProgress {
		index.Unlock()
		return fmt.Errorf("indexing already in progress")
	}
	index.indexingInProgress = true
	index.Unlock()
	defer func() {
		index.Lock()
		index.indexingInProgress = false
		index.Unlock()
	}()
	var wg sync.WaitGroup
	ch := make(chan job, len(records))
	for i := 0; i < index.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range ch {
				select {
				case <-ctx.Done():
					return
				default:
					freq := j.rec.getFrequency()
					index.Lock()
					index.indexDoc(j, freq)
					index.TotalDocs++
					index.Unlock()

					for _, cb := range callbacks {
						if err := cb(j.rec); err != nil {
							log.Printf("callback error: %v", err)
						}
					}
				}
			}
		}()
	}

	for i, rec := range records {
		select {
		case <-ctx.Done():
			break
		default:
			ch <- job{i + 1, rec}
		}
	}
	close(ch)
	wg.Wait()
	index.update()
	return nil
}

func (index *Index) BuildFromStruct(ctx context.Context, slice any, callbacks ...func(v GenericRecord) error) error {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("not a slice")
	}
	var records []GenericRecord
	for i := 0; i < v.Len(); i++ {
		b, err := json.Marshal(v.Index(i).Interface())
		if err != nil {
			return fmt.Errorf("error marshalling element %d: %v", i, err)
		}
		var rec GenericRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return fmt.Errorf("error unmarshalling element %d: %v", i, err)
		}
		records = append(records, rec)
	}
	return index.BuildFromRecords(ctx, records, callbacks...)
}

func (index *Index) indexDoc(r job, freq map[string]int) {
	index.Documents[r.id] = r.rec
	docLen := 0
	for t, count := range freq {
		index.Index[t] = append(index.Index[t], Posting{DocID: r.id, Frequency: count})
		docLen += count
	}
	index.DocLengths[r.id] = docLen
}

func (index *Index) update() {
	total := 0
	for _, l := range index.DocLengths {
		total += l
	}
	if index.TotalDocs > 0 {
		index.AvgDocLength = float64(total) / float64(index.TotalDocs)
	}
}

func (index *Index) bm25Score(queryTokens []string, docID int, k1, b float64) float64 {
	index.RLock()
	defer index.RUnlock()
	score := 0.0
	docLength := float64(index.DocLengths[docID])
	for _, term := range queryTokens {
		postings, ok := index.Index[term]
		if !ok {
			continue
		}
		df := float64(len(postings))
		var tf int
		for _, p := range postings {
			if p.DocID == docID {
				tf = p.Frequency
				break
			}
		}
		if tf == 0 {
			continue
		}
		idf := math.Log((float64(index.TotalDocs)-df+0.5)/(df+0.5) + 1)
		tfScore := (float64(tf) * (k1 + 1)) / (float64(tf) + k1*(1-b+b*(docLength/index.AvgDocLength)))
		score += idf * tfScore
	}
	return score
}

type BM25 struct {
	K float64
	B float64
}

var defaultBM25 = BM25{K: 1.2, B: 0.75}

func (index *Index) Search(ctx context.Context, q Query, queryText string, bm ...BM25) ([]ScoredDoc, error) {
	index.RLock()
	if index.indexingInProgress {
		index.RUnlock()
		return nil, fmt.Errorf("indexing in progress; please try again later")
	}
	index.RUnlock()
	params := defaultBM25
	if len(bm) > 0 {
		params = bm[0]
	}
	queryTokens := utils.Tokenize(queryText)
	docIDs := q.Evaluate(index)
	var (
		scored []ScoredDoc
		wg     sync.WaitGroup
		mu     sync.Mutex
	)
	ch := make(chan int, len(docIDs))
	for _, id := range docIDs {
		ch <- id
	}
	close(ch)
	for i := 0; i < index.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for docID := range ch {
				select {
				case <-ctx.Done():
					return
				default:
					score := index.bm25Score(queryTokens, docID, params.K, params.B)
					mu.Lock()
					scored = append(scored, ScoredDoc{DocID: docID, Score: score})
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})
	return scored, nil
}

func Paginate(docs []ScoredDoc, page, perPage int) []ScoredDoc {
	start := (page - 1) * perPage
	if start >= len(docs) {
		return nil
	}
	end := start + perPage
	if end > len(docs) {
		end = len(docs)
	}
	return docs[start:end]
}

func (index *Index) AddDocument(rec GenericRecord) {
	index.Lock()
	defer index.Unlock()
	docID := index.TotalDocs + 1
	j := job{id: docID, rec: rec}
	freq := rec.getFrequency()
	index.indexDoc(j, freq)
	index.TotalDocs++
	index.update()
}

func (index *Index) UpdateDocument(docID int, rec GenericRecord) error {
	index.Lock()
	defer index.Unlock()
	if _, ok := index.Documents[docID]; !ok {
		return fmt.Errorf("document %d does not exist", docID)
	}

	oldRec := index.Documents[docID]
	oldFreq := oldRec.getFrequency()
	for term, count := range oldFreq {
		if posting, exists := index.Index[term]; exists {
			if len(posting) == 0 {
				delete(index.Index, term)
			}
		}
		index.DocLengths[docID] -= count
	}

	index.Documents[docID] = rec
	newFreq := rec.getFrequency()
	docLen := 0
	for term, count := range newFreq {
		docLen += count
		if posting, exists := index.Index[term]; exists {
			posting = append(posting, Posting{DocID: docID, Frequency: count})
			index.Index[term] = posting
		} else {
			index.Index[term] = []Posting{{DocID: count}}
		}
	}
	index.DocLengths[docID] = docLen
	index.update()
	return nil
}

func (index *Index) DeleteDocument(docID int) error {
	index.Lock()
	defer index.Unlock()
	if _, ok := index.Documents[docID]; !ok {
		return fmt.Errorf("document %d does not exist", docID)
	}
	rec := index.Documents[docID]
	freq := rec.getFrequency()
	for term := range freq {
		if posting, exists := index.Index[term]; exists {
			if len(posting) == 0 {
				delete(index.Index, term)
			}
		}
	}
	delete(index.Documents, docID)
	delete(index.DocLengths, docID)
	index.TotalDocs--
	index.update()
	return nil
}

type QueryFunc func(index *Index) []int

func (f QueryFunc) Evaluate(index *Index) []int {
	return f(index)
}
