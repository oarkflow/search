// Filename: search_index.go
package v1

import (
	"bytes"
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
	"github.com/oarkflow/xid"

	"github.com/oarkflow/search/v1/utils"
)

type GenericRecord map[string]any

func (rec GenericRecord) String() string {
	var parts []string
	for _, v := range rec {
		switch val := v.(type) {
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

type InvertedIndex struct {
	sync.RWMutex
	ID                 string
	Index              map[string][]Posting
	DocLengths         map[int]int
	Documents          map[int]GenericRecord
	TotalDocs          int
	AvgDocLength       float64
	indexingInProgress bool
}

func NewIndex() *InvertedIndex {
	return &InvertedIndex{
		ID:         xid.New().String(),
		Index:      make(map[string][]Posting),
		DocLengths: make(map[int]int),
		Documents:  make(map[int]GenericRecord),
	}
}

func (index *InvertedIndex) FuzzySearch(term string, threshold int) []string {
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

func (index *InvertedIndex) BuildFromReader(r io.Reader, callback ...func(v GenericRecord) error) error {
	index.Lock()
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

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				freq := j.rec.getFrequency()
				results <- result{id: j.id, rec: j.rec, freq: freq}
			}
		}()
	}

	go func() {
		docID := 0
		for decoder.More() {
			var rec GenericRecord
			if err := decoder.Decode(&rec); err != nil {
				log.Printf("Skipping invalid record: %v", err)
				continue
			}
			docID++
			jobs <- job{id: docID, rec: rec}
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		index.Lock()
		index.indexDoc(job{id: r.id, rec: r.rec}, r.freq)
		index.TotalDocs++
		index.Unlock()

		if len(callback) > 0 {
			if err := callback[0](r.rec); err != nil {
				log.Printf("callback error: %v", err)
			}
		}
	}
	index.update()
	return nil
}

func (index *InvertedIndex) Build(input any, callback ...func(v GenericRecord) error) error {
	switch v := input.(type) {
	case string:
		if strings.HasPrefix(strings.TrimSpace(v), "[") {
			return index.BuildFromReader(strings.NewReader(v), callback...)
		}
		return index.BuildFromFile(v, callback...)
	case []byte:
		return index.BuildFromReader(bytes.NewReader(v), callback...)
	case io.Reader:
		return index.BuildFromReader(v, callback...)
	case []GenericRecord:
		return index.BuildFromRecords(v, callback...)
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			return index.BuildFromStruct(v, callback...)
		}
	}
	return fmt.Errorf("unsupported input type: %T", input)
}

func (index *InvertedIndex) BuildFromFile(path string, callback ...func(v GenericRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return index.BuildFromReader(file, callback...)
}

func (index *InvertedIndex) BuildFromRecords(records []GenericRecord, callback ...func(v GenericRecord) error) error {
	index.Lock()
	index.indexingInProgress = true
	index.Unlock()

	defer func() {
		index.Lock()
		index.indexingInProgress = false
		index.Unlock()
	}()

	var wg sync.WaitGroup
	ch := make(chan job, len(records))

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range ch {
				freq := job.rec.getFrequency()
				index.Lock()
				index.indexDoc(job, freq)
				index.TotalDocs++
				index.Unlock()

				if len(callback) > 0 {
					if err := callback[0](job.rec); err != nil {
						log.Printf("callback error: %v", err)
					}
				}
			}
		}()
	}

	for i, rec := range records {
		ch <- job{i + 1, rec}
	}
	close(ch)
	wg.Wait()
	index.update()
	return nil
}

func (index *InvertedIndex) BuildFromStruct(slice any, callback ...func(v GenericRecord) error) error {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("not a slice")
	}
	var records []GenericRecord
	for i := 0; i < v.Len(); i++ {
		b, _ := json.Marshal(v.Index(i).Interface())
		var rec GenericRecord
		_ = json.Unmarshal(b, &rec)
		records = append(records, rec)
	}
	return index.BuildFromRecords(records, callback...)
}

func (index *InvertedIndex) indexDoc(r job, freq map[string]int) {
	index.Documents[r.id] = r.rec
	docLen := 0
	for t, count := range freq {
		index.Index[t] = append(index.Index[t], Posting{DocID: r.id, Frequency: count})
		docLen += count
	}
	index.DocLengths[r.id] = docLen
}

func (index *InvertedIndex) update() {
	total := 0
	for _, l := range index.DocLengths {
		total += l
	}
	if index.TotalDocs > 0 {
		index.AvgDocLength = float64(total) / float64(index.TotalDocs)
	}
}

func (index *InvertedIndex) bm25Score(queryTokens []string, docID int, k1, b float64) float64 {
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

func (index *InvertedIndex) Search(q Query, queryText string, bm ...BM25) []ScoredDoc {
	index.RLock()
	if index.indexingInProgress {
		index.RUnlock()
		return nil // Avoid searching during active index mutation
	}
	index.RUnlock()

	params := defaultBM25
	if len(bm) > 0 {
		params = bm[0]
	}
	queryTokens := utils.Tokenize(queryText)
	docIDs := q.Evaluate(index)

	var scored []ScoredDoc
	var wg sync.WaitGroup
	var mu sync.Mutex

	ch := make(chan int, len(docIDs))
	for _, id := range docIDs {
		ch <- id
	}
	close(ch)

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for docID := range ch {
				score := index.bm25Score(queryTokens, docID, params.K, params.B)
				mu.Lock()
				scored = append(scored, ScoredDoc{DocID: docID, Score: score})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})
	return scored
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
