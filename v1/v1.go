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
	freq := make(map[string]int)
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
	ID           string
	Index        map[string][]Posting
	DocLengths   map[int]int
	Documents    map[int]GenericRecord
	TotalDocs    int
	AvgDocLength float64
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
	var results []string
	for token := range index.Index {
		if utils.BoundedLevenshtein(term, token, threshold) <= threshold {
			results = append(results, token)
		}
	}
	return results
}

func (index *InvertedIndex) BuildFromFile(path string, callback ...func(v GenericRecord) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	return index.BuildFromReader(f, callback...)
}

func (index *InvertedIndex) BuildFromBytes(data []byte, callback ...func(v GenericRecord) error) error {
	return index.BuildFromReader(bytes.NewReader(data), callback...)
}

func (index *InvertedIndex) BuildFromString(jsonStr string, callback ...func(v GenericRecord) error) error {
	return index.BuildFromReader(strings.NewReader(jsonStr), callback...)
}

func (index *InvertedIndex) BuildFromReader(r io.Reader, callback ...func(v GenericRecord) error) error {
	decoder := json.NewDecoder(r)
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected '[' at beginning of JSON array, got: %v", token)
	}
	jobs := make(chan job, 100)
	results := make(chan result, 100)
	workerCount := runtime.NumCPU()
	var workerWg sync.WaitGroup
	worker := func() {
		defer workerWg.Done()
		for j := range jobs {
			freq := j.rec.getFrequency()
			results <- result{id: j.id, rec: j.rec, freq: freq}
		}
	}
	for i := 0; i < workerCount; i++ {
		workerWg.Add(1)
		go worker()
	}
	docID := 0
	go func() {
		for decoder.More() {
			var rec GenericRecord
			if err := decoder.Decode(&rec); err != nil {
				log.Printf("Skipping invalid record: %v", err)
				continue
			}
			docID++
			jobs <- job{id: docID, rec: rec}
			index.TotalDocs++
		}
		close(jobs)
		_, err := decoder.Token()
		if err != nil {
			log.Printf("read closing token: %v", err)
		}
	}()
	done := make(chan struct{})
	go func() {
		for r := range results {
			index.indexDoc(job{id: r.id, rec: r.rec}, r.freq)
			if len(callback) > 0 {
				if err := callback[0](r.rec); err != nil {
					log.Printf("callback error: %v", err)
				}
			}
		}
		done <- struct{}{}
	}()
	workerWg.Wait()
	close(results)
	<-done
	index.update()
	return nil
}

func (index *InvertedIndex) BuildFromRecords(records []GenericRecord, callback ...func(v GenericRecord) error) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	jobs := make(chan job, len(records))
	workerCount := runtime.NumCPU()
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				freq := job.rec.getFrequency()
				mu.Lock()
				index.indexDoc(job, freq)
				index.TotalDocs++
				if len(callback) > 0 {
					if err := callback[0](job.rec); err != nil {
						log.Printf("callback error: %v", err)
					}
				}
				mu.Unlock()
			}
		}()
	}
	for i, rec := range records {
		jobs <- job{i + 1, rec}
	}
	close(jobs)
	wg.Wait()
	index.update()
	return nil
}

func (index *InvertedIndex) BuildFromStruct(slice any, callback ...func(v GenericRecord) error) error {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("BuildFromStructs needs a slice, got %T", slice)
	}
	records := make([]GenericRecord, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		b, err := json.Marshal(elem)
		if err != nil {
			return fmt.Errorf("marshal element %d: %w", i, err)
		}
		var rec GenericRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return fmt.Errorf("unmarshal element %d: %w", i, err)
		}
		records[i] = rec
	}
	return index.BuildFromRecords(records, callback...)
}

func (index *InvertedIndex) Build(input any, callback ...func(v GenericRecord) error) error {
	switch v := input.(type) {
	case string:
		trim := strings.TrimSpace(v)
		if strings.HasPrefix(trim, "[") || strings.HasPrefix(trim, "{") {
			return index.BuildFromString(v, callback...)
		}
		return index.BuildFromFile(v, callback...)
	case []byte:
		return index.BuildFromBytes(v, callback...)
	case io.Reader:
		return index.BuildFromReader(v, callback...)
	case []GenericRecord:
		return index.BuildFromRecords(v, callback...)
	default:
		rv := reflect.ValueOf(input)
		if rv.Kind() == reflect.Slice {
			elem := rv.Type().Elem()
			if elem.Kind() == reflect.Struct ||
				elem.Kind() == reflect.Map && elem.Key().Kind() == reflect.String && elem.Elem().Kind() == reflect.Interface {
				return index.BuildFromStruct(input)
			}
		}
		return fmt.Errorf("unsupported input type: %T", input)
	}
}

func (index *InvertedIndex) bm25Score(queryTokens []string, docID int, k1, b float64) float64 {
	score := 0.0
	docLength := float64(index.DocLengths[docID])
	for _, term := range queryTokens {
		postings, ok := index.Index[term]
		if !ok {
			continue
		}
		df := float64(len(postings))
		tf := 0
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

func (index *InvertedIndex) update() {
	totalLength := 0
	for _, l := range index.DocLengths {
		totalLength += l
	}
	if index.TotalDocs > 0 {
		index.AvgDocLength = float64(totalLength) / float64(index.TotalDocs)
	}
}

func (index *InvertedIndex) indexDoc(r job, freq map[string]int) {
	index.Documents[r.id] = r.rec
	docLen := 0
	for t, count := range freq {
		docLen += count
		postings := index.Index[t]
		postings = append(postings, Posting{DocID: r.id, Frequency: count})
		index.Index[t] = postings
	}
	index.DocLengths[r.id] = docLen
}

type BM25 struct {
	K float64
	B float64
}

var defaultBM25 = BM25{K: 1.2, B: 0.75}

func (index *InvertedIndex) Search(q Query, queryText string, bm ...BM25) []ScoredDoc {
	p := defaultBM25
	if len(bm) > 0 {
		p = bm[0]
	}
	docIDs := q.Evaluate(index)
	queryTokens := utils.Tokenize(queryText)
	var scored []ScoredDoc
	for _, docID := range docIDs {
		score := index.bm25Score(queryTokens, docID, p.K, p.B)
		scored = append(scored, ScoredDoc{DocID: docID, Score: score})
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})
	return scored
}

func Paginate(docs []ScoredDoc, page, perPage int) []ScoredDoc {
	start := (page - 1) * perPage
	if start >= len(docs) {
		return []ScoredDoc{}
	}
	end := start + perPage
	if end > len(docs) {
		end = len(docs)
	}
	return docs[start:end]
}
