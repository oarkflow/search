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
	Documents          *BPTree[int, GenericRecord]
	TotalDocs          int
	AvgDocLength       float64
	order              int
	storage            string
	cacheCapacity      int
	indexingInProgress bool
	numWorkers         int
	searchCache        map[string][]ScoredDoc
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

func WithOrder(order int) Options {
	return func(index *Index) {
		index.order = order
	}
}

func WithCacheCapacity(capacity int) Options {
	return func(index *Index) {
		index.cacheCapacity = capacity
	}
}

func WithStorage(storage string) Options {
	return func(index *Index) {
		index.storage = storage
	}
}

func NewIndex(id string, opts ...Options) *Index {
	index := &Index{
		ID:            id,
		numWorkers:    runtime.NumCPU(),
		Index:         make(map[string][]Posting),
		DocLengths:    make(map[int]int),
		order:         3,
		storage:       "data/storage.dat",
		cacheCapacity: 10000,
		searchCache:   make(map[string][]ScoredDoc),
	}
	for _, opt := range opts {
		opt(index)
	}
	index.Documents = NewBPTree[int, GenericRecord](index.order, index.storage, index.cacheCapacity)
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

type partialIndex struct {
	docs      map[int]GenericRecord
	lengths   map[int]int
	inverted  map[string][]Posting
	totalDocs int
}

func (index *Index) mergePartial(partial partialIndex) {
	index.Lock()
	for id, rec := range partial.docs {
		index.Documents.Insert(id, rec)
	}
	for id, length := range partial.lengths {
		index.DocLengths[id] = length
	}
	for term, postings := range partial.inverted {
		index.Index[term] = append(index.Index[term], postings...)
	}
	index.TotalDocs += partial.totalDocs
	index.Unlock()
}

func (index *Index) BuildFromReader(ctx context.Context, r io.Reader, callbacks ...func(v GenericRecord) error) error {
	index.Lock()
	if index.indexingInProgress {
		index.Unlock()
		return fmt.Errorf("indexing already in progress")
	}
	index.indexingInProgress = true
	index.searchCache = make(map[string][]ScoredDoc)
	index.Unlock()
	defer func() {
		index.Lock()
		index.indexingInProgress = false
		index.Unlock()
	}()
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	tok, err := decoder.Token()
	if err != nil || tok != json.Delim('[') {
		return fmt.Errorf("invalid JSON array")
	}
	jobs := make(chan GenericRecord, 50)
	const flushThreshold = 100
	var wg sync.WaitGroup
	for w := 0; w < index.numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			partial := partialIndex{
				docs:     make(map[int]GenericRecord),
				lengths:  make(map[int]int),
				inverted: make(map[string][]Posting),
			}
			var localID, count int
			for rec := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				localID++
				docID := workerID*100000 + localID
				partial.docs[docID] = rec
				freq := rec.getFrequency()
				docLen := 0
				for term, cnt := range freq {
					partial.inverted[term] = append(partial.inverted[term], Posting{DocID: docID, Frequency: cnt})
					docLen += cnt
				}
				partial.lengths[docID] = docLen
				partial.totalDocs++
				count++
				if count >= flushThreshold {
					index.mergePartial(partial)
					partial = partialIndex{
						docs:     make(map[int]GenericRecord),
						lengths:  make(map[int]int),
						inverted: make(map[string][]Posting),
					}
					count = 0
				}
				for _, cb := range callbacks {
					if err := cb(rec); err != nil {
						log.Printf("callback error: %v", err)
					}
				}
			}
			if count > 0 {
				index.mergePartial(partial)
			}
		}(w)
	}
	go func() {
		for decoder.More() {
			select {
			case <-ctx.Done():
				break
			default:
			}
			var rec GenericRecord
			if err := decoder.Decode(&rec); err != nil {
				log.Printf("Skipping invalid record: %v", err)
				continue
			}
			jobs <- rec
		}
		close(jobs)
	}()
	wg.Wait()
	index.Lock()
	index.update()
	index.Unlock()
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
	index.searchCache = make(map[string][]ScoredDoc)
	index.Unlock()
	jobs := make(chan GenericRecord, 50)
	const flushThreshold = 100
	var wg sync.WaitGroup
	for w := 0; w < index.numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			partial := partialIndex{
				docs:     make(map[int]GenericRecord),
				lengths:  make(map[int]int),
				inverted: make(map[string][]Posting),
			}
			var localID, count int
			for rec := range jobs {
				localID++
				docID := workerID*100000 + localID
				partial.docs[docID] = rec
				freq := rec.getFrequency()
				docLen := 0
				for term, cnt := range freq {
					partial.inverted[term] = append(partial.inverted[term], Posting{DocID: docID, Frequency: cnt})
					docLen += cnt
				}
				partial.lengths[docID] = docLen
				partial.totalDocs++
				count++
				if count >= flushThreshold {
					index.mergePartial(partial)
					partial = partialIndex{
						docs:     make(map[int]GenericRecord),
						lengths:  make(map[int]int),
						inverted: make(map[string][]Posting),
					}
					count = 0
				}
				for _, cb := range callbacks {
					if err := cb(rec); err != nil {
						log.Printf("callback error: %v", err)
					}
				}
			}
			if count > 0 {
				index.mergePartial(partial)
			}
		}(w)
	}
	go func() {
		for _, rec := range records {
			select {
			case <-ctx.Done():
				break
			default:
			}
			jobs <- rec
		}
		close(jobs)
	}()
	wg.Wait()
	index.Lock()
	index.update()
	index.Unlock()
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

func (index *Index) indexDoc(docID int, rec GenericRecord, freq map[string]int) {
	index.Documents.Insert(docID, rec)
	docLen := 0
	for t, count := range freq {
		index.Index[t] = append(index.Index[t], Posting{DocID: docID, Frequency: count})
		docLen += count
	}
	index.DocLengths[docID] = docLen
}

func (index *Index) update() {
	total := 0
	for _, l := range index.DocLengths {
		total += l
	}
	if index.TotalDocs > 0 {
		index.AvgDocLength = float64(total) / float64(index.TotalDocs)
	}
	index.indexingInProgress = false
	log.Println(fmt.Sprintf("Indexing completed for %s", index.ID))
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

type Pagination struct {
	Page    int
	PerPage int
}

type SearchParams struct {
	Page       int
	PerPage    int
	BM25Params BM25
	SortFields []SortField
}

func (index *Index) Search(ctx context.Context, q Query, paramList ...SearchParams) (Page, error) {
	var params SearchParams
	if len(paramList) > 0 {
		params = paramList[0]
	}
	queryTokens := q.Tokens()
	var key string
	if len(queryTokens) > 0 {
		key = strings.Join(queryTokens, " ")
	} else {
		key = fmt.Sprintf("%T:%v", q, q)
	}
	page := params.Page
	perPage := params.PerPage
	index.RLock()
	if res, found := index.searchCache[key]; found {
		index.RUnlock()
		if page < 1 {
			page = 1
		}
		if perPage < 1 {
			perPage = 10
		}
		if len(params.SortFields) > 0 {
			index.sortData(res, params.SortFields)
		} else {
			sort.Slice(res, func(i, j int) bool {
				return res[i].Score > res[j].Score
			})
		}
		return smartPaginate(res, page, perPage), nil
	}
	if index.indexingInProgress {
		index.RUnlock()
		return Page{}, fmt.Errorf("indexing in progress; please try again later")
	}
	index.RUnlock()
	bm25 := defaultBM25
	if params.BM25Params.K != 0 || params.BM25Params.B != 0 {
		bm25 = params.BM25Params
	}
	var (
		scored []ScoredDoc
		wg     sync.WaitGroup
		mu     sync.Mutex
	)
	docIDs := q.Evaluate(index)
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
				}
				score := index.bm25Score(queryTokens, docID, bm25.K, bm25.B)
				mu.Lock()
				scored = append(scored, ScoredDoc{DocID: docID, Score: score})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(params.SortFields) > 0 {
		index.sortData(scored, params.SortFields)
	} else {
		sort.Slice(scored, func(i, j int) bool {
			return scored[i].Score > scored[j].Score
		})
	}
	index.Lock()
	index.searchCache[key] = scored
	index.Unlock()
	return smartPaginate(scored, page, perPage), nil
}

func (index *Index) sortData(scored []ScoredDoc, fields []SortField) {
	sort.SliceStable(scored, func(i, j int) bool {
		docI, _ := index.Documents.Search(scored[i].DocID)
		docJ, _ := index.Documents.Search(scored[j].DocID)
		for _, field := range fields {
			valI, okI := docI[field.Field]
			valJ, okJ := docJ[field.Field]
			if !okI || !okJ {
				continue
			}
			cmp := utils.Compare(valI, valJ)
			if cmp == 0 {
				continue
			}
			if field.Ascending {
				return cmp < 0
			}
			return cmp > 0
		}
		return scored[i].Score > scored[j].Score
	})
}

type SortField struct {
	Field     string
	Ascending bool
}

type Page struct {
	Results    []ScoredDoc
	Total      int
	Page       int
	PerPage    int
	TotalPages int
	NextPage   *int
	PrevPage   *int
}

func smartPaginate(docs []ScoredDoc, page, perPage int) Page {
	total := len(docs)
	totalPages := (total + perPage - 1) / perPage
	if page < 1 {
		page = 1
	} else if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * perPage
	end := start + perPage
	if end > total {
		end = total
	}
	var next, prev *int
	if page < totalPages {
		np := page + 1
		next = &np
	}
	if page > 1 {
		pp := page - 1
		prev = &pp
	}
	return Page{
		Results:    docs[start:end],
		Total:      total,
		Page:       page,
		PerPage:    perPage,
		TotalPages: totalPages,
		NextPage:   next,
		PrevPage:   prev,
	}
}

func (index *Index) AddDocument(rec GenericRecord) {
	index.Lock()
	index.searchCache = make(map[string][]ScoredDoc)
	docID := index.TotalDocs + 1
	freq := rec.getFrequency()
	index.indexDoc(docID, rec, freq)
	index.TotalDocs++
	index.update()
	index.Unlock()
}

func (index *Index) UpdateDocument(docID int, rec GenericRecord) error {
	index.Lock()
	index.searchCache = make(map[string][]ScoredDoc)
	oldRec, ok := index.Documents.Search(docID)
	if !ok {
		index.Unlock()
		return fmt.Errorf("document %d does not exist", docID)
	}

	oldFreq := oldRec.getFrequency()
	for term := range oldFreq {
		if postings, exists := index.Index[term]; exists {
			newPostings := postings[:0]
			for _, p := range postings {
				if p.DocID != docID {
					newPostings = append(newPostings, p)
				}
			}
			if len(newPostings) == 0 {
				delete(index.Index, term)
			} else {
				index.Index[term] = newPostings
			}
		}
		index.DocLengths[docID] -= oldFreq[term]
	}
	index.Documents.Insert(docID, rec)
	newFreq := rec.getFrequency()
	docLen := 0
	for term, count := range newFreq {
		docLen += count
		index.Index[term] = append(index.Index[term], Posting{DocID: docID, Frequency: count})
	}
	index.DocLengths[docID] = docLen
	index.update()
	index.Unlock()
	return nil
}

func (index *Index) DeleteDocument(docID int) error {
	index.Lock()
	index.searchCache = make(map[string][]ScoredDoc)
	rec, ok := index.Documents.Search(docID)
	if !ok {
		index.Unlock()
		return fmt.Errorf("document %d does not exist", docID)
	}
	freq := rec.getFrequency()
	for term := range freq {
		if postings, exists := index.Index[term]; exists {
			newPostings := postings[:0]
			for _, p := range postings {
				if p.DocID != docID {
					newPostings = append(newPostings, p)
				}
			}
			if len(newPostings) == 0 {
				delete(index.Index, term)
			} else {
				index.Index[term] = newPostings
			}
		}
	}
	index.Documents.Delete(docID)
	delete(index.DocLengths, docID)
	index.TotalDocs--
	index.update()
	index.Unlock()
	return nil
}

func (index *Index) GetDocument(id int) (any, bool) {
	return index.Documents.Search(id)
}

type QueryFunc func(index *Index) []int

func (f QueryFunc) Evaluate(index *Index) []int {
	return f(index)
}
