package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/oarkflow/json"
)

type GenericRecord map[string]any

type Posting struct {
	DocID     int
	Frequency int
}

type InvertedIndex struct {
	Index        map[string][]Posting
	DocLengths   map[int]int
	Documents    map[int]GenericRecord
	TotalDocs    int
	AvgDocLength float64
}

func NewIndex() *InvertedIndex {
	return &InvertedIndex{
		Index:      make(map[string][]Posting),
		DocLengths: make(map[int]int),
		Documents:  make(map[int]GenericRecord),
	}
}

type ScoredDoc struct {
	DocID int
	Score float64
}

func Tokenize(text string) []string {
	text = strings.ToLower(text)
	var sb strings.Builder
	for _, r := range text {

		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			sb.WriteRune(r)
		} else {
			sb.WriteRune(' ')
		}
	}
	return strings.Fields(sb.String())
}

func min(a, b, c int) int {
	if a < b && a < c {
		return a
	} else if b < c {
		return b
	}
	return c
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func BoundedLevenshtein(a, b string, threshold int) int {
	la, lb := len(a), len(b)
	if abs(la-lb) > threshold {
		return threshold + 1
	}
	prev := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		current := make([]int, lb+1)
		current[0] = i
		minVal := current[0]
		for j := 1; j <= lb; j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			current[j] = min(
				current[j-1]+1,
				prev[j]+1,
				prev[j-1]+cost,
			)
			if current[j] < minVal {
				minVal = current[j]
			}
		}
		if minVal > threshold {
			return threshold + 1
		}
		prev = current
	}
	if prev[lb] > threshold {
		return threshold + 1
	}
	return prev[lb]
}

func FuzzySearch(term string, threshold int, index *InvertedIndex) []string {
	var results []string
	for token := range index.Index {
		if BoundedLevenshtein(term, token, threshold) <= threshold {
			results = append(results, token)
		}
	}
	return results
}

func CombinedText(rec GenericRecord) string {
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

// BuildIndexFromFile is your old BuildIndex, now just a thin wrapper.
func BuildIndexFromFile(path string) (*InvertedIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return BuildIndexFromReader(f)
}

// BuildIndexFromBytes lets you index raw JSON bytes.
func BuildIndexFromBytes(data []byte) (*InvertedIndex, error) {
	return BuildIndexFromReader(bytes.NewReader(data))
}

// BuildIndexFromString lets you index a JSON string.
func BuildIndexFromString(jsonStr string) (*InvertedIndex, error) {
	return BuildIndexFromReader(strings.NewReader(jsonStr))
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

// BuildIndexFromReader reads a JSON array of objects from any io.Reader.
func BuildIndexFromReader(r io.Reader) (*InvertedIndex, error) {
	index := NewIndex()
	decoder := json.NewDecoder(r)
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected '[' at beginning of JSON array, got: %v", token)
	}
	jobs := make(chan job, 100)
	results := make(chan result, 100)
	workerCount := runtime.NumCPU()
	var workerWg sync.WaitGroup
	worker := func() {
		defer workerWg.Done()
		for j := range jobs {
			freq := getFrequency(j.rec)
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
			indexDoc(index, job{id: r.id, rec: r.rec}, r.freq)
		}
		done <- struct{}{}
	}()
	workerWg.Wait()
	close(results)
	<-done
	index.update()
	return index, nil
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

func getFrequency(rec GenericRecord) map[string]int {
	combined := CombinedText(rec)
	tokens := Tokenize(combined)
	freq := make(map[string]int)
	for _, t := range tokens {
		freq[t]++
	}
	return freq
}

func indexDoc(index *InvertedIndex, r job, freq map[string]int) {
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

// BuildIndexFromRecords builds directly from a slice of already‐decoded GenericRecord.
func BuildIndexFromRecords(records []GenericRecord) (*InvertedIndex, error) {
	index := NewIndex()
	var mu sync.Mutex
	var wg sync.WaitGroup
	jobs := make(chan job, len(records))
	workerCount := runtime.NumCPU()
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				freq := getFrequency(job.rec)
				mu.Lock()
				indexDoc(index, job, freq)
				index.TotalDocs++
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
	return index, nil
}

func BuildIndexFromStruct(slice any) (*InvertedIndex, error) {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("BuildIndexFromStructs needs a slice, got %T", slice)
	}
	records := make([]GenericRecord, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		b, err := json.Marshal(elem)
		if err != nil {
			return nil, fmt.Errorf("marshal element %d: %w", i, err)
		}
		var rec GenericRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("unmarshal element %d: %w", i, err)
		}
		records[i] = rec
	}
	return BuildIndexFromRecords(records)
}

func BuildIndex(input any) (*InvertedIndex, error) {
	switch v := input.(type) {
	case string:
		trim := strings.TrimSpace(v)
		if strings.HasPrefix(trim, "[") || strings.HasPrefix(trim, "{") {
			return BuildIndexFromString(v)
		}
		return BuildIndexFromFile(v)
	case []byte:
		return BuildIndexFromBytes(v)
	case io.Reader:
		return BuildIndexFromReader(v)
	case []GenericRecord:
		return BuildIndexFromRecords(v)
	default:
		rv := reflect.ValueOf(input)
		if rv.Kind() == reflect.Slice {
			elem := rv.Type().Elem()
			if elem.Kind() == reflect.Struct ||
				elem.Kind() == reflect.Map && elem.Key().Kind() == reflect.String && elem.Elem().Kind() == reflect.Interface {
				return BuildIndexFromStruct(input)
			}
		}
		return nil, fmt.Errorf("unsupported input type: %T", input)
	}
}

func BM25Score(queryTokens []string, docID int, index *InvertedIndex, k1, b float64) float64 {
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
		tfScore := (float64(tf) * (k1 + 1)) / (float64(tf) + k1*(1-b+b*(docLength/float64(index.AvgDocLength))))
		score += idf * tfScore
	}
	return score
}

type Query interface {
	Evaluate(index *InvertedIndex) []int
}

type TermQuery struct {
	Term           string
	Fuzzy          bool
	FuzzyThreshold int
}

func (tq TermQuery) Evaluate(index *InvertedIndex) []int {
	var tokens []string
	if tq.Fuzzy {
		tokens = FuzzySearch(strings.ToLower(tq.Term), tq.FuzzyThreshold, index)
	} else {
		tokens = []string{strings.ToLower(tq.Term)}
	}
	docSet := make(map[int]struct{})
	for _, token := range tokens {
		if postings, ok := index.Index[token]; ok {
			for _, p := range postings {
				docSet[p.DocID] = struct{}{}
			}
		}
	}
	var result []int
	for docID := range docSet {
		result = append(result, docID)
	}
	return result
}

type PhraseQuery struct {
	Phrase string
}

func (pq PhraseQuery) Evaluate(index *InvertedIndex) []int {
	var result []int
	phrase := strings.ToLower(pq.Phrase)
	for docID, rec := range index.Documents {
		combined := strings.ToLower(CombinedText(rec))
		if strings.Contains(combined, phrase) {
			result = append(result, docID)
		}
	}
	return result
}

type RangeQuery struct {
	Field string
	Lower float64
	Upper float64
}

func (rq RangeQuery) Evaluate(index *InvertedIndex) []int {
	var result []int
	for docID, rec := range index.Documents {
		val, ok := rec[rq.Field]
		if !ok {
			continue
		}
		var num float64
		switch v := val.(type) {
		case float64:
			num = v
		case int:
			num = float64(v)
		case string:
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				num = parsed
			} else {
				continue
			}
		default:
			continue
		}
		if num >= rq.Lower && num <= rq.Upper {
			result = append(result, docID)
		}
	}
	return result
}

type BoolQuery struct {
	Must    []Query
	Should  []Query
	MustNot []Query
}

func (bq BoolQuery) Evaluate(index *InvertedIndex) []int {
	var mustResult []int
	if len(bq.Must) > 0 {
		mustResult = bq.Must[0].Evaluate(index)
		for i := 1; i < len(bq.Must); i++ {
			mustResult = intersect(mustResult, bq.Must[i].Evaluate(index))
		}
	} else {
		for docID := range index.Documents {
			mustResult = append(mustResult, docID)
		}
	}
	var shouldResult []int
	for _, q := range bq.Should {
		shouldResult = union(shouldResult, q.Evaluate(index))
	}
	if len(bq.Should) > 0 {
		mustResult = intersect(mustResult, shouldResult)
	}
	for _, q := range bq.MustNot {
		mustResult = subtract(mustResult, q.Evaluate(index))
	}
	return mustResult
}

func intersect(a, b []int) []int {
	m := make(map[int]bool)
	for _, id := range a {
		m[id] = true
	}
	var result []int
	for _, id := range b {
		if m[id] {
			result = append(result, id)
		}
	}
	return result
}

func union(a, b []int) []int {
	m := make(map[int]bool)
	for _, id := range a {
		m[id] = true
	}
	for _, id := range b {
		m[id] = true
	}
	var result []int
	for id := range m {
		result = append(result, id)
	}
	return result
}

func subtract(a, b []int) []int {
	m := make(map[int]bool)
	for _, id := range b {
		m[id] = true
	}
	var result []int
	for _, id := range a {
		if !m[id] {
			result = append(result, id)
		}
	}
	return result
}

func ScoreQuery(q Query, index *InvertedIndex, queryText string, k1, b float64) []ScoredDoc {
	docIDs := q.Evaluate(index)
	queryTokens := Tokenize(queryText)
	var scored []ScoredDoc
	for _, docID := range docIDs {
		score := BM25Score(queryTokens, docID, index, k1, b)
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

func main() {
	jsonFilePath := "charge_master.json"
	startTime := time.Now()
	index, err := BuildIndex(jsonFilePath)
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	fmt.Printf("Index built for %d documents in %s\n", index.TotalDocs, time.Since(startTime))

	termQ := TermQuery{
		Term:           "33965",
		Fuzzy:          true,
		FuzzyThreshold: 1,
	}
	rangeQ := RangeQuery{
		Field: "work_item_id",
		Lower: 30,
		Upper: 40,
	}
	boolQ := BoolQuery{
		Must: []Query{termQ, rangeQ},
	}

	scoredDocs := ScoreQuery(boolQ, index, "33965", 1.2, 0.75)
	page := 1
	perPage := 10
	paginatedResults := Paginate(scoredDocs, page, perPage)
	fmt.Printf("Found %d matching documents (showing page %d):\n", len(scoredDocs), page)
	for _, sd := range paginatedResults {
		rec := index.Documents[sd.DocID]
		fmt.Printf("DocID: %d | Score: %.4f | Data: %+v\n", sd.DocID, sd.Score, rec)
	}
}
