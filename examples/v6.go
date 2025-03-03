package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/goccy/go-json"
)

// --------------------------
// Generic Data Structures
// --------------------------

// GenericRecord represents any JSON object.
type GenericRecord map[string]interface{}

// Posting holds a document ID and the term frequency in that document.
type Posting struct {
	DocID     int
	Frequency int
}

// InvertedIndex is our full‑text index.
type InvertedIndex struct {
	Index        map[string][]Posting  // token -> list of postings
	DocLengths   map[int]int           // document length (in tokens)
	Documents    map[int]GenericRecord // docID -> original record
	TotalDocs    int
	AvgDocLength float64
}

// ScoredDoc represents a document and its BM25 score.
type ScoredDoc struct {
	DocID int
	Score float64
}

// --------------------------
// Tokenization & Helpers
// --------------------------

// Tokenize normalizes text (lowercase, remove punctuation) and splits into tokens.
func Tokenize(text string) []string {
	text = strings.ToLower(text)
	var sb strings.Builder
	for _, r := range text {
		// Allow letters, digits, and spaces.
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

// --------------------------
// Bounded Levenshtein & Fuzzy Search
// --------------------------

// BoundedLevenshtein computes edit distance between a and b but stops if threshold is exceeded.
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

// FuzzySearch returns tokens in the index within the given threshold.
func FuzzySearch(term string, threshold int, index *InvertedIndex) []string {
	var results []string
	for token := range index.Index {
		if BoundedLevenshtein(term, token, threshold) <= threshold {
			results = append(results, token)
		}
	}
	return results
}

// --------------------------
// Building a Generic Inverted Index
// --------------------------

// CombinedText extracts a combined text string from all string and numeric values in a record.
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

// BuildIndex streams through any JSON file (an array of objects) and builds the index.
func BuildIndex(filePath string) (*InvertedIndex, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %v", err)
	}
	defer file.Close()

	index := &InvertedIndex{
		Index:      make(map[string][]Posting),
		DocLengths: make(map[int]int),
		Documents:  make(map[int]GenericRecord),
	}

	reader := bufio.NewReader(file)
	decoder := json.NewDecoder(reader)

	// Expect JSON array.
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected '[' at beginning of JSON array, got: %v", token)
	}

	docID := 0
	for decoder.More() {
		var rec GenericRecord
		if err := decoder.Decode(&rec); err != nil {
			log.Printf("Skipping invalid record: %v", err)
			continue
		}
		docID++
		index.Documents[docID] = rec

		combined := CombinedText(rec)
		tokens := Tokenize(combined)
		index.DocLengths[docID] = len(tokens)
		for _, token := range tokens {
			postings := index.Index[token]
			found := false
			for i, p := range postings {
				if p.DocID == docID {
					postings[i].Frequency++
					found = true
					break
				}
			}
			if !found {
				postings = append(postings, Posting{DocID: docID, Frequency: 1})
			}
			index.Index[token] = postings
		}
		index.TotalDocs++
	}
	// Consume closing ']' token.
	_, err = decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("read closing token: %v", err)
	}
	totalLength := 0
	for _, l := range index.DocLengths {
		totalLength += l
	}
	if index.TotalDocs > 0 {
		index.AvgDocLength = float64(totalLength) / float64(index.TotalDocs)
	}
	return index, nil
}

// --------------------------
// BM25 Ranking
// --------------------------

// BM25Score computes the BM25 score for a document given a query.
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

// --------------------------
// Query Types & Evaluation
// --------------------------

// Query is the interface for our search queries.
type Query interface {
	Evaluate(index *InvertedIndex) []int // Returns matching document IDs.
}

// TermQuery performs a term search with optional fuzzy matching.
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

// PhraseQuery finds documents containing the given phrase (searched in combined text).
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

// RangeQuery performs a numeric range search on a specified field.
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

// BoolQuery combines sub‑queries using must, should, and must_not clauses.
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

// Helper functions for set operations.
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

// --------------------------
// Query Scoring & Pagination
// --------------------------

// ScoreQuery applies BM25 scoring to documents returned by a query.
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

// Paginate returns a page of results given the page number and page size.
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

// --------------------------
// Main: Generic Indexing & Query Example
// --------------------------

func main() {
	// Use any JSON file containing an array of objects.
	jsonFilePath := "charge_master.json"
	startTime := time.Now()
	index, err := BuildIndex(jsonFilePath)
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	fmt.Printf("Index built for %d documents in %s\n", index.TotalDocs, time.Since(startTime))

	termQ := TermQuery{
		Term:           "MANDI",
		Fuzzy:          true,
		FuzzyThreshold: 1, // allow small typos
	}
	rangeQ := RangeQuery{
		Field: "work_item_id",
		Lower: 30,
		Upper: 40,
	}
	boolQ := BoolQuery{
		Must: []Query{termQ, rangeQ},
		// You can also add Should and MustNot queries here.
	}

	// Score using BM25 (with typical parameters k1=1.2, b=0.75) and paginate.
	scoredDocs := ScoreQuery(boolQ, index, "MANDI", 1.2, 0.75)
	page := 1
	perPage := 10
	paginatedResults := Paginate(scoredDocs, page, perPage)
	fmt.Printf("Found %d matching documents (showing page %d):\n", len(scoredDocs), page)
	for _, sd := range paginatedResults {
		rec := index.Documents[sd.DocID]
		fmt.Printf("DocID: %d | Score: %.4f | Data: %+v\n", sd.DocID, sd.Score, rec)
	}
}
