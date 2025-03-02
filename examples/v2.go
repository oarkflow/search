package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/msgpack"
)

// -------------------- BM25 Scoring --------------------

// BM25Params holds BM25 configuration parameters.
type BM25Params struct {
	K float64
	B float64
}

// BM25 computes the BM25 score for a term.
func BM25(frequency float64, docLength int, avgDocLength float64, totalDocs int, tokenOccurrence int, params BM25Params) float64 {
	idf := 0.0
	if tokenOccurrence > 0 {
		// Adding 1 to the log term to avoid negative scores.
		idf = math.Log((float64(totalDocs)-float64(tokenOccurrence)+0.5)/(float64(tokenOccurrence)+0.5) + 1)
	}
	tf := (frequency * (params.K + 1)) / (frequency + params.K*(1.0-params.B+params.B*(float64(docLength)/avgDocLength)))
	return idf * tf
}

// -------------------- Bounded Levenshtein (Fuzzy Matching) --------------------

// BoundedLevenshtein computes the Levenshtein distance between strings a and b,
// but stops early if the distance exceeds maxDist. It returns the computed distance
// and a boolean indicating if the distance is within the bound.
func BoundedLevenshtein(a, b string, maxDist int) (int, bool) {
	lenA := len(a)
	lenB := len(b)
	if abs(lenA-lenB) > maxDist {
		return maxDist + 1, false
	}

	prev := make([]int, lenB+1)
	for j := 0; j <= lenB; j++ {
		prev[j] = j
	}

	for i := 1; i <= lenA; i++ {
		current := make([]int, lenB+1)
		current[0] = i
		minInRow := current[0]
		for j := 1; j <= lenB; j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			current[j] = minVal(
				prev[j]+1,      // deletion
				current[j-1]+1, // insertion
				prev[j-1]+cost, // substitution
			)
			if current[j] < minInRow {
				minInRow = current[j]
			}
		}
		if minInRow > maxDist {
			return maxDist + 1, false
		}
		prev = current
	}
	distance := prev[lenB]
	return distance, distance <= maxDist
}

func minVal(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

// -------------------- Improved Analyzer --------------------

// Analyzer defines an interface for text analysis.
type Analyzer interface {
	Analyze(text string) []string
}

// EnhancedAnalyzer implements Analyzer by lowercasing, splitting on whitespace,
// removing stop words, and (optionally) returning only unique tokens.
type EnhancedAnalyzer struct {
	StopWords map[string]bool
	Unique    bool
}

// NewEnhancedAnalyzer creates an EnhancedAnalyzer with the given stop words list.
func NewEnhancedAnalyzer(stopWords []string, unique bool) *EnhancedAnalyzer {
	swMap := make(map[string]bool)
	for _, word := range stopWords {
		swMap[strings.ToLower(word)] = true
	}
	return &EnhancedAnalyzer{
		StopWords: swMap,
		Unique:    unique,
	}
}

// Analyze tokenizes text, removes stop words, and returns unique tokens if requested.
func (a *EnhancedAnalyzer) Analyze(text string) []string {
	words := strings.Fields(text)
	var tokens []string
	seen := make(map[string]bool)
	for _, word := range words {
		token := strings.ToLower(word)
		if a.StopWords[token] {
			continue
		}
		if a.Unique {
			if seen[token] {
				continue
			}
			seen[token] = true
		}
		tokens = append(tokens, token)
	}
	return tokens
}

// -------------------- Field Mapping and Schema --------------------

// FieldMapping defines how a field should be indexed and/or stored.
type FieldMapping struct {
	FieldName string
	Analyzer  Analyzer
	Index     bool
	Store     bool
}

// Mapping defines the schema for an index.
type Mapping struct {
	Fields map[string]FieldMapping
}

// -------------------- Document Type --------------------

// Document represents a JSON document.
type Document map[string]interface{}

// -------------------- ES‑Style Inverted Index --------------------

// ESIndex implements an ElasticSearch–style inverted index with BM25 scoring and fuzzy matching.
type ESIndex struct {
	// invertedIndex: field -> token -> (docID -> frequency)
	invertedIndex map[string]map[string]map[int64]int
	// docs stores the original documents.
	docs map[int64]Document
	// mapping defines which fields to index.
	mapping Mapping

	// For BM25: document lengths per field, average field length, and token occurrence counts.
	fieldLengths     map[string]map[int64]int  // field -> (docID -> token count)
	avgFieldLength   map[string]float64        // field -> average token count
	tokenOccurrences map[string]map[string]int // field -> (token -> total frequency)

	mu sync.RWMutex
}

// NewESIndex creates a new ESIndex for a given mapping.
func NewESIndex(mapping Mapping) *ESIndex {
	return &ESIndex{
		invertedIndex:    make(map[string]map[string]map[int64]int),
		docs:             make(map[int64]Document),
		mapping:          mapping,
		fieldLengths:     make(map[string]map[int64]int),
		avgFieldLength:   make(map[string]float64),
		tokenOccurrences: make(map[string]map[string]int),
	}
}

// Insert adds a document with a given id to the index.
func (idx *ESIndex) Insert(doc Document, id int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.docs[id] = doc

	for field, fieldMapping := range idx.mapping.Fields {
		if !fieldMapping.Index {
			continue
		}
		val, exists := doc[field]
		if !exists {
			continue
		}
		text := fmt.Sprintf("%v", val)
		tokens := fieldMapping.Analyzer.Analyze(text)
		tokenFreq := make(map[string]int)
		for _, token := range tokens {
			tokenFreq[token]++
		}
		if _, ok := idx.invertedIndex[field]; !ok {
			idx.invertedIndex[field] = make(map[string]map[int64]int)
		}
		for token, count := range tokenFreq {
			if _, ok := idx.invertedIndex[field][token]; !ok {
				idx.invertedIndex[field][token] = make(map[int64]int)
			}
			idx.invertedIndex[field][token][id] = count
		}
		if _, ok := idx.fieldLengths[field]; !ok {
			idx.fieldLengths[field] = make(map[int64]int)
		}
		docLength := len(tokens)
		idx.fieldLengths[field][id] = docLength
		if _, ok := idx.tokenOccurrences[field]; !ok {
			idx.tokenOccurrences[field] = make(map[string]int)
		}
		for token, count := range tokenFreq {
			idx.tokenOccurrences[field][token] += count
		}
		n := float64(len(idx.fieldLengths[field]))
		if n == 0 {
			idx.avgFieldLength[field] = float64(docLength)
		} else {
			oldAvg := idx.avgFieldLength[field]
			idx.avgFieldLength[field] = (oldAvg*(n-1) + float64(docLength)) / n
		}
	}
	return nil
}

// SearchResult holds a matching document and its computed score.
type SearchResult struct {
	DocID    int64
	Score    float64
	Document Document
}

const fuzzyThreshold = 1

// Search performs a multi‑field OR search on the query, using BM25 scoring and fuzzy matching.
func (idx *ESIndex) Search(query string, bm25Params BM25Params) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	scoreMap := make(map[int64]float64)
	totalDocs := len(idx.docs)

	for field, fieldMapping := range idx.mapping.Fields {
		if !fieldMapping.Index {
			continue
		}
		queryTokens := fieldMapping.Analyzer.Analyze(query)
		for _, qToken := range queryTokens {
			posting, exists := idx.invertedIndex[field][qToken]
			if !exists {
				// Fuzzy matching: try to match tokens within the fuzzyThreshold.
				for token, candidatePosting := range idx.invertedIndex[field] {
					if dist, ok := BoundedLevenshtein(qToken, token, fuzzyThreshold); ok && dist <= fuzzyThreshold {
						if posting == nil {
							posting = make(map[int64]int)
						}
						for docID, freq := range candidatePosting {
							posting[docID] += freq
						}
					}
				}
			}
			if posting != nil {
				for docID, freq := range posting {
					docLength := idx.fieldLengths[field][docID]
					avgLength := idx.avgFieldLength[field]
					tokenOccurrence := idx.tokenOccurrences[field][qToken]
					if tokenOccurrence == 0 {
						tokenOccurrence = 1
					}
					score := BM25(float64(freq), docLength, avgLength, totalDocs, tokenOccurrence, bm25Params)
					scoreMap[docID] += score
				}
			}
		}
	}

	var results []SearchResult
	for docID, score := range scoreMap {
		if doc, ok := idx.docs[docID]; ok {
			results = append(results, SearchResult{DocID: docID, Score: score, Document: doc})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	return results, nil
}

// -------------------- Persistence --------------------

// Save persists the index state to disk using msgpack.
func (idx *ESIndex) Save(prefix, id string) error {
	filePath := filename(prefix, id)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	encoder := msgpack.NewEncoder(writer)
	return encoder.Encode(idx)
}

// Load restores the index state from disk.
func (idx *ESIndex) Load(prefix, id string) error {
	filePath := filename(prefix, id)
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	decoder := msgpack.NewDecoder(reader)
	return decoder.Decode(idx)
}

var (
	indexDir  = "index"
	extension = ".index"
)

func filename(prefix, id string) string {
	dir := filepath.Join("data", prefix, indexDir)
	os.MkdirAll(dir, os.ModePerm)
	return filepath.Join(dir, id+extension)
}

// -------------------- Engine API --------------------

// Engine wraps an ESIndex and provides high-level full text search functionality.
type Engine struct {
	index      *ESIndex
	bm25Params BM25Params
}

// NewEngine creates a new Engine with the given mapping and BM25 parameters.
func NewEngine(mapping Mapping, bm25Params BM25Params) *Engine {
	return &Engine{
		index:      NewESIndex(mapping),
		bm25Params: bm25Params,
	}
}

// generateDocID creates a unique document ID.
func generateDocID() int64 {
	return time.Now().UnixNano()
}

// Insert adds a document to the engine.
func (e *Engine) Insert(doc Document) (int64, error) {
	id := generateDocID()
	err := e.index.Insert(doc, id)
	return id, err
}

// Search performs a query against the engine.
func (e *Engine) Search(query string) ([]SearchResult, error) {
	return e.index.Search(query, e.bm25Params)
}

// -------------------- Utility Functions --------------------

// getMemoryUsageMB returns current memory usage in MB.
func getMemoryUsageMB() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

// readJSONFile loads JSON documents from a file.
func readJSONFile(filename string) ([]Document, error) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var docs []Document
	err = json.Unmarshal(bytes, &docs)
	return docs, err
}

// -------------------- Main --------------------

func main() {
	// Load documents from JSON file.
	docs, err := readJSONFile("cpt_codes.json")
	if err != nil {
		fmt.Println("Error loading JSON:", err)
		return
	}

	// Define a stop words list.
	stopWords := []string{"the", "and", "a", "an", "of", "to", "in", "for", "on", "at"}
	// Create an EnhancedAnalyzer with stop word removal and unique tokens.
	analyzer := NewEnhancedAnalyzer(stopWords, true)

	// Define a mapping for the fields to index.
	mapping := Mapping{
		Fields: map[string]FieldMapping{
			"charge_type": {
				FieldName: "charge_type",
				Analyzer:  analyzer,
				Index:     true,
				Store:     true,
			},
			"client_internal_code": {
				FieldName: "client_internal_code",
				Analyzer:  analyzer,
				Index:     true,
				Store:     true,
			},
			"client_proc_desc": {
				FieldName: "client_proc_desc",
				Analyzer:  analyzer,
				Index:     true,
				Store:     true,
			},
			"cpt_hcpcs_code": {
				FieldName: "cpt_hcpcs_code",
				Analyzer:  analyzer,
				Index:     true,
				Store:     true,
			},
		},
	}

	// Set BM25 parameters.
	bm25Params := BM25Params{
		K: 1.2,
		B: 0.75,
	}

	// Create the search engine.
	engine := NewEngine(mapping, bm25Params)

	// Report memory usage before indexing.
	beforeMem := getMemoryUsageMB()
	startIndex := time.Now()

	// Index documents.
	for _, doc := range docs {
		_, err := engine.Insert(doc)
		if err != nil {
			fmt.Println("Error indexing document:", err)
			continue
		}
	}
	indexDuration := time.Since(startIndex)
	afterMem := getMemoryUsageMB()

	fmt.Printf("Indexing took: %s\n", indexDuration)
	fmt.Printf("Memory Usage: Before: %dMB, After: %dMB, Delta: %dMB\n", beforeMem, afterMem, afterMem-beforeMem)

	// Run sample queries.
	queries := []string{
		"cryosurg ablation",
		"ed_profee",
		"AN55873",
	}

	for _, query := range queries {
		fmt.Printf("\nQuery: %q\n", query)
		startSearch := time.Now()
		results, err := engine.Search(query)
		if err != nil {
			fmt.Println("Search error:", err)
			continue
		}
		searchDuration := time.Since(startSearch)
		fmt.Printf("Found %d results (search took %s):\n", len(results), searchDuration)
	}
}
