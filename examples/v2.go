package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/search/tokenizer/stopwords"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/msgpack"
)

type BM25Params struct {
	K float64
	B float64
}

func BM25(frequency float64, docLength int, avgDocLength float64, totalDocs int, docFreq int, params BM25Params) float64 {
	idf := 0.0
	if docFreq > 0 {
		idf = math.Log((float64(totalDocs)-float64(docFreq)+0.5)/(float64(docFreq)+0.5) + 1)
	}
	tf := (frequency * (params.K + 1)) / (frequency + params.K*(1.0-params.B+params.B*(float64(docLength)/avgDocLength)))
	return idf * tf
}

func BoundedLevenshtein(a, b string, maxDist int) (int, bool) {
	lenA, lenB := len(a), len(b)
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
			current[j] = minVal(prev[j]+1, current[j-1]+1, prev[j-1]+cost)
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

const fuzzyThreshold = 1

type Analyzer interface {
	Analyze(text string) []string
}

type EnhancedAnalyzer struct {
	Unique bool
}

func NewEnhancedAnalyzer(unique bool) *EnhancedAnalyzer {
	return &EnhancedAnalyzer{
		Unique: unique,
	}
}

func (a *EnhancedAnalyzer) Analyze(text string) []string {
	words := strings.Fields(text)
	var tokens []string
	seen := make(map[string]bool)
	for _, word := range words {
		token := strings.ToLower(word)
		if _, ok := stopwords.English[token]; ok {
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

type FieldMapping struct {
	FieldName string
	Analyzer  Analyzer
	Index     bool
	Store     bool
}

type Mapping struct {
	Fields map[string]FieldMapping
}

type Document map[string]interface{}

type ESIndex struct {
	invertedIndex    map[string]map[string]map[int64][]int
	docs             map[int64]Document
	mapping          Mapping
	fieldLengths     map[string]map[int64]int
	avgFieldLength   map[string]float64
	tokenOccurrences map[string]map[string]int
	mu               sync.RWMutex
}

func NewESIndex(mapping Mapping) *ESIndex {
	return &ESIndex{
		invertedIndex:    make(map[string]map[string]map[int64][]int),
		docs:             make(map[int64]Document),
		mapping:          mapping,
		fieldLengths:     make(map[string]map[int64]int),
		avgFieldLength:   make(map[string]float64),
		tokenOccurrences: make(map[string]map[string]int),
	}
}

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

		if _, ok := idx.invertedIndex[field]; !ok {
			idx.invertedIndex[field] = make(map[string]map[int64][]int)
		}
		if _, ok := idx.fieldLengths[field]; !ok {
			idx.fieldLengths[field] = make(map[int64]int)
		}
		if _, ok := idx.tokenOccurrences[field]; !ok {
			idx.tokenOccurrences[field] = make(map[string]int)
		}
		seenTokens := make(map[string]bool)
		for pos, token := range tokens {
			token = strings.ToLower(token)
			if _, ok := idx.invertedIndex[field][token]; !ok {
				idx.invertedIndex[field][token] = make(map[int64][]int)
			}
			idx.invertedIndex[field][token][id] = append(idx.invertedIndex[field][token][id], pos)
			if !seenTokens[token] {
				seenTokens[token] = true
				idx.tokenOccurrences[field][token]++
			}
		}
		docLength := len(tokens)
		idx.fieldLengths[field][id] = docLength
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
			qToken = strings.ToLower(qToken)
			posting, exists := idx.invertedIndex[field][qToken]
			if !exists {
				for token, candidatePosting := range idx.invertedIndex[field] {
					if dist, ok := BoundedLevenshtein(qToken, token, fuzzyThreshold); ok && dist <= fuzzyThreshold {
						if posting == nil {
							posting = make(map[int64][]int)
						}
						for docID, posList := range candidatePosting {
							posting[docID] = append(posting[docID], posList...)
						}
					}
				}
			}
			if posting != nil {
				for docID, posList := range posting {
					freq := len(posList)
					docLength := idx.fieldLengths[field][docID]
					avgLength := idx.avgFieldLength[field]
					docFreq := idx.tokenOccurrences[field][qToken]
					if docFreq == 0 {
						docFreq = 1
					}
					score := BM25(float64(freq), docLength, avgLength, totalDocs, docFreq, bm25Params)
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

func (idx *ESIndex) Save(prefix, id string) error {
	filePath := filename(prefix, id)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	writer := bufio.NewWriter(file)
	defer func() {
		_ = writer.Flush()
	}()
	encoder := msgpack.NewEncoder(writer)
	return encoder.Encode(idx)
}

func (idx *ESIndex) Load(prefix, id string) error {
	filePath := filename(prefix, id)
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
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
	_ = os.MkdirAll(dir, os.ModePerm)
	return filepath.Join(dir, id+extension)
}

type SearchResult struct {
	DocID    int64
	Score    float64
	Document Document
}

type Query interface {
	Execute(e *Engine) (map[int64]float64, error)
}

type TermQuery struct {
	Field string
	Term  string
	Fuzzy bool
}

func (tq *TermQuery) Execute(e *Engine) (map[int64]float64, error) {
	result := make(map[int64]float64)
	var fieldsToSearch []string
	if tq.Field != "" {
		fieldsToSearch = []string{tq.Field}
	} else {
		for field, fm := range e.index.mapping.Fields {
			if fm.Index {
				fieldsToSearch = append(fieldsToSearch, field)
			}
		}
	}
	term := strings.ToLower(tq.Term)
	for _, field := range fieldsToSearch {
		posting, exists := e.index.invertedIndex[field][term]
		if !exists && tq.Fuzzy {
			for token, candidatePosting := range e.index.invertedIndex[field] {
				if dist, ok := BoundedLevenshtein(term, token, fuzzyThreshold); ok && dist <= fuzzyThreshold {
					if posting == nil {
						posting = make(map[int64][]int)
					}
					for docID, posList := range candidatePosting {
						posting[docID] = append(posting[docID], posList...)
					}
				}
			}
		}
		if posting != nil {
			for docID, posList := range posting {
				result[docID] += float64(len(posList))
			}
		}
	}
	return result, nil
}

type RangeQuery struct {
	Field     string
	Lower     float64
	Upper     float64
	Inclusive bool
}

func (rq *RangeQuery) Execute(e *Engine) (map[int64]float64, error) {
	result := make(map[int64]float64)
	for docID, doc := range e.index.docs {
		val, ok := doc[rq.Field]
		if !ok {
			continue
		}
		var num float64
		switch v := val.(type) {
		case float64:
			num = v
		case int:
			num = float64(v)
		case int64:
			num = float64(v)
		case string:
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				continue
			}
			num = n
		default:
			continue
		}
		if rq.Inclusive {
			if num >= rq.Lower && num <= rq.Upper {
				result[docID] = 1
			}
		} else {
			if num > rq.Lower && num < rq.Upper {
				result[docID] = 1
			}
		}
	}
	return result, nil
}

type BoolOperator int

const (
	BoolMust BoolOperator = iota
	BoolShould
	BoolMustNot
)

type BoolQuery struct {
	Operator BoolOperator
	Queries  []Query
}

func (bq *BoolQuery) Execute(e *Engine) (map[int64]float64, error) {
	if len(bq.Queries) == 0 {
		return nil, fmt.Errorf("no subqueries in BoolQuery")
	}
	results := make([]map[int64]float64, len(bq.Queries))
	for i, q := range bq.Queries {
		res, err := q.Execute(e)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}
	switch bq.Operator {
	case BoolMust:
		final := results[0]
		for i := 1; i < len(results); i++ {
			newFinal := make(map[int64]float64)
			for docID, score := range final {
				if s, ok := results[i][docID]; ok {
					newFinal[docID] = score + s
				}
			}
			final = newFinal
		}
		return final, nil
	case BoolShould:
		final := make(map[int64]float64)
		for _, res := range results {
			for docID, score := range res {
				final[docID] += score
			}
		}
		return final, nil
	case BoolMustNot:
		exclude := make(map[int64]bool)
		for _, res := range results {
			for docID := range res {
				exclude[docID] = true
			}
		}
		final := make(map[int64]float64)
		for docID := range e.index.docs {
			if !exclude[docID] {
				final[docID] = 1
			}
		}
		return final, nil
	default:
		return nil, fmt.Errorf("unsupported bool operator")
	}
}

type MatchAllQuery struct{}

func (q *MatchAllQuery) Execute(e *Engine) (map[int64]float64, error) {
	result := make(map[int64]float64)
	for docID := range e.index.docs {
		result[docID] = 1.0
	}
	return result, nil
}

type PrefixQuery struct {
	Field  string
	Prefix string
}

func (pq *PrefixQuery) Execute(e *Engine) (map[int64]float64, error) {
	result := make(map[int64]float64)
	var fieldsToSearch []string
	if pq.Field != "" {
		fieldsToSearch = []string{pq.Field}
	} else {
		for field, fm := range e.index.mapping.Fields {
			if fm.Index {
				fieldsToSearch = append(fieldsToSearch, field)
			}
		}
	}
	prefix := strings.ToLower(pq.Prefix)
	for _, field := range fieldsToSearch {
		for token, posting := range e.index.invertedIndex[field] {
			if strings.HasPrefix(token, prefix) {
				for docID, posList := range posting {
					result[docID] += float64(len(posList))
				}
			}
		}
	}
	return result, nil
}

type PhraseQuery struct {
	Field  string
	Phrase string
}

func (pq *PhraseQuery) Execute(e *Engine) (map[int64]float64, error) {
	result := make(map[int64]float64)
	field := pq.Field
	fm, exists := e.index.mapping.Fields[field]
	if !exists || !fm.Index {
		return nil, fmt.Errorf("field %s not found or not indexed", field)
	}
	phraseTokens := fm.Analyzer.Analyze(pq.Phrase)
	if len(phraseTokens) == 0 {
		return result, nil
	}
	firstToken := strings.ToLower(phraseTokens[0])
	posting, exists := e.index.invertedIndex[field][firstToken]
	if !exists {
		return result, nil
	}
	for docID, positions := range posting {
		for _, pos := range positions {
			match := true
			for i := 1; i < len(phraseTokens); i++ {
				token := strings.ToLower(phraseTokens[i])
				nextPosting, exists := e.index.invertedIndex[field][token]
				if !exists {
					match = false
					break
				}
				posList, exists := nextPosting[docID]
				if !exists {
					match = false
					break
				}
				found := false
				for _, p := range posList {
					if p == pos+i {
						found = true
						break
					}
				}
				if !found {
					match = false
					break
				}
			}
			if match {
				result[docID] += 1.0
				break
			}
		}
	}
	return result, nil
}

type BoostQuery struct {
	Query Query
	Boost float64
}

func (bq *BoostQuery) Execute(e *Engine) (map[int64]float64, error) {
	res, err := bq.Query.Execute(e)
	if err != nil {
		return nil, err
	}
	for docID, score := range res {
		res[docID] = score * bq.Boost
	}
	return res, nil
}

type Engine struct {
	index      *ESIndex
	bm25Params BM25Params
}

func NewEngine(mapping Mapping, bm25Params BM25Params) *Engine {
	return &Engine{
		index:      NewESIndex(mapping),
		bm25Params: bm25Params,
	}
}

func generateDocID() int64 {
	return time.Now().UnixNano()
}

func (e *Engine) Insert(doc Document) (int64, error) {
	id := generateDocID()
	err := e.index.Insert(doc, id)
	return id, err
}

func (e *Engine) Search(query string) ([]SearchResult, error) {
	return e.index.Search(query, e.bm25Params)
}

func (e *Engine) SearchQuery(q Query) ([]SearchResult, error) {
	scores, err := q.Execute(e)
	if err != nil {
		return nil, err
	}
	var results []SearchResult
	for docID, score := range scores {
		if doc, ok := e.index.docs[docID]; ok {
			results = append(results, SearchResult{DocID: docID, Score: score, Document: doc})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	return results, nil
}

func (e *Engine) Delete(docID int64) error {
	e.index.mu.Lock()
	defer e.index.mu.Unlock()
	doc, exists := e.index.docs[docID]
	if !exists {
		return fmt.Errorf("document %d not found", docID)
	}
	for field, fm := range e.index.mapping.Fields {
		if !fm.Index {
			continue
		}
		val, exists := doc[field]
		if !exists {
			continue
		}
		text := fmt.Sprintf("%v", val)
		tokens := fm.Analyzer.Analyze(text)
		seenTokens := make(map[string]bool)
		for _, token := range tokens {
			token = strings.ToLower(token)
			if posting, ok := e.index.invertedIndex[field][token]; ok {
				delete(posting, docID)
				if len(posting) == 0 {
					delete(e.index.invertedIndex[field], token)
				}
			}
			if !seenTokens[token] {
				seenTokens[token] = true
				if occ, ok := e.index.tokenOccurrences[field][token]; ok {
					occ--
					if occ <= 0 {
						delete(e.index.tokenOccurrences[field], token)
					} else {
						e.index.tokenOccurrences[field][token] = occ
					}
				}
			}
		}
		delete(e.index.fieldLengths[field], docID)
		total := 0
		count := len(e.index.fieldLengths[field])
		for _, l := range e.index.fieldLengths[field] {
			total += l
		}
		if count > 0 {
			e.index.avgFieldLength[field] = float64(total) / float64(count)
		} else {
			e.index.avgFieldLength[field] = 0
		}
	}
	delete(e.index.docs, docID)
	return nil
}

func (e *Engine) Update(docID int64, doc Document) error {
	err := e.Delete(docID)
	if err != nil {
		return err
	}
	e.index.mu.Lock()
	defer e.index.mu.Unlock()
	e.index.docs[docID] = doc
	for field, fm := range e.index.mapping.Fields {
		if !fm.Index {
			continue
		}
		val, exists := doc[field]
		if !exists {
			continue
		}
		text := fmt.Sprintf("%v", val)
		tokens := fm.Analyzer.Analyze(text)
		if _, ok := e.index.invertedIndex[field]; !ok {
			e.index.invertedIndex[field] = make(map[string]map[int64][]int)
		}
		if _, ok := e.index.fieldLengths[field]; !ok {
			e.index.fieldLengths[field] = make(map[int64]int)
		}
		if _, ok := e.index.tokenOccurrences[field]; !ok {
			e.index.tokenOccurrences[field] = make(map[string]int)
		}
		seenTokens := make(map[string]bool)
		for pos, token := range tokens {
			token = strings.ToLower(token)
			if _, ok := e.index.invertedIndex[field][token]; !ok {
				e.index.invertedIndex[field][token] = make(map[int64][]int)
			}
			e.index.invertedIndex[field][token][docID] = append(e.index.invertedIndex[field][token][docID], pos)
			if !seenTokens[token] {
				seenTokens[token] = true
				e.index.tokenOccurrences[field][token]++
			}
		}
		docLength := len(tokens)
		e.index.fieldLengths[field][docID] = docLength
		total := 0
		count := len(e.index.fieldLengths[field])
		for _, l := range e.index.fieldLengths[field] {
			total += l
		}
		if count > 0 {
			e.index.avgFieldLength[field] = float64(total) / float64(count)
		} else {
			e.index.avgFieldLength[field] = 0
		}
	}
	return nil
}

func getMemoryUsageMB() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

func readJSONFile(filename string) ([]Document, error) {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var docs []Document
	err = json.Unmarshal(bytes, &docs)
	return docs, err
}

func main() {
	docs, err := readJSONFile("cpt_codes.json")
	if err != nil {
		fmt.Println("Error loading JSON:", err)
		return
	}
	analyzer := NewEnhancedAnalyzer(true)
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
	bm25Params := BM25Params{
		K: 1.2,
		B: 0.75,
	}
	engine := NewEngine(mapping, bm25Params)
	beforeMem := getMemoryUsageMB()
	startIndex := time.Now()
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
	fmt.Println("\nFull Text Search Query: 'cryosurg ablation'")
	results, err := engine.Search("cryosurg ablation")
	if err != nil {
		fmt.Println("Search error:", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
	}
	termQuery := &TermQuery{
		Field: "client_proc_desc",
		Term:  "cryosurg",
		Fuzzy: false,
	}
	fmt.Println("\nTerm Query (Field: client_proc_desc, Term: 'cryosurg')")
	results, err = engine.SearchQuery(termQuery)
	if err != nil {
		fmt.Println("Term Query error:", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
	}
	rangeQuery := &RangeQuery{
		Field:     "work_item_id",
		Lower:     30,
		Upper:     50,
		Inclusive: true,
	}
	fmt.Println("\nRange Query (Field: work_item_id, Between 30 and 50)")
	results, err = engine.SearchQuery(rangeQuery)
	if err != nil {
		fmt.Println("Range Query error:", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
	}
	boolQuery := &BoolQuery{
		Operator: BoolMust,
		Queries: []Query{
			termQuery,
			rangeQuery,
		},
	}
	fmt.Println("\nBoolean Query (MUST: term 'cryosurg' AND work_item_id between 30 and 50)")
	results, err = engine.SearchQuery(boolQuery)
	if err != nil {
		fmt.Println("Boolean Query error:", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
	}
	matchAll := &MatchAllQuery{}
	fmt.Println("\nMatch All Query")
	results, err = engine.SearchQuery(matchAll)
	if err != nil {
		fmt.Println("Match All Query error:", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
	}
	prefixQuery := &PrefixQuery{
		Field:  "client_proc_desc",
		Prefix: "cryo",
	}
	fmt.Println("\nPrefix Query (Field: client_proc_desc, Prefix: 'cryo')")
	results, err = engine.SearchQuery(prefixQuery)
	if err != nil {
		fmt.Println("Prefix Query error:", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
	}

	phraseQuery := &PhraseQuery{
		Field:  "client_proc_desc",
		Phrase: "cryosurg ablation",
	}
	fmt.Println("\nPhrase Query (Field: client_proc_desc, Phrase: 'cryosurg ablation')")
	results, err = engine.SearchQuery(phraseQuery)
	if err != nil {
		fmt.Println("Phrase Query error:", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
	}

	boostQuery := &BoostQuery{
		Query: termQuery,
		Boost: 2.0,
	}
	fmt.Println("\nBoost Query (Boosting term 'cryosurg' by 2.0)")
	results, err = engine.SearchQuery(boostQuery)
	if err != nil {
		fmt.Println("Boost Query error:", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
	}
}
