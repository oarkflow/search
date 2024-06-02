package main

/*
import (
	"fmt"
	"strings"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/tokenizer/stopwords"
)

type Engine struct {
	indexes map[string]map[string]*Token
	store   map[int64]map[string]any
}

func New() *Engine {
	return &Engine{
		indexes: make(map[string]map[string]*Token),
		store:   make(map[int64]map[string]any),
	}
}

// Token represents a structure to store the total occurrences and the associated row IDs
type Token struct {
	Total int
	Rows  []int64
}

// NormalizeToken converts a token to lowercase
func NormalizeToken(token string) string {
	token = strings.ToLower(token)
	if _, ok := stopwords.English[token]; ok {
		return ""
	}
	return token
}

// IndexChunk - Process a chunk of data: tokenize and index
func (e *Engine) IndexChunk(chunk []map[string]any, fields []string) {
	for _, row := range chunk {
		rowID := xid.New().Int64()
		e.store[rowID] = row
		if len(fields) == 0 {
			for field, value := range row {
				e.index(rowID, field, value)
			}
		} else {
			for _, field := range fields {
				e.index(rowID, field, row[field])
			}
		}
	}
}

func (e *Engine) index(rowID int64, field string, value any) {
	str := lib.ToString(value)
	tokens := strings.Fields(str)
	if e.indexes[field] == nil {
		e.indexes[field] = make(map[string]*Token)
	}
	for _, token := range tokens {
		normalizedToken := NormalizeToken(token)
		if normalizedToken != "" {
			if entry, exists := e.indexes[field][normalizedToken]; exists {
				entry.Total++
				entry.Rows = append(entry.Rows, rowID)
			} else {
				e.indexes[field][normalizedToken] = &Token{Total: 1, Rows: []int64{rowID}}
			}
		}
	}
}

// Index - Process the entire dataset in chunks
func (e *Engine) Index(data []map[string]any, chunkSize int, fields []string) {
	for start := 0; start < len(data); start += chunkSize {
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]
		e.IndexChunk(chunk, fields)
	}
}

// TokenizeQuery - Tokenize the search query
func TokenizeQuery(query string) []string {
	return strings.Fields(query)
}

// Intersect - Find the intersection of multiple slices of integers
func Intersect(ids ...[]int64) []int64 {
	if len(ids) == 0 {
		return nil
	}

	result := ids[0]
	for _, idSet := range ids[1:] {
		result = IntersectTwo(result, idSet)
		if len(result) == 0 {
			break
		}
	}
	return result
}

// IntersectTwo - Find the intersection of two slices of integers
func IntersectTwo(a, b []int64) []int64 {
	m := make(map[int64]bool)
	for _, item := range a {
		m[item] = true
	}
	var result []int64
	for _, item := range b {
		if m[item] {
			result = append(result, item)
		}
	}
	return result
}

// Search for row IDs by a provided string containing multiple words
func (e *Engine) Search(query string, fields []string, filters map[string]any) (rows []map[string]any) {
	tokens := TokenizeQuery(query)
	var allRows [][]int64

	for _, token := range tokens {
		var tokenRows []int64
		token = NormalizeToken(token)
		if token != "" {
			if len(fields) > 0 {
				for _, field := range fields {
					if index, exists := e.indexes[field]; exists {
						if entry, exists := index[token]; exists {
							tokenRows = append(tokenRows, entry.Rows...)
						}
					}
				}
			} else {
				for _, index := range e.indexes {
					if entry, exists := index[token]; exists {
						tokenRows = append(tokenRows, entry.Rows...)
					}
				}
			}
		}
		if len(tokenRows) == 0 {
			return nil // If any token is not found in the specified fields, return an empty result
		}
		allRows = append(allRows, tokenRows)
	}

	// Perform intersection of token results
	resultIDs := Intersect(allRows...)

	// Apply additional filters
	var filteredResult []int64
	for _, id := range resultIDs {
		if row, exists := e.store[id]; exists {
			match := true
			for key, value := range filters {
				if rowValue, exists := row[key]; !exists || rowValue != value {
					match = false
					break
				}
			}
			if match {
				filteredResult = append(filteredResult, id)
			}
		}
	}
	for _, key := range filteredResult {
		if row, exists := e.store[key]; exists {
			rows = append(rows, row)
		}
	}
	return rows
}

func main() {
	data := lib.ReadFileAsMap("icd10_codes.json")
	chunkSize := 1000                         // Process 1000 rows at a time (adjust as needed)
	fieldsToIndex := []string{"code", "desc"} // Index specified fields (empty means all fields)
	engine := New()
	// Process the dataset
	engine.Index(data, chunkSize, fieldsToIndex)

	// Search for row IDs by a provided query string with additional filters
	query := "presence of artificial right"
	searchFields := []string{"desc"} // Restrict search to specified fields (empty means all fields)
	rowIDs := engine.Search(query, searchFields, nil)
	fmt.Printf("\nSearch query: \"%s\"\nRow IDs: %v\n", query, rowIDs)
}
*/
