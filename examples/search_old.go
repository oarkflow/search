package main

/*
import (
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/search/lib"
)

// Token represents a structure to store the total occurrences and the associated row IDs
type Token struct {
	Total int
	Rows  []int64
}

// Tokenize specified fields in the map or all fields if none specified
func tokenizeRow(row map[string]interface{}, fields []string) []string {
	var tokens []string
	if len(fields) == 0 {
		for _, value := range row {
			if str, ok := value.(string); ok {
				tokens = append(tokens, strings.Fields(str)...)
			}
		}
	} else {
		for _, field := range fields {
			if value, ok := row[field].(string); ok {
				tokens = append(tokens, strings.Fields(value)...)
			}
		}
	}
	return tokens
}

// Process a chunk of data: tokenize and index
func processChunk(chunk []map[string]interface{}, indexes map[string]map[string]*Token, fields []string) {
	for _, row := range chunk {
		rowID := xid.New().Int64()
		if len(fields) == 0 {
			for field, value := range row {
				if str, ok := value.(string); ok {
					tokens := strings.Fields(str)
					if indexes[field] == nil {
						indexes[field] = make(map[string]*Token)
					}
					for _, token := range tokens {
						token := lib.ToLower(token)
						if entry, exists := indexes[field][token]; exists {
							entry.Total++
							entry.Rows = append(entry.Rows, rowID)
						} else {
							indexes[field][token] = &Token{Total: 1, Rows: []int64{rowID}}
						}
					}
				}
			}
		} else {
			for _, field := range fields {
				if value, ok := row[field].(string); ok {
					tokens := strings.Fields(value)
					if indexes[field] == nil {
						indexes[field] = make(map[string]*Token)
					}
					for _, token := range tokens {
						token := lib.ToLower(token)
						if entry, exists := indexes[field][token]; exists {
							entry.Total++
							entry.Rows = append(entry.Rows, rowID)
						} else {
							indexes[field][token] = &Token{Total: 1, Rows: []int64{rowID}}
						}
					}
				}
			}
		}
	}
}

// Process the entire dataset in chunks
func processDataset(data []map[string]interface{}, chunkSize int, fields []string) map[string]map[string]*Token {
	indexes := make(map[string]map[string]*Token)
	for start := 0; start < len(data); start += chunkSize {
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]
		processChunk(chunk, indexes, fields)
	}
	return indexes
}

// Tokenize the search query
func tokenizeQuery(query string) []string {
	return strings.Fields(query)
}

// Find the intersection of multiple slices of integers
func intersect(ids ...[]int64) []int64 {
	if len(ids) == 0 {
		return nil
	}

	result := ids[0]
	for _, idSet := range ids[1:] {
		result = intersectTwo(result, idSet)
		if len(result) == 0 {
			break
		}
	}
	return result
}

// Find the intersection of two slices of integers
func intersectTwo(a, b []int64) []int64 {
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
func search(indexes map[string]map[string]*Token, query string, fields ...string) []int64 {
	tokens := tokenizeQuery(query)
	var allRows [][]int64

	for _, token := range tokens {
		token := lib.ToLower(token)
		var tokenRows []int64
		if len(fields) > 0 {
			for _, field := range fields {
				if index, exists := indexes[field]; exists {
					if entry, exists := index[token]; exists {
						tokenRows = append(tokenRows, entry.Rows...)
					}
				}
			}
		} else {
			for _, index := range indexes {
				if entry, exists := index[token]; exists {
					tokenRows = append(tokenRows, entry.Rows...)
				}
			}
		}

		if len(tokenRows) == 0 {
			return nil // If any token is not found in the specified fields, return an empty result
		}
		allRows = append(allRows, tokenRows)
	}

	return intersect(allRows...)
}

// Search for row IDs by a provided string containing multiple words and additional filters
func search(indexes map[string]map[string]*Token, data []map[string]interface{}, query string, fields []string, filters map[string]interface{}) []int {
	tokens := tokenizeQuery(query)
	var allRows [][]int64

	for _, token := range tokens {
		token := lib.ToLower(token)
		var tokenRows []int64
		if len(fields) > 0 {
			for _, field := range fields {
				if index, exists := indexes[field]; exists {
					if entry, exists := index[token]; exists {
						tokenRows = append(tokenRows, entry.Rows...)
					}
				}
			}
		} else {
			for _, index := range indexes {
				if entry, exists := index[token]; exists {
					tokenRows = append(tokenRows, entry.Rows...)
				}
			}
		}

		if len(tokenRows) == 0 {
			return nil // If any token is not found in the specified fields, return an empty result
		}
		allRows = append(allRows, tokenRows)
	}

	// Filter the intersected rows by the specified filters
	intersectedRows := intersect(allRows...)
	return filterRows(data, intersectedRows, filters)
}

// Filter rows based on specified key-value conditions
func filterRows(data []map[string]interface{}, rowIDs []int64, filters map[string]interface{}) []int {
	var result []int
	for _, rowID := range rowIDs {
		row := data[rowID]
		matches := true
		for key, value := range filters {
			switch v := value.(type) {
			case string:
				if row[key] != v {
					matches = false
					break
				}
			case func(interface{}) bool:
				if !v(row[key]) {
					matches = false
					break
				}
			}
		}
		if matches {
			result = append(result, rowID)
		}
	}
	return result
}

func main() {
	data := readFileAsMap("icd10_codes.json")

	chunkSize := 1000                         // Process 1000 rows at a time (adjust as needed)
	fieldsToIndex := []string{"code", "desc"} // Index specified fields (empty means all fields)

	var startTime = time.Now()
	before := stats()
	// Process the dataset
	index := processDataset(data, chunkSize, fieldsToIndex)
	after := stats()
	var diff uint64
	if after > before {
		diff = after - before
	} else {
		diff = -(before - after)
	}
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", diff, before, after))
	fmt.Println("Total Documents", len(index))
	fmt.Println("Indexing took", time.Since(startTime))
	// Search for row IDs by a provided query string
	query := "presence"
	startTime = time.Now()
	rowIDs := search(index, query)
	fmt.Println("Searching took", time.Since(startTime))
	fmt.Printf("\nSearch query: \"%s\"\nRow IDs: %v\n", query, rowIDs)
}
*/
