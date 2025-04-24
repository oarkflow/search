package utils

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode"

	"github.com/oarkflow/json"
)

func Intersect(a, b []int) []int {
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

func Union(a, b []int) []int {
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

func Subtract(a, b []int) []int {
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

func Abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func RowCount(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer func() {
		_ = file.Close()
	}()
	reader := bufio.NewReader(file)
	dec := json.NewDecoder(reader)
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('[') {
		return 0, fmt.Errorf("expected JSON array start")
	}
	count := 0
	for dec.More() {
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return count, fmt.Errorf("decode error: %w", err)
		}
		count++
	}
	return count, nil
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

func BoundedLevenshtein(a, b string, threshold int) int {
	la, lb := len(a), len(b)
	if Abs(la-lb) > threshold {
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
