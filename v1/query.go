package v1

import (
	"strconv"
	"strings"

	"github.com/oarkflow/search/v1/utils"
)

type Query interface {
	Evaluate(index *InvertedIndex) []int
}

type TermQuery struct {
	Term           string
	Fuzzy          bool
	FuzzyThreshold int
}

func NewTermQuery(term string, fuzzy bool, threshold int) TermQuery {
	return TermQuery{
		Term:           term,
		Fuzzy:          fuzzy,
		FuzzyThreshold: threshold,
	}
}

func (tq TermQuery) Evaluate(index *InvertedIndex) []int {
	var tokens []string
	if tq.Fuzzy {
		tokens = index.FuzzySearch(strings.ToLower(tq.Term), tq.FuzzyThreshold)
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
		combined := strings.ToLower(rec.String())
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

func NewRangeQuery(field string, lower, upper float64) RangeQuery {
	return RangeQuery{
		Field: field,
		Lower: lower,
		Upper: upper,
	}
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
			mustResult = utils.Intersect(mustResult, bq.Must[i].Evaluate(index))
		}
	} else {
		for docID := range index.Documents {
			mustResult = append(mustResult, docID)
		}
	}
	var shouldResult []int
	for _, q := range bq.Should {
		shouldResult = utils.Union(shouldResult, q.Evaluate(index))
	}
	if len(bq.Should) > 0 {
		mustResult = utils.Intersect(mustResult, shouldResult)
	}
	for _, q := range bq.MustNot {
		mustResult = utils.Subtract(mustResult, q.Evaluate(index))
	}
	return mustResult
}
