package v1

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/oarkflow/search/v1/utils"
)

type Query interface {
	Evaluate(index *Index) []int
	Tokens() []string
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

func (tq TermQuery) Evaluate(index *Index) []int {
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

func (tq TermQuery) Tokens() []string {
	return []string{strings.ToLower(tq.Term)}
}

type PhraseQuery struct {
	Phrase string
}

func (pq PhraseQuery) Evaluate(index *Index) []int {
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

func (pq PhraseQuery) Tokens() []string {
	return utils.Tokenize(strings.ToLower(pq.Phrase))
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

func (rq RangeQuery) Evaluate(index *Index) []int {
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

func (rq RangeQuery) Tokens() []string {
	return []string{}
}

type BoolQuery struct {
	Must    []Query
	Should  []Query
	MustNot []Query
}

func (bq BoolQuery) Evaluate(index *Index) []int {
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

func (bq BoolQuery) Tokens() []string {
	tokensMap := make(map[string]struct{})
	for _, q := range bq.Must {
		if qt, ok := q.(interface{ Tokens() []string }); ok {
			for _, t := range qt.Tokens() {
				tokensMap[t] = struct{}{}
			}
		}
	}
	if len(tokensMap) == 0 {
		for _, q := range bq.Should {
			if qt, ok := q.(interface{ Tokens() []string }); ok {
				for _, t := range qt.Tokens() {
					tokensMap[t] = struct{}{}
				}
			}
		}
	}
	var tokens []string
	for t := range tokensMap {
		tokens = append(tokens, t)
	}
	return tokens
}

type MatchQuery struct {
	Field string
	Query string
}

func (mq MatchQuery) Evaluate(index *Index) []int {
	var result []int
	search := strings.ToLower(mq.Query)
	for docID, rec := range index.Documents {
		if val, ok := rec[mq.Field]; ok {
			strVal := strings.ToLower(strings.TrimSpace(utils.ToString(val)))
			if strings.Contains(strVal, search) {
				result = append(result, docID)
			}
		}
	}
	return result
}

func (mq MatchQuery) Tokens() []string {
	// Tokenize the match query string.
	return utils.Tokenize(strings.ToLower(mq.Query))
}

// WildcardQuery evaluates a query using wildcard patterns on a field.
type WildcardQuery struct {
	Field   string
	Pattern string // supports '*' as wildcard
}

func (wq WildcardQuery) Evaluate(index *Index) []int {
	var result []int
	// Convert wildcard '*' to regex pattern
	regexPattern := "^" + regexp.QuoteMeta(wq.Pattern) + "$"
	regexPattern = strings.ReplaceAll(regexPattern, "\\*", ".*")
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return result
	}
	for docID, rec := range index.Documents {
		if val, ok := rec[wq.Field]; ok {
			strVal := utils.ToString(val)
			if re.MatchString(strVal) {
				result = append(result, docID)
			}
		}
	}
	return result
}

func (wq WildcardQuery) Tokens() []string {
	// Return the pattern as token (wildcard removed for simplicity).
	return []string{strings.ToLower(wq.Pattern)}
}

// SQLQuery supports simple SQL queries: only equality conditions in the WHERE clause.
// Example supported query: "SELECT * FROM docs WHERE field = 'value'"
type SQLQuery struct {
	SQL string
}

func (sq SQLQuery) Evaluate(index *Index) []int {
	var result []int
	parts := strings.SplitN(sq.SQL, "WHERE", 2)
	if len(parts) != 2 {
		return result
	}
	condition := strings.TrimSpace(parts[1])
	// Assume condition is in the format: "field = 'value'" (or "field = value")
	condParts := strings.SplitN(condition, "=", 2)
	if len(condParts) != 2 {
		return result
	}
	field := strings.TrimSpace(condParts[0])
	value := strings.TrimSpace(condParts[1])
	// Remove quotes if present
	value = strings.Trim(value, "'\"")
	value = strings.ToLower(value)
	for docID, rec := range index.Documents {
		if val, ok := rec[field]; ok {
			strVal := strings.ToLower(strings.TrimSpace(utils.ToString(val)))
			if strVal == value {
				result = append(result, docID)
			}
		}
	}
	return result
}

func (sq SQLQuery) Tokens() []string {
	// SQL query is parsed internally; no tokens extracted.
	return []string{}
}
