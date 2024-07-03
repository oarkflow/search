package search

import (
	"github.com/oarkflow/search/hash"
	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/radix"
)

type FindParams struct {
	Tokens    map[string]int
	BoolMode  Mode
	Exact     bool
	Tolerance int
	Relevance BM25Params
	DocsCount int
}

type IndexParams struct {
	Id        int64
	Tokens    map[string]int
	DocsCount int
}

type Index[T hash.Hashable] struct {
	data             *radix.Trie[T]
	avgFieldLength   float64
	fieldLengths     map[T]int
	tokenOccurrences map[string]int
}

func NewIndex[T hash.Hashable]() *Index[T] {
	return &Index[T]{
		data:             radix.New[T](),
		fieldLengths:     make(map[T]int),
		tokenOccurrences: make(map[string]int),
	}
}

func (idx *Index[T]) Insert(id T, tokens map[string]int, docsCount int) {
	totalTokens := len(tokens)
	for token, count := range tokens {
		tokenFrequency := float64(count) / float64(totalTokens)
		idx.data.Insert(id, token, tokenFrequency)
	}

	idx.avgFieldLength = (idx.avgFieldLength*float64(docsCount-1) + float64(totalTokens)) / float64(docsCount)
	idx.fieldLengths[id] = totalTokens
}

func (idx *Index[T]) Delete(id T, tokens map[string]int, docsCount int) {
	for token := range tokens {
		idx.data.Delete(id, token)
		idx.tokenOccurrences[token]--
		if idx.tokenOccurrences[token] == 0 {
			delete(idx.tokenOccurrences, token)
		}
	}

	idx.avgFieldLength = (idx.avgFieldLength*float64(docsCount) - float64(len(tokens))) / float64(docsCount-1)
	delete(idx.fieldLengths, id)
}

func (idx *Index[T]) Find(params *FindParams) map[T]float64 {
	idScores := make(map[T]float64)
	idTokensCount := make(map[T]int)
	for token := range params.Tokens {
		infos := idx.data.Find(token, params.Tolerance, params.Exact)
		for id, frequency := range infos {
			idScores[id] += lib.BM25(
				frequency,
				idx.tokenOccurrences[token],
				idx.fieldLengths[id],
				idx.avgFieldLength,
				params.DocsCount,
				params.Relevance.K,
				params.Relevance.B,
				params.Relevance.D,
			)
			idTokensCount[id]++
		}
	}
	for id, tokensCount := range idTokensCount {
		if params.BoolMode == AND && tokensCount != len(params.Tokens) {
			delete(idScores, id)
		}
	}
	return idScores
}
