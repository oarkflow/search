package search

import (
	"fmt"
	"sync"

	"github.com/oarkflow/xid"

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

type Index struct {
	data             *radix.Trie
	avgFieldLength   float64
	fieldLengths     map[int64]int
	tokenOccurrences map[string]int
	mu               sync.RWMutex // Use a mutex for thread safety
}

func NewIndex(key string) *Index {
	key = fmt.Sprintf("%s:%s", key, xid.New().String())
	return &Index{
		data:             radix.New(key),
		fieldLengths:     make(map[int64]int),
		tokenOccurrences: make(map[string]int),
	}
}

func (idx *Index) Insert(id int64, tokens map[string]int, docsCount int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	totalTokens := len(tokens)
	for token, count := range tokens {
		tokenFrequency := float64(count) / float64(totalTokens)
		idx.data.Insert(id, token, tokenFrequency)
	}

	idx.avgFieldLength = (idx.avgFieldLength*float64(docsCount-1) + float64(totalTokens)) / float64(docsCount)
	idx.fieldLengths[id] = totalTokens
}

func (idx *Index) Delete(id int64, tokens map[string]int, docsCount int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
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

func (idx *Index) Find(params *FindParams) map[int64]float64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	idScores := make(map[int64]float64)
	idTokensCount := make(map[int64]int)
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
