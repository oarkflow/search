package search

import (
	"bufio"
	"github.com/oarkflow/search/tokenizer"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/oarkflow/msgpack"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/radix"
)

var (
	indexDir  = "index"
	extension = ".index"
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
	Data             *radix.Trie
	AvgFieldLength   float64
	FieldLengths     map[int64]int
	TokenOccurrences map[string]int
	mu               sync.RWMutex // per-index lock
	ID               string
	Prefix           string
}

func NewIndex(prefix, id string) *Index {
	return &Index{
		Data:             radix.New(prefix, id),
		FieldLengths:     make(map[int64]int),
		TokenOccurrences: make(map[string]int),
		ID:               id,
		Prefix:           prefix,
	}
}

func (idx *Index) Insert(id int64, tokens map[string]int, docsCount int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	totalTokens := len(tokens)
	for token, count := range tokens {
		tokenFrequency := float64(count) / float64(totalTokens)
		idx.Data.Insert(id, token, tokenFrequency)
		idx.TokenOccurrences[token] += count
	}
	idx.AvgFieldLength = (idx.AvgFieldLength*float64(docsCount-1) + float64(totalTokens)) / float64(docsCount)
	idx.FieldLengths[id] = totalTokens
}

func (idx *Index) Delete(id int64, tokens map[string]int, docsCount int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for token := range tokens {
		idx.Data.Delete(id, token)
		idx.TokenOccurrences[token] -= tokens[token]
		if idx.TokenOccurrences[token] <= 0 {
			delete(idx.TokenOccurrences, token)
		}
	}
	idx.AvgFieldLength = (idx.AvgFieldLength*float64(docsCount) - float64(len(tokens))) / float64(docsCount-1)
	delete(idx.FieldLengths, id)
}

func (idx *Index) Find(params *FindParams) map[int64]float64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	idScores := make(map[int64]float64)
	idTokensCount := make(map[int64]int)
	for token := range params.Tokens {
		infos := idx.Data.Find(token, params.Tolerance, params.Exact)
		for id, frequency := range infos {
			idScores[id] += lib.BM25(
				frequency,
				idx.TokenOccurrences[token],
				idx.FieldLengths[id],
				idx.AvgFieldLength,
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

func (idx *Index) FileName() string {
	return filename(idx.Prefix, idx.ID)
}

func filename(prefix, id string) string {
	prefix = filepath.Join(DefaultPath, prefix, indexDir)
	os.MkdirAll(prefix, os.ModePerm)
	return filepath.Join(prefix, id+extension)
}

func (idx *Index) Save() error {
	filePath := idx.FileName()
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	bufferedWriter := bufio.NewWriter(file)
	defer bufferedWriter.Flush()

	encoder := msgpack.NewEncoder(bufferedWriter)
	return encoder.Encode(idx)
}

func (idx *Index) Load() (*Index, error) {
	filePath := idx.FileName()
	return NewFromFile(idx.Prefix, filePath)
}

func NewFromFile(prefix string, filePath string) (*Index, error) {
	filePath = filename(prefix, filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	bufferedReader := bufio.NewReader(file)
	return NewFromReader(bufferedReader, prefix)
}

func NewFromReader(reader io.Reader, prefix string) (*Index, error) {
	decoder := msgpack.NewDecoder(reader)
	t := NewIndex(prefix, xid.New().String())
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (db *Engine[Schema]) buildIndexes() {
	var s Schema
	for key := range db.flattenSchema(s) {
		db.addIndex(key)
	}
}

func (db *Engine[Schema]) addIndexes(keys []string) {
	for _, key := range keys {
		db.addIndex(key)
	}
}

func (db *Engine[Schema]) addIndex(key string) {
	db.indexes.Set(key, NewIndex(db.key, key))
	db.indexKeys = lib.Unique(append(db.indexKeys, key))
}

func (db *Engine[Schema]) indexDocument(id int64, document map[string]any, language tokenizer.Language) {
	docsCount := db.DocumentLen() // compute once
	// Copy current indexes to avoid holding global lock during tokenization.
	indexesCopy := make(map[string]*Index)
	db.m.RLock()
	db.indexes.ForEach(func(propName string, index *Index) bool {
		indexesCopy[propName] = index
		return true
	})
	db.m.RUnlock()

	for propName, index := range indexesCopy {
		text := lib.ToString(document[propName])
		tokens := tokensPool.Get()
		clear(tokens)
		_ = tokenizer.Tokenize(tokenizer.TokenizeParams{
			Text:            text,
			Language:        language,
			AllowDuplicates: true,
		}, *db.tokenizerConfig, tokens)
		index.Insert(id, tokens, docsCount)
		clear(tokens)
		tokensPool.Put(tokens)
	}
}

func (db *Engine[Schema]) deIndexDocument(id int64, document map[string]any, language tokenizer.Language) {
	docsCount := db.DocumentLen()
	indexesCopy := make(map[string]*Index)
	db.m.RLock()
	db.indexes.ForEach(func(propName string, index *Index) bool {
		indexesCopy[propName] = index
		return true
	})
	db.m.RUnlock()

	for propName, index := range indexesCopy {
		tokens := tokensPool.Get()
		clear(tokens)
		_ = tokenizer.Tokenize(tokenizer.TokenizeParams{
			Text:            lib.ToString(document[propName]),
			Language:        language,
			AllowDuplicates: false,
		}, *db.tokenizerConfig, tokens)
		index.Delete(id, tokens, docsCount)
		clear(tokens)
		tokensPool.Put(tokens)
	}
}
