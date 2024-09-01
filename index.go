package search

import (
	"bufio"
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
	mu               sync.RWMutex // Use a m for thread safety
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
	}

	idx.AvgFieldLength = (idx.AvgFieldLength*float64(docsCount-1) + float64(totalTokens)) / float64(docsCount)
	idx.FieldLengths[id] = totalTokens
}

func (idx *Index) Delete(id int64, tokens map[string]int, docsCount int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for token := range tokens {
		idx.Data.Delete(id, token)
		idx.TokenOccurrences[token]--
		if idx.TokenOccurrences[token] == 0 {
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

	// Use a buffered reader to improve read performance
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
