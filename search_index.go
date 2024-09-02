package search

import (
	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/tokenizer"
)

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
	db.m.Lock()
	defer db.m.Unlock()
	db.indexes.ForEach(func(propName string, index *Index) bool {
		text := lib.ToString(document[propName])
		tokens := tokensPool.Get()
		clear(tokens)
		_ = tokenizer.Tokenize(tokenizer.TokenizeParams{
			Text:            text,
			Language:        language,
			AllowDuplicates: true,
		}, *db.tokenizerConfig, tokens)
		index.Insert(id, tokens, db.DocumentLen())
		clear(tokens)
		tokensPool.Put(tokens)
		return true
	})
}

func (db *Engine[Schema]) deIndexDocument(id int64, document map[string]any, language tokenizer.Language) {
	db.m.Lock()
	defer db.m.Unlock()
	db.indexes.ForEach(func(propName string, index *Index) bool {
		tokens := tokensPool.Get()
		clear(tokens)
		_ = tokenizer.Tokenize(tokenizer.TokenizeParams{
			Text:            lib.ToString(document[propName]),
			Language:        language,
			AllowDuplicates: false,
		}, *db.tokenizerConfig, tokens)
		index.Delete(id, tokens, db.DocumentLen())
		clear(tokens)
		tokensPool.Put(tokens)
		return true
	})
}
