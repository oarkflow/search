package search

import (
	"errors"
	"fmt"

	"github.com/oarkflow/maps"

	"github.com/oarkflow/search/hash"
	"github.com/oarkflow/search/tokenizer"
)

var engines maps.IMap[string, any]

func init() {
	engines = maps.New[string, any]()
}

var DefaultPath = "documentStorage/fts"

func GetConfig(key string) *Config {
	return &Config{
		Key:             key,
		DefaultLanguage: tokenizer.ENGLISH,
		TokenizerConfig: &tokenizer.Config{
			EnableStemming:  true,
			EnableStopWords: true,
		},
	}
}

func AvailableEngines[Schema SchemaProps, ID hash.Hashable]() (types []map[string]any) {
	engines.ForEach(func(key string, e any) bool {
		engine := e.(*Engine[Schema, ID])
		types = append(types, map[string]any{
			"key":   key,
			"count": engine.DocumentLen(),
		})
		return true
	})
	return
}

func GetEngine[Schema SchemaProps, ID hash.Hashable](key string) (*Engine[Schema, ID], error) {
	eng, _ := engines.Get(key)
	if eng != nil {
		return eng.(*Engine[Schema, ID]), nil
	}
	return nil, errors.New(fmt.Sprintf("Engine for key %s not available", key))
}

func SetEngine[Schema SchemaProps, ID hash.Hashable](key string, config *Config) (*Engine[Schema, ID], error) {
	_, ok := engines.Get(key)
	if ok {
		return nil, errors.New(fmt.Sprintf("Engine for key %s already exists", key))
	}
	eng, err := New[Schema, ID](config)
	if err != nil {
		return nil, err
	}
	AddEngine(key, eng)
	return eng, nil
}
func GetOrSetEngine[Schema SchemaProps, ID hash.Hashable](key string, config *Config) (*Engine[Schema, ID], error) {
	eng1, ok := engines.Get(key)
	if ok && eng1 != nil {
		return eng1.(*Engine[Schema, ID]), nil
	}
	eng, err := New[Schema, ID](config)
	if err != nil {
		return nil, err
	}
	AddEngine(key, eng)
	return eng, nil
}

func AddEngine(key string, engine any) {
	engines.Set(key, engine)
}
