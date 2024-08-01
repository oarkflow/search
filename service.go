package search

import (
	"errors"
	"fmt"

	"github.com/oarkflow/maps"

	"github.com/oarkflow/search/tokenizer"
)

var engines maps.IMap[string, any]

func init() {
	engines = maps.New[string, any]()
}

var DefaultPath = "documentStorage/fts"

func GetConfig[Schema SchemaProps](key string) *Config[Schema] {
	return &Config[Schema]{
		Key:             key,
		DefaultLanguage: tokenizer.ENGLISH,
		TokenizerConfig: &tokenizer.Config{
			EnableStemming:  true,
			EnableStopWords: true,
		},
	}
}

func AvailableEngines[Schema SchemaProps]() (types []map[string]any) {
	engines.ForEach(func(key string, e any) bool {
		engine := e.(*Engine[Schema])
		types = append(types, map[string]any{
			"key":   key,
			"count": engine.DocumentLen(),
		})
		return true
	})
	return
}

func GetEngine[Schema SchemaProps](key string) (*Engine[Schema], error) {
	eng, _ := engines.Get(key)
	if eng != nil {
		return eng.(*Engine[Schema]), nil
	}
	return nil, errors.New(fmt.Sprintf("Engine for key %s not available", key))
}

func SetEngine[Schema SchemaProps](key string, config *Config[Schema]) (*Engine[Schema], error) {
	_, ok := engines.Get(key)
	if ok {
		return nil, errors.New(fmt.Sprintf("Engine for key %s already exists", key))
	}
	eng, err := New[Schema](config)
	if err != nil {
		return nil, err
	}
	AddEngine(key, eng)
	return eng, nil
}
func GetOrSetEngine[Schema SchemaProps](key string, config *Config[Schema]) (*Engine[Schema], error) {
	eng1, ok := engines.Get(key)
	if ok && eng1 != nil {
		return eng1.(*Engine[Schema]), nil
	}
	eng, err := New[Schema](config)
	if err != nil {
		return nil, err
	}
	AddEngine(key, eng)
	return eng, nil
}

func AddEngine(key string, engine any) {
	engines.Set(key, engine)
}
