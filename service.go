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

var DefaultPath = "storage/fts"

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

func GetEngine[Schema SchemaProps](key string) (*Engine[Schema], error) {
	eng, _ := engines.Get(key)
	if eng != nil {
		return eng.(*Engine[Schema]), nil
	}
	return nil, errors.New(fmt.Sprintf("Engine for key %s not available", key))
}

func SetEngine[Schema SchemaProps](key string, config *Config) error {
	_, ok := engines.Get(key)
	if ok {
		return errors.New(fmt.Sprintf("Engine for key %s already exists", key))
	}
	eng, err := New[Schema](config)
	if err != nil {
		return err
	}
	AddEngine(key, eng)
	return nil
}

func AddEngine(key string, engine any) {
	engines.Set(key, engine)
}
