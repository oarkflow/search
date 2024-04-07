package search

import (
	"github.com/oarkflow/maps"

	"github.com/oarkflow/search/tokenizer"
)

var engines maps.IMap[string, any]

func init() {
	engines = maps.New[string, any]()
}

func GetConfig(key, path string) *Config {
	return &Config{
		Key:             key,
		DefaultLanguage: tokenizer.ENGLISH,
		TokenizerConfig: &tokenizer.Config{
			EnableStemming:  true,
			EnableStopWords: true,
		},
		Path: path,
	}
}

func GetEngine[Schema SchemaProps](key, path string) (*Engine[Schema], error) {
	eng, _ := engines.Get(key)
	if eng != nil {
		return eng.(*Engine[Schema]), nil
	}
	config := GetConfig(key, path)
	eng, err := New[Schema](config)
	if err != nil {
		return nil, err
	}
	engines.Set(key, eng)
	return eng.(*Engine[Schema]), nil
}

func AddEngine(key string, engine any) {
	engines.Set(key, engine)
}
