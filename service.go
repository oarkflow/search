package search

import (
	"github.com/oarkflow/maps"

	"github.com/oarkflow/search/tokenizer"
)

var engines maps.IMap[string, any]

func init() {
	engines = maps.New[string, any]()
}

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
	config := GetConfig(key)
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
