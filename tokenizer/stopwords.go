package tokenizer

import (
	stopwords2 "github.com/oarkflow/search/tokenizer/stopwords"
)

type StopWords map[string]struct{}

var stopWords = map[Language]StopWords{
	ENGLISH:   stopwords2.English,
	FRENCH:    stopwords2.French,
	HUNGARIAN: stopwords2.Hungarian,
	NORWEGIAN: stopwords2.Norwegian,
	RUSSIAN:   stopwords2.Russian,
	SPANISH:   stopwords2.Spanish,
	SWEDISH:   stopwords2.Swedish,
}
