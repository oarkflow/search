package tokenizer

import (
	stopwords2 "github.com/oarkflow/search/tokenizer/stopwords"
)

type StopWords map[string]struct{}

var stopWords = map[Language]StopWords{
	ENGLISH: stopwords2.English,
}
