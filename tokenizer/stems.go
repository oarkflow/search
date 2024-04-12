package tokenizer

import (
	"github.com/oarkflow/search/snowball/english"
)

type Stem func(string, bool) string

var stems = map[Language]Stem{
	ENGLISH: english.Stem,
}
