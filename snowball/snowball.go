package snowball

import (
	"fmt"

	"github.com/kljensen/snowball/english"
)

const (
	VERSION string = "v0.7.0"
)

// Stem a word in the specified language.
func Stem(word, language string, stemStopWords bool) (stemmed string, err error) {

	var f func(string, bool) string
	switch language {
	case "english":
		f = english.Stem
	default:
		err = fmt.Errorf("Unknown language: %s", language)
		return
	}
	stemmed = f(word, stemStopWords)
	return

}
