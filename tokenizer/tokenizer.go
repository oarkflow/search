package tokenizer

import (
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/snowball"
)

const (
	ENGLISH Language = "en"
)

var splitRules = map[Language]*regexp.Regexp{
	ENGLISH: regexp.MustCompile(`[^A-Za-zàèéìòóù0-9_'-:.]`),
}

var normalizer = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)

type Language string

type Config struct {
	EnableStemming  bool
	EnableStopWords bool
}

type TokenizeParams struct {
	Text            string
	Language        Language
	AllowDuplicates bool
}

type normalizeParams struct {
	token    string
	language Language
}

func IsSupportedLanguage(language Language) bool {
	_, ok := splitRules[language]
	return ok
}

var separators = map[byte]struct{}{
	' ': {},
	'.': {},
	',': {},
	';': {},
	'!': {},
	'?': {},
	'-': {},
}

func splitSentence(text string) []string {
	textLen := len(text)
	words := make([]string, 0, strings.Count(text, " ")+1)
	start := 0
	for i := 0; i < textLen; i++ {
		if _, exists := separators[text[i]]; exists {
			if start < i {
				words = append(words, text[start:i])
			}
			start = i + 1
		}
	}
	if start < textLen {
		words = append(words, text[start:])
	}
	return words
}

func Tokenize(params TokenizeParams, config Config) (map[string]int, error) {
	tokens := tokenize(lib.ToLower(params.Text))

	for token := range tokens {
		stemmed := Stem(token)
		if stemmed != token {
			tokens[stemmed] += tokens[token]
			delete(tokens, token)
		}
	}

	FilterStopWords(tokens, stopWords[params.Language])
	return tokens, nil
}

func tokenize(input string) map[string]int {
	tokenCounts := map[string]int{}
	start := -1
	for i, r := range input {
		if unicode.IsLetter(r) {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 {
				token := input[start:i]
				tokenCounts[token]++
				start = -1
			}
		}
	}
	if start != -1 {
		token := input[start:]
		tokenCounts[token]++
	}
	return tokenCounts
}

// Simplified Porter Stemmer
func Stem(word string) string {
	// Implement a simple version of the Porter Stemmer rules
	if len(word) <= 3 {
		return word
	}

	switch {
	case len(word) > 4 && word[len(word)-4:] == "sses":
		word = word[:len(word)-2]
	case len(word) > 3 && word[len(word)-3:] == "ies":
		word = word[:len(word)-2]
	case len(word) > 2 && word[len(word)-2:] == "ss":
		// Do nothing
	case len(word) > 1 && word[len(word)-1:] == "s":
		word = word[:len(word)-1]
	}

	return word
}

// FilterStopWords filters out stop words from the token map
func FilterStopWords(tokens map[string]int, stopWords map[string]struct{}) {
	for token := range tokens {
		if _, found := stopWords[token]; found {
			delete(tokens, token)
		}
	}
}
func normalizeToken(params normalizeParams, config Config) string {
	token := params.token
	if config.EnableStopWords {
		if _, ok := stopWords[params.language][token]; ok {
			return ""
		}
	}
	if config.EnableStemming {
		// return english.Stem(params.token, false)
		token = lib.FromByte(snowball.Stem(lib.ToByte(token)))
	}
	/*if normToken, _, err := transform.String(normalizer, token); err == nil {
		return normToken
	}*/
	return token
}
