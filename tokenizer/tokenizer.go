package tokenizer

import (
	"errors"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/snowball/english"
)

const (
	ENGLISH Language = "en"
)

var Languages = []Language{ENGLISH}

var splitRules = map[Language]*regexp.Regexp{
	ENGLISH: regexp.MustCompile(`[^A-Za-zàèéìòóù0-9_'-:.]`),
}

var normalizer = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)

var (
	LanguageNotSupported = errors.New("language not supported")
)

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

var separators = map[byte]bool{
	' ': true,
	'.': true,
	',': true,
	';': true,
	'!': true,
	'?': true,
	'-': true,
}

func splitSentence(text string) []string {
	textLen := len(text)
	words := make([]string, 0, strings.Count(text, " ")+1)
	start := 0
	for i := 0; i < textLen; i++ {
		char := text[i]
		if separators[char] {
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

func Tokenize(params *TokenizeParams, config *Config) ([]string, error) {
	params.Text = lib.ToLower(params.Text)
	splitText := splitSentence(params.Text)
	tokens := make([]string, 0)
	uniqueTokens := make(map[string]struct{})
	for _, token := range splitText {
		normParams := normalizeParams{
			token:    token,
			language: params.Language,
		}
		if normToken := normalizeToken(&normParams, config); normToken != "" {
			if _, ok := uniqueTokens[normToken]; (!ok && !params.AllowDuplicates) || params.AllowDuplicates {
				uniqueTokens[normToken] = struct{}{}
				tokens = append(tokens, normToken)
			}
		}
	}

	return tokens, nil
}

func normalizeToken(params *normalizeParams, config *Config) string {
	if _, ok := stopWords[params.language][params.token]; config.EnableStopWords && ok {
		return ""
	}
	if config.EnableStemming {
		return english.Stem(params.token, false)
	}
	return params.token
}
