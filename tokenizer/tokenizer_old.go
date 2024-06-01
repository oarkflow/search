package tokenizer

/*
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
		if separators[text[i]] {
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
	tokens := make(map[string]int) // Pre-allocate map size
	for _, token := range splitSentence(lib.ToLower(params.Text)) {
		if normToken := normalizeToken(normalizeParams{token: token, language: params.Language}, config); normToken != "" {
			if _, ok := tokens[normToken]; (!ok && !params.AllowDuplicates) || params.AllowDuplicates {
				tokens[normToken]++
			}
		}
	}
	return tokens, nil
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
	// if normToken, _, err := transform.String(normalizer, token); err == nil {
	// 	  return normToken
	// }
	return token
}
*/