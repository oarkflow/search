package tokenizer

import (
	"errors"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
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

func splitSentence(text string) []string {
	// Define separators (space, punctuation)
	separators := map[byte]bool{
		' ': true,
		'.': true,
		',': true,
		';': true,
		'!': true,
		'?': true,
		'-': true, // Optional for hyphenated words
		// Add more separators as needed
	}

	var words []string
	start := 0

	for i := 0; i < len(text); i++ {
		if isSeparator(text[i], separators) {
			// Found a separator, append the current word
			if start < i {
				words = append(words, text[start:i])
			}
			start = i + 1
		}
	}

	// Append the last word if it exists
	if start < len(text) {
		words = append(words, text[start:])
	}

	return words
}

func isSeparator(char byte, separators map[byte]bool) bool {
	_, ok := separators[char]
	return ok
}

func Tokenize(params *TokenizeParams, config *Config) ([]string, error) {
	/*splitRule, ok := splitRules[params.Language]
	if !ok {
		return nil, LanguageNotSupported
	}*/

	// Lowercase and split text in one step
	lowerText := strings.ToLower(params.Text)
	// splitText := splitRule.Split(lowerText, -1)
	splitText := splitSentence(lowerText)
	tokens := make([]string, 0, len(splitText))               // Pre-allocate based on split length (optional)
	uniqueTokens := make(map[string]struct{}, len(splitText)) // Pre-allocate map size (optional)

	for _, token := range splitText {
		normParams := normalizeParams{
			token:    token,
			language: params.Language,
		}
		normToken := normalizeToken(&normParams, config)
		if normToken != "" {
			if _, ok := uniqueTokens[normToken]; !ok || params.AllowDuplicates {
				uniqueTokens[normToken] = struct{}{}
				tokens = append(tokens, normToken)
			}
		}
	}

	return tokens, nil
}

func normalizeToken(params *normalizeParams, config *Config) string {
	token := params.token

	if _, ok := stopWords[params.language][token]; config.EnableStopWords && ok {
		return ""
	}

	if stem, ok := stems[params.language]; config.EnableStemming && ok {
		token = stem(token, false)
	}

	/*if normToken, _, err := transform.String(normalizer, token); err == nil {
		return normToken
	}*/

	return token
}
