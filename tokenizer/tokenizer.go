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

func Tokenize(params *TokenizeParams, config *Config) ([]string, error) {
	splitRule, ok := splitRules[params.Language]
	if !ok {
		return nil, LanguageNotSupported
	}

	params.Text = strings.ToLower(params.Text)
	splitText := splitRule.Split(params.Text, -1)
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
