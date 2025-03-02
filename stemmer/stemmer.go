package stemmer

import (
	"unicode"
)

func isConsonant(s []rune, i int) bool {
	var result bool
	switch s[i] {
	case 'a', 'e', 'i', 'o', 'u':
		result = false
	case 'y':
		if 0 == i {
			result = true
		} else {
			result = !isConsonant(s, i-1)
		}
	default:
		result = true
	}
	return result
}

func measure(s []rune) uint {
	lenS := len(s)
	result := uint(0)
	i := 0
	if 0 == lenS {
		return result
	}
	for isConsonant(s, i) {
		i++
		if i >= lenS {
			return result
		}
	}
Outer:
	for i < lenS {
		for !isConsonant(s, i) {
			i++
			if i >= lenS {
				break Outer
			}
		}
		for isConsonant(s, i) {
			i++
			if i >= lenS {
				result++
				break Outer
			}
		}
		result++
	}
	return result
}

func hasSuffix(s, suffix []rune) bool {
	lenSMinusOne := len(s) - 1
	lenSuffixMinusOne := len(suffix) - 1
	if lenSMinusOne <= lenSuffixMinusOne {
		return false
	} else if s[lenSMinusOne] != suffix[lenSuffixMinusOne] {
		return false
	}
	for i := 0; i < lenSuffixMinusOne; i++ {
		if suffix[i] != s[lenSMinusOne-lenSuffixMinusOne+i] {
			return false
		}
	}
	return true
}

func containsVowel(s []rune) bool {
	lenS := len(s)
	for i := 0; i < lenS; i++ {
		if !isConsonant(s, i) {
			return true
		}
	}
	return false
}

func hasRepeatDoubleConsonantSuffix(s []rune) bool {
	lenS := len(s)
	result := false
	if 2 > lenS {
		result = false
	} else if s[lenS-1] == s[lenS-2] && isConsonant(s, lenS-1) {
		result = true
	} else {
		result = false
	}
	return result
}

func hasConsonantVowelConsonantSuffix(s []rune) bool {
	lenS := len(s)
	result := false
	if 3 > lenS {
		result = false
	} else if isConsonant(s, lenS-3) && !isConsonant(s, lenS-2) && isConsonant(s, lenS-1) {
		result = true
	} else {
		result = false
	}
	return result
}

func step1a(s []rune) []rune {
	var result = s
	lenS := len(s)
	if suffix := []rune("sses"); hasSuffix(s, suffix) {
		lenTrim := 2
		subSlice := s[:lenS-lenTrim]
		result = subSlice
	} else if suffix := []rune("ies"); hasSuffix(s, suffix) {
		lenTrim := 2
		subSlice := s[:lenS-lenTrim]
		result = subSlice
	} else if suffix := []rune("ss"); hasSuffix(s, suffix) {
		result = s
	} else if suffix := []rune("s"); hasSuffix(s, suffix) {
		lenSuffix := 1
		subSlice := s[:lenS-lenSuffix]
		result = subSlice
	}
	return result
}

func step1b(s []rune) []rune {
	var result = s
	lenS := len(s)
	if suffix := []rune("eed"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 0 < m {
			lenTrim := 1
			result = s[:lenS-lenTrim]
		}
	} else if suffix := []rune("ed"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		if containsVowel(subSlice) {
			if suffix2 := []rune("at"); hasSuffix(subSlice, suffix2) {
				lenTrim := -1
				result = s[:lenS-lenSuffix-lenTrim]
			} else if suffix2 := []rune("bl"); hasSuffix(subSlice, suffix2) {
				lenTrim := -1
				result = s[:lenS-lenSuffix-lenTrim]
			} else if suffix2 := []rune("iz"); hasSuffix(subSlice, suffix2) {
				lenTrim := -1
				result = s[:lenS-lenSuffix-lenTrim]
			} else if c := subSlice[len(subSlice)-1]; 'l' != c && 's' != c && 'z' != c && hasRepeatDoubleConsonantSuffix(subSlice) {
				lenTrim := 1
				lenSubSlice := len(subSlice)
				result = subSlice[:lenSubSlice-lenTrim]
			} else if c := subSlice[len(subSlice)-1]; 1 == measure(subSlice) && hasConsonantVowelConsonantSuffix(subSlice) && 'w' != c && 'x' != c && 'y' != c {
				lenTrim := -1
				result = s[:lenS-lenSuffix-lenTrim]
				result[len(result)-1] = 'e'
			} else {
				result = subSlice
			}
		}
	} else if suffix := []rune("ing"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		if containsVowel(subSlice) {
			if suffix2 := []rune("at"); hasSuffix(subSlice, suffix2) {
				lenTrim := -1
				result = s[:lenS-lenSuffix-lenTrim]
				result[len(result)-1] = 'e'
			} else if suffix2 := []rune("bl"); hasSuffix(subSlice, suffix2) {
				lenTrim := -1
				result = s[:lenS-lenSuffix-lenTrim]
				result[len(result)-1] = 'e'
			} else if suffix2 := []rune("iz"); hasSuffix(subSlice, suffix2) {
				lenTrim := -1
				result = s[:lenS-lenSuffix-lenTrim]
				result[len(result)-1] = 'e'
			} else if c := subSlice[len(subSlice)-1]; 'l' != c && 's' != c && 'z' != c && hasRepeatDoubleConsonantSuffix(subSlice) {
				lenTrim := 1
				lenSubSlice := len(subSlice)
				result = subSlice[:lenSubSlice-lenTrim]
			} else if c := subSlice[len(subSlice)-1]; 1 == measure(subSlice) && hasConsonantVowelConsonantSuffix(subSlice) && 'w' != c && 'x' != c && 'y' != c {
				lenTrim := -1
				result = s[:lenS-lenSuffix-lenTrim]
				result[len(result)-1] = 'e'
			} else {
				result = subSlice
			}
		}
	}
	return result
}

func step1c(s []rune) []rune {
	lenS := len(s)
	result := s
	if 2 > lenS {
		return result
	}
	if 'y' == s[lenS-1] && containsVowel(s[:lenS-1]) {
		result[lenS-1] = 'i'
	} else if 'Y' == s[lenS-1] && containsVowel(s[:lenS-1]) {
		result[lenS-1] = 'I'
	}
	return result
}

func step2(s []rune) []rune {
	lenS := len(s)
	result := s
	if suffix := []rune("ational"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-5] = 'e'
			result = result[:lenS-4]
		}
	} else if suffix := []rune("tional"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = result[:lenS-2]
		}
	} else if suffix := []rune("enci"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-1] = 'e'
		}
	} else if suffix := []rune("anci"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-1] = 'e'
		}
	} else if suffix := []rune("izer"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-1]
		}
	} else if suffix := []rune("bli"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-1] = 'e'
		}
	} else if suffix := []rune("alli"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-2]
		}
	} else if suffix := []rune("entli"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-2]
		}
	} else if suffix := []rune("eli"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-2]
		}
	} else if suffix := []rune("ousli"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-2]
		}
	} else if suffix := []rune("ization"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-5] = 'e'

			result = s[:lenS-4]
		}
	} else if suffix := []rune("ation"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-3] = 'e'

			result = s[:lenS-2]
		}
	} else if suffix := []rune("ator"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-2] = 'e'

			result = s[:lenS-1]
		}
	} else if suffix := []rune("alism"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-3]
		}
	} else if suffix := []rune("iveness"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-4]
		}
	} else if suffix := []rune("fulness"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-4]
		}
	} else if suffix := []rune("ousness"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-4]
		}
	} else if suffix := []rune("aliti"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result = s[:lenS-3]
		}
	} else if suffix := []rune("iviti"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-3] = 'e'

			result = result[:lenS-2]
		}
	} else if suffix := []rune("biliti"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			result[lenS-5] = 'l'
			result[lenS-4] = 'e'

			result = result[:lenS-3]
		}
	} else if suffix := []rune("logi"); hasSuffix(s, suffix) {
		if 0 < measure(s[:lenS-len(suffix)]) {
			lenTrim := 1

			result = s[:lenS-lenTrim]
		}
	}
	return result
}

func step3(s []rune) []rune {
	lenS := len(s)
	result := s
	if suffix := []rune("icate"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		if 0 < measure(s[:lenS-lenSuffix]) {
			result = result[:lenS-3]
		}
	} else if suffix := []rune("ative"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 0 < m {
			result = subSlice
		}
	} else if suffix := []rune("alize"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		if 0 < measure(s[:lenS-lenSuffix]) {
			result = result[:lenS-3]
		}
	} else if suffix := []rune("iciti"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		if 0 < measure(s[:lenS-lenSuffix]) {
			result = result[:lenS-3]
		}
	} else if suffix := []rune("ical"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		if 0 < measure(s[:lenS-lenSuffix]) {
			result = result[:lenS-2]
		}
	} else if suffix := []rune("ful"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 0 < m {
			result = subSlice
		}
	} else if suffix := []rune("ness"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 0 < m {
			result = subSlice
		}
	}
	return result
}

func step4(s []rune) []rune {
	lenS := len(s)
	result := s
	if suffix := []rune("al"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = result[:lenS-lenSuffix]
		}
	} else if suffix := []rune("ance"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = result[:lenS-lenSuffix]
		}
	} else if suffix := []rune("ence"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = result[:lenS-lenSuffix]
		}
	} else if suffix := []rune("er"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ic"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("able"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ible"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ant"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ement"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ment"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ent"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ion"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		c := subSlice[len(subSlice)-1]
		if 1 < m && ('s' == c || 't' == c) {
			result = subSlice
		}
	} else if suffix := []rune("ou"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ism"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ate"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("iti"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ous"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ive"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	} else if suffix := []rune("ize"); hasSuffix(s, suffix) {
		lenSuffix := len(suffix)
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	}
	return result
}

func step5a(s []rune) []rune {
	lenS := len(s)
	result := s
	if 'e' == s[lenS-1] {
		lenSuffix := 1
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		} else if 1 == m {
			if c := subSlice[len(subSlice)-1]; !(hasConsonantVowelConsonantSuffix(subSlice) && 'w' != c && 'x' != c && 'y' != c) {
				result = subSlice
			}
		}
	}
	return result
}

func step5b(s []rune) []rune {
	lenS := len(s)
	result := s
	if 2 < lenS && 'l' == s[lenS-2] && 'l' == s[lenS-1] {
		lenSuffix := 1
		subSlice := s[:lenS-lenSuffix]
		m := measure(subSlice)
		if 1 < m {
			result = subSlice
		}
	}
	return result
}

func StemString(s string) string {
	runeArr := []rune(s)
	runeArr = Stem(runeArr)
	return string(runeArr)
}

func Stem(s []rune) []rune {
	lenS := len(s)
	if 0 == lenS {
		return s
	}
	for i := 0; i < lenS; i++ {
		s[i] = unicode.ToLower(s[i])
	}
	return StemWithoutLowerCasing(s)
}

func StemWithoutLowerCasing(s []rune) []rune {
	lenS := len(s)
	if 2 >= lenS {
		return s
	}
	s = step1a(s)
	s = step1b(s)
	s = step1c(s)
	s = step2(s)
	s = step3(s)
	s = step4(s)
	s = step5a(s)
	s = step5b(s)
	return s
}
