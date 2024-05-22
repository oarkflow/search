package snowball

import (
	"bytes"

	"github.com/oarkflow/search/lib"
)

func Consonant(body []byte, offset int) bool {
	switch body[offset] {
	case 'A', 'E', 'I', 'O', 'U', 'a', 'e', 'i', 'o', 'u':
		return false
	case 'Y', 'y':
		if offset == 0 {
			return true
		}
		return offset > 0 && !Consonant(body, offset-1)
	}
	return true
}

func Vowel(body []byte, offset int) bool {
	return !Consonant(body, offset)
}

const (
	vowelState = iota
	consonantState
)

func Measure(body []byte) int {
	measure := 0
	if len(body) > 0 {
		var state int
		if Vowel(body, 0) {
			state = vowelState
		} else {
			state = consonantState
		}
		for i := 0; i < len(body); i++ {
			if Vowel(body, i) && state == consonantState {
				state = vowelState
			} else if Consonant(body, i) && state == vowelState {
				state = consonantState
				measure++
			}
		}
	}
	return measure
}

func hasVowel(body []byte) bool {
	for i := 0; i < len(body); i++ {
		if Vowel(body, i) {
			return true
		}
	}
	return false
}

func oneA(body []byte) []byte {
	if hasSuffix(body, []byte("sses")) || hasSuffix(body, []byte("ies")) {
		return body[:len(body)-2]
	} else if hasSuffix(body, []byte("ss")) {
		return body
	} else if hasSuffix(body, []byte("s")) {
		return body[:len(body)-1]
	}
	return body
}

func starO(body []byte) bool {
	size := len(body) - 1
	if size >= 2 && Consonant(body, size-2) && Vowel(body, size-1) && Consonant(body, size) {
		return body[size] != 'w' && body[size] != 'x' && body[size] != 'y'
	}
	return false
}
func oneBA(body []byte) []byte {

	size := len(body)
	if hasSuffix(body, []byte("at")) {
		return append(body, 'e')
	} else if hasSuffix(body, []byte("bl")) {
		return append(body, 'e')
	} else if hasSuffix(body, []byte("iz")) {
		return append(body, 'e')
	} else if Consonant(body, size-1) && Consonant(body, size-2) && body[size-1] == body[size-2] {
		if body[size-1] != 'l' && body[size-1] != 's' && body[size-1] != 'z' {
			return body[:size-1]
		}
	} else if starO(body) && Measure(body) == 1 {
		return append(body, 'e')
	}
	return body
}

func oneB(body []byte) []byte {
	if hasSuffix(body, []byte("eed")) {
		if Measure(body[:len(body)-3]) > 0 {
			return body[:len(body)-1]
		}
	} else if hasSuffix(body, []byte("ed")) {
		if hasVowel(body[:len(body)-2]) {
			return oneBA(body[:len(body)-2])
		}
	} else if hasSuffix(body, []byte("ing")) {
		if hasVowel(body[:len(body)-3]) {
			return oneBA(body[:len(body)-3])
		}
	}
	return body
}

func oneC(body []byte) []byte {
	if hasSuffix(body, []byte("y")) && hasVowel(body[:len(body)-1]) {
		body[len(body)-1] = 'i'
		return body
	}
	return body
}

func two(body []byte) []byte {
	if hasSuffix(body, []byte("ational")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], []byte("ate")...)
		}
	} else if hasSuffix(body, []byte("tional")) {
		if Measure(body[:len(body)-6]) > 0 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, []byte("enci")) || hasSuffix(body, []byte("anci")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-1], 'e')
		}
	} else if hasSuffix(body, []byte("izer")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-4], []byte("ize")...)
		}
	} else if hasSuffix(body, []byte("abli")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-4], []byte("able")...)
		}
		// To match the published algorithm, delete the following phrase
	} else if hasSuffix(body, []byte("bli")) {
		if Measure(body[:len(body)-3]) > 0 {
			return append(body[:len(body)-1], 'e')
		}
	} else if hasSuffix(body, []byte("alli")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-4], []byte("al")...)
		}
	} else if hasSuffix(body, []byte("entli")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], []byte("ent")...)
		}
	} else if hasSuffix(body, []byte("eli")) {
		if Measure(body[:len(body)-3]) > 0 {
			return append(body[:len(body)-3], []byte("e")...)
		}
	} else if hasSuffix(body, []byte("ousli")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], []byte("ous")...)
		}
	} else if hasSuffix(body, []byte("ization")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], []byte("ize")...)
		}
	} else if hasSuffix(body, []byte("ation")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], []byte("ate")...)
		}
	} else if hasSuffix(body, []byte("ator")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-4], []byte("ate")...)
		}
	} else if hasSuffix(body, []byte("alism")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], []byte("al")...)
		}
	} else if hasSuffix(body, []byte("iveness")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], []byte("ive")...)
		}
	} else if hasSuffix(body, []byte("fulness")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], []byte("ful")...)
		}
	} else if hasSuffix(body, []byte("ousness")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], []byte("ous")...)
		}
	} else if hasSuffix(body, []byte("aliti")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], []byte("al")...)
		}
	} else if hasSuffix(body, []byte("iviti")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], []byte("ive")...)
		}
	} else if hasSuffix(body, []byte("biliti")) {
		if Measure(body[:len(body)-6]) > 0 {
			return append(body[:len(body)-6], []byte("ble")...)
		}
		// To match the published algorithm, delete the following phrase
	} else if hasSuffix(body, []byte("logi")) {
		if Measure(body[:len(body)-4]) > 0 {
			return body[:len(body)-1]
		}
	}
	return body
}

func three(body []byte) []byte {
	if hasSuffix(body, []byte("icate")) {
		if Measure(body[:len(body)-5]) > 0 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ative")) {
		if Measure(body[:len(body)-5]) > 0 {
			return body[:len(body)-5]
		}
	} else if hasSuffix(body, []byte("alize")) {
		if Measure(body[:len(body)-5]) > 0 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("iciti")) {
		if Measure(body[:len(body)-5]) > 0 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ical")) {
		if Measure(body[:len(body)-4]) > 0 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, []byte("ful")) {
		if Measure(body[:len(body)-3]) > 0 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ness")) {
		if Measure(body[:len(body)-4]) > 0 {
			return body[:len(body)-4]
		}
	}
	return body
}

func four(body []byte) []byte {
	if hasSuffix(body, []byte("al")) {
		if Measure(body[:len(body)-2]) > 1 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, []byte("ance")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, []byte("ence")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, []byte("er")) {
		if Measure(body[:len(body)-2]) > 1 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, []byte("ic")) {
		if Measure(body[:len(body)-2]) > 1 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, []byte("able")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, []byte("ible")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, []byte("ant")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ement")) {
		if Measure(body[:len(body)-5]) > 1 {
			return body[:len(body)-5]
		}
	} else if hasSuffix(body, []byte("ment")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, []byte("ent")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ion")) {
		if Measure(body[:len(body)-3]) > 1 {
			if len(body) > 4 && (body[len(body)-4] == 's' || body[len(body)-4] == 't') {
				return body[:len(body)-3]
			}
		}
	} else if hasSuffix(body, []byte("ou")) {
		if Measure(body[:len(body)-2]) > 1 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, []byte("ism")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ate")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("iti")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ous")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ive")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, []byte("ize")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	}
	return body
}

func fiveA(body []byte) []byte {
	if hasSuffix(body, []byte("e")) && Measure(body[:len(body)-1]) > 1 {
		return body[:len(body)-1]
	} else if hasSuffix(body, []byte("e")) && Measure(body[:len(body)-1]) == 1 && !starO(body[:len(body)-1]) {
		return body[:len(body)-1]
	}
	return body
}

func fiveB(body []byte) []byte {
	size := len(body)
	if Measure(body) > 1 && Consonant(body, size-1) && Consonant(body, size-2) && body[size-1] == body[size-2] && body[size-1] == 'l' {
		return body[:len(body)-1]
	}
	return body
}

func Stem(body []byte) []byte {
	word := bytes.TrimSpace(lib.ToLowerBytes(body))
	if len(word) > 2 {
		return fiveB(fiveA(four(three(two(oneC(oneB(oneA(word))))))))
	}
	return word
}

func hasSuffix(body []byte, suffix []byte) bool {
	size := len(body)
	if size < len(suffix) {
		return false
	}
	for i := 0; i < len(suffix); i++ {
		if body[size-i-1] != suffix[len(suffix)-i-1] {
			return false
		}
	}
	return true
}
