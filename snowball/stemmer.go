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
	if hasSuffix(body, lib.ToByte("sses")) || hasSuffix(body, lib.ToByte("ies")) {
		return body[:len(body)-2]
	} else if hasSuffix(body, lib.ToByte("ss")) {
		return body
	} else if hasSuffix(body, lib.ToByte("s")) {
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
	if hasSuffix(body, lib.ToByte("at")) {
		return append(body, 'e')
	} else if hasSuffix(body, lib.ToByte("bl")) {
		return append(body, 'e')
	} else if hasSuffix(body, lib.ToByte("iz")) {
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
	if hasSuffix(body, lib.ToByte("eed")) {
		if Measure(body[:len(body)-3]) > 0 {
			return body[:len(body)-1]
		}
	} else if hasSuffix(body, lib.ToByte("ed")) {
		if hasVowel(body[:len(body)-2]) {
			return oneBA(body[:len(body)-2])
		}
	} else if hasSuffix(body, lib.ToByte("ing")) {
		if hasVowel(body[:len(body)-3]) {
			return oneBA(body[:len(body)-3])
		}
	}
	return body
}

func oneC(body []byte) []byte {
	if hasSuffix(body, lib.ToByte("y")) && hasVowel(body[:len(body)-1]) {
		body[len(body)-1] = 'i'
		return body
	}
	return body
}

func two(body []byte) []byte {
	if hasSuffix(body, lib.ToByte("ational")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], lib.ToByte("ate")...)
		}
	} else if hasSuffix(body, lib.ToByte("tional")) {
		if Measure(body[:len(body)-6]) > 0 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, lib.ToByte("enci")) || hasSuffix(body, lib.ToByte("anci")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-1], 'e')
		}
	} else if hasSuffix(body, lib.ToByte("izer")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-4], lib.ToByte("ize")...)
		}
	} else if hasSuffix(body, lib.ToByte("abli")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-4], lib.ToByte("able")...)
		}
		// To match the published algorithm, delete the following phrase
	} else if hasSuffix(body, lib.ToByte("bli")) {
		if Measure(body[:len(body)-3]) > 0 {
			return append(body[:len(body)-1], 'e')
		}
	} else if hasSuffix(body, lib.ToByte("alli")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-4], lib.ToByte("al")...)
		}
	} else if hasSuffix(body, lib.ToByte("entli")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], lib.ToByte("ent")...)
		}
	} else if hasSuffix(body, lib.ToByte("eli")) {
		if Measure(body[:len(body)-3]) > 0 {
			return append(body[:len(body)-3], lib.ToByte("e")...)
		}
	} else if hasSuffix(body, lib.ToByte("ousli")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], lib.ToByte("ous")...)
		}
	} else if hasSuffix(body, lib.ToByte("ization")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], lib.ToByte("ize")...)
		}
	} else if hasSuffix(body, lib.ToByte("ation")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], lib.ToByte("ate")...)
		}
	} else if hasSuffix(body, lib.ToByte("ator")) {
		if Measure(body[:len(body)-4]) > 0 {
			return append(body[:len(body)-4], lib.ToByte("ate")...)
		}
	} else if hasSuffix(body, lib.ToByte("alism")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], lib.ToByte("al")...)
		}
	} else if hasSuffix(body, lib.ToByte("iveness")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], lib.ToByte("ive")...)
		}
	} else if hasSuffix(body, lib.ToByte("fulness")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], lib.ToByte("ful")...)
		}
	} else if hasSuffix(body, lib.ToByte("ousness")) {
		if Measure(body[:len(body)-7]) > 0 {
			return append(body[:len(body)-7], lib.ToByte("ous")...)
		}
	} else if hasSuffix(body, lib.ToByte("aliti")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], lib.ToByte("al")...)
		}
	} else if hasSuffix(body, lib.ToByte("iviti")) {
		if Measure(body[:len(body)-5]) > 0 {
			return append(body[:len(body)-5], lib.ToByte("ive")...)
		}
	} else if hasSuffix(body, lib.ToByte("biliti")) {
		if Measure(body[:len(body)-6]) > 0 {
			return append(body[:len(body)-6], lib.ToByte("ble")...)
		}
		// To match the published algorithm, delete the following phrase
	} else if hasSuffix(body, lib.ToByte("logi")) {
		if Measure(body[:len(body)-4]) > 0 {
			return body[:len(body)-1]
		}
	}
	return body
}

func three(body []byte) []byte {
	if hasSuffix(body, lib.ToByte("icate")) {
		if Measure(body[:len(body)-5]) > 0 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ative")) {
		if Measure(body[:len(body)-5]) > 0 {
			return body[:len(body)-5]
		}
	} else if hasSuffix(body, lib.ToByte("alize")) {
		if Measure(body[:len(body)-5]) > 0 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("iciti")) {
		if Measure(body[:len(body)-5]) > 0 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ical")) {
		if Measure(body[:len(body)-4]) > 0 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, lib.ToByte("ful")) {
		if Measure(body[:len(body)-3]) > 0 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ness")) {
		if Measure(body[:len(body)-4]) > 0 {
			return body[:len(body)-4]
		}
	}
	return body
}

func four(body []byte) []byte {
	if hasSuffix(body, lib.ToByte("al")) {
		if Measure(body[:len(body)-2]) > 1 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, lib.ToByte("ance")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, lib.ToByte("ence")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, lib.ToByte("er")) {
		if Measure(body[:len(body)-2]) > 1 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, lib.ToByte("ic")) {
		if Measure(body[:len(body)-2]) > 1 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, lib.ToByte("able")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, lib.ToByte("ible")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, lib.ToByte("ant")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ement")) {
		if Measure(body[:len(body)-5]) > 1 {
			return body[:len(body)-5]
		}
	} else if hasSuffix(body, lib.ToByte("ment")) {
		if Measure(body[:len(body)-4]) > 1 {
			return body[:len(body)-4]
		}
	} else if hasSuffix(body, lib.ToByte("ent")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ion")) {
		if Measure(body[:len(body)-3]) > 1 {
			if len(body) > 4 && (body[len(body)-4] == 's' || body[len(body)-4] == 't') {
				return body[:len(body)-3]
			}
		}
	} else if hasSuffix(body, lib.ToByte("ou")) {
		if Measure(body[:len(body)-2]) > 1 {
			return body[:len(body)-2]
		}
	} else if hasSuffix(body, lib.ToByte("ism")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ate")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("iti")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ous")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ive")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	} else if hasSuffix(body, lib.ToByte("ize")) {
		if Measure(body[:len(body)-3]) > 1 {
			return body[:len(body)-3]
		}
	}
	return body
}

func fiveA(body []byte) []byte {
	if hasSuffix(body, lib.ToByte("e")) && Measure(body[:len(body)-1]) > 1 {
		return body[:len(body)-1]
	} else if hasSuffix(body, lib.ToByte("e")) && Measure(body[:len(body)-1]) == 1 && !starO(body[:len(body)-1]) {
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
