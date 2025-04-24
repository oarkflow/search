package lib

import (
	"math"
	"strings"
	"unicode"
)

func ToTitleCase(s string) string {
	words := strings.FieldsFunc(s, func(r rune) bool {
		return r == '-' || r == '_' || unicode.IsSpace(r)
	})
	for i, word := range words {
		words[i] = strings.Title(strings.ToLower(word))
	}
	return strings.Join(words, " ")
}

type BM25Params struct {
	K float64
	B float64
}

func BM25V2(frequency float64, docLength int, avgDocLength float64, totalDocs int, docFreq int, params BM25Params) float64 {
	idf := 0.0
	if docFreq > 0 {
		idf = math.Log((float64(totalDocs)-float64(docFreq)+0.5)/(float64(docFreq)+0.5) + 1)
	}
	tf := (frequency * (params.K + 1)) / (frequency + params.K*(1.0-params.B+params.B*(float64(docLength)/avgDocLength)))
	return idf * tf
}

func BM25(tf float64, matchingDocsCount int, fieldLength int, avgFieldLength float64, docsCount int, k float64, b float64, d float64) float64 {
	idf := math.Log(1 + (float64(docsCount-matchingDocsCount)+0.5)/(float64(matchingDocsCount)+0.5))
	return idf * (d + tf*(k+1)) / (tf + k*(1-b+(b*float64(fieldLength))/avgFieldLength))
}

func Paginate(offset int, limit int, sliceLength int) (int, int) {
	if offset > sliceLength {
		offset = sliceLength
	}

	end := offset + limit
	if end > sliceLength {
		end = sliceLength
	}

	return offset, end
}

func CommonPrefix(a []rune, b []rune) ([]rune, bool) {
	lenA := len(a)
	lenB := len(b)
	minLength := lenA
	if lenB < lenA {
		minLength = lenB
	}

	var i int
	for i = 0; i < minLength; i++ {
		if a[i] != b[i] {
			break
		}
	}

	return a[:i], lenA == lenB && i == minLength
}

func BoundedLevenshtein(a []rune, b []rune, tolerance int) (int, bool) {
	distance := boundedLevenshtein(a, b, tolerance)
	return distance, distance >= 0
}

/**
 * Inspired by:
 * https://github.com/Yomguithereal/talisman/blob/86ae55cbd040ff021d05e282e0e6c71f2dde21f8/src/metrics/levenshtein.js#L218-L340
 */
func boundedLevenshtein(a []rune, b []rune, tolerance int) int {
	// the strings are the same
	if string(a) == string(b) {
		return 0
	}

	// a should be the shortest string
	if len(a) > len(b) {
		a, b = b, a
	}

	// ignore common suffix
	lenA, lenB := len(a), len(b)
	for lenA > 0 && a[lenA-1] == b[lenB-1] {
		lenA--
		lenB--
	}

	// early return when the smallest string is empty
	if lenA == 0 {
		if lenB > tolerance {
			return -1
		}
		return lenB
	}

	// ignore common prefix
	startIdx := 0
	for startIdx < lenA && a[startIdx] == b[startIdx] {
		startIdx++
	}
	lenA -= startIdx
	lenB -= startIdx

	// early return when the smallest string is empty
	if lenA == 0 {
		if lenB > tolerance {
			return -1
		}
		return lenB
	}

	delta := lenB - lenA

	if tolerance > lenB {
		tolerance = lenB
	} else if delta > tolerance {
		return -1
	}

	i := 0
	row := make([]int, lenB)
	characterCodeCache := make([]int, lenB)

	for i < tolerance {
		characterCodeCache[i] = int(b[startIdx+i])
		row[i] = i + 1
		i++
	}

	for i < lenB {
		characterCodeCache[i] = int(b[startIdx+i])
		row[i] = tolerance + 1
		i++
	}

	offset := tolerance - delta
	haveMax := tolerance < lenB

	jStart := 0
	jEnd := tolerance

	var current, left, above, charA, j int

	// Starting the nested loops
	for i := 0; i < lenA; i++ {
		left = i
		current = i + 1

		charA = int(a[startIdx+i])
		if i > offset {
			jStart = 1
		}
		if jEnd < lenB {
			jEnd++
		}

		for j = jStart; j < jEnd; j++ {
			above = current

			current = left
			left = row[j]

			if charA != characterCodeCache[j] {
				// insert current
				if left < current {
					current = left
				}

				// delete current
				if above < current {
					current = above
				}

				current++
			}

			row[j] = current
		}

		if haveMax && row[i+delta] > tolerance {
			return -1
		}
	}

	if current <= tolerance {
		return current
	}

	return -1
}

const (
	toLowerTable = "\x00\x01\x02\x03\x04\x05\x06\a\b\t\n\v\f\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\"#$%&'()*+,-./0123456789:;<=>?@abcdefghijklmnopqrstuvwxyz[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\u007f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff"
)

// ToLower is the equivalent of strings.ToLower
func ToLower(b string) string {
	res := make([]byte, len(b))
	copy(res, b)
	for i := 0; i < len(res); i++ {
		res[i] = toLowerTable[res[i]]
	}

	return FromByte(res)
}

func Unique[T comparable](slice []T) (result []T) {
	seen := make(map[T]struct{})
	for _, v := range slice {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	clear(seen)
	return result
}
