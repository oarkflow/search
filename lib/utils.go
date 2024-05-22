package lib

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/exp/constraints"
)

func IsEqual(dataVal, val any) bool {
	switch val := val.(type) {
	case string:
		switch gtVal := dataVal.(type) {
		case string:
			return strings.EqualFold(val, gtVal)
		case []string:
			return slices.Contains(gtVal, val)
		case []any:
			return slices.Contains(gtVal, any(val))
		default:
			gtVal1 := fmt.Sprint(gtVal)
			return strings.EqualFold(val, gtVal1)
		}
	case int:
		switch gtVal := dataVal.(type) {
		case int:
			return val == gtVal
		case uint:
			return val == int(gtVal)
		case float64:
			return float64(val) == gtVal
		case []any:
			return slices.Contains(gtVal, any(val))
		case string:
			v, err := strconv.Atoi(gtVal)
			if err != nil {
				return false
			}
			return val == v
		}
		return false
	case float64:
		switch gtVal := dataVal.(type) {
		case int:
			return val == float64(gtVal)
		case uint:
			return val == float64(gtVal)
		case float64:
			return val == gtVal
		case []any:
			return slices.Contains(gtVal, any(val))
		case string:
			v, err := strconv.ParseFloat(gtVal, 32)
			if err != nil {
				return false
			}
			return val == v
		}
		return false
	case bool:
		switch gtVal := dataVal.(type) {
		case bool:
			return val == gtVal
		case []any:
			return slices.Contains(gtVal, any(val))
		case string:
			v, err := strconv.ParseBool(gtVal)
			if err != nil {
				return false
			}
			return val == v
		}
		return false
	default:
		dataVal1 := fmt.Sprint(dataVal)
		val1 := fmt.Sprint(val)
		return strings.EqualFold(dataVal1, val1)
	}
}

// IntersectionOld computes the list of values that are the intersection of all the slices.
// Each value in the result should be present in each of the provided slices.
func IntersectionOld[T comparable](p ...[]T) []T {
	// sort.Slice(p, func(i, j int) bool { return len(p[i]) < len(p[j]) })
	var rs []T
	pLen := len(p)
	if pLen == 0 {
		return rs
	}
	if pLen == 1 {
		return p[0]
	}
	first := p[0]
	rest := p[1:]
	rLen := len(rest)
	for _, f := range first {
		j := 0
		for _, rs := range rest {
			if !slices.Contains(rs, f) {
				break
			}
			j++
		}
		if j == rLen {
			rs = append(rs, f)
		}
	}

	return rs
}

func Intersection[T constraints.Ordered](pS ...[]T) []T {
	hash := make(map[T]*int) // value, counter
	result := make([]T, 0)
	for _, slice := range pS {
		duplicationHash := make(map[T]struct{}) // duplication checking for individual slice
		for _, value := range slice {
			_, isDup := duplicationHash[value]
			if isDup {
				continue
			}
			if counter := hash[value]; counter != nil { // is found in hash counter map
				if *counter++; *counter >= len(pS) { // is found in every slice
					result = append(result, value)
				}
			} else { // not found in hash counter map
				i := 1
				hash[value] = &i
			}
			duplicationHash[value] = struct{}{}
		}
	}
	return result
}

// ToByte converts a string to a byte slice without memory allocation.
// NOTE: The returned byte slice MUST NOT be modified since it shares the same backing array
// with the given string.
func ToByte(s string) []byte {
	p := unsafe.StringData(s)
	b := unsafe.Slice(p, len(s))
	return b
}

// FromByte converts bytes to a string without memory allocation.
// NOTE: The given bytes MUST NOT be modified since they share the same backing array
// with the returned string.
func FromByte(b []byte) string {
	// Ignore if your IDE shows an error here; it's a false positive.
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}
