package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/exp/constraints"
)

func defaultCheck(dataVal, val any) bool {
	return fmt.Sprintf("%v", dataVal) == fmt.Sprintf("%v", val)
}

func ToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return string(rune(reflect.ValueOf(value).Int()))
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 64) // Adjust precision and bit size as needed
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64) // Adjust precision and bit size as needed
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return "nil"
	default:
		return reflect.TypeOf(value).String()
	}
}

func IsEqual(dataVal, val any) bool {
	if reflect.TypeOf(dataVal) == reflect.TypeOf(val) {
		return reflect.DeepEqual(dataVal, val)
	}

	switch v := val.(type) {
	case string:
		return compareString(dataVal, v)
	case int:
		return compareInt(dataVal, v)
	case int64:
		return compareInt64(dataVal, v)
	case float64:
		return compareFloat64(dataVal, v)
	case bool:
		return compareBool(dataVal, v)
	default:
		return defaultCheck(dataVal, val)
	}
}

func compareString(dataVal any, val string) bool {
	switch dataVal := dataVal.(type) {
	case string:
		return strings.EqualFold(dataVal, val)
	case []string:
		return slices.Contains(dataVal, val)
	case []any:
		return slices.Contains(dataVal, any(val))
	default:
		return defaultCheck(dataVal, val)
	}
}

func compareInt(dataVal any, val int) bool {
	switch dataVal := dataVal.(type) {
	case int:
		return val == dataVal
	case int64:
		return int64(val) == dataVal
	case uint:
		return val == int(dataVal)
	case float64:
		return float64(val) == dataVal
	case []any:
		return slices.Contains(dataVal, any(val))
	case string:
		if v, err := strconv.Atoi(dataVal); err == nil {
			return val == v
		}
	default:
		return defaultCheck(dataVal, val)
	}
	return false
}

func compareInt64(dataVal any, val int64) bool {
	switch dataVal := dataVal.(type) {
	case int:
		return val == int64(dataVal)
	case int64:
		return val == dataVal
	case uint:
		return val == int64(dataVal)
	case float64:
		return float64(val) == dataVal
	case []any:
		return slices.Contains(dataVal, any(val))
	case string:
		if v, err := strconv.ParseInt(dataVal, 10, 64); err == nil {
			return val == v
		}
	default:
		return defaultCheck(dataVal, val)
	}
	return false
}

func compareFloat64(dataVal any, val float64) bool {
	switch dataVal := dataVal.(type) {
	case int:
		return val == float64(dataVal)
	case int64:
		return val == float64(dataVal)
	case uint:
		return val == float64(dataVal)
	case float64:
		return val == dataVal
	case []any:
		return slices.Contains(dataVal, any(val))
	case string:
		if v, err := strconv.ParseFloat(dataVal, 64); err == nil {
			return val == v
		}
	default:
		return defaultCheck(dataVal, val)
	}
	return false
}

func compareBool(dataVal any, val bool) bool {
	switch dataVal := dataVal.(type) {
	case bool:
		return val == dataVal
	case []any:
		return slices.Contains(dataVal, any(val))
	case string:
		if v, err := strconv.ParseBool(dataVal); err == nil {
			return val == v
		}
	default:
		return defaultCheck(dataVal, val)
	}
	return false
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

func ReadFileAsMap(file string) (icds []map[string]any) {
	jsonData, err := os.ReadFile(file)
	if err != nil {
		panic("failed to read json file, error: " + err.Error())
		return
	}

	if err := json.Unmarshal(jsonData, &icds); err != nil {
		fmt.Printf("failed to unmarshal json file, error: %v", err)
		return
	}
	return
}

func Stats() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / (1024 * 1024)
}
