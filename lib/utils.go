package lib

import (
	"slices"
	"strconv"
	"strings"
)

func IsEqual(dataVal, val any) bool {
	switch val := val.(type) {
	case string:
		switch gtVal := dataVal.(type) {
		case string:
			return strings.EqualFold(val, gtVal)
		}
		return false
	case int:
		switch gtVal := dataVal.(type) {
		case int:
			return val == gtVal
		case uint:
			return val == int(gtVal)
		case float64:
			return float64(val) == gtVal
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
		}
		return false
	case bool:
		switch gtVal := dataVal.(type) {
		case bool:
			return val == gtVal
		case string:
			v, err := strconv.ParseBool(gtVal)
			if err != nil {
				return false
			}
			return val == v
		}
		return false
	}
	return false
}

// Intersection computes the list of values that are the intersection of all the slices.
// Each value in the result should be present in each of the provided slices.
func Intersection[T comparable](params ...[]T) []T {
	var result []T

	for i := 0; i < len(params[0]); i++ {
		item := params[0][i]
		if slices.Contains(result, item) {
			continue
		}
		var j int
		for j = 1; j < len(params); j++ {
			if !slices.Contains(params[j], item) {
				break
			}
		}

		if j == len(params) {
			result = append(result, item)
		}
	}

	return result
}
