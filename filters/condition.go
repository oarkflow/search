package filters

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/oarkflow/json/sjson"
	"github.com/oarkflow/pkg/timeutil"

	"github.com/oarkflow/search/filters/utils"
)

func checkEq[T any](val T, filter Filter) bool {
	switch val := any(val).(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case string:
			return strings.EqualFold(val, gtVal)
		default:
			gtVal1 := fmt.Sprint(gtVal)
			return strings.EqualFold(val, gtVal1)
		}
	case int:
		switch gtVal := filter.Value.(type) {
		case int:
			return val == gtVal
		case uint:
			return val == int(gtVal)
		case float64:
			return float64(val) == gtVal
		case string:
			v, err := strconv.Atoi(gtVal)
			if err != nil {
				return false
			}
			return val == v
		}
		return false
	case float64:
		switch gtVal := filter.Value.(type) {
		case int:
			return val == float64(gtVal)
		case uint:
			return val == float64(gtVal)
		case float64:
			return val == gtVal
		case string:
			v, err := strconv.ParseFloat(gtVal, 32)
			if err != nil {
				return false
			}
			return val == v
		}
		return false
	case bool:
		switch gtVal := filter.Value.(type) {
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
	default:
		dataVal1 := fmt.Sprint(filter.Value)
		val1 := fmt.Sprint(val)
		return strings.EqualFold(dataVal1, val1)
	}
}

func checkNeq[T any](val T, filter Filter) bool {
	switch val := any(val).(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case string:
			return !strings.EqualFold(val, gtVal)
		}
		return false
	case int:
		switch gtVal := filter.Value.(type) {
		case int:
			return val != gtVal
		case float64:
			return float64(val) != gtVal
		}
		return false
	case float64:
		switch gtVal := filter.Value.(type) {
		case int:
			return val != float64(gtVal)
		case float64:
			return val != gtVal
		}
		return false
	case bool:
		switch gtVal := filter.Value.(type) {
		case bool:
			return val != gtVal
		case string:
			v, err := strconv.ParseBool(gtVal)
			if err != nil {
				return false
			}
			return val != v
		}
		return false
	}

	return false
}

func checkGt[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		from, err := timeutil.ParseTime(val)
		if err != nil {
			return false
		}
		switch gtVal := filter.Value.(type) {
		case string:
			smaller, err := timeutil.ParseTime(gtVal)
			if err != nil {
				return false
			}
			return from.After(smaller)
		}
		return false
	case int:
		switch gtVal := filter.Value.(type) {
		case int:
			return val > gtVal
		case float64:
			return float64(val) > gtVal
		}
		return false
	case float64:
		switch gtVal := filter.Value.(type) {
		case int:
			return val > float64(gtVal)
		case float64:
			return val > gtVal
		}
		return false
	}

	return false
}

func checkLt[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		from, err := timeutil.ParseTime(val)
		if err != nil {
			return false
		}
		switch gtVal := filter.Value.(type) {
		case string:
			smaller, err := timeutil.ParseTime(gtVal)
			if err != nil {
				return false
			}
			return from.Before(smaller)
		}
		return false
	case int:
		switch ltVal := filter.Value.(type) {
		case int:
			return val < ltVal
		case uint:
			return val < int(ltVal)
		case float64:
			return float64(val) < ltVal
		}
		return false
	case float64:
		switch ltVal := filter.Value.(type) {
		case int:
			return val < float64(ltVal)
		case float64:
			return val < ltVal
		}
		return false
	}

	return false
}

func checkGte[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		from, err := timeutil.ParseTime(val)
		if err != nil {
			return false
		}
		switch gtVal := filter.Value.(type) {
		case string:
			smaller, err := timeutil.ParseTime(gtVal)
			if err != nil {
				return false
			}
			return from.After(smaller) || from.Equal(smaller)
		}
		return false
	case int:
		switch gtVal := filter.Value.(type) {
		case int:
			return val >= gtVal
		case float64:
			return float64(val) >= gtVal
		}
		return false
	case float64:
		switch gtVal := filter.Value.(type) {
		case int:
			return val >= float64(gtVal)
		case float64:
			return val >= gtVal
		}
		return false
	}
	return false
}

func checkLte[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		from, err := timeutil.ParseTime(val)
		if err != nil {
			return false
		}
		switch gtVal := filter.Value.(type) {
		case string:
			smaller, err := timeutil.ParseTime(gtVal)
			if err != nil {
				return false
			}
			return from.Before(smaller) || from.Equal(smaller)
		}
		return false
	case int:
		switch ltVal := filter.Value.(type) {
		case int:
			return val <= ltVal
		case float64:
			return float64(val) <= ltVal
		}
		return false
	case float64:
		switch ltVal := filter.Value.(type) {
		case int:
			return val <= float64(ltVal)
		case float64:
			return val <= ltVal
		}
		return false
	}

	return false
}

func checkBetween[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case []string:
			from, err := timeutil.ParseTime(val)
			if err != nil {
				return false
			}
			start, err := timeutil.ParseTime(gtVal[0])
			if err != nil {
				return false
			}
			last, err := timeutil.ParseTime(gtVal[1])
			if err != nil {
				return false
			}
			return (from.After(start) || from.Equal(start)) && (from.Before(last) || from.Equal(last))
		}
		return false
	case int:
		switch ltVal := filter.Value.(type) {
		case []int:
			return val >= ltVal[0] && val <= ltVal[1]
		case []float64:
			return float64(val) >= ltVal[0] && float64(val) <= ltVal[1]
		}
		return false
	case float64:
		switch ltVal := filter.Value.(type) {
		case []int:
			return val >= float64(ltVal[0]) && val <= float64(ltVal[1])
		case []float64:
			return val >= ltVal[0] && val <= ltVal[1]
		}
		return false
	}

	return false
}

func checkIn[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case []string:
			for _, v := range gtVal {
				if strings.EqualFold(val, v) {
					return true
				}
			}
			return false
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(val, fmt.Sprintf("%v", v)) {
					return true
				}
			}
			return false
		}
		return false
	case int:
		switch gtVal := filter.Value.(type) {
		case []int:
			for _, v := range gtVal {
				if val == v {
					return true
				}
			}
			return false
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(strconv.Itoa(val), fmt.Sprintf("%v", v)) {
					return true
				}
			}
			return false
		}
		return false
	case float64:
		switch gtVal := filter.Value.(type) {
		case []float64:
			for _, v := range gtVal {
				if val == v {
					return true
				}
			}
			return false
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(strconv.Itoa(int(val)), fmt.Sprintf("%v", v)) {
					return true
				}
			}
			return false
		}
		return false
	case interface{}:
		switch nested := val.(type) {
		case []interface{}:
			switch target := filter.Value.(type) {
			case []interface{}:
				return utils.SearchDeeplyNestedSlice(nested, target)
			}
		}
	}

	return false
}

func checkNotIn[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case []string:
			for _, v := range gtVal {
				if strings.EqualFold(val, v) {
					return false
				}
			}
			return true
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(val, fmt.Sprintf("%v", v)) {
					return false
				}
			}
			return true
		}
		return false
	case int:
		switch gtVal := filter.Value.(type) {
		case []int:
			for _, v := range gtVal {
				if val == v {
					return false
				}
			}
			return true
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(strconv.Itoa(val), fmt.Sprintf("%v", v)) {
					return false
				}
			}
			return true
		}
		return false
	case float64:
		switch gtVal := filter.Value.(type) {
		case []float64:
			for _, v := range gtVal {
				if val == v {
					return false
				}
			}
			return true
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(strconv.Itoa(int(val)), fmt.Sprintf("%v", v)) {
					return false
				}
			}
			return true
		}
		return false
	case interface{}:
		switch nested := val.(type) {
		case []interface{}:
			switch target := filter.Value.(type) {
			case []interface{}:
				return !utils.SearchDeeplyNestedSlice(nested, target)
			}
		}
	}

	return false
}

func checkContains[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case string:
			return strings.Contains(val, gtVal)
		}
		return false
	}

	return false
}

func checkNotContains[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case string:
			return !strings.Contains(val, gtVal)
		}
		return false
	}
	return false
}

func checkStartsWith[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case string:
			return strings.HasPrefix(val, gtVal)
		}
		return false
	}
	return false
}

func checkEndsWith[T any](data T, filter Filter) bool {
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field).Value()
	switch val := result.(type) {
	case string:
		switch gtVal := filter.Value.(type) {
		case string:
			return strings.HasSuffix(val, gtVal)
		}
		return false
	}
	return false
}

func checkEqCount[T any](data T, filter Filter) bool {
	var d any
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field)
	if result.Exists() {
		d = result.Value()
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := filter.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", filter.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() == gtVal && valKind.Len() != 0
}

func checkNeqCount[T any](data T, filter Filter) bool {
	var d any
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field)
	if result.Exists() {
		d = result.Value()
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := filter.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", filter.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() != gtVal && valKind.Len() != 0
}

func checkGtCount[T any](data T, filter Filter) bool {
	var d any
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field)
	if result.Exists() {
		d = result.Value()
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := filter.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", filter.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() > gtVal && valKind.Len() != 0
}

func checkGteCount[T any](data T, filter Filter) bool {
	var d any
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field)
	if result.Exists() {
		d = result.Value()
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := filter.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", filter.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() >= gtVal && valKind.Len() != 0
}

func checkLtCount[T any](data T, filter Filter) bool {
	var d any
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field)
	if result.Exists() {
		d = result.Value()
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := filter.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", filter.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() < gtVal && valKind.Len() != 0
}

func checkLteCount[T any](data T, filter Filter) bool {
	var d any
	// use sjson to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	result := sjson.GetBytes(dataJson, filter.Field)
	if result.Exists() {
		d = result.Value()
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := filter.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", filter.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() <= gtVal && valKind.Len() != 0
}

func checkNotNull[T any](data T, filter Filter) bool {
	// use sjon to get the Value
	dataJson, err := json.Marshal(data)
	if err != nil {
		return false
	}
	val := sjson.GetBytes(dataJson, filter.Field)
	if val.Type == sjson.JSON {
		switch val.Value().(type) {
		case []interface{}:
			// this is the case when we have # in the filter.Field
			// so we need to check if any of the values in the slice is nil
			flat := utils.FlattenSlice(val.Value().([]interface{}))
			if slices.Contains(flat, nil) {
				// if the slice contains nil, we know it is not notnull
				return false
			}
			if len(flat) == 0 {
				// if all the values are missing, then we get this case
				return false
			} else {
				// this is for the case when one of the values is missing in the slice
				// remove everything after last # with multiple #s in filter.Field
				// to get the count of the slice
				filters := strings.Split(filter.Field, "#")
				filterCount := strings.Join(filters[:len(filters)-1], "#") + "#"
				// valCount here is the number of values in the slice
				valCount := sjson.GetBytes(dataJson, filterCount)
				switch valCount.Type {
				case sjson.JSON:
					// if we have a nested slice, we get a nested count
					// so we need to flatten the slice and check if the count matches
					// len(flat) is the number of values in the slice
					// sumIntSlice(flatCount) is the number of values that should be in the slice
					flatCount := utils.FlattenSlice(valCount.Value().([]interface{}))
					return utils.SumIntSlice(flatCount) == len(flat)
				case sjson.Number:
					// if we have a flat slice, we get a flat count
					// here len(val.Value().([]interface{})) is the number of values in the slice
					// int(valCount.Value().(float64)) is the number of values that should be in the slice
					return int(valCount.Value().(float64)) == len(val.Value().([]interface{}))
				}
			}
		case map[string]interface{}:
			// when the value is a map, we need to check if the map is empty
			// if the map is empty, then we know it is not notnull
			return len(val.Value().(map[string]interface{})) != 0
		}
	}
	return val.Value() != nil
}
