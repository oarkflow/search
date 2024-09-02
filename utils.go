package search

import (
	"fmt"
	"github.com/oarkflow/filters"
	"reflect"
)

// getFieldsFromMap retrieves fields from a map in dot notation.
func getFieldsFromMap(obj map[string]any, prefix ...string) []string {
	var fields []string
	for field, val := range obj {
		fullField := field
		if len(prefix) > 0 {
			fullField = fmt.Sprintf("%s.%s", prefix[0], field)
		}

		switch val := val.(type) {
		case map[string]any:
			fields = append(fields, getFieldsFromMap(val, fullField)...)
		case []any:
			fields = append(fields, getFieldsFromArray(val, fullField)...)
		default:
			fields = append(fields, fullField)
		}
	}
	return fields
}

func getFieldsFromArray(array []any, prefix ...string) []string {
	var fields []string
	// Check if the array contains maps
	if len(array) > 0 {
		if _, ok := array[0].(map[string]any); ok {
			fullFieldPrefix := fmt.Sprintf("%s.#", prefix[0])
			for _, val := range array {
				switch val := val.(type) {
				case map[string]any:
					fields = append(fields, getFieldsFromMap(val, fullFieldPrefix)...)
				case []any:
					fields = append(fields, getFieldsFromArray(val, fullFieldPrefix)...)
				default:
					fields = append(fields, fullFieldPrefix)
				}
			}
			return fields
		}
	}
	// If the array does not contain maps, treat it as a simple field
	if len(prefix) > 0 {
		fields = append(fields, prefix[0])
	}
	return fields
}

func getFieldsFromStruct(obj any, prefix ...string) []string {
	var fields []string
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	visibleFields := reflect.VisibleFields(t)
	hasIndexField := false
	for i, field := range visibleFields {
		if propName, ok := field.Tag.Lookup("index"); ok {
			hasIndexField = true
			if len(prefix) == 1 {
				propName = fmt.Sprintf("%s.%s", prefix[0], propName)
			}

			if field.Type.Kind() == reflect.Struct {
				for _, key := range DocFields(v.Field(i).Interface(), propName) {
					fields = append(fields, key)
				}
			} else {
				fields = append(fields, propName)
			}
		}
	}

	if !hasIndexField {
		for i, field := range visibleFields {
			propName := field.Name
			if len(prefix) == 1 {
				propName = fmt.Sprintf("%s.%s", prefix[0], propName)
			}

			if field.Type.Kind() == reflect.Struct {
				for _, key := range DocFields(v.Field(i).Interface(), propName) {
					fields = append(fields, key)
				}
			} else {
				fields = append(fields, propName)
			}
		}
	}
	return fields
}

func DocFields(obj any, prefix ...string) []string {
	if obj == nil {
		return nil
	}

	switch obj := obj.(type) {
	case map[string]any:
		return getFieldsFromMap(obj, prefix...)
	case map[string]string:
		data := make(map[string]any)
		for k, v := range obj {
			data[k] = v
		}
		return getFieldsFromMap(data, prefix...)
	default:
		switch obj := obj.(type) {
		case map[string]any:
			return getFieldsFromMap(obj, prefix...)
		case map[string]string:
			data := make(map[string]any)
			for k, v := range obj {
				data[k] = v
			}
			return getFieldsFromMap(data, prefix...)
		default:
			return getFieldsFromStruct(obj, prefix...)
		}
	}
}

func ProcessQueryAndFilters(params *Params, seq *filters.Rule) {
	var extraFilters []*filters.Filter
	if params.Query != "" {
		for _, filter := range params.Filters {
			if filter.Field != "q" {
				extraFilters = append(extraFilters, filter)
			}
		}
		params.Filters = extraFilters
		return
	}
	foundQ := false
	for _, filter := range params.Filters {
		if filter.Field == "q" {
			params.Query = fmt.Sprintf("%v", filter.Value)
			params.Properties = append(params.Properties, filter.Field)
			foundQ = true
		} else {
			extraFilters = append(extraFilters, filter)
		}
	}

	if !foundQ {
		for _, filter := range extraFilters {
			if filter.Operator == filters.Equal {
				params.Query = fmt.Sprintf("%v", filter.Value)
				params.Properties = append(params.Properties, filter.Field)
				extraFilters = removeFilter(extraFilters, filter)
				break
			}
		}
	}
	if params.Query == "" && seq != nil {
		firstFilter, err := filters.FirstTermFilter(seq)
		if err == nil {
			params.Query = fmt.Sprintf("%v", firstFilter.Value)
			params.Properties = append(params.Properties, firstFilter.Field)
		}
	}

	// Update the filters in params
	params.Filters = extraFilters
}

func removeFilter(filters []*filters.Filter, filterToRemove *filters.Filter) []*filters.Filter {
	for i, filter := range filters {
		if filter == filterToRemove {
			return append(filters[:i], filters[i+1:]...)
		}
	}
	return filters
}
