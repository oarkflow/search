package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// FilterType represents the type of filter operation.
type FilterType string

const (
	EQUAL              FilterType = "eq"
	LESS_THAN          FilterType = "lt"
	LESS_THAN_EQUAL    FilterType = "le"
	GREATER_THAN       FilterType = "gt"
	GREATER_THAN_EQUAL FilterType = "ge"
	NOT_EQUAL          FilterType = "ne"
	CONTAINS           FilterType = "contains"
	NOT_CONTAINS       FilterType = "not_contains"
	BETWEEN            FilterType = "between"
	IN                 FilterType = "in"
	STARTS_WITH        FilterType = "starts_with"
	ENDS_WITH          FilterType = "ends_with"
)

// Filter represents a filter condition.
type Filter struct {
	Field    string
	Operator FilterType
	Value    interface{}
}

// TermQuery represents a term query in Elasticsearch
type TermQuery struct {
	Term map[string]interface{} `json:"term"`
}

// RangeQuery represents a range query in Elasticsearch
type RangeQuery struct {
	Range map[string]map[string]interface{} `json:"range"`
}

// WildcardQuery represents a wildcard query in Elasticsearch
type WildcardQuery struct {
	Wildcard map[string]interface{} `json:"wildcard"`
}

// BoolQuery represents a bool query in Elasticsearch
type BoolQuery struct {
	Must []interface{} `json:"must"`
}

// Query represents the top-level query structure in Elasticsearch
type Query struct {
	Bool BoolQuery `json:"bool"`
}

// validateFilters validates the filters to ensure they have correct fields, operators, and values.
func validateFilters(filters []Filter) error {
	validOperators := map[FilterType]struct{}{
		EQUAL:              {},
		LESS_THAN:          {},
		LESS_THAN_EQUAL:    {},
		GREATER_THAN:       {},
		GREATER_THAN_EQUAL: {},
		NOT_EQUAL:          {},
		CONTAINS:           {},
		NOT_CONTAINS:       {},
		BETWEEN:            {},
		IN:                 {},
		STARTS_WITH:        {},
		ENDS_WITH:          {},
	}

	for _, filter := range filters {
		if filter.Field == "" {
			return errors.New("filter field cannot be empty")
		}
		if _, exists := validOperators[filter.Operator]; !exists {
			return fmt.Errorf("invalid operator: %s", filter.Operator)
		}
		if filter.Operator == BETWEEN {
			if reflect.TypeOf(filter.Value).Kind() != reflect.Slice || reflect.ValueOf(filter.Value).Len() != 2 {
				return errors.New("between filter must have a slice of two elements as value")
			}
		}
		if filter.Operator == IN {
			if reflect.TypeOf(filter.Value).Kind() != reflect.Slice {
				return errors.New("in filter must have a slice as value")
			}
		}
	}

	return nil
}

func filterToQuery(filter Filter) interface{} {
	switch filter.Operator {
	case EQUAL:
		return TermQuery{Term: map[string]interface{}{filter.Field: filter.Value}}
	case LESS_THAN:
		return RangeQuery{Range: map[string]map[string]interface{}{filter.Field: {"lt": filter.Value}}}
	case LESS_THAN_EQUAL:
		return RangeQuery{Range: map[string]map[string]interface{}{filter.Field: {"le": filter.Value}}}
	case GREATER_THAN:
		return RangeQuery{Range: map[string]map[string]interface{}{filter.Field: {"gt": filter.Value}}}
	case GREATER_THAN_EQUAL:
		return RangeQuery{Range: map[string]map[string]interface{}{filter.Field: {"ge": filter.Value}}}
	case NOT_EQUAL:
		return BoolQuery{Must: []interface{}{TermQuery{Term: map[string]interface{}{filter.Field: map[string]interface{}{"ne": filter.Value}}}}}
	case CONTAINS:
		return WildcardQuery{Wildcard: map[string]interface{}{filter.Field: fmt.Sprintf("*%v*", filter.Value)}}
	case NOT_CONTAINS:
		return BoolQuery{Must: []interface{}{WildcardQuery{Wildcard: map[string]interface{}{filter.Field: map[string]interface{}{"not": fmt.Sprintf("*%v*", filter.Value)}}}}}
	case STARTS_WITH:
		return WildcardQuery{Wildcard: map[string]interface{}{filter.Field: fmt.Sprintf("%v*", filter.Value)}}
	case ENDS_WITH:
		return WildcardQuery{Wildcard: map[string]interface{}{filter.Field: fmt.Sprintf("*%v", filter.Value)}}
	case IN:
		return TermQuery{Term: map[string]interface{}{filter.Field: map[string]interface{}{"in": filter.Value}}}
	case BETWEEN:
		values, ok := filter.Value.([]interface{})
		if !ok || len(values) != 2 {
			return nil
		}
		return RangeQuery{Range: map[string]map[string]interface{}{filter.Field: {"gte": values[0], "lte": values[1]}}}
	default:
		return nil
	}
}

func filtersToQuery(filters []Filter) (Query, error) {
	if err := validateFilters(filters); err != nil {
		return Query{}, err
	}

	var mustQueries []interface{}
	for _, filter := range filters {
		query := filterToQuery(filter)
		if query != nil {
			mustQueries = append(mustQueries, query)
		}
	}
	return Query{
		Bool: BoolQuery{
			Must: mustQueries,
		},
	}, nil
}

func applyFilters(filters []Filter, data []map[string]interface{}) ([]map[string]interface{}, error) {
	if err := validateFilters(filters); err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for _, item := range data {
		if matchesAllFilters(item, filters) {
			result = append(result, item)
		}
	}

	return result, nil
}

func matchesAllFilters(item map[string]interface{}, filters []Filter) bool {
	for _, filter := range filters {
		if !matchesFilter(item, filter) {
			return false
		}
	}
	return true
}

func matchesFilter(item map[string]interface{}, filter Filter) bool {
	value, exists := item[filter.Field]
	if !exists {
		return false
	}

	switch filter.Operator {
	case EQUAL:
		return value == filter.Value
	case LESS_THAN:
		return compare(value, filter.Value, func(a, b float64) bool { return a < b })
	case LESS_THAN_EQUAL:
		return compare(value, filter.Value, func(a, b float64) bool { return a <= b })
	case GREATER_THAN:
		return compare(value, filter.Value, func(a, b float64) bool { return a > b })
	case GREATER_THAN_EQUAL:
		return compare(value, filter.Value, func(a, b float64) bool { return a >= b })
	case NOT_EQUAL:
		return value != filter.Value
	case CONTAINS:
		return contains(value, filter.Value)
	case NOT_CONTAINS:
		return !contains(value, filter.Value)
	case STARTS_WITH:
		return strings.HasPrefix(fmt.Sprintf("%v", value), fmt.Sprintf("%v", filter.Value))
	case ENDS_WITH:
		return strings.HasSuffix(fmt.Sprintf("%v", value), fmt.Sprintf("%v", filter.Value))
	case IN:
		return in(value, filter.Value)
	case BETWEEN:
		values, ok := filter.Value.([]interface{})
		if !ok || len(values) != 2 {
			return false
		}
		return compare(value, values[0], func(a, b float64) bool { return a >= b }) &&
			compare(value, values[1], func(a, b float64) bool { return a <= b })
	default:
		return false
	}
}

func compare(value interface{}, compareTo interface{}, cmpFunc func(a, b float64) bool) bool {
	valFloat, ok1 := toFloat64(value)
	compFloat, ok2 := toFloat64(compareTo)
	return ok1 && ok2 && cmpFunc(valFloat, compFloat)
}

func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case float64:
		return v, true
	case string:
		var f float64
		if _, err := fmt.Sscan(v, &f); err == nil {
			return f, true
		}
	}
	return 0, false
}

func contains(value interface{}, substring interface{}) bool {
	strVal := fmt.Sprintf("%v", value)
	strSub := fmt.Sprintf("%v", substring)
	return strings.Contains(strVal, strSub)
}

func in(value interface{}, list interface{}) bool {
	valStr := fmt.Sprintf("%v", value)
	valList, ok := list.([]interface{})
	if !ok {
		return false
	}

	for _, item := range valList {
		if valStr == fmt.Sprintf("%v", item) {
			return true
		}
	}
	return false
}

func main() {
	// Example filters
	filters := []Filter{
		{Field: "age", Operator: GREATER_THAN, Value: 27},
		{Field: "city", Operator: CONTAINS, Value: "York"},
		{Field: "created_at", Operator: BETWEEN, Value: []interface{}{"2022-01-01", "2023-01-01"}},
	}

	// Example data
	data := []map[string]interface{}{
		{"name": "Alice", "age": 30, "city": "New York", "created_at": "2022-06-15"},
		{"name": "Bob", "age": 25, "city": "Los Angeles", "created_at": "2021-12-30"},
		{"name": "Charlie", "age": 28, "city": "York", "created_at": "2022-11-22"},
	}

	// Apply filters to data
	filteredData, err := applyFilters(filters, data)
	if err != nil {
		fmt.Println("Error applying filters:", err)
		return
	}

	// Print filtered data
	for _, item := range filteredData {
		itemJSON, _ := json.Marshal(item)
		fmt.Println(string(itemJSON))
	}
}
