package main

import (
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
	Value    any
}

// Query represents the top-level query structure.
type Query struct {
	Bool BoolQuery `json:"bool"`
}

// BoolQuery represents a bool query in Elasticsearch.
type BoolQuery struct {
	Must []any `json:"must"`
}

// TermQuery represents a term query in Elasticsearch.
type TermQuery struct {
	Term map[string]any `json:"term"`
}

// RangeQuery represents a range query in Elasticsearch.
type RangeQuery struct {
	Range map[string]map[string]any `json:"range"`
}

// WildcardQuery represents a wildcard query in Elasticsearch.
type WildcardQuery struct {
	Wildcard map[string]string `json:"wildcard"`
}

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

func filterToQuery(filter Filter) any {
	switch filter.Operator {
	case EQUAL:
		return TermQuery{Term: map[string]any{filter.Field: filter.Value}}
	case LESS_THAN:
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"lt": filter.Value}}}
	case LESS_THAN_EQUAL:
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"le": filter.Value}}}
	case GREATER_THAN:
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"gt": filter.Value}}}
	case GREATER_THAN_EQUAL:
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"ge": filter.Value}}}
	case NOT_EQUAL:
		return BoolQuery{Must: []any{TermQuery{Term: map[string]any{filter.Field: map[string]any{"ne": filter.Value}}}}}
	case CONTAINS:
		return WildcardQuery{Wildcard: map[string]string{filter.Field: fmt.Sprintf("*%v*", filter.Value)}}
	case NOT_CONTAINS:
		return BoolQuery{Must: []any{WildcardQuery{Wildcard: map[string]string{filter.Field: fmt.Sprintf("!*%v*", filter.Value)}}}}
	case STARTS_WITH:
		return WildcardQuery{Wildcard: map[string]string{filter.Field: fmt.Sprintf("%v*", filter.Value)}}
	case ENDS_WITH:
		return WildcardQuery{Wildcard: map[string]string{filter.Field: fmt.Sprintf("*%v", filter.Value)}}
	case IN:
		return TermQuery{Term: map[string]any{filter.Field: map[string]any{"in": filter.Value}}}
	case BETWEEN:
		values, ok := filter.Value.([]any)
		if !ok || len(values) != 2 {
			return nil
		}
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"gte": values[0], "lte": values[1]}}}
	default:
		return nil
	}
}

func filtersToQuery(filters []Filter) (Query, error) {
	if err := validateFilters(filters); err != nil {
		return Query{}, err
	}

	var mustQueries []any
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

func applyFilters(filters []Filter, data []map[string]any) ([]map[string]any, error) {
	if err := validateFilters(filters); err != nil {
		return nil, err
	}

	var result []map[string]any
	for _, item := range data {
		matches := true
		for _, filter := range filters {
			if !matchFilter(item, filter) {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, item)
		}
	}

	return result, nil
}

func matchFilter(item map[string]any, filter Filter) bool {
	fieldVal, exists := item[filter.Field]
	if !exists {
		return false
	}
	switch filter.Operator {
	case EQUAL:
		return reflect.DeepEqual(fieldVal, filter.Value)
	case LESS_THAN:
		return compare(fieldVal, filter.Value) < 0
	case LESS_THAN_EQUAL:
		return compare(fieldVal, filter.Value) <= 0
	case GREATER_THAN:
		return compare(fieldVal, filter.Value) > 0
	case GREATER_THAN_EQUAL:
		return compare(fieldVal, filter.Value) >= 0
	case NOT_EQUAL:
		return !reflect.DeepEqual(fieldVal, filter.Value)
	case CONTAINS:
		return strings.Contains(fieldVal.(string), filter.Value.(string))
	case NOT_CONTAINS:
		return !strings.Contains(fieldVal.(string), filter.Value.(string))
	case STARTS_WITH:
		return strings.HasPrefix(fieldVal.(string), filter.Value.(string))
	case ENDS_WITH:
		return strings.HasSuffix(fieldVal.(string), filter.Value.(string))
	case IN:
		values := filter.Value.([]any)
		for _, v := range values {
			if reflect.DeepEqual(fieldVal, v) {
				return true
			}
		}
		return false
	case BETWEEN:
		values := filter.Value.([]any)
		return compare(fieldVal, values[0]) >= 0 && compare(fieldVal, values[1]) <= 0
	default:
		return false
	}
}

func compare(a, b any) int {
	switch a.(type) {
	case int:
		ai := a.(int)
		bi := b.(int)
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	case string:
		as := a.(string)
		bs := b.(string)
		return strings.Compare(as, bs)
	default:
		return 0
	}
}

func main() {
	// Example filters
	filters := []Filter{
		{Field: "age", Operator: GREATER_THAN, Value: 27},
		{Field: "city", Operator: CONTAINS, Value: "York"},
		{Field: "created_at", Operator: BETWEEN, Value: []any{"2022-01-01", "2023-01-01"}},
	}

	// Example data
	data := []map[string]any{
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
		fmt.Println(item)
	}
}
