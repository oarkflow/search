package main

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
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

// BooleanOperator represents the type of boolean operation.
type BooleanOperator string

const (
	AND BooleanOperator = "AND"
	OR  BooleanOperator = "OR"
	NOT BooleanOperator = "NOT"
)

// Filter represents a filter condition.
type Filter struct {
	Field    string
	Operator FilterType
	Value    any
}

// FilterGroup represents a group of filters with a boolean operator.
type FilterGroup struct {
	Operator BooleanOperator
	Filters  []Filter
}

// Query represents the top-level query structure.
type Query struct {
	Bool BoolQuery `json:"bool"`
}

// BoolQuery represents a bool query in Elasticsearch.
type BoolQuery struct {
	Must    []any `json:"must"`
	Should  []any `json:"should"`
	MustNot []any `json:"must_not"`
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

// BinaryExpr represents a binary expression tree node.
type BinaryExpr struct {
	Left     *FilterGroup
	Operator BooleanOperator
	Right    *FilterGroup
}

// ApplyFilterGroups applies filter groups with boolean operators recursively.
func ApplyFilterGroups(data []map[string]any, expr BinaryExpr) ([]map[string]any, error) {
	if expr.Left == nil || expr.Right == nil {
		return nil, errors.New("missing left or right filter group")
	}

	leftResult, err := applyFilters(data, []FilterGroup{*expr.Left})
	if err != nil {
		return nil, err
	}

	rightResult, err := applyFilters(data, []FilterGroup{*expr.Right})
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case AND:
		return intersection(leftResult, rightResult), nil
	case OR:
		return union(leftResult, rightResult), nil
	default:
		return nil, errors.New("unsupported boolean operator")
	}
}

// Intersection returns the intersection of two sets of data maps.
func intersection(a, b []map[string]any) []map[string]any {
	set := make(map[string]struct{})
	var intersection []map[string]any

	for _, item := range a {
		set[serialize(item)] = struct{}{}
	}

	for _, item := range b {
		if _, exists := set[serialize(item)]; exists {
			intersection = append(intersection, item)
		}
	}

	return intersection
}

// Union returns the union of two sets of data maps.
func union(a, b []map[string]any) []map[string]any {
	set := make(map[string]struct{})
	var union []map[string]any

	for _, item := range a {
		set[serialize(item)] = struct{}{}
		union = append(union, item)
	}

	for _, item := range b {
		if _, exists := set[serialize(item)]; !exists {
			union = append(union, item)
		}
	}

	return union
}

// Serialize converts a data map into a string for set operations.
func serialize(item map[string]any) string {
	var keys []string
	for k := range item {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder
	for _, k := range keys {
		builder.WriteString(fmt.Sprintf("%s:%v|", k, item[k]))
	}

	return builder.String()
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

func applyFilters(data []map[string]any, filterGroups []FilterGroup) ([]map[string]any, error) {
	var result []map[string]any
	for _, item := range data {
		matches := true
		for _, group := range filterGroups {
			if !matchFilterGroup(item, group) {
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

func matchFilterGroup(item map[string]any, group FilterGroup) bool {
	switch group.Operator {
	case AND:
		for _, filter := range group.Filters {
			if !matchFilter(item, filter) {
				return false
			}
		}
		return true
	case OR:
		for _, filter := range group.Filters {
			if matchFilter(item, filter) {
				return true
			}
		}
		return false
	case NOT:
		for _, filter := range group.Filters {
			if matchFilter(item, filter) {
				return false
			}
		}
		return true
	default:
		return false
	}
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
	case time.Time:
		at, err := parseTime(a.(string))
		if err != nil {
			return 0
		}
		bt, err := parseTime(b.(string))
		if err != nil {
			return 0
		}
		switch {
		case at.Before(bt):
			return -1
		case at.After(bt):
			return 1
		default:
			return 0
		}
	default:
		return 0
	}
}

func parseTime(value string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	var t time.Time
	var err error
	for _, format := range formats {
		t, err = time.Parse(format, value)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, err
}

func main() {
	// Sample data
	data := []map[string]any{
		{"age": 25, "city": "New York", "created_at": "2023-01-01 12:00:00", "name": "John Doe"},
		{"age": 30, "city": "Los Angeles", "created_at": "2022-06-15 15:30:00", "name": "Jane Doe"},
		{"age": 35, "city": "Chicago", "created_at": "2021-12-25 08:45:00", "name": "Alice Smith"},
		{"age": 40, "city": "Houston", "created_at": "2022-11-11 20:15:00", "name": "Bob Johnson"},
	}

	group1 := FilterGroup{
		Operator: AND,
		Filters: []Filter{
			{Field: "age", Operator: GREATER_THAN, Value: 27},
			{Field: "city", Operator: CONTAINS, Value: "Hous"},
		},
	}
	group2 := FilterGroup{
		Operator: OR,
		Filters: []Filter{
			{Field: "created_at", Operator: BETWEEN, Value: []any{"2022-01-01", "2023-01-01"}},
			{Field: "name", Operator: STARTS_WITH, Value: "Jane"},
		},
	}

	// Apply filters to data
	filteredData, err := applyFilters(data, []FilterGroup{group1, group2})
	if err != nil {
		fmt.Println("Error applying filters:", err)
		return
	}

	// Print filtered data
	for _, item := range filteredData {
		fmt.Println(item)
	}

	// Create a binary expression
	binaryExpr := BinaryExpr{
		Left:     &group1,
		Operator: AND,
		Right:    &group2,
	}

	// Apply filters to data using binary expression
	filteredData, err = ApplyFilterGroups(data, binaryExpr)
	if err != nil {
		fmt.Println("Error applying filters:", err)
		return
	}

	// Print filtered data
	for _, item := range filteredData {
		fmt.Println(item)
	}
}
