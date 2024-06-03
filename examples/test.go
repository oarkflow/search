package main

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

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

type BooleanOperator string

const (
	AND BooleanOperator = "AND"
	OR  BooleanOperator = "OR"
	NOT BooleanOperator = "NOT"
)

type Filter struct {
	Field    string
	Operator FilterType
	Value    any
}

type FilterGroup struct {
	Operator BooleanOperator
	Filters  []Filter
}

type Query struct {
	Bool BoolQuery `json:"bool"`
}

type BoolQuery struct {
	Must    []any `json:"must"`
	Should  []any `json:"should"`
	MustNot []any `json:"must_not"`
}

type TermQuery struct {
	Term map[string]any `json:"term"`
}

type RangeQuery struct {
	Range map[string]map[string]any `json:"range"`
}

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

type BinaryExpr[T any] struct {
	Left     *FilterGroup
	Operator BooleanOperator
	Right    *FilterGroup
}

func ApplyFilterGroups[T any](data []T, expr BinaryExpr[T]) ([]T, error) {
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

func intersection[T any](a, b []T) []T {
	set := make(map[string]struct{})
	var intersection []T

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

func union[T any](a, b []T) []T {
	set := make(map[string]struct{})
	var union []T

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

func serialize[T any](item T) string {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Map {
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].String() < keys[j].String()
		})
		var builder strings.Builder
		for _, k := range keys {
			builder.WriteString(fmt.Sprintf("%s:%v|", k, v.MapIndex(k)))
		}
		return builder.String()
	}

	var builder strings.Builder
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		builder.WriteString(fmt.Sprintf("%s:%v|", t.Field(i).Name, v.Field(i).Interface()))
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

func applyFilters[T any](data []T, filterGroups []FilterGroup) ([]T, error) {
	var result []T
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

func matchFilterGroup[T any](item T, group FilterGroup) bool {
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

func matchFilter[T any](item T, filter Filter) bool {
	fieldVal := reflect.ValueOf(item)
	var fieldValue reflect.Value
	var val any

	if fieldVal.Kind() == reflect.Map {
		mapKey := reflect.ValueOf(filter.Field)
		if !mapKey.IsValid() {
			return false
		}
		fieldValue = fieldVal.MapIndex(mapKey)
		if !fieldValue.IsValid() {
			return false
		}
		val = fieldValue.Interface()
	} else {
		fieldValue = fieldVal.FieldByName(filter.Field)
		if !fieldValue.IsValid() {
			return false
		}
		val = fieldValue.Interface()
	}

	if val == nil {
		return false
	}

	switch filter.Operator {
	case EQUAL:
		return reflect.DeepEqual(val, filter.Value)
	case LESS_THAN:
		return compare(val, filter.Value) < 0
	case LESS_THAN_EQUAL:
		return compare(val, filter.Value) <= 0
	case GREATER_THAN:
		return compare(val, filter.Value) > 0
	case GREATER_THAN_EQUAL:
		return compare(val, filter.Value) >= 0
	case NOT_EQUAL:
		return !reflect.DeepEqual(val, filter.Value)
	case CONTAINS:
		return strings.Contains(val.(string), filter.Value.(string))
	case NOT_CONTAINS:
		return !strings.Contains(val.(string), filter.Value.(string))
	case STARTS_WITH:
		return strings.HasPrefix(val.(string), filter.Value.(string))
	case ENDS_WITH:
		return strings.HasSuffix(val.(string), filter.Value.(string))
	case IN:
		values := filter.Value.([]any)
		for _, v := range values {
			if reflect.DeepEqual(val, v) {
				return true
			}
		}
		return false
	case BETWEEN:
		values := filter.Value.([]any)
		return compare(val, values[0]) >= 0 && compare(val, values[1]) <= 0
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
	// Sample data (map type)
	mapData := []map[string]any{
		{"Age": 25, "City": "New York", "CreatedAt": "2023-01-01 12:00:00", "Name": "John Doe"},
		{"Age": 30, "City": "Los Angeles", "CreatedAt": "2022-06-15 15:30:00", "Name": "Jane Doe"},
		{"Age": 35, "City": "Chicago", "CreatedAt": "2021-12-25 08:45:00", "Name": "Alice Smith"},
		{"Age": 40, "City": "Houston", "CreatedAt": "2022-11-11 20:15:00", "Name": "Bob Johnson"},
	}
	group2 := FilterGroup{
		Operator: AND,
		Filters: []Filter{
			{Field: "CreatedAt", Operator: BETWEEN, Value: []any{"2022-06-01", "2023-01-01"}},
			{Field: "Name", Operator: STARTS_WITH, Value: "Jane"},
		},
	}

	// Apply filters to map data
	filteredMapData, err := applyFilters(mapData, []FilterGroup{group2})
	if err != nil {
		fmt.Println("Error applying filters:", err)
	}

	// Print filtered map data
	fmt.Println("Filtered Map Data")
	for _, item := range filteredMapData {
		fmt.Println(item)
	}

	// Sample data (struct type)
	type Person struct {
		Age       int
		City      string
		CreatedAt string
		Name      string
	}

	structData := []Person{
		{Age: 25, City: "New York", CreatedAt: "2023-01-01 12:00:00", Name: "John Doe"},
		{Age: 30, City: "Los Angeles", CreatedAt: "2022-06-15 15:30:00", Name: "Jane Doe"},
		{Age: 35, City: "Chicago", CreatedAt: "2021-12-25 08:45:00", Name: "Alice Smith"},
		{Age: 40, City: "Houston", CreatedAt: "2022-11-11 20:15:00", Name: "Bob Johnson"},
	}

	// Apply filters to struct data
	filteredStructData, err := applyFilters(structData, []FilterGroup{group2})
	if err != nil {
		fmt.Println("Error applying filters:", err)
		return
	}

	fmt.Println("Filtered Struct Data")
	// Print filtered struct data
	for _, item := range filteredStructData {
		fmt.Println(item)
	}

	group1 := FilterGroup{
		Operator: AND,
		Filters: []Filter{
			{Field: "Age", Operator: GREATER_THAN, Value: 27},
			{Field: "City", Operator: CONTAINS, Value: "Hous"},
		},
	}
	// Create a binary expression
	binaryExpr := BinaryExpr[map[string]any]{
		Left:     &group1,
		Operator: AND,
		Right:    &group2,
	}

	// Apply filters to map data using binary expression
	filteredMapData, err = ApplyFilterGroups(mapData, binaryExpr)
	if err != nil {
		fmt.Println("Error applying filters:", err)
		return
	}

	// Print filtered map data
	for _, item := range filteredMapData {
		fmt.Println(item)
	}
}
