package filters

import (
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/search/filters/utils"
)

type Operator string

const (
	Equal            Operator = "eq"
	LessThan         Operator = "lt"
	LessThanEqual    Operator = "le"
	GreaterThan      Operator = "gt"
	GreaterThanEqual Operator = "ge"
	NotEqual         Operator = "ne"
	Contains         Operator = "contains"
	NotContains      Operator = "not_contains"
	Between          Operator = "between"
	In               Operator = "in"
	StartsWith       Operator = "starts_with"
	EndsWith         Operator = "ends_with"
	EqCount          Operator = "eq_count"
	NeqCount         Operator = "ne_count"
	GtCount          Operator = "gt_count"
	LtCount          Operator = "lt_count"
	GteCount         Operator = "ge_count"
	LteCount         Operator = "le_count"
	NotIn            Operator = "not_in"
	NotZero          Operator = "not_zero"
	IsZero           Operator = "is_zero"
	IsNull           Operator = "is_null"
	NotNull          Operator = "not_null"
)

var (
	validOperators = map[Operator]struct{}{
		Equal:            {},
		LessThan:         {},
		LessThanEqual:    {},
		GreaterThan:      {},
		GreaterThanEqual: {},
		NotEqual:         {},
		Contains:         {},
		NotContains:      {},
		Between:          {},
		In:               {},
		StartsWith:       {},
		EndsWith:         {},
	}
)

type BooleanOperator string

const (
	AND BooleanOperator = "AND"
	OR  BooleanOperator = "OR"
	NOT BooleanOperator = "NOT"
)

type Filter struct {
	Field    string
	Operator Operator
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

func ValidateFilters(filters ...Filter) error {
	for _, filter := range filters {
		if filter.Field == "" {
			return errors.New("filter field cannot be empty")
		}
		if _, exists := validOperators[filter.Operator]; !exists {
			return fmt.Errorf("invalid operator: %s", filter.Operator)
		}
		if filter.Operator == Between {
			if reflect.TypeOf(filter.Value).Kind() != reflect.Slice || reflect.ValueOf(filter.Value).Len() != 2 {
				return errors.New("between filter must have a slice of two elements as value")
			}
		}
		if filter.Operator == In {
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

func ApplyBinaryFilter[T any](data []T, expr BinaryExpr[T]) ([]T, error) {
	if expr.Left == nil || expr.Right == nil {
		return nil, errors.New("missing left or right filter group")
	}

	leftResult, err := ApplyGroup(data, []FilterGroup{*expr.Left})
	if err != nil {
		return nil, err
	}

	rightResult, err := ApplyGroup(data, []FilterGroup{*expr.Right})
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case AND:
		return utils.Intersection(leftResult, rightResult), nil
	case OR:
		return utils.Union(leftResult, rightResult), nil
	default:
		return nil, errors.New("unsupported boolean operator")
	}
}

func filterToQuery(filter Filter) any {
	switch filter.Operator {
	case Equal:
		return TermQuery{Term: map[string]any{filter.Field: filter.Value}}
	case LessThan:
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"lt": filter.Value}}}
	case LessThanEqual:
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"le": filter.Value}}}
	case GreaterThan:
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"gt": filter.Value}}}
	case GreaterThanEqual:
		return RangeQuery{Range: map[string]map[string]any{filter.Field: {"ge": filter.Value}}}
	case NotEqual:
		return BoolQuery{Must: []any{TermQuery{Term: map[string]any{filter.Field: map[string]any{"ne": filter.Value}}}}}
	case Contains:
		return WildcardQuery{Wildcard: map[string]string{filter.Field: fmt.Sprintf("*%v*", filter.Value)}}
	case NotContains:
		return BoolQuery{Must: []any{WildcardQuery{Wildcard: map[string]string{filter.Field: fmt.Sprintf("!*%v*", filter.Value)}}}}
	case StartsWith:
		return WildcardQuery{Wildcard: map[string]string{filter.Field: fmt.Sprintf("%v*", filter.Value)}}
	case EndsWith:
		return WildcardQuery{Wildcard: map[string]string{filter.Field: fmt.Sprintf("*%v", filter.Value)}}
	case In:
		return TermQuery{Term: map[string]any{filter.Field: map[string]any{"in": filter.Value}}}
	case Between:
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
	if err := ValidateFilters(filters...); err != nil {
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

func ApplyGroup[T any](data []T, filterGroups []FilterGroup) ([]T, error) {
	var result []T
	for _, item := range data {
		matches := true
		for _, group := range filterGroups {
			if !MatchGroup(item, group) {
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

func MatchGroup[T any](item T, group FilterGroup) bool {
	switch group.Operator {
	case AND:
		for _, filter := range group.Filters {
			if !Match(item, filter) {
				return false
			}
		}
		return true
	case OR:
		for _, filter := range group.Filters {
			if Match(item, filter) {
				return true
			}
		}
		return false
	case NOT:
		for _, filter := range group.Filters {
			if Match(item, filter) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func Match[T any](item T, filter Filter) bool {
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
		fieldValue = utils.GetFieldName(fieldVal, filter.Field)
		if !fieldValue.IsValid() {
			return false
		}
		val = fieldValue.Interface()
	}

	if val == nil {
		return false
	}
	switch filter.Operator {
	case Equal:
		return checkEq(val, filter)
	case NotEqual:
		return checkNeq(val, filter)
	case GreaterThan:
		return checkGt(val, filter)
	case LessThan:
		return checkLt(val, filter)
	case GreaterThanEqual:
		return checkGte(val, filter)
	case LessThanEqual:
		return checkLte(val, filter)
	case Between:
		return checkBetween(val, filter)
	case In:
		return checkIn(val, filter)
	case NotIn:
		return checkNotIn(val, filter)
	case Contains:
		return checkContains(val, filter)
	case NotContains:
		return checkNotContains(val, filter)
	case StartsWith:
		return checkStartsWith(val, filter)
	case EndsWith:
		return checkEndsWith(val, filter)
	case IsZero:
		return reflect.ValueOf(val).IsZero()
	case NotZero:
		return !fieldValue.IsZero()
	case IsNull:
		return fieldValue.IsNil()
	case NotNull:
		return checkNotNull(val, filter)
	case EqCount:
		return checkEqCount(val, filter)
	case NeqCount:
		return checkNeqCount(val, filter)
	case GtCount:
		return checkGtCount(val, filter)
	case GteCount:
		return checkGteCount(val, filter)
	case LtCount:
		return checkLtCount(val, filter)
	case LteCount:
		return checkLteCount(val, filter)
	}
	return false
}

// Converts string to appropriate type (int, float, time, or string)
func convertValue(value string) (any, error) {
	if i, err := strconv.Atoi(value); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f, nil
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	return value, nil
}

func New(field string, operator Operator, value any) Filter {
	return Filter{
		Field:    field,
		Operator: operator,
		Value:    value,
	}
}

// ParseQuery parses the query string and returns Filter or Query.
func ParseQuery(queryString string) ([]Filter, error) {
	queryParams, err := url.ParseQuery(strings.TrimPrefix(queryString, "?"))
	if err != nil {
		return nil, err
	}
	var filters []Filter

	for key, values := range queryParams {
		if strings.Contains(key, ":") {
			parts := strings.Split(key, ":")
			if len(parts) == 2 {
				filters = append(filters, New(parts[0], Equal, parts[1]))
			} else if len(parts) == 3 {
				// Handle complex field:operator:value
				field := parts[0]
				operator := parts[1]
				opValue := parts[2]
				if _, exists := validOperators[Operator(strings.ToLower(operator))]; !exists {
					return nil, errors.New("invalid operator")
				}
				// For between operator, split values into two parts
				var val any
				if strings.Contains(opValue, ",") {
					betweenParts := strings.Split(opValue, ",")
					if Operator(operator) == Between && len(betweenParts) != 2 {
						return nil, errors.New("operator must have at least two values")
					}
					if Operator(operator) == In && len(betweenParts) < 1 {
						return nil, errors.New("operator must have at least two values")
					}
					for i, p := range betweenParts {
						p = strings.TrimSpace(p)
						betweenParts[i] = p
					}

					val = betweenParts
				} else {
					val = opValue
				}
				filters = append(filters, New(field, Operator(operator), val))
			}
		} else {
			if len(values) == 1 {
				filters = append(filters, New(key, Equal, values[0]))
			} else if len(values) > 1 {
				filters = append(filters, New(key, In, values))
			}
		}
	}
	return filters, nil
}

// Helper function to remove a filter from a slice of filters
func removeFilter(filters []Filter, filterToRemove Filter) []Filter {
	for i, filter := range filters {
		if filter.Field == filterToRemove.Field && filter.Operator == filterToRemove.Operator && reflect.DeepEqual(filter.Value, filterToRemove.Value) {
			return append(filters[:i], filters[i+1:]...)
		}
	}
	return filters
}
