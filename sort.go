package search

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
)

func Sort[Schema SchemaProps](slice Hits[Schema], fieldName string, ascending bool) error {
	val := reflect.ValueOf(slice)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("expected a slice, got %s", val.Kind())
	}

	if val.Len() == 0 {
		return nil // Nothing to sort
	}

	elemType := val.Index(0).Type()
	if elemType.Kind() == reflect.Struct {
		return SortSliceByField(slice, fieldName, ascending)
	} else if elemType.Kind() == reflect.Map {
		return SortMapSliceByField(slice, fieldName, ascending)
	}
	return nil
}

// SortSliceByField sorts a slice of structs by a given field name in ascending or descending order.
func SortSliceByField[Schema SchemaProps](slice []Schema, fieldName string, ascending bool) error {
	val := reflect.ValueOf(slice)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("expected a slice, got %s", val.Kind())
	}

	if val.Len() == 0 {
		return nil // Nothing to sort
	}

	elemType := val.Index(0).Type()
	field, found := elemType.FieldByName(fieldName)
	if !found {
		return fmt.Errorf("field %s not found in struct %s", fieldName, elemType.Name())
	}

	sort.Slice(slice, func(i, j int) bool {
		a := reflect.ValueOf(slice).Index(i).FieldByIndex(field.Index)
		b := reflect.ValueOf(slice).Index(j).FieldByIndex(field.Index)

		// Compare values based on the field type
		switch a.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if ascending {
				return a.Int() < b.Int()
			}
			return a.Int() > b.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if ascending {
				return a.Uint() < b.Uint()
			}
			return a.Uint() > b.Uint()
		case reflect.Float32, reflect.Float64:
			if ascending {
				return a.Float() < b.Float()
			}
			return a.Float() > b.Float()
		case reflect.String:
			if ascending {
				return a.String() < b.String()
			}
			return a.String() > b.String()
		default:
			return false
		}
	})

	return nil
}

// SortMapSliceByField sorts a slice of any type by a given field name in ascending or descending order.
func SortMapSliceByField[Schema SchemaProps](slice []Schema, fieldName string, ascending bool) error {
	val := reflect.ValueOf(slice)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("expected a slice, got %s", val.Kind())
	}

	if val.Len() == 0 {
		return nil // Nothing to sort
	}

	sort.Slice(slice, func(i, j int) bool {
		a := val.Index(i)
		b := val.Index(j)

		var aField, bField reflect.Value

		if a.Kind() == reflect.Map {
			aField = reflect.ValueOf(a.MapIndex(reflect.ValueOf(fieldName)).Interface())
			bField = reflect.ValueOf(b.MapIndex(reflect.ValueOf(fieldName)).Interface())
		} else {
			aField = a.FieldByName(fieldName)
			bField = b.FieldByName(fieldName)
		}

		// Convert values to strings to remove commas, then parse to float64 for comparison
		aStr := fmt.Sprintf("%v", aField.Interface())
		bStr := fmt.Sprintf("%v", bField.Interface())
		aFloat, errA := strconv.ParseFloat(aStr, 64)
		bFloat, errB := strconv.ParseFloat(bStr, 64)

		// Compare based on successful conversion
		if errA == nil && errB == nil {
			if ascending {
				return aFloat < bFloat
			}
			return aFloat > bFloat
		}

		// If conversion fails, compare as strings
		if ascending {
			return aStr < bStr
		}
		return aStr > bStr
	})

	return nil
}
