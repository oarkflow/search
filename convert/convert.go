package convert

import (
	"fmt"
	"strconv"
	"time"

	"github.com/relvacode/iso8601"
)

func To[T any](src T, dst any) (T, bool) {
	switch any(src).(type) {
	case string:
		val, ok := ToString(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case bool:
		val, ok := ToBool(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case time.Time:
		val, ok := ToTime(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case float32:
		val, ok := ToFloat32(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case float64:
		val, ok := ToFloat64(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case uint:
		val, ok := ToUint(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case uint8:
		val, ok := ToUint8(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case uint16:
		val, ok := ToUint16(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case uint32:
		val, ok := ToUint32(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case uint64:
		val, ok := ToUint64(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case int:
		val, ok := ToInt(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case int8:
		val, ok := ToInt8(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case int16:
		val, ok := ToInt16(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case int32:
		val, ok := ToInt32(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case int64:
		val, ok := ToInt64(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []string:
		val, ok := ToSliceString(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []bool:
		val, ok := ToSliceBool(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []time.Time:
		val, ok := ToSliceTime(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []float32:
		val, ok := ToSliceFloat32(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []float64:
		val, ok := ToSliceFloat64(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []uint:
		val, ok := ToSliceUint(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []uint8:
		val, ok := ToSliceUint8(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []uint16:
		val, ok := ToSliceUint16(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []uint32:
		val, ok := ToSliceUint32(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []uint64:
		val, ok := ToSliceUint64(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []int:
		val, ok := ToSliceInt(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []int8:
		val, ok := ToSliceInt8(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []int16:
		val, ok := ToSliceInt16(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []int32:
		val, ok := ToSliceInt32(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	case []int64:
		val, ok := ToSliceInt64(dst)
		if !ok {
			return *new(T), false
		}
		return any(val).(T), true
	default:
		return *new(T), false
	}
}

// Basic type conversion functions
func ToString(val any) (string, bool) {
	switch v := val.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case fmt.Stringer:
		return v.String(), true
	default:
		return fmt.Sprintf("%v", val), true
	}
}

func ToBool(val any) (bool, bool) {
	switch v := val.(type) {
	case bool:
		return v, true
	case string:
		b, err := strconv.ParseBool(v)
		return b, err == nil
	default:
		return false, false
	}
}

func ToTime(val any) (time.Time, bool) {
	switch v := val.(type) {
	case time.Time:
		return v, true
	case string:
		t, err := iso8601.ParseString(v)
		return t, err == nil
	default:
		return time.Time{}, false
	}
}

func ToFloat32(val any) (float32, bool) {
	switch v := val.(type) {
	case float32:
		return v, true
	case float64:
		return float32(v), true
	case string:
		f, err := strconv.ParseFloat(v, 32)
		return float32(f), err == nil
	case int:
		return float32(v), true
	case int64:
		return float32(v), true
	case uint:
		return float32(v), true
	case uint64:
		return float32(v), true
	default:
		return 0, false
	}
}

func ToFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func ToUint(val any) (uint, bool) {
	switch v := val.(type) {
	case uint:
		return v, true
	case uint64:
		return uint(v), true
	case int:
		return uint(v), v >= 0
	case int64:
		return uint(v), v >= 0
	case string:
		u, err := strconv.ParseUint(v, 10, 64)
		return uint(u), err == nil
	default:
		return 0, false
	}
}

func ToUint8(val any) (uint8, bool) {
	switch v := val.(type) {
	case uint8:
		return v, true
	case uint:
		return uint8(v), v <= 255
	case int:
		return uint8(v), v >= 0 && v <= 255
	case string:
		u, err := strconv.ParseUint(v, 10, 8)
		return uint8(u), err == nil
	default:
		return 0, false
	}
}

func ToUint16(val any) (uint16, bool) {
	switch v := val.(type) {
	case uint16:
		return v, true
	case uint:
		return uint16(v), v <= 65535
	case int:
		return uint16(v), v >= 0 && v <= 65535
	case string:
		u, err := strconv.ParseUint(v, 10, 16)
		return uint16(u), err == nil
	default:
		return 0, false
	}
}

func ToUint32(val any) (uint32, bool) {
	switch v := val.(type) {
	case uint32:
		return v, true
	case uint:
		return uint32(v), v <= 4294967295
	case int:
		return uint32(v), v >= 0 && v <= 4294967295
	case string:
		u, err := strconv.ParseUint(v, 10, 32)
		return uint32(u), err == nil
	default:
		return 0, false
	}
}

func ToUint64(val any) (uint64, bool) {
	switch v := val.(type) {
	case uint64:
		return v, true
	case uint:
		return uint64(v), true
	case int:
		return uint64(v), v >= 0
	case int64:
		return uint64(v), v >= 0
	case string:
		u, err := strconv.ParseUint(v, 10, 64)
		return u, err == nil
	default:
		return 0, false
	}
}

func ToInt(val any) (int, bool) {
	switch v := val.(type) {
	case int:
		return v, true
	case int64:
		return int(v), v <= int64(^uint(0)>>1) && v >= -int64(^uint(0)>>1)-1
	case uint:
		return int(v), v <= uint(^uint(0)>>1)
	case string:
		i, err := strconv.Atoi(v)
		return i, err == nil
	default:
		return 0, false
	}
}

func ToInt8(val any) (int8, bool) {
	switch v := val.(type) {
	case int8:
		return v, true
	case int:
		return int8(v), v >= -128 && v <= 127
	case string:
		i, err := strconv.ParseInt(v, 10, 8)
		return int8(i), err == nil
	default:
		return 0, false
	}
}

func ToInt16(val any) (int16, bool) {
	switch v := val.(type) {
	case int16:
		return v, true
	case int:
		return int16(v), v >= -32768 && v <= 32767
	case string:
		i, err := strconv.ParseInt(v, 10, 16)
		return int16(i), err == nil
	default:
		return 0, false
	}
}

func ToInt32(val any) (int32, bool) {
	switch v := val.(type) {
	case int32:
		return v, true
	case int:
		return int32(v), v >= -2147483648 && v <= 2147483647
	case string:
		i, err := strconv.ParseInt(v, 10, 32)
		return int32(i), err == nil
	default:
		return 0, false
	}
}

func ToInt64(val any) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

// Slice conversion functions
func ToSliceString(val any) ([]string, bool) {
	switch v := val.(type) {
	case []string:
		return v, true
	case []any:
		result := make([]string, len(v))
		for i, elem := range v {
			str, ok := ToString(elem)
			if !ok {
				return nil, false
			}
			result[i] = str
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceBool(val any) ([]bool, bool) {
	switch v := val.(type) {
	case []bool:
		return v, true
	case []any:
		result := make([]bool, len(v))
		for i, elem := range v {
			b, ok := ToBool(elem)
			if !ok {
				return nil, false
			}
			result[i] = b
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceTime(val any) ([]time.Time, bool) {
	switch v := val.(type) {
	case []time.Time:
		return v, true
	case []any:
		result := make([]time.Time, len(v))
		for i, elem := range v {
			t, ok := ToTime(elem)
			if !ok {
				return nil, false
			}
			result[i] = t
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceFloat32(val any) ([]float32, bool) {
	switch v := val.(type) {
	case []float32:
		return v, true
	case []any:
		result := make([]float32, len(v))
		for i, elem := range v {
			f, ok := ToFloat32(elem)
			if !ok {
				return nil, false
			}
			result[i] = f
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceFloat64(val any) ([]float64, bool) {
	switch v := val.(type) {
	case []float64:
		return v, true
	case []any:
		result := make([]float64, len(v))
		for i, elem := range v {
			f, ok := ToFloat64(elem)
			if !ok {
				return nil, false
			}
			result[i] = f
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceUint(val any) ([]uint, bool) {
	switch v := val.(type) {
	case []uint:
		return v, true
	case []any:
		result := make([]uint, len(v))
		for i, elem := range v {
			u, ok := ToUint(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceUint8(val any) ([]uint8, bool) {
	switch v := val.(type) {
	case []uint8:
		return v, true
	case []any:
		result := make([]uint8, len(v))
		for i, elem := range v {
			u, ok := ToUint8(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceUint16(val any) ([]uint16, bool) {
	switch v := val.(type) {
	case []uint16:
		return v, true
	case []any:
		result := make([]uint16, len(v))
		for i, elem := range v {
			u, ok := ToUint16(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceUint32(val any) ([]uint32, bool) {
	switch v := val.(type) {
	case []uint32:
		return v, true
	case []any:
		result := make([]uint32, len(v))
		for i, elem := range v {
			u, ok := ToUint32(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceUint64(val any) ([]uint64, bool) {
	switch v := val.(type) {
	case []uint64:
		return v, true
	case []any:
		result := make([]uint64, len(v))
		for i, elem := range v {
			u, ok := ToUint64(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceInt(val any) ([]int, bool) {
	switch v := val.(type) {
	case []int:
		return v, true
	case []any:
		result := make([]int, len(v))
		for i, elem := range v {
			u, ok := ToInt(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceInt8(val any) ([]int8, bool) {
	switch v := val.(type) {
	case []int8:
		return v, true
	case []any:
		result := make([]int8, len(v))
		for i, elem := range v {
			u, ok := ToInt8(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceInt16(val any) ([]int16, bool) {
	switch v := val.(type) {
	case []int16:
		return v, true
	case []any:
		result := make([]int16, len(v))
		for i, elem := range v {
			u, ok := ToInt16(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceInt32(val any) ([]int32, bool) {
	switch v := val.(type) {
	case []int32:
		return v, true
	case []any:
		result := make([]int32, len(v))
		for i, elem := range v {
			u, ok := ToInt32(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}

func ToSliceInt64(val any) ([]int64, bool) {
	switch v := val.(type) {
	case []int64:
		return v, true
	case []any:
		result := make([]int64, len(v))
		for i, elem := range v {
			u, ok := ToInt64(elem)
			if !ok {
				return nil, false
			}
			result[i] = u
		}
		return result, true
	default:
		return nil, false
	}
}
