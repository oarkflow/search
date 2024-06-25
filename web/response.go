package web

import (
	"fmt"
	"reflect"
	"time"

	"github.com/oarkflow/frame"
	"github.com/oarkflow/frame/pkg/protocol/consts"
)

var TimeFormat = time.DateOnly

type Response struct {
	Additional any    `json:"additional,omitempty"`
	Data       any    `json:"data"`
	Message    string `json:"message,omitempty"`
	Code       int    `json:"code"`
	Success    bool   `json:"success"`
}

func getResponse(code int, message string, additional any, stackTrace ...string) Response {
	response := Response{
		Code:       code,
		Message:    message,
		Success:    false,
		Additional: additional,
	}

	return response
}

func Abort(ctx *frame.Context, code int, message string, additional any, stackTrace ...string) {
	ctx.AbortWithJSON(consts.StatusOK, getResponse(code, message, additional, stackTrace...))
}

func Failed(ctx *frame.Context, code int, message string, additional any, stackTrace ...string) {
	ctx.JSON(consts.StatusOK, getResponse(code, message, additional, stackTrace...))
}

func Success(ctx *frame.Context, code int, data any, message ...string) {
	reformatTimes(reflect.ValueOf(&data))
	response := Response{
		Code:    code,
		Data:    data,
		Success: true,
	}
	if len(message) > 0 {
		response.Message = message[0]
	}
	ctx.JSON(consts.StatusOK, response)
}

func File(ctx *frame.Context, data []byte, header string) {
	ctx.Bytes(200, data, header)
}

func DownloadBytes(ctx *frame.Context, data []byte, filename string, header string) {
	ctx.Response.Header.Set("content-disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	ctx.Bytes(200, data, header)
}

func Render(ctx *frame.Context, view string, data any, layouts ...string) {
	err := ctx.HTML(consts.StatusOK, view, data, layouts...)
	if err != nil {
		ctx.HTML(consts.StatusOK, "errors/404", data)
		return
	}
}

// Helper function to reformat time fields in a value using reflection
func reformatTimes(val reflect.Value) {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.Map:
		for _, key := range val.MapKeys() {
			mapValue := val.MapIndex(key)
			if mapValue.Kind() == reflect.Interface {
				mapValue = mapValue.Elem()
			}
			reformatTimes(mapValue)
		}
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			if field.Kind() == reflect.Struct && field.Type() == reflect.TypeOf(time.Time{}) {
				t := field.Interface().(time.Time)
				formatted := t.Format(TimeFormat)
				field.Set(reflect.ValueOf(formatted))
			} else {
				reformatTimes(field)
			}
		}
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			if elem.Kind() == reflect.Interface {
				elem = elem.Elem()
			}
			reformatTimes(elem)
		}
	case reflect.Interface:
		reformatTimes(val.Elem())
	}
}
