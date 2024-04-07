package web

import (
	"fmt"

	"github.com/oarkflow/frame"
	"github.com/oarkflow/frame/pkg/protocol/consts"
)

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
