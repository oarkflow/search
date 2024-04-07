package web

import (
	"context"
	"slices"
	"strings"

	"github.com/oarkflow/frame/pkg/protocol/consts"
	"github.com/oarkflow/frame/server"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/tokenizer"

	"github.com/oarkflow/frame"
	"github.com/oarkflow/frame/pkg/common/utils"
	"github.com/oarkflow/frame/pkg/route"
)

type Query struct {
	Query     string         `json:"q" query:"q"`
	Match     string         `json:"m" query:"m"`
	Language  string         `json:"l" query:"l"`
	Fields    []string       `json:"f" query:"f"`
	Exact     bool           `json:"e" query:"e"`
	Tolerance int            `json:"t" query:"t"`
	Offset    int            `json:"o" query:"o"`
	Size      int            `json:"s" query:"s"`
	Extra     map[string]any `json:"extra"`
}

type IndexRequest struct {
	Data map[string]any `json:"data"`
}

type IndexInBatchRequest struct {
	Data []map[string]any `json:"data"`
}

type FulltextController struct{}

func NewFulltextController() *FulltextController {
	return &FulltextController{}
}

var controller = NewFulltextController()

func Index[Schema search.SchemaProps](key string, data Schema, eng ...*search.Engine[any]) (search.Record[any], error) {
	var err error
	var engine *search.Engine[any]
	if len(eng) > 0 {
		engine = eng[0]
	} else {
		engine, err = search.GetEngine[any](key)
		if err != nil {
			return search.Record[any]{}, err
		}
	}
	return engine.Insert(data)
}

func (f *FulltextController) Index(_ context.Context, ctx *frame.Context) {
	var req IndexRequest
	err := ctx.Bind(&req)
	keyType := ctx.Param("type")
	if err != nil || keyType == "" || req.Data == nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	record, err := Index(keyType, req.Data)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	Success(ctx, consts.StatusOK, record)
}

func (f *FulltextController) IndexInBatch(_ context.Context, ctx *frame.Context) {
	var req IndexInBatchRequest
	keyType := ctx.Param("type")
	err := ctx.Bind(&req)
	if err != nil || keyType == "" || req.Data == nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	engine, err := search.GetEngine[any](keyType)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	var records []search.Record[any]
	for _, data := range req.Data {
		record, err := Index[any](keyType, data, engine)
		if err != nil {
			Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
			return
		}
		records = append(records, record)
	}
	Success(ctx, consts.StatusOK, records)
}

func (f *FulltextController) Search(_ context.Context, ctx *frame.Context) {
	var query Query
	err := ctx.Bind(&query)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	var extra map[string]any
	err = ctx.Bind(&extra)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	keyType := ctx.Param("type")
	engine, err := search.GetEngine[any](keyType)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	if extra != nil && query.Extra == nil {
		query.Extra = extra
	}
	for key, val := range query.Extra {
		if slices.Contains([]string{"q", "s", "o", "m", "f", "e", "t"}, key) {
			delete(query.Extra, key)
		} else {
			switch val := val.(type) {
			case []any:
				if len(val) > 0 {
					query.Extra[key] = val[0]
				} else {
					delete(query.Extra, key)
				}
			}
		}
	}
	if strings.ToLower(query.Match) == "any" {
		query.Match = "OR"
	} else {
		query.Match = "AND"
	}
	params := &search.Params{
		Query:      query.Query,
		Limit:      query.Size,
		Offset:     query.Offset,
		Language:   tokenizer.Language(query.Language),
		BoolMode:   search.Mode(query.Match),
		Properties: query.Fields,
		Exact:      query.Exact,
		Tolerance:  query.Tolerance,
		Extra:      query.Extra,
	}

	if query.Size > 0 {
		params.Paginate = true
	}
	var records []map[string]any

	result, err := engine.Search(params)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	for _, record := range result.Hits {
		switch d := record.Data.(type) {
		case map[any]any:
			tmp := make(map[string]any)
			for k, v := range d {
				switch k := k.(type) {
				case string:
					tmp[k] = v
				}
			}
			records = append(records, tmp)
		case map[string]any:
			records = append(records, d)
		}
	}
	Success(ctx, consts.StatusOK, utils.H{
		keyType: records,
		"total": result.Count,
	})
}

func (f *FulltextController) TotalDocuments(_ context.Context, ctx *frame.Context) {
	keyType := ctx.Param("type")
	engine, err := search.GetEngine[any](keyType)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	engine.DocumentLen()
	Success(ctx, consts.StatusOK, utils.H{
		"count": engine.DocumentLen(),
	})
}

func SearchRoutes(route route.IRouter) route.IRouter {
	route.POST("/index/:type/batch", controller.IndexInBatch)
	route.POST("/index/:type", controller.Index)
	route.POST("/search/:type", controller.Search)
	route.GET("/search/:type", controller.Search)
	route.GET("/count/:type", controller.TotalDocuments)
	return route
}

func StartServer(addr string, routePrefix ...string) {
	prefix := "/"
	if len(routePrefix) > 0 {
		prefix = routePrefix[0]
	}
	srv := server.New(
		server.WithDisablePrintRoute(true),
		server.WithHostPorts(addr),
		server.WithHandleMethodNotAllowed(true),
		server.WithStreamBody(true),
	)
	SearchRoutes(srv.Group(prefix))
	srv.Spin()
}
