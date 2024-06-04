package web

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/frame/pkg/protocol/consts"
	"github.com/oarkflow/frame/server"
	"github.com/oarkflow/metadata"

	"github.com/oarkflow/filters"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/tokenizer"

	"github.com/oarkflow/frame"
	"github.com/oarkflow/frame/pkg/common/utils"
	"github.com/oarkflow/frame/pkg/route"
)

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

func Index[Schema search.SchemaProps](key string, data Schema, eng ...*search.Engine[Schema]) (search.Record[Schema], error) {
	var err error
	var engine *search.Engine[Schema]
	if len(eng) > 0 {
		engine = eng[0]
	} else {
		engine, err = search.GetEngine[Schema](key)
		if err != nil {
			return search.Record[Schema]{}, err
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

func (f *FulltextController) NewEngine(_ context.Context, ctx *frame.Context) {
	var req NewEngine
	err := ctx.Bind(&req)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	if req.Key == "" {
		Failed(ctx, consts.StatusBadRequest, "Key not provided", nil)
		return
	}
	cfg := search.GetConfig(req.Key)
	cfg.Storage = req.Storage
	cfg.IndexKeys = req.FieldsToIndex
	cfg.Storage = req.Storage
	cfg.FieldsToStore = req.FieldsToStore
	cfg.FieldsToExclude = req.FieldsToExclude
	cfg.Compress = req.Compress
	cfg.ResetPath = req.Reset
	_, err = search.SetEngine[map[string]any](req.Key, cfg)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	Success(ctx, consts.StatusOK, utils.H{"key": req.Key, "index_fields": req.FieldsToIndex}, "New fulltext data list added")
}

func (f *FulltextController) IndexInBatch(_ context.Context, ctx *frame.Context) {
	var req IndexInBatchRequest
	keyType := ctx.Param("type")
	err := ctx.Bind(&req)
	if err != nil || keyType == "" || req.Data == nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	engine, err := search.GetEngine[map[string]any](keyType)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	var records []search.Record[map[string]any]
	for _, data := range req.Data {
		record, err := Index[map[string]any](keyType, data, engine)
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
	var extra []filters.Filter
	err = ctx.Bind(&extra)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	if len(extra) == 0 {
		extra, err = filters.ParseQuery(ctx.QueryArgs().String())
		if err != nil {
			Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
			return
		}
	}

	keyType := ctx.Param("type")
	engine, err := search.GetEngine[map[string]any](keyType)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	if extra != nil && query.Filters == nil {
		query.Filters = extra
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
		Filters:    query.Filters,
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
		switch d := any(record.Data).(type) {
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

func (f *FulltextController) IndexFromDatabase(_ context.Context, ctx *frame.Context) {
	var dbConfig Database
	err := ctx.Bind(&dbConfig)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}

	con := metadata.New(metadata.Config{
		Name:     dbConfig.IndexKey,
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		Driver:   dbConfig.Driver,
		Username: dbConfig.Username,
		Password: dbConfig.Password,
		Database: dbConfig.Database,
		SslMode:  dbConfig.SslMode,
	})
	db, err := con.Connect()
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	start := time.Now()
	go func(db metadata.DataSource, dbConfig Database, start time.Time) {
		if dbConfig.Query != "" && strings.Contains(dbConfig.Query, "LIMIT") {
			err := IndexFromDB(db, dbConfig, start)
			if err != nil {
				fmt.Println(err.Error())
			}
		} else {
			err := IndexFromDBWithPaginate(db, dbConfig, start)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}(db, dbConfig, start)
	Success(ctx, consts.StatusOK, utils.H{
		"index_key":  dbConfig.IndexKey,
		"started_at": start,
	}, "Indexing started in background")
}

func SearchRoutes(route route.IRouter) route.IRouter {
	route.POST("/new", controller.NewEngine)
	route.POST("/database/index", controller.IndexFromDatabase)
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
