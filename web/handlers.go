package web

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/oarkflow/frame/middlewares/server/cors"
	"github.com/oarkflow/frame/middlewares/server/monitor"
	"github.com/oarkflow/frame/pkg/protocol/consts"
	"github.com/oarkflow/frame/server"
	"github.com/oarkflow/log"
	"github.com/oarkflow/metadata"

	"github.com/oarkflow/filters"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
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

func (f *FulltextController) Metadata(_ context.Context, ctx *frame.Context) {
	keyType := ctx.Param("type")
	engine, err := search.GetEngine[map[string]any](keyType)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	details := engine.Metadata()
	details["key"] = keyType
	Success(ctx, consts.StatusOK, details)
}

func (f *FulltextController) ClearCache(_ context.Context, ctx *frame.Context) {
	keyType := ctx.Param("type")
	engine, err := search.GetEngine[map[string]any](keyType)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	engine.ClearCache()
	Success(ctx, consts.StatusOK, nil, "Cache cleared...")
}

func (f *FulltextController) IndexTypes(_ context.Context, ctx *frame.Context) {
	Success(ctx, consts.StatusOK, search.AvailableEngines[map[string]any]())
}

func (f *FulltextController) EngineIndex(_ context.Context, ctx *frame.Context) {
	Success(ctx, consts.StatusOK, search.Engines())
}

func (f *FulltextController) NewEngine(_ context.Context, ctx *frame.Context) {
	var req Options
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
	cfg.IndexKeys = req.FieldsToIndex
	cfg.FieldsToStore = req.FieldsToStore
	cfg.FieldsToExclude = req.FieldsToExclude
	cfg.Compress = req.Compress
	cfg.ResetPath = req.Reset
	cfg.SortOrder = req.SortOrder
	cfg.SortField = req.SortField
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

var builtInFields = []string{"q", "m", "l", "f", "t", "o", "s", "e", "condition", "sort_field", "sort_order"}

func (f *FulltextController) prepareQuery(ctx *frame.Context) (Query, error) {
	var query Query
	err := ctx.Bind(&query)
	if err != nil {
		return query, err
	}
	var extraMap map[string]any
	var extra []*filters.Filter
	err = ctx.Bind(&extra)
	if err != nil {
		return query, err
	}
	err = ctx.Bind(&extraMap)
	if err != nil {
		return query, err
	}
	if extraMap != nil {
		for k, v := range extraMap {
			if slices.Contains(builtInFields, k) {
				delete(extraMap, k)
			} else {
				vt := reflect.TypeOf(v).Kind()
				if vt == reflect.Slice {
					extra = append(extra, &filters.Filter{
						Field:    k,
						Operator: filters.In,
						Value:    v,
					})
				} else {
					extra = append(extra, &filters.Filter{
						Field:    k,
						Operator: filters.Equal,
						Value:    v,
					})
				}
			}
		}
	}
	if len(extra) == 0 {
		extra, err = filters.ParseQuery(ctx.QueryArgs().String(), builtInFields...)
		if err != nil {
			return query, err
		}
	}
	if extra != nil && query.Filters == nil {
		query.Filters = extra
	}

	if strings.ToLower(query.Match) == "any" {
		query.Match = "OR"
	} else {
		query.Match = "AND"
	}
	return query, nil
}

func (f *FulltextController) Search(_ context.Context, ctx *frame.Context) {
	query, err := f.prepareQuery(ctx)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	start := time.Now()
	before := lib.Stats()
	keyType := ctx.Param("type")
	records, result, err := f.search(keyType, query)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	after := lib.Stats()
	log.Info().
		Int("filtered_total", result.FilteredTotal).
		Int("total", result.Total).
		Int("count", result.Count).
		Str("latency", fmt.Sprintf("%s", time.Since(start))).
		Str("memory_usage", fmt.Sprintf("%dMB", after-before)).
		Str("fts_key", keyType).
		Msg("Searching completed")
	Success(ctx, consts.StatusOK, utils.H{
		keyType:          records,
		"count":          result.Count,
		"filtered_total": result.FilteredTotal,
		"total":          result.Total,
	}, result.Message)
}

func (f *FulltextController) GlobalSearch(c context.Context, ctx *frame.Context) {
	query, err := f.prepareQuery(ctx)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
	query.Size = 10
	var rs []utils.H
	for _, keyType := range search.Engines() {
		records, result, err := f.search(keyType.Key, query)
		if err != nil {
			Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
			return
		}
		rs = append(rs, utils.H{
			keyType.Key:      records,
			"count":          result.Count,
			"filtered_total": result.FilteredTotal,
			"total":          result.Total,
		})
	}
	Success(ctx, consts.StatusOK, rs)
}

func (f *FulltextController) search(keyType string, query Query) ([]map[string]any, search.Result[map[string]any], error) {
	engine, err := search.GetEngine[map[string]any](keyType)
	if err != nil {
		return nil, search.Result[map[string]any]{}, err
	}
	params := &search.Params{
		Query:      query.Query,
		Limit:      query.Size,
		Offset:     query.Offset,
		Condition:  query.Condition,
		Language:   tokenizer.Language(query.Language),
		BoolMode:   search.Mode(query.Match),
		Properties: query.Fields,
		Exact:      query.Exact,
		Filters:    query.Filters,
		SortField:  query.SortField,
		SortOrder:  query.SortOrder,
	}
	if params.Limit == 0 {
		params.Limit = 100
	}
	params.Paginate = true
	var records []map[string]any
	result, err := engine.Search(params)
	if err != nil {
		return nil, search.Result[map[string]any]{}, err
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
	return records, result, nil
}

func (f *FulltextController) TotalDocuments(_ context.Context, ctx *frame.Context) {
	keyType := ctx.Param("type")
	engine, err := search.GetEngine[any](keyType)
	if err != nil {
		Failed(ctx, consts.StatusBadRequest, err.Error(), nil)
		return
	}
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

func SearchRoutes(route route.IRouter, assetPath ...string) route.IRouter {
	root := "./dist"
	if len(assetPath) > 0 {
		root = assetPath[0]
	}
	_, file, _, ok := runtime.Caller(0)
	if ok {
		dir := filepath.Dir(file)
		root = filepath.Join(dir, root)
	}
	route.StaticFS("/", &frame.FS{
		Root:       root,
		IndexNames: []string{"index.html"},
		Compress:   true,
	})
	route.POST("/new", controller.NewEngine)
	route.GET("/engines", controller.EngineIndex)
	route.GET("/types", controller.IndexTypes)
	route.GET("/count/:type", controller.TotalDocuments)
	route.POST("/index/:type", controller.Index)
	route.GET("/search/:type", controller.Search)
	route.POST("/search/:type", controller.Search)
	route.POST("/global/search", controller.GlobalSearch)
	route.GET("/metadata/:type", controller.Metadata)
	route.POST("/database/index", controller.IndexFromDatabase)
	route.POST("/cache/:type/clear", controller.ClearCache)
	route.POST("/index/:type/batch", controller.IndexInBatch)
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
	srv.Use(cors.Default())
	srv.GET("/monitor", monitor.New())
	SearchRoutes(srv.Group(prefix))
	srv.Spin()
}
