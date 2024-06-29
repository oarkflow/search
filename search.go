package search

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/oarkflow/gopool"
	"github.com/oarkflow/gopool/spinlock"
	"github.com/oarkflow/log"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/filters"

	"github.com/oarkflow/maps"

	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/storage"
	"github.com/oarkflow/search/tokenizer"
)

const (
	AND Mode = "AND"
	OR  Mode = "OR"
)

const WILDCARD = "*"

type Mode string

type SchemaProps any

type Record[Schema SchemaProps] struct {
	Id   int64  `json:"id"`
	Data Schema `json:"data"`
}

type InsertParams[Schema SchemaProps] struct {
	Document Schema
	Language tokenizer.Language
}

type InsertBatchParams[Schema SchemaProps] struct {
	Documents []Schema
	BatchSize int
	Language  tokenizer.Language
}

type UpdateParams[Schema SchemaProps] struct {
	Id       int64
	Document Schema
	Language tokenizer.Language
}

type DeleteParams[Schema SchemaProps] struct {
	Id       int64
	Language tokenizer.Language
}

type Params struct {
	Filters    []*filters.Filter  `json:"filters"`
	Query      string             `json:"query"`
	Condition  string             `json:"condition"`
	Properties []string           `json:"properties"`
	BoolMode   Mode               `json:"boolMode"`
	Exact      bool               `json:"exact"`
	Tolerance  int                `json:"tolerance"`
	Relevance  BM25Params         `json:"relevance"`
	Paginate   bool               `json:"paginate"`
	Offset     int                `json:"offset"`
	Limit      int                `json:"limit"`
	Language   tokenizer.Language `json:"lang"`
}

func (p *Params) ToInt64() uint64 {
	bt, err := json.Marshal(p)
	if err != nil {
		return 0
	}
	f := fnv.New64()
	_, _ = f.Write(bt)
	return f.Sum64()
}

type BM25Params struct {
	K float64 `json:"k"`
	B float64 `json:"b"`
	D float64 `json:"d"`
}

type Result[Schema SchemaProps] struct {
	Hits    Hits[Schema] `json:"hits"`
	Count   int          `json:"count"`
	Total   int          `json:"total"`
	Message string       `json:"message"`
}

type Hit[Schema SchemaProps] struct {
	Id    int64   `json:"id"`
	Data  Schema  `json:"data"`
	Score float64 `json:"score"`
}

type Hits[Schema SchemaProps] []Hit[Schema]

func (r Hits[Schema]) Len() int { return len(r) }

func (r Hits[Schema]) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

func (r Hits[Schema]) Less(i, j int) bool { return r[i].Score > r[j].Score }

type Config struct {
	Key             string             `json:"key"`
	Storage         string             `json:"storage"`
	DefaultLanguage tokenizer.Language `json:"default_language"`
	TokenizerConfig *tokenizer.Config
	IndexKeys       []string        `json:"index_keys"`
	FieldsToStore   []string        `json:"fields_to_store"`
	FieldsToExclude []string        `json:"fields_to_exclude"`
	Rules           map[string]bool `json:"rules"`
	SliceField      string          `json:"slice_field"`
	Path            string          `json:"path"`
	Compress        bool            `json:"compress"`
	ResetPath       bool            `json:"reset_path"`
	SampleSize      int             `json:"sample_size"`
}

type Engine[Schema SchemaProps] struct {
	mutex           sync.RWMutex
	documents       storage.Store[int64, Schema]
	indexes         maps.IMap[string, *Index]
	indexKeys       []string
	defaultLanguage tokenizer.Language
	tokenizerConfig *tokenizer.Config
	rules           map[string]bool
	cache           maps.IMap[uint64, map[int64]float64]
	key             string
	sliceField      string
	path            string
	cfg             *Config
}

func getStore[Schema SchemaProps](c *Config) (storage.Store[int64, Schema], error) {
	if c.SampleSize == 0 {
		c.SampleSize = 20
	}
	switch c.Storage {
	case "flydb":
		return storage.NewFlyDB[int64, Schema](c.Path, c.Compress, c.SampleSize)
	default:
		return storage.NewMemDB[int64, Schema](c.SampleSize)
	}
}

func New[Schema SchemaProps](cfg ...*Config) (*Engine[Schema], error) {
	c := &Config{}
	if len(cfg) > 0 {
		c = cfg[0]
	}
	if c.TokenizerConfig == nil {
		c.TokenizerConfig = &tokenizer.Config{
			EnableStemming:  true,
			EnableStopWords: true,
		}
	}
	if c.DefaultLanguage == "" {
		c.DefaultLanguage = tokenizer.ENGLISH
	}
	if c.Key == "" {
		c.Key = xid.New().String()
	}
	if c.Path != "" {
		c.Path = filepath.Join(DefaultPath, c.Path, c.Key)
	} else {
		c.Path = filepath.Join(DefaultPath, c.Key)
	}
	if c.ResetPath {
		err := os.RemoveAll(c.Path)
		if err != nil {
			return nil, err
		}
	}
	store, err := getStore[Schema](c)
	if err != nil {
		return nil, err
	}
	db := &Engine[Schema]{
		key:             c.Key,
		documents:       store,
		indexes:         maps.New[string, *Index](),
		defaultLanguage: c.DefaultLanguage,
		tokenizerConfig: c.TokenizerConfig,
		rules:           c.Rules,
		sliceField:      c.SliceField,
		path:            c.Path,
		cfg:             c,
	}
	db.buildIndexes()
	if len(db.indexKeys) == 0 {
		db.addIndexes(c.IndexKeys)
	}
	return db, nil
}

func (db *Engine[Schema]) Compress() error {
	/*err := lib.CompressFolder(db.path, db.path+".zip")
	if err != nil {
		return err
	}
	return os.RemoveAll(db.path)*/
	return nil
}

func (db *Engine[Schema]) Metadata() map[string]any {
	cfg := map[string]any{
		"key":               db.key,
		"index_keys":        db.indexKeys,
		"language":          db.defaultLanguage,
		"storage":           db.cfg.Storage,
		"fields_to_store":   db.cfg.FieldsToStore,
		"fields_to_exclude": db.cfg.FieldsToExclude,
	}
	return cfg
}

func (db *Engine[Schema]) GetDocument(id int64) (Schema, bool) {
	return db.documents.Get(id)
}

func (db *Engine[Schema]) DelDocument(id int64) error {
	return db.documents.Del(id)
}

func (db *Engine[Schema]) SetDocument(id int64, doc Schema) error {
	return db.documents.Set(id, doc)
}

func (db *Engine[Schema]) DocumentLen() int {
	return int(db.documents.Len())
}

func (db *Engine[Schema]) buildIndexes() {
	var s Schema
	for key := range db.flattenSchema(s) {
		db.addIndex(key)
	}
}

func (db *Engine[Schema]) Insert(doc Schema, lang ...tokenizer.Language) (Record[Schema], error) {
	if len(db.cfg.FieldsToStore) > 0 {
		switch doc := any(doc).(type) {
		case map[string]any:
			for k := range doc {
				if !slices.Contains(db.cfg.FieldsToStore, k) {
					delete(doc, k)
				}
			}
		case map[any]any:
			for k := range doc {
				switch k := k.(type) {
				case string:
					if !slices.Contains(db.cfg.FieldsToStore, k) {
						delete(doc, k)
					}
				}
			}
		}
	}
	if len(db.cfg.FieldsToExclude) > 0 {
		switch doc := any(doc).(type) {
		case map[string]any:
			for k := range doc {
				if slices.Contains(db.cfg.FieldsToExclude, k) {
					delete(doc, k)
				}
			}
		case map[any]any:
			for k := range doc {
				switch k := k.(type) {
				case string:
					if slices.Contains(db.cfg.FieldsToExclude, k) {
						delete(doc, k)
					}
				}
			}
		}
	}
	if len(db.indexKeys) == 0 {
		indexKeys := DocFields(doc)
		db.addIndexes(indexKeys)
	}
	language := tokenizer.ENGLISH
	if len(lang) > 0 {
		language = lang[0]
	}
	id := xid.New().Int64()
	document := db.flattenSchema(doc)

	if language == "" {
		language = db.defaultLanguage

	} else if !tokenizer.IsSupportedLanguage(language) {
		return Record[Schema]{}, fmt.Errorf("not supported language")
	}

	err := db.SetDocument(id, doc)
	if err != nil {
		return Record[Schema]{}, err
	}
	db.indexDocument(id, document, language)
	return Record[Schema]{Id: id, Data: doc}, nil
}

func (db *Engine[Schema]) InsertWithPool(docs []Schema, noOfWorker, batchSize int, lang ...tokenizer.Language) []error {
	docLen := len(docs)
	if docLen == 0 {
		return nil
	}
	var errs []error
	pool := gopool.NewGoPool(noOfWorker,
		gopool.WithTaskQueueSize(batchSize),
		gopool.WithLock(new(spinlock.SpinLock)),
		gopool.WithErrorCallback(func(err error) {
			errs = append(errs, err)
		}),
	)
	defer pool.Release()
	language := tokenizer.ENGLISH
	if len(lang) > 0 {
		language = lang[0]
	}
	for i, doc := range docs {
		if i == 0 {
			// Checking for first doc to make sure indexKeys are added on first Insert
			// if not already exists
			_, err := db.Insert(doc, language)
			if err != nil {
				errs = append(errs, err)
			}
		} else {
			pool.AddTask(func() (interface{}, error) {
				return db.Insert(doc, language)
			})
		}
	}
	pool.Wait()
	return errs
}

func (db *Engine[Schema]) Update(params *UpdateParams[Schema]) (Record[Schema], error) {
	document := db.flattenSchema(params.Document)

	language := params.Language
	if language == "" {
		language = db.defaultLanguage

	} else if !tokenizer.IsSupportedLanguage(language) {
		return Record[Schema]{}, fmt.Errorf("not supported language")
	}

	oldDocument, ok := db.GetDocument(params.Id)
	if !ok {
		return Record[Schema]{}, fmt.Errorf("document not found")
	}
	db.indexDocument(params.Id, document, language)
	document = db.flattenSchema(oldDocument)
	db.deindexDocument(params.Id, document, language)
	err := db.SetDocument(params.Id, params.Document)
	if err != nil {
		return Record[Schema]{}, err
	}
	return Record[Schema]{Id: params.Id, Data: params.Document}, nil
}

func (db *Engine[Schema]) Delete(params *DeleteParams[Schema]) error {
	language := params.Language
	if language == "" {
		language = db.defaultLanguage

	} else if !tokenizer.IsSupportedLanguage(language) {
		return fmt.Errorf("not supported language")
	}

	document, ok := db.GetDocument(params.Id)
	if !ok {
		return fmt.Errorf("document not found")
	}
	doc := db.flattenSchema(document)
	db.deindexDocument(params.Id, doc, language)
	return db.DelDocument(params.Id)
}

func (db *Engine[Schema]) ClearCache() {
	db.cache = nil
}

// CheckCondition function checks if a key-value map exists in any type of data
func (db *Engine[Schema]) CheckCondition(data Schema, filter *filters.Sequence) bool {
	return filter.Match(data)
}

// Check function checks if a key-value map exists in any type of data
func (db *Engine[Schema]) Check(data Schema, filter []*filters.Filter) bool {
	return filters.MatchGroup(data, &filters.FilterGroup{Operator: filters.AND, Filters: filter})
}

func (db *Engine[Schema]) Sample(params storage.SampleParams) (Result[Schema], error) {
	results := make(Hits[Schema], 0)
	sampleDocs, err := db.documents.Sample(params)
	if err != nil {
		return Result[Schema]{}, err
	}
	for k, doc := range sampleDocs {
		parseInt, _ := strconv.ParseInt(k, 10, 64)
		results = append(results, Hit[Schema]{Id: parseInt, Data: doc, Score: 0})
	}
	return db.prepareResult(results, &Params{Paginate: false})
}

func ProcessQueryAndFilters(params *Params, seq *filters.Sequence) {
	var extraFilters []*filters.Filter
	if params.Query != "" {
		for _, filter := range params.Filters {
			if filter.Field != "q" {
				extraFilters = append(extraFilters, filter)
			}
		}
		params.Filters = extraFilters
		return
	}
	foundQ := false
	for _, filter := range params.Filters {
		if filter.Field == "q" {
			params.Query = fmt.Sprintf("%v", filter.Value)
			params.Properties = append(params.Properties, filter.Field)
			foundQ = true
		} else {
			extraFilters = append(extraFilters, filter)
		}
	}

	if !foundQ {
		for _, filter := range extraFilters {
			if filter.Operator == filters.Equal {
				params.Query = fmt.Sprintf("%v", filter.Value)
				params.Properties = append(params.Properties, filter.Field)
				extraFilters = removeFilter(extraFilters, filter)
				break
			}
		}
	}
	if params.Query == "" && seq != nil {
		firstFilter, err := filters.FirstTermFilter(seq)
		if err == nil {
			params.Query = fmt.Sprintf("%v", firstFilter.Value)
			params.Properties = append(params.Properties, firstFilter.Field)
		}
	}

	// Update the filters in params
	params.Filters = extraFilters
}

func removeFilter(filters []*filters.Filter, filterToRemove *filters.Filter) []*filters.Filter {
	for i, filter := range filters {
		if filter == filterToRemove {
			return append(filters[:i], filters[i+1:]...)
		}
	}
	return filters
}

// Search - uses params to search
func (db *Engine[Schema]) Search(params *Params) (Result[Schema], error) {
	if db.cache == nil {
		db.cache = maps.New[uint64, map[int64]float64]()
	}
	cachedKey := params.ToInt64()
	if cachedKey != 0 {
		if scores, ok := db.cache.Get(cachedKey); ok && scores != nil {
			if len(scores) > 0 {
				return db.prepareResult(db.getDocuments(scores), params)
			}
		}
	}
	if params.BoolMode == "" {
		params.BoolMode = AND
	}
	var filter *filters.Sequence
	var err error
	if params.Condition != "" {
		filter, err = filters.ParseSQL(params.Condition)
		if err != nil {
			log.Error().Err(err).Msg("Unable to parse condition")
		}
	}
	ProcessQueryAndFilters(params, filter)
	if params.Query == "" && len(params.Filters) == 0 {
		return db.sampleWithFilters(storage.SampleParams{Size: params.Limit, Sequence: filter, Filters: params.Filters})
	}
	allIdScores, err := db.findWithParams(params)
	if err != nil {
		return Result[Schema]{}, err
	}
	if len(allIdScores) == 0 && params.Query == "" {
		return db.sampleWithFilters(storage.SampleParams{Size: params.Limit, Sequence: filter, Filters: params.Filters})
	}
	results := make(Hits[Schema], 0)
	cache := make(map[int64]float64)
	defer func() {
		results = nil
		cache = nil
	}()
	for id, score := range allIdScores {
		doc, ok := db.GetDocument(id)
		if !ok {
			continue
		}
		if len(params.Filters) == 0 && params.Condition == "" {
			cache[id] = score
			results = append(results, Hit[Schema]{Id: id, Data: doc, Score: score})
			continue
		}
		if params.Condition != "" {
			if db.CheckCondition(doc, filter) {
				cache[id] = score
				results = append(results, Hit[Schema]{Id: id, Data: doc, Score: score})
			}
		} else if db.Check(doc, params.Filters) {
			cache[id] = score
			results = append(results, Hit[Schema]{Id: id, Data: doc, Score: score})
		}
	}
	if cachedKey != 0 && len(cache) > 0 {
		db.cache.Set(cachedKey, cache)
	}
	return db.prepareResult(results, params)
}

func (db *Engine[Schema]) addIndexes(keys []string) {
	for _, key := range keys {
		db.addIndex(key)
	}
}

func (db *Engine[Schema]) sampleWithFilters(params storage.SampleParams) (Result[Schema], error) {
	rs, err := db.Sample(params)
	if err != nil {
		return rs, err
	}
	if params.Filters == nil && params.Sequence == nil {
		rs.Message = "[WARN] - Query or Filters not applied"
	}
	return rs, nil
}

func (db *Engine[Schema]) addIndex(key string) {
	db.indexes.Set(key, NewIndex())
	db.indexKeys = lib.Unique(append(db.indexKeys, key))
}

func (db *Engine[Schema]) findWithParams(params *Params) (map[int64]float64, error) {
	allIdScores := make(map[int64]float64)

	properties := params.Properties
	if len(params.Properties) == 0 {
		properties = db.indexKeys
	}
	language := params.Language
	if language == "" {
		language = db.defaultLanguage
	} else if !tokenizer.IsSupportedLanguage(language) {
		return nil, fmt.Errorf("not supported language")
	}
	if language == "" {
		language = tokenizer.ENGLISH
	}
	tokens := tokensPool.Get()
	clear(tokens)
	tokenizer.Tokenize(tokenizer.TokenizeParams{
		Text:            params.Query,
		Language:        language,
		AllowDuplicates: false,
	}, *db.tokenizerConfig, tokens)
	for _, prop := range properties {
		index, ok := db.indexes.Get(prop)
		if !ok {
			continue
		}
		idScores := index.Find(&FindParams{
			Tokens:    tokens,
			BoolMode:  params.BoolMode,
			Exact:     params.Exact,
			Tolerance: params.Tolerance,
			Relevance: params.Relevance,
			DocsCount: db.DocumentLen(),
		})
		for id, score := range idScores {
			allIdScores[id] += score
		}
	}
	clear(tokens)
	tokensPool.Put(tokens)
	return allIdScores, nil
}

func (db *Engine[Schema]) prepareResult(results Hits[Schema], params *Params) (Result[Schema], error) {
	sort.Sort(results)
	if !params.Paginate {
		return Result[Schema]{Hits: results, Count: len(results), Total: db.DocumentLen()}, nil
	}
	if params.Limit == 0 {
		params.Limit = 20
	}
	start, stop := lib.Paginate(params.Offset, params.Limit, len(results))
	return Result[Schema]{Hits: results[start:stop], Count: len(results), Total: db.DocumentLen()}, nil
}

func (db *Engine[Schema]) getDocuments(scores map[int64]float64) Hits[Schema] {
	results := make(Hits[Schema], 0)
	for id, score := range scores {
		if doc, ok := db.GetDocument(id); ok {
			results = append(results, Hit[Schema]{Id: id, Data: doc, Score: score})
		}
	}
	return results
}

func (db *Engine[Schema]) indexDocument(id int64, document map[string]any, language tokenizer.Language) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.indexes.ForEach(func(propName string, index *Index) bool {
		text := lib.ToString(document[propName])
		tokens := tokensPool.Get()
		clear(tokens)
		tokenizer.Tokenize(tokenizer.TokenizeParams{
			Text:            text,
			Language:        language,
			AllowDuplicates: true,
		}, *db.tokenizerConfig, tokens)
		index.Insert(id, tokens, db.DocumentLen())
		clear(tokens)
		tokensPool.Put(tokens)
		return true
	})
}

func (db *Engine[Schema]) deindexDocument(id int64, document map[string]any, language tokenizer.Language) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.indexes.ForEach(func(propName string, index *Index) bool {
		tokens := tokensPool.Get()
		clear(tokens)
		tokenizer.Tokenize(tokenizer.TokenizeParams{
			Text:            lib.ToString(document[propName]),
			Language:        language,
			AllowDuplicates: false,
		}, *db.tokenizerConfig, tokens)
		index.Delete(id, tokens, db.DocumentLen())
		clear(tokens)
		tokensPool.Put(tokens)
		return true
	})
}

func (db *Engine[Schema]) getFieldsFromMap(obj any) map[string]any {
	m, ok := obj.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	if db.rules == nil {
		return m
	}
	fields := make(map[string]any)
	rules := db.rules
	for field, val := range m {
		if rules == nil || rules[field] {
			if str, ok := val.(string); ok {
				fields[field] = str
			} else {
				fields[field] = val
			}
		}
	}
	return fields
}

func (db *Engine[Schema]) getFieldsFromStruct(obj any, prefix ...string) map[string]any {
	fields := make(map[string]any)
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	visibleFields := reflect.VisibleFields(t)
	hasIndexField := false
	for i, field := range visibleFields {
		if propName, ok := field.Tag.Lookup("index"); ok {
			hasIndexField = true
			if len(prefix) == 1 {
				propName = fmt.Sprintf("%s.%s", prefix[0], propName)
			}
			if field.Type.Kind() == reflect.Struct {
				for key, value := range db.flattenSchema(v.Field(i).Interface(), propName) {
					fields[key] = value
				}
			} else {
				fields[propName] = v.Field(i).String()
			}
		}
	}
	if !hasIndexField {
		for i, field := range visibleFields {
			propName := field.Name
			if len(prefix) == 1 {
				propName = fmt.Sprintf("%s.%s", prefix[0], propName)
			}

			if field.Type.Kind() == reflect.Struct {
				for key, value := range db.flattenSchema(v.Field(i).Interface(), propName) {
					fields[key] = value
				}
			} else {
				fields[propName] = v.Field(i).String()
			}
		}
	}
	return fields
}

func (db *Engine[Schema]) flattenSchema(obj any, prefix ...string) map[string]any {
	if obj == nil {
		return nil
	}
	switch reflect.TypeOf(obj).Kind() {
	case reflect.Struct:
		return db.getFieldsFromStruct(obj, prefix...)
	case reflect.Map:
		return db.getFieldsFromMap(obj)
	default:
		return map[string]any{
			db.sliceField: fmt.Sprint(obj),
		}
	}
}

func getFieldsFromMap(obj map[string]any) []string {
	var fields []string
	rules := make(map[string]bool)
	for field, val := range obj {
		if reflect.TypeOf(field).Kind() == reflect.Map {
			for _, key := range DocFields(val, field) {
				fields = append(fields, key)
			}
		} else {
			if len(rules) > 0 {
				if canIndex, ok := rules[field]; ok && canIndex {
					fields = append(fields, field)
				}
			} else {
				fields = append(fields, field)
			}
		}
	}
	return fields
}

func getFieldsFromStruct(obj any, prefix ...string) []string {
	var fields []string
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	visibleFields := reflect.VisibleFields(t)
	hasIndexField := false
	for i, field := range visibleFields {
		if propName, ok := field.Tag.Lookup("index"); ok {
			hasIndexField = true
			if len(prefix) == 1 {
				propName = fmt.Sprintf("%s.%s", prefix[0], propName)
			}

			if field.Type.Kind() == reflect.Struct {
				for _, key := range DocFields(v.Field(i).Interface(), propName) {
					fields = append(fields, key)
				}
			} else {
				fields = append(fields, propName)
			}
		}
	}

	if !hasIndexField {
		for i, field := range visibleFields {
			propName := field.Name
			if len(prefix) == 1 {
				propName = fmt.Sprintf("%s.%s", prefix[0], propName)
			}

			if field.Type.Kind() == reflect.Struct {
				for _, key := range DocFields(v.Field(i).Interface(), propName) {
					fields = append(fields, key)
				}
			} else {
				fields = append(fields, propName)
			}
		}
	}
	return fields
}

func DocFields(obj any, prefix ...string) []string {
	if obj == nil {
		return nil
	}

	switch obj := obj.(type) {
	case map[string]any:
		return getFieldsFromMap(obj)
	case map[string]string:
		data := make(map[string]any)
		for k, v := range obj {
			data[k] = v
		}
		return getFieldsFromMap(data)
	default:
		switch obj := obj.(type) {
		case map[string]any:
			return getFieldsFromMap(obj)
		case map[string]string:
			data := make(map[string]any)
			for k, v := range obj {
				data[k] = v
			}
			return getFieldsFromMap(data)
		default:
			return getFieldsFromStruct(obj, prefix...)
		}
	}
}
