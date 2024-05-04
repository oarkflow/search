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

	"github.com/devchat-ai/gopool"
	"github.com/oarkflow/xid"

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
	Extra      map[string]any     `json:"extra"`
	Query      string             `json:"query"`
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
	Hits  Hits[Schema] `json:"hits"`
	Count int          `json:"count"`
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

func New[Schema SchemaProps](c *Config) (*Engine[Schema], error) {
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
	store, err := storage.NewFlyDB[int64, Schema](c.Path, c.Compress)
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
	err := lib.CompressFolder(db.path, db.path+".zip")
	if err != nil {
		return err
	}
	return os.RemoveAll(db.path)
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

func (db *Engine[Schema]) InsertBatch(docs []Schema, batchSize int, lang ...tokenizer.Language) []error {
	docLen := len(docs)
	if docLen == 0 {
		return nil
	}
	var errs []error
	pool := gopool.NewGoPool(batchSize, gopool.WithTaskQueueSize(1000), gopool.WithErrorCallback(func(err error) {
		errs = append(errs, err)
	}))
	defer pool.Release()
	language := tokenizer.ENGLISH
	if len(lang) > 0 {
		language = lang[0]
	}
	for _, doc := range docs {
		pool.AddTask(func() (interface{}, error) {
			db.Insert(doc, language)
			return nil, nil
		})
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

// Check function checks if a key-value map exists in any type of data
func (db *Engine[Schema]) Check(data Schema, filter map[string]any) bool {
	switch reflect.TypeOf(data).Kind() {
	case reflect.Map:
		dataMap := reflect.ValueOf(data)
		for key, value := range filter {
			keyValue := reflect.ValueOf(key)
			dataValue := dataMap.MapIndex(keyValue)
			if !dataValue.IsValid() || !lib.IsEqual(dataValue.Interface(), value) {
				return false
			}
		}
		return true

	case reflect.Struct:
		dataValue := reflect.ValueOf(data)
		for key, value := range filter {
			fieldValue := dataValue.FieldByName(key)
			if !dataValue.IsValid() || !lib.IsEqual(fieldValue.Interface(), value) {
				return false
			}
		}
		return true

	default:
		return false
	}
}

// Deprecated: use Search function instead. It's optimized version. This function will be removed in future version
func (db *Engine[Schema]) SearchOld(params *Params) (Result[Schema], error) {
	if db.cache == nil {
		db.cache = maps.New[uint64, map[int64]float64]()
	}
	cachedKey := params.ToInt64()
	if cachedKey != 0 {
		if score, ok := db.cache.Get(cachedKey); ok {
			return db.prepareResult(db.getDocuments(score), params)
		}
	}
	if params.Query == "" && len(params.Extra) > 0 {
		for key, val := range params.Extra {
			params.Query = fmt.Sprintf("%v", val)
			params.Properties = append(params.Properties, key)
			delete(params.Extra, key)
			break
		}
	}
	allIdScores, err := db.findWithParams(params)
	if err != nil {
		return Result[Schema]{}, err
	}
	if len(params.Extra) == 0 {
		if cachedKey != 0 {
			db.cache.Set(cachedKey, allIdScores)
		}
		return db.prepareResult(db.getDocuments(allIdScores), params)
	}
	idScores := make(map[int64]float64)
	commonKeys := make(map[string][]int64)
	for key, val := range params.Extra {
		param := &Params{
			Query:      fmt.Sprintf("%v", val),
			Properties: []string{key},
			BoolMode:   params.BoolMode,
			Exact:      true,
			Tolerance:  params.Tolerance,
			Relevance:  params.Relevance,
			Language:   params.Language,
		}
		scores, err := db.findWithParams(param)
		if err != nil {
			return Result[Schema]{}, err
		}
		for id := range scores {
			if v, k := allIdScores[id]; k {
				idScores[id] = v
				commonKeys[key] = append(commonKeys[key], id)
			}
		}
		var keys [][]int64
		for _, k := range commonKeys {
			keys = append(keys, k)
		}
		if len(keys) > 0 {
			d := lib.Intersection(keys...)
			for id := range idScores {
				if !slices.Contains(d, id) {
					delete(idScores, id)
				}
			}
		}
	}
	commonKeys = nil
	if cachedKey != 0 {
		db.cache.Set(cachedKey, idScores)
	}
	return db.prepareResult(db.getDocuments(idScores), params)
}

// Search - uses params to search
func (db *Engine[Schema]) Search(params *Params) (Result[Schema], error) {
	if db.cache == nil {
		db.cache = maps.New[uint64, map[int64]float64]()
	}
	cachedKey := params.ToInt64()
	if cachedKey != 0 {
		if scores, ok := db.cache.Get(cachedKey); ok {
			return db.prepareResult(db.getDocuments(scores), params)
		}
	}
	if params.Query == "" && len(params.Extra) > 0 {
		for key, val := range params.Extra {
			params.Query = fmt.Sprintf("%v", val)
			params.Properties = append(params.Properties, key)
			delete(params.Extra, key)
			break
		}
	}
	allIdScores, err := db.findWithParams(params)
	if err != nil {
		return Result[Schema]{}, err
	}
	results := make(Hits[Schema], 0)
	if len(allIdScores) == 0 && params.Query == "" {
		sampleDocs, err := db.documents.Sample()
		if err != nil {
			return Result[Schema]{}, err
		}
		for k, doc := range sampleDocs {
			parseInt, _ := strconv.ParseInt(k, 10, 64)
			results = append(results, Hit[Schema]{Id: parseInt, Data: doc, Score: 0})
		}
		return db.prepareResult(results, params)
	}
	cache := make(map[int64]float64)
	defer func() {
		results = nil
		cache = nil
	}()
	for id, score := range allIdScores {
		if doc, ok := db.GetDocument(id); ok {
			if len(params.Extra) > 0 {
				if db.Check(doc, params.Extra) {
					cache[id] = score
					results = append(results, Hit[Schema]{Id: id, Data: doc, Score: score})
				}
			} else {
				cache[id] = score
				results = append(results, Hit[Schema]{Id: id, Data: doc, Score: score})
			}
		}
	}
	if cachedKey != 0 {
		db.cache.Set(cachedKey, cache)
	}
	return db.prepareResult(results, params)
}

func (db *Engine[Schema]) addIndexes(keys []string) {
	for _, key := range keys {
		db.addIndex(key)
	}
}

func (db *Engine[Schema]) addIndex(key string) {
	db.indexes.Set(key, NewIndex())
	db.indexKeys = append(db.indexKeys, key)
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
	tokens, _ := tokenizer.Tokenize(&tokenizer.TokenizeParams{
		Text:            params.Query,
		Language:        language,
		AllowDuplicates: false,
	}, db.tokenizerConfig)

	for _, prop := range properties {
		if index, ok := db.indexes.Get(prop); ok {
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
	}
	return allIdScores, nil
}

func (db *Engine[Schema]) prepareResult(results Hits[Schema], params *Params) (Result[Schema], error) {
	sort.Sort(results)
	if !params.Paginate {
		return Result[Schema]{Hits: results, Count: len(results)}, nil
	}
	if params.Limit == 0 {
		params.Limit = 20
	}
	start, stop := lib.Paginate(params.Offset, params.Limit, len(results))
	return Result[Schema]{Hits: results[start:stop], Count: len(results)}, nil
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

func (db *Engine[Schema]) indexDocument(id int64, document map[string]string, language tokenizer.Language) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.indexes.ForEach(func(propName string, index *Index) bool {
		tokens, _ := tokenizer.Tokenize(&tokenizer.TokenizeParams{
			Text:            document[propName],
			Language:        language,
			AllowDuplicates: true,
		}, db.tokenizerConfig)

		index.Insert(&IndexParams{
			Id:        id,
			Tokens:    tokens,
			DocsCount: db.DocumentLen(),
		})
		return true
	})
}

func (db *Engine[Schema]) deindexDocument(id int64, document map[string]string, language tokenizer.Language) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.indexes.ForEach(func(propName string, index *Index) bool {
		tokens, _ := tokenizer.Tokenize(&tokenizer.TokenizeParams{
			Text:            document[propName],
			Language:        language,
			AllowDuplicates: false,
		}, db.tokenizerConfig)

		index.Delete(&IndexParams{
			Id:        id,
			Tokens:    tokens,
			DocsCount: db.DocumentLen(),
		})
		return true
	})
}

func (db *Engine[Schema]) getFieldsFromMap(obj any) map[string]string {
	fields := make(map[string]string)
	m, ok := obj.(map[string]any)
	if !ok {
		return fields
	}
	rules := db.rules
	for field, val := range m {
		if rules == nil || rules[field] {
			if str, ok := val.(string); ok {
				fields[field] = str
			} else {
				fields[field] = fmt.Sprint(val)
			}
		}
	}
	return fields
}

func (db *Engine[Schema]) getFieldsFromStruct(obj any, prefix ...string) map[string]string {
	fields := make(map[string]string)
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

func (db *Engine[Schema]) flattenSchema(obj any, prefix ...string) map[string]string {
	if obj == nil {
		return nil
	}
	switch reflect.TypeOf(obj).Kind() {
	case reflect.Struct:
		return db.getFieldsFromStruct(obj, prefix...)
	case reflect.Map:
		return db.getFieldsFromMap(obj)
	default:
		return map[string]string{
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
