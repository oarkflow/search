package search

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/oarkflow/gopool"
	"github.com/oarkflow/log"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/filters"

	maps "github.com/oarkflow/xsync"

	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/storage"
	"github.com/oarkflow/search/tokenizer"
)

// Engine holds the search engine implementation.
type Engine[Schema SchemaProps] struct {
	m                  sync.RWMutex
	documentStorage    storage.Store[int64, Schema]
	indexes            maps.IMap[string, *Index]
	indexKeys          []string
	defaultLanguage    tokenizer.Language
	tokenizerConfig    *tokenizer.Config
	rules              map[string]bool
	cache              maps.IMap[int64, map[int64]float64]
	key                string
	sliceField         string
	path               string
	lastAccessedTS     time.Time
	cfg                *Config
	fieldCache         sync.Map        // Cache for reflection metadata: map[reflect.Type][]reflect.StructField
	fieldsToStoreMap   map[string]bool // Cached fields to store
	fieldsToExcludeMap map[string]bool // Cached fields to exclude
}

// New creates a new Engine instance.
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
	if c.CleanupPeriod == 0 {
		c.CleanupPeriod = 5 * time.Minute
	}
	if c.EvictionDuration == 0 {
		c.EvictionDuration = 30 * time.Minute
	}
	store, err := getStore[Schema](c)
	if err != nil {
		return nil, err
	}
	if c.IDGenerator == nil {
		c.IDGenerator = defaultIDGenerator
	}
	db := &Engine[Schema]{
		key:             c.Key,
		documentStorage: store,
		indexes:         maps.NewMap[string, *Index](),
		defaultLanguage: c.DefaultLanguage,
		tokenizerConfig: c.TokenizerConfig,
		rules:           c.Rules,
		sliceField:      c.SliceField,
		path:            c.Path,
		cfg:             c,
	}
	if len(c.FieldsToStore) > 0 {
		db.fieldsToStoreMap = make(map[string]bool)
		for _, field := range c.FieldsToStore {
			db.fieldsToStoreMap[field] = true
		}
	}
	if len(c.FieldsToExclude) > 0 {
		db.fieldsToExcludeMap = make(map[string]bool)
		for _, field := range c.FieldsToExclude {
			db.fieldsToExcludeMap[field] = true
		}
	}

	db.buildIndexes()
	if len(db.indexKeys) == 0 {
		db.addIndexes(c.IndexKeys)
	}
	if c.OffloadIndex {
		go db.offloadIndex()
	}
	db.updateLastAccessedTS()
	return db, nil
}

func (db *Engine[Schema]) Reset() error {
	err := db.documentStorage.Close()
	if err != nil {
		return err
	}
	db.indexes.Clear()
	store, err := getStore[Schema](db.cfg)
	if err != nil {
		return err
	}
	db.documentStorage = store
	db.indexes = maps.NewMap[string, *Index]()
	return nil
}

func (db *Engine[Schema]) offloadIndex() {
	ticker := time.NewTicker(db.cfg.CleanupPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			if now.Sub(db.lastAccessedTS) > db.cfg.EvictionDuration {
				var keysToOffload []string
				db.m.RLock()
				db.indexes.ForEach(func(key string, index *Index) bool {
					if index != nil {
						keysToOffload = append(keysToOffload, key)
					}
					return true
				})
				db.m.RUnlock()
				for _, key := range keysToOffload {
					go func(key string) {
						index, ok := db.indexes.Get(key)
						if ok && index != nil {
							log.Info().Str("engine_key", db.key).Str("index_key", key).Msg("Performing index cleanup...")
							if err := index.Save(); err == nil {
								db.indexes.Set(key, nil)
							}
						}
					}(key)
				}
			}
		}
	}
}

func (db *Engine[Schema]) SetStorage(store storage.Store[int64, Schema]) {
	db.documentStorage = store
}

func (db *Engine[Schema]) updateLastAccessedTS() {
	db.m.Lock()
	defer db.m.Unlock()
	db.lastAccessedTS = time.Now()
}

func (db *Engine[Schema]) Metadata() map[string]any {
	cfg := map[string]any{
		"key":               db.key,
		"index_keys":        db.indexKeys,
		"language":          db.defaultLanguage,
		"documentStorage":   db.documentStorage.Name(),
		"fields_to_store":   db.cfg.FieldsToStore,
		"fields_to_exclude": db.cfg.FieldsToExclude,
	}
	return cfg
}

func (db *Engine[Schema]) GetDocument(id int64) (Schema, bool) {
	return db.documentStorage.Get(id)
}

func (db *Engine[Schema]) DelDocument(id int64) error {
	return db.documentStorage.Del(id)
}

func (db *Engine[Schema]) SetDocument(id int64, doc Schema) error {
	return db.documentStorage.Set(id, doc)
}

func (db *Engine[Schema]) DocumentLen() int {
	return int(db.documentStorage.Len())
}

func (db *Engine[Schema]) Storage() storage.Store[int64, Schema] {
	return db.documentStorage
}

func (db *Engine[Schema]) Insert(doc Schema, lang ...tokenizer.Language) (Record[Schema], error) {
	if len(db.cfg.FieldsToStore) > 0 {
		switch d := any(doc).(type) {
		case map[string]any:
			for k := range d {
				if !db.fieldsToStoreMap[k] {
					delete(d, k)
				}
			}
		case map[any]any:
			for k := range d {
				switch key := k.(type) {
				case string:
					if !db.fieldsToStoreMap[key] {
						delete(d, k)
					}
				}
			}
		}
	}
	if len(db.cfg.FieldsToExclude) > 0 {
		switch d := any(doc).(type) {
		case map[string]any:
			for k := range d {
				if db.fieldsToExcludeMap[k] {
					delete(d, k)
				}
			}
		case map[any]any:
			for k := range d {
				switch key := k.(type) {
				case string:
					if db.fieldsToExcludeMap[key] {
						delete(d, k)
					}
				}
			}
		}
	}
	id := db.cfg.IDGenerator(doc)
	if len(db.indexKeys) == 0 {
		indexKeys := DocFields(doc)
		db.addIndexes(indexKeys)
	}
	language := tokenizer.ENGLISH
	if len(lang) > 0 {
		language = lang[0]
	}
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
	language := tokenizer.ENGLISH
	if len(lang) > 0 {
		language = lang[0]
	}

	pool, err := gopool.NewPoolSimple(noOfWorker, func(doc gopool.Job[Schema], workerID int) error {
		_, err := db.Insert(doc.Payload, language)
		return err
	})
	if err != nil {
		return []error{err}
	}
	for i, doc := range docs {
		if i == 0 {
			// Ensure indexKeys are set up on first Insert.
			_, err := db.Insert(doc, language)
			if err != nil {
				errs = append(errs, err)
			}
		} else {
			pool.Submit(doc)
		}
	}
	pool.StopAndWait()
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
	oldDocFlatten := db.flattenSchema(oldDocument)
	db.deIndexDocument(params.Id, oldDocFlatten, language)
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
	db.deIndexDocument(params.Id, doc, language)
	return db.DelDocument(params.Id)
}

func (db *Engine[Schema]) ClearCache() {
	db.cache = nil
}

// CheckCondition verifies if a document matches the given filter rule.
func (db *Engine[Schema]) CheckCondition(data Schema, filter *filters.Rule) bool {
	return filter.Match(data)
}

// Check verifies if a document matches a set of filters.
func (db *Engine[Schema]) Check(data Schema, filter []*filters.Filter) bool {
	var conditions []filters.Condition
	for _, f := range filter {
		conditions = append(conditions, f)
	}
	return filters.MatchGroup(data, &filters.FilterGroup{Operator: filters.AND, Filters: conditions})
}

func (db *Engine[Schema]) Sample(params storage.SampleParams) (Result[Schema], error) {
	results := make(Hits[Schema], 0)
	sampleDocs, err := db.documentStorage.Sample(params)
	if err != nil {
		return Result[Schema]{}, err
	}
	for k, doc := range sampleDocs {
		parseInt, _ := strconv.ParseInt(k, 10, 64)
		results = append(results, Hit[Schema]{Id: parseInt, Data: doc, Score: 0})
	}
	return db.prepareResult(results, &Params{Paginate: false, Filters: params.Filters, SortOrder: params.SortOrder, SortField: params.SortField})
}

// Search performs a query using the provided parameters.
func (db *Engine[Schema]) Search(params *Params) (Result[Schema], error) {
	if params.SortOrder == "" {
		params.SortOrder = db.cfg.SortOrder
	}
	if params.SortOrder == "" {
		params.SortOrder = "asc"
	}
	if params.SortField == "" {
		params.SortField = db.cfg.SortField
	}
	db.updateLastAccessedTS()
	if db.cache == nil {
		db.cache = maps.NewMap[int64, map[int64]float64]()
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
	var filter *filters.Rule
	var err error
	if params.Condition != "" {
		filter, err = filters.ParseSQL(params.Condition)
		if err != nil {
			log.Error().Err(err).Str("condition", params.Condition).Msg("Unable to parse condition")
		}
	}
	ProcessQueryAndFilters(params, filter)
	if params.Query == "" && len(params.Filters) == 0 {
		return db.sampleWithFilters(storage.SampleParams{Size: params.Limit, Rule: filter, Filters: params.Filters, SortOrder: params.SortOrder, SortField: params.SortField})
	}
	allIdScores, err := db.findWithParams(params)
	if err != nil {
		return Result[Schema]{}, err
	}
	if len(allIdScores) == 0 && params.Query == "" {
		return db.sampleWithFilters(storage.SampleParams{Size: params.Limit, Rule: filter, Filters: params.Filters, SortOrder: params.SortOrder, SortField: params.SortField})
	}
	keys := make([]int64, 0, len(allIdScores))
	for key := range allIdScores {
		keys = append(keys, key)
	}
	result := make(Hits[Schema], 0)
	cache := make(map[int64]float64)
	defer func() {
		result = nil
		cache = nil
	}()
	for _, id := range keys {
		score := allIdScores[id]
		doc, ok := db.GetDocument(id)
		if !ok {
			continue
		}
		if len(params.Filters) == 0 && params.Condition == "" {
			cache[id] = score
			result = append(result, Hit[Schema]{Id: id, Data: doc, Score: score})
			continue
		}
		if params.Condition != "" {
			if db.CheckCondition(doc, filter) {
				cache[id] = score
				result = append(result, Hit[Schema]{Id: id, Data: doc, Score: score})
			}
		} else if db.Check(doc, params.Filters) {
			cache[id] = score
			result = append(result, Hit[Schema]{Id: id, Data: doc, Score: score})
		}
	}
	if cachedKey != 0 && len(cache) > 0 {
		db.cache.Set(cachedKey, cache)
	}
	return db.prepareResult(result, params)
}

func (db *Engine[Schema]) sampleWithFilters(params storage.SampleParams) (Result[Schema], error) {
	rs, err := db.Sample(params)
	if err != nil {
		return rs, err
	}
	if params.Filters == nil && params.Rule == nil {
		rs.Message = "[WARN] - Query or Filters not applied"
	}
	return rs, nil
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
	var err error
	for _, prop := range properties {
		index, ok := db.indexes.Get(prop)
		if !ok || index == nil {
			index, err = NewFromFile(db.key, prop)
			if err != nil || index == nil {
				continue
			}
			db.indexes.Set(prop, index)
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
	sortable := SortableHits[Schema]{Hits: results, SortField: params.SortField, SortOrder: params.SortOrder}
	sort.Sort(sortable)
	resultLen := len(results)
	if !params.Paginate {
		return Result[Schema]{Hits: results, Count: resultLen, FilteredTotal: resultLen, Total: db.DocumentLen()}, nil
	}
	if params.Limit == 0 {
		params.Limit = 20
	}
	start, stop := lib.Paginate(params.Offset, params.Limit, resultLen)
	paginatedResult := results[start:stop]
	return Result[Schema]{Hits: paginatedResult, Count: len(paginatedResult), FilteredTotal: len(results), Total: db.DocumentLen()}, nil
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
	var visibleFields []reflect.StructField
	if cached, ok := db.fieldCache.Load(t); ok {
		visibleFields = cached.([]reflect.StructField)
	} else {
		visibleFields = reflect.VisibleFields(t)
		db.fieldCache.Store(t, visibleFields)
	}
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
				// Use type assertion for efficiency.
				if s, ok := v.Field(i).Interface().(string); ok {
					fields[propName] = s
				} else {
					fields[propName] = fmt.Sprint(v.Field(i).Interface())
				}
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
				if s, ok := v.Field(i).Interface().(string); ok {
					fields[propName] = s
				} else {
					fields[propName] = fmt.Sprint(v.Field(i).Interface())
				}
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
