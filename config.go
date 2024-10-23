package search

import (
	"reflect"
	"time"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/search/lib"
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
	SortOrder  string             `json:"sort_order"`
	SortField  string             `json:"sort_field"`
	Language   tokenizer.Language `json:"lang"`
}

func (p *Params) ToInt64() int64 {
	return lib.CRC32Checksum(p)

}

type BM25Params struct {
	K float64 `json:"k"`
	B float64 `json:"b"`
	D float64 `json:"d"`
}

type Result[Schema SchemaProps] struct {
	Hits          Hits[Schema] `json:"hits"`
	Count         int          `json:"count"`
	FilteredTotal int          `json:"filtered_total"`
	Total         int          `json:"total"`
	Message       string       `json:"message"`
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

// Wrapper around Hits with additional sorting context.
type SortableHits[Schema any] struct {
	Hits      Hits[Schema]
	SortField string
	SortOrder string
}

func (s SortableHits[Schema]) Len() int {
	return len(s.Hits)
}

func (s SortableHits[Schema]) Swap(i, j int) {
	s.Hits[i], s.Hits[j] = s.Hits[j], s.Hits[i]
}

func (s SortableHits[Schema]) Less(i, j int) bool {
	dataI := s.Hits[i].Data
	dataJ := s.Hits[j].Data
	var fieldI, fieldJ reflect.Value
	if reflect.TypeOf(dataI).Kind() == reflect.Struct {
		fieldI = reflect.ValueOf(dataI).FieldByName(s.SortField)
		fieldJ = reflect.ValueOf(dataJ).FieldByName(s.SortField)
	}
	if reflect.TypeOf(dataI).Kind() == reflect.Map {
		mapI, okI := any(dataI).(map[string]any)
		mapJ, okJ := any(dataJ).(map[string]any)
		if okI && okJ {
			valI, existsI := mapI[s.SortField]
			valJ, existsJ := mapJ[s.SortField]
			if existsI && existsJ {
				fieldI = reflect.ValueOf(valI)
				fieldJ = reflect.ValueOf(valJ)
			} else {
				return s.Hits[i].Score > s.Hits[j].Score
			}
		}
	}
	if fieldI.IsValid() && fieldJ.IsValid() {
		switch fieldI.Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32:
			if s.SortOrder == "asc" {
				return fieldI.Int() < fieldJ.Int()
			} else {
				return fieldI.Int() > fieldJ.Int()
			}
		case reflect.Float64, reflect.Float32:
			if s.SortOrder == "asc" {
				return fieldI.Float() < fieldJ.Float()
			} else {
				return fieldI.Float() > fieldJ.Float()
			}
		case reflect.String:
			if s.SortOrder == "asc" {
				return fieldI.String() < fieldJ.String()
			} else {
				return fieldI.String() > fieldJ.String()
			}
		default:
			return false
		}
	}
	return s.Hits[i].Score > s.Hits[j].Score
}

func defaultIDGenerator(_ any) int64 {
	return xid.New().Int64()
}

type Config struct {
	Key                string             `json:"key"`
	DefaultLanguage    tokenizer.Language `json:"default_language"`
	TokenizerConfig    *tokenizer.Config
	CleanupPeriod      time.Duration   `json:"cleanup_period"`
	EvictionDuration   time.Duration   `json:"eviction_duration"`
	IndexKeys          []string        `json:"index_keys"`
	FieldsToStore      []string        `json:"fields_to_store"`
	FieldsToExclude    []string        `json:"fields_to_exclude"`
	Rules              map[string]bool `json:"rules"`
	SliceField         string          `json:"slice_field"`
	Storage            string          `json:"storage"`
	Path               string          `json:"path"`
	Compress           bool            `json:"compress"`
	OffloadIndex       bool            `json:"offload_index"`
	ResetPath          bool            `json:"reset_path"`
	MaxRecordsInMemory int             `json:"max_records_in_memory"`
	SampleSize         int             `json:"sample_size"`
	SortField          string          `json:"sort_field"`
	SortOrder          string          `json:"sort_order"`
	IDGenerator        func(doc any) int64
}

// MergeConfigs merges multiple Config structs into one.
func MergeConfigs(configs ...*Config) *Config {
	mergedConfig := &Config{
		Rules: make(map[string]bool),
	}

	for _, cfg := range configs {
		if cfg.Key != "" {
			mergedConfig.Key = cfg.Key
		}
		if cfg.DefaultLanguage != "" {
			mergedConfig.DefaultLanguage = cfg.DefaultLanguage
		}
		if cfg.TokenizerConfig != nil {
			mergedConfig.TokenizerConfig = cfg.TokenizerConfig
		}
		if cfg.CleanupPeriod != 0 {
			mergedConfig.CleanupPeriod = cfg.CleanupPeriod
		}
		if cfg.EvictionDuration != 0 {
			mergedConfig.EvictionDuration = cfg.EvictionDuration
		}
		if len(cfg.IndexKeys) > 0 {
			mergedConfig.IndexKeys = append(mergedConfig.IndexKeys, cfg.IndexKeys...)
		}
		if len(cfg.FieldsToStore) > 0 {
			mergedConfig.FieldsToStore = append(mergedConfig.FieldsToStore, cfg.FieldsToStore...)
		}
		if len(cfg.FieldsToExclude) > 0 {
			mergedConfig.FieldsToExclude = append(mergedConfig.FieldsToExclude, cfg.FieldsToExclude...)
		}
		for k, v := range cfg.Rules {
			mergedConfig.Rules[k] = v
		}
		if cfg.SliceField != "" {
			mergedConfig.SliceField = cfg.SliceField
		}
		if cfg.Storage != "" {
			mergedConfig.Storage = cfg.Storage
		}
		if cfg.Path != "" {
			mergedConfig.Path = cfg.Path
		}
		if cfg.Compress {
			mergedConfig.Compress = cfg.Compress
		}
		if cfg.OffloadIndex {
			mergedConfig.OffloadIndex = cfg.OffloadIndex
		}
		if cfg.ResetPath {
			mergedConfig.ResetPath = cfg.ResetPath
		}
		if cfg.MaxRecordsInMemory != 0 {
			mergedConfig.MaxRecordsInMemory = cfg.MaxRecordsInMemory
		}
		if cfg.SampleSize != 0 {
			mergedConfig.SampleSize = cfg.SampleSize
		}
		if cfg.IDGenerator != nil {
			mergedConfig.IDGenerator = cfg.IDGenerator
		}
	}

	return mergedConfig
}
