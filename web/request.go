package web

import (
	"github.com/oarkflow/filters"
)

type Query struct {
	Filters   []*filters.Filter `json:"filters"`
	Query     string            `json:"q" query:"q" validate:"required"`
	Condition string            `json:"condition" query:"condition"`
	Match     string            `json:"m" query:"m"`
	Language  string            `json:"l" query:"l"`
	Fields    []string          `json:"f" query:"f"`
	Tolerance int               `json:"t" query:"t"`
	Offset    int               `json:"o" query:"o"`
	Size      int               `json:"s" query:"s"`
	Exact     bool              `json:"e" query:"e"`
	SortField string            `json:"sort_field" query:"sort_field"`
	SortOrder string            `json:"sort_order" query:"sort_order"`
}

type Options struct {
	Key             string   `json:"key"`
	FieldsToIndex   []string `json:"fields_to_index"`
	FieldsToStore   []string `json:"fields_to_store"`
	FieldsToExclude []string `json:"fields_to_exclude"`
	Reset           bool     `json:"reset"`
	Compress        bool     `json:"compress"`
	SortField       string   `json:"sort_field" query:"sort_field"`
	SortOrder       string   `json:"sort_order" query:"sort_order"`
}

type Database struct {
	TableName       string   `json:"table_name"`
	Database        string   `json:"database"`
	Query           string   `json:"query"`
	Driver          string   `json:"driver"`
	IndexKey        string   `json:"index_key"`
	Password        string   `json:"password"`
	Host            string   `json:"host"`
	SslMode         string   `json:"ssl_mode"`
	Username        string   `json:"username"`
	FieldsToIndex   []string `json:"fields_to_index"`
	FieldsToStore   []string `json:"fields_to_store"`
	FieldsToExclude []string `json:"fields_to_exclude"`
	Port            int      `json:"port"`
	BatchSize       int      `json:"batch_size"`
	Reset           bool     `json:"reset"`
	Compress        bool     `json:"compress"`
}
