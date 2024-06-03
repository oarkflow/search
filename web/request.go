package web

import (
	"github.com/oarkflow/filters"
)

type Query struct {
	Filters   []filters.Filter `json:"filters"`
	Query     string           `json:"q" query:"q" validate:"required"`
	Match     string           `json:"m" query:"m"`
	Language  string           `json:"l" query:"l"`
	Fields    []string         `json:"f" query:"f"`
	Tolerance int              `json:"t" query:"t"`
	Offset    int              `json:"o" query:"o"`
	Size      int              `json:"s" query:"s"`
	Exact     bool             `json:"e" query:"e"`
}

type NewEngine struct {
	Key             string   `json:"key"`
	FieldsToIndex   []string `json:"fields_to_index"`
	FieldsToStore   []string `json:"fields_to_store"`
	FieldsToExclude []string `json:"fields_to_exclude"`
	Storage         string   `json:"storage"`
	Reset           bool     `json:"reset"`
	Compress        bool     `json:"compress"`
}

type Database struct {
	TableName       string   `json:"table_name"`
	Database        string   `json:"database"`
	Query           string   `json:"query"`
	Driver          string   `json:"driver"`
	IndexKey        string   `json:"index_key"`
	Password        string   `json:"password"`
	Host            string   `json:"host"`
	Storage         string   `json:"storage"`
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
