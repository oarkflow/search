package web

type Query struct {
	Query     string         `json:"q" query:"q" validate:"required"`
	Match     string         `json:"m" query:"m"`
	Language  string         `json:"l" query:"l"`
	Fields    []string       `json:"f" query:"f"`
	Exact     bool           `json:"e" query:"e"`
	Tolerance int            `json:"t" query:"t"`
	Offset    int            `json:"o" query:"o"`
	Size      int            `json:"s" query:"s"`
	Extra     map[string]any `json:"extra"`
}

type NewEngine struct {
	Key             string   `json:"key"`
	FieldsToIndex   []string `json:"fields_to_index"`
	FieldsToStore   []string `json:"fields_to_store"`
	FieldsToExclude []string `json:"fields_to_exclude"`
	Reset           bool     `json:"reset"`
	Compress        bool     `json:"compress"`
}

type Database struct {
	IndexKey        string   `json:"index_key"`
	Host            string   `json:"host"`
	Port            int      `json:"port"`
	Driver          string   `json:"driver"`
	Username        string   `json:"username"`
	Password        string   `json:"password"`
	Database        string   `json:"database"`
	SslMode         string   `json:"ssl_mode"`
	TableName       string   `json:"table_name"`
	Query           string   `json:"query"`
	BatchSize       int      `json:"batch_size"`
	FieldsToIndex   []string `json:"fields_to_index"`
	FieldsToStore   []string `json:"fields_to_store"`
	FieldsToExclude []string `json:"fields_to_exclude"`
	Reset           bool     `json:"reset"`
	Compress        bool     `json:"compress"`
}
