package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/metadata"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/tokenizer"
	"github.com/oarkflow/search/web"
)

func main() {
	req := web.Options{
		Key:     "cpt",
		Storage: "memory",
	}
	cfg := search.GetConfig(req.Key)
	cfg.Storage = req.Storage
	cfg.IndexKeys = []string{"charge_type", "client_internal_code", "client_proc_desc", "work_item_id", "effective_date"}
	cfg.FieldsToStore = []string{"charge_type", "client_internal_code", "client_proc_desc", "work_item_id", "min_effective_date", "max_effective_date"}
	cfg.FieldsToExclude = req.FieldsToExclude
	cfg.Compress = req.Compress
	cfg.ResetPath = req.Reset
	_, err := search.SetEngine[map[string]any](req.Key, cfg)
	if err != nil {
		panic(err)
	}
	dbConfig := web.Database{
		Database: "clear_dev",
		Query:    "SELECT client_proc_desc, cpt_hcpcs_code, client_internal_code, work_item_id, charge_type, MAX(end_effective_date) as end_effective_date, MIN(effective_date) as min_effective_date, MAX(effective_date) as max_effective_date FROM vw_cpt_master GROUP BY client_proc_desc, cpt_hcpcs_code, client_internal_code, work_item_id, charge_type HAVING work_item_id=58 LIMIT 10;",
		Driver:   "postgresql",
		IndexKey: "cpt",
		Password: "postgres",
		Host:     "localhost",
		Username: "postgres",
		Port:     5432,
		Storage:  cfg.Storage,
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
		panic(err)
	}
	start := time.Now()
	if dbConfig.Query != "" && strings.Contains(dbConfig.Query, "LIMIT") {
		err := web.IndexFromDB(db, dbConfig, start)
		if err != nil {
			fmt.Println(err.Error())
		}
	} else {
		err := web.IndexFromDBWithPaginate(db, dbConfig, start)
		if err != nil {
			fmt.Println(err.Error())
		}
	}
	searchQuery(req)
	time.Sleep(3 * time.Second)
	searchQuery(req)
}

func searchQuery(req web.Options) {
	query := web.Query{
		Query: "58",
	}
	keyType := req.Key
	engine, err := search.GetEngine[map[string]any](keyType)
	if err != nil {
		panic(err)
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
	result, err := engine.Search(params)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}
