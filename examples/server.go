package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/date"
	"github.com/oarkflow/filters/utils"
	"github.com/oarkflow/metadata"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/tokenizer"
	"github.com/oarkflow/search/web"
)

func builtInAge(dob string) (any, error) {
	t, err := utils.ParseTime(dob)
	if err != nil {
		return nil, err
	}
	return date.CalculateToNow(t), nil
}
func main() {
	req := web.Options{
		Key: "cpt",
	}
	cfg := search.GetConfig(req.Key)
	cfg.IndexKeys = []string{"charge_type", "cpt_hcpcs_code", "client_internal_code", "client_proc_desc", "work_item_id", "effective_date"}
	cfg.FieldsToStore = []string{"charge_type", "cpt_hcpcs_code", "client_internal_code", "client_proc_desc", "work_item_id", "min_effective_date", "max_effective_date"}
	cfg.FieldsToExclude = req.FieldsToExclude
	cfg.Compress = req.Compress
	cfg.ResetPath = req.Reset
	_, err := search.SetEngine[map[string]any](req.Key, cfg)
	if err != nil {
		panic(err)
	}
	dbConfig := web.Database{
		Database: "clear_dev",
		Query:    "SELECT client_proc_desc, cpt_hcpcs_code, client_internal_code, work_item_id, charge_type, MAX(end_effective_date) as end_effective_date, MIN(effective_date) as min_effective_date, MAX(effective_date) as max_effective_date FROM vw_cpt_master GROUP BY client_proc_desc, cpt_hcpcs_code, client_internal_code, work_item_id, charge_type LIMIT 1000;",
		Driver:   "postgresql",
		IndexKey: "cpt",
		Password: "postgres",
		Host:     "localhost",
		Username: "postgres",
		Port:     5432,
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
	// searchByCondition()
}

/*
	func searchByCondition() {
		jet.DefaultSet(jet.WithDelims("{{", "}}"))
		jet.AddDefaultVariables(map[string]any{
			"age": builtInAge,
		})
		var data = map[string]any{
			"patient": map[string]any{
				"first_name":   "John",
				"gender":       "male",
				"salary":       25000,
				"dob":          "1989-04-19",
				"work_item_id": 58,
			},
			"cpt": map[string]any{
				"code": "G8707",
			},
		}
		lookupType := "fts"
		lookupSource := "cpt"
		lookup := &filters.Lookup{
			Handler: func(request any, condition string) (any, error) {
				c, err := jet.Parse(condition, request)
				if err != nil {
					return nil, err
				}
				engine, err := search.GetEngine[map[string]any](lookupSource)
				if err != nil {
					return nil, err
				}
				param := &search.Params{Condition: c}
				rs, err := engine.Search(param)
				if err != nil {
					return nil, err
				}
				var mp []map[string]any
				for _, d := range rs.Hits {
					mp = append(mp, d.Data)
				}
				return mp, nil
			},
			Type:             lookupType,
			Source:           lookupSource,
			HandlerCondition: "work_item_id={{patient.work_item_id}} AND min_age < {{patient.dob|age}}",
			Condition:        "map(lookup, .cpt_hcpcs_code)",
		}
		filter := filters.NewFilter("cpt.code", filters.In, "lookup")
		filter.SetLookup(lookup)
		fmt.Println(filters.Match(data, filter))
	}
*/
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
