package web

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/gookit/color"
	"github.com/oarkflow/metadata"
	"github.com/oarkflow/squealx"

	"github.com/oarkflow/search"
)

func IndexFromDB(db metadata.DataSource, dbConfig Database, start time.Time) error {
	if dbConfig.BatchSize == 0 {
		dbConfig.BatchSize = 20000
	}
	query := fmt.Sprintf("SELECT * FROM %s", dbConfig.TableName)
	if dbConfig.Query != "" {
		query = strings.TrimSuffix(dbConfig.Query, ";")
	}
	cfg := search.GetConfig(dbConfig.IndexKey)
	cfg.IndexKeys = dbConfig.FieldsToIndex
	cfg.FieldsToStore = dbConfig.FieldsToStore
	cfg.FieldsToExclude = dbConfig.FieldsToExclude
	cfg.Compress = dbConfig.Compress
	cfg.ResetPath = dbConfig.Reset
	searchEngine, err := search.GetOrSetEngine[map[string]any](dbConfig.IndexKey, cfg)
	if err != nil {
		return err
	}
	fromDB, err := db.GetRawCollection(query)
	if err != nil {
		return err
	}
	totalCount := len(fromDB)
	for _, t := range fromDB {
		_, err := searchEngine.Insert(t)
		if err != nil {
			return err
		}
	}
	fromDB = fromDB[:0]
	runtime.GC()
	db.Close()
	color.Greenf("Documents %s: %d, latency: %s \n", dbConfig.IndexKey, totalCount, time.Since(start))
	return nil
}

func IndexFromDBWithPaginate(db metadata.DataSource, dbConfig Database, start time.Time) error {
	if dbConfig.BatchSize == 0 {
		dbConfig.BatchSize = 20000
	}
	query := fmt.Sprintf("SELECT * FROM %s", dbConfig.TableName)
	if dbConfig.Query != "" {
		query = strings.Split(strings.TrimSuffix(dbConfig.Query, ";"), "LIMIT")[0]
	}
	cfg := search.GetConfig(dbConfig.IndexKey)
	cfg.IndexKeys = dbConfig.FieldsToIndex
	cfg.FieldsToStore = dbConfig.FieldsToStore
	cfg.FieldsToExclude = dbConfig.FieldsToExclude
	cfg.Compress = dbConfig.Compress
	cfg.ResetPath = dbConfig.Reset
	searchEngine, err := search.GetOrSetEngine[map[string]any](dbConfig.IndexKey, cfg)
	if err != nil {
		return err
	}
	totalCount := 0
	last := false
	paging := &squealx.Paging{
		Limit: dbConfig.BatchSize,
		Page:  1,
	}
	for !last {
		resp := db.GetRawPaginatedCollection(query, *paging)
		if resp.Error != nil {
			return resp.Error
		}
		fromDB := resp.Items
		switch fromDB := fromDB.(type) {
		case []map[string]any:
			if fromDB == nil || len(fromDB) == 0 {
				last = true
				break
			}
			for i, t := range fromDB {
				_, err := searchEngine.Insert(t)
				if err != nil {
					return err
				}
				totalCount++
				fromDB[i] = nil
			}
			fromDB = fromDB[:0]
		case *[]map[string]any:
			if fromDB == nil || len(*fromDB) == 0 {
				last = true
				break
			}
			for _, t := range *fromDB {
				_, err := searchEngine.Insert(t)
				if err != nil {
					return err
				}
				totalCount++
			}
		default:
			fmt.Println(reflect.TypeOf(fromDB))
		}
		fromDB = nil
		runtime.GC()
		paging.Page++
	}
	db.Close()
	color.Greenf("Documents %s: %d, latency: %s \n", dbConfig.IndexKey, totalCount, time.Since(start))
	return nil
}
