package search

import (
	"github.com/oarkflow/search/storage"
	"github.com/oarkflow/search/storage/flydb"
	"github.com/oarkflow/search/storage/memdb"
	"github.com/oarkflow/search/storage/mmap"
)

func getStore[Schema SchemaProps](c *Config) (storage.Store[int64, Schema], error) {
	if c.SampleSize == 0 {
		c.SampleSize = 20
	}
	if c.MaxRecordsInMemory == 0 {
		c.MaxRecordsInMemory = 100
	}
	switch c.Storage {
	case "flydb":
		return flydb.New[int64, Schema](c.Path, c.SampleSize)
	case "mmap":
		cfg := mmap.Config{
			Path:               c.Path,
			SampleSize:         c.SampleSize,
			CleanupPeriod:      c.CleanupPeriod,
			EvictionDuration:   c.EvictionDuration,
			MaxRecordsInMemory: c.MaxRecordsInMemory,
		}
		return mmap.New[int64, Schema](cfg, storage.Int64Comparator)
	default:
		return memdb.New[int64, Schema](c.SampleSize, storage.Int64Comparator)
	}
}
