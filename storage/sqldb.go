package storage

import (
	"errors"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/mssql"
	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/drivers/postgres"
)

// SQLDB represents a SQL database connection
type SQLDB struct {
	db              *squealx.DB
	listQuery       string
	rowQuery        string
	identifierField string
	storeTable      string
}

type Config struct {
	squealx.Config
	listQuery       string
	rowQuery        string
	identifierField string
	storeTable      string
}

// NewSQLDB creates a new SQLDB instance and connects to the database
func NewSQLDB(config Config) (*SQLDB, error) {
	dsn := config.ToString()
	switch config.Driver {
	case "mysql", "mariadb":
		db, err := mysql.Open(dsn, "mysql")
		if err != nil {
			return nil, err
		}
		return &SQLDB{db: db, listQuery: config.listQuery, rowQuery: config.rowQuery, identifierField: config.identifierField, storeTable: config.storeTable}, nil
	case "postgres", "psql", "postgresql":
		db, err := postgres.Open(dsn, "postgres")
		if err != nil {
			return nil, err
		}
		return &SQLDB{db: db, listQuery: config.listQuery, rowQuery: config.rowQuery, identifierField: config.identifierField, storeTable: config.storeTable}, nil
	case "sql-server", "sqlserver", "mssql", "ms-sql":
		db, err := mssql.Open(dsn, "mssql")
		if err != nil {
			return nil, err
		}
		return &SQLDB{db: db, listQuery: config.listQuery, rowQuery: config.rowQuery, identifierField: config.identifierField, storeTable: config.storeTable}, nil
	}
	return nil, errors.New("No acceptable driver provided")
}

// Set inserts a key-value pair into the database
func (s *SQLDB) Set(key string, value interface{}) error {
	_, err := s.db.Exec("INSERT INTO kv_store (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", key, value)
	return err
}

// Get retrieves a value from the database by key
func (s *SQLDB) Get(key string) (interface{}, bool) {
	var value interface{}
	err := s.db.Get(&value, "SELECT value FROM kv_store WHERE key = $1", key)
	if err != nil {
		return nil, false
	}
	return value, true
}

// Del deletes a key-value pair from the database
func (s *SQLDB) Del(key string) error {
	_, err := s.db.Exec("DELETE FROM kv_store WHERE key = $1", key)
	return err
}

// Len returns the number of key-value pairs in the database
func (s *SQLDB) Len() (uint32, error) {
	var count uint32
	err := s.db.Get(&count, "SELECT COUNT(*) FROM kv_store")
	return count, err
}

// Name returns the name of the store
func (s *SQLDB) Name() string {
	return "sqldb"
}

// Sample retrieves a sample of key-value pairs based on the given parameters
func (s *SQLDB) Sample(params SampleParams) (map[string]interface{}, error) {
	if params.Size == 0 {
		params.Size = 100
	}
	return nil, nil
}

// Close closes the database connection
func (s *SQLDB) Close() error {
	return s.db.Close()
}
