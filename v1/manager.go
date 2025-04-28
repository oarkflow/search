package v1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/json"
)

type Manager struct {
	indexes map[string]*Index
	mutex   sync.Mutex
}

func NewManager() *Manager {
	return &Manager{
		indexes: make(map[string]*Index),
	}
}

func (m *Manager) AddIndex(name string, index *Index) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.indexes[name] = index
}

func (m *Manager) GetIndex(name string) (*Index, bool) {
	m.mutex.Lock() // Replace with RLock if mutex becomes RWMutex; otherwise, minimal change
	defer m.mutex.Unlock()
	index, ok := m.indexes[name]
	return index, ok
}

func (m *Manager) DeleteIndex(name string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.indexes, name)
}

func (m *Manager) ListIndexes() []string {
	m.mutex.Lock() // Replace with RLock if mutex becomes RWMutex
	defer m.mutex.Unlock()
	names := make([]string, 0, len(m.indexes))
	for name := range m.indexes {
		names = append(names, name)
	}
	return names
}

func (m *Manager) Build(ctx context.Context, name string, req any) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	index, ok := m.indexes[name]
	if !ok {
		return fmt.Errorf("index %s not found", name)
	}
	return index.Build(ctx, req)
}

func (m *Manager) Search(ctx context.Context, name string, q string) ([]GenericRecord, error) {
	m.mutex.Lock()
	index, ok := m.indexes[name]
	m.mutex.Unlock()
	if !ok {
		fmt.Printf("index %s not found\n", name)
		return nil, fmt.Errorf("index %s not found", name)
	}
	params := SearchParams{
		Page:    1,
		PerPage: 10,
	}
	results, err := index.Search(ctx, NewTermQuery(q, true, 1), params)
	if err != nil {
		return nil, err
	}
	var data []GenericRecord
	for _, sd := range results.Results {
		rec, ok := index.GetDocument(sd.DocID)
		if ok {
			data = append(data, rec.(GenericRecord))
		}
	}
	return data, nil
}

type NewIndexRequest struct {
	ID string `json:"id"`
}

type Request struct {
	Filters   []*filters.Filter `json:"filters"`
	Query     string            `json:"q" query:"q"`
	Condition string            `json:"condition" query:"condition"`
	Match     string            `json:"m" query:"m"`
	Fields    []string          `json:"f" query:"f"`
	Offset    int               `json:"o" query:"o"`
	Size      int               `json:"s" query:"s"`
	SortField string            `json:"sort_field" query:"sort_field"`
	SortOrder string            `json:"sort_order" query:"sort_order"`
	Page      int               `json:"p" query:"p"`
	Reverse   bool              `json:"reverse" query:"reverse"`
}

var builtInFields = []string{"q", "m", "l", "f", "t", "o", "s", "e", "p", "condition", "sort_field", "sort_order"}

func prepareQuery(r *http.Request) (Request, error) {
	var query Request
	extraMap := make(map[string]any)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return query, err
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	if bodyBytes != nil && len(bodyBytes) > 0 {
		err = json.Unmarshal(bodyBytes, &query)
		if err != nil {
			return query, fmt.Errorf("error unmarshalling query: %v", err)
		}
		err = json.Unmarshal(bodyBytes, &extraMap)
		if err != nil {
			return query, fmt.Errorf("error unmarshalling extra: %v", err)
		}
	}
	var extra []*filters.Filter
	for k, v := range extraMap {
		if slices.Contains(builtInFields, k) {
			continue
		}
		vt := reflect.TypeOf(v).Kind()
		operator := filters.Equal
		if vt == reflect.Slice {
			operator = filters.In
		}
		extra = append(extra, filters.NewFilter(k, operator, v))
	}
	if len(extra) == 0 {
		rawQuery := r.URL.RawQuery
		extra, err = filters.ParseQuery(rawQuery, builtInFields...)
		if err != nil {
			return query, err
		}
	}
	if extra != nil && query.Filters == nil {
		query.Filters = extra
	}
	query.Match = "AND"
	if strings.ToLower(query.Match) == "any" {
		query.Match = "OR"
	}
	return query, nil
}

func (m *Manager) StartHTTP(addr string) {
	http.HandleFunc("/index/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusBadRequest)
			return
		}
		var req NewIndexRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, fmt.Sprintf("Error unmarshalling request: %v", err), http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(req.ID) == "" {
			http.Error(w, "Index ID required in request body", http.StatusBadRequest)
			return
		}
		index := NewIndex(req.ID)
		m.AddIndex(req.ID, index)
		w.Write([]byte(fmt.Sprintf("Index %s created successfully", req.ID)))
	})
	http.HandleFunc("/indexes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
			return
		}
		indexes := m.ListIndexes()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(indexes)
	})
	http.HandleFunc("/{index}/build", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
			return
		}
		indexName := r.PathValue("index")
		if strings.TrimSpace(indexName) == "" {
			http.Error(w, "Index name required in path", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusBadRequest)
			return
		}
		var req IndexRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, fmt.Sprintf("Error unmarshalling request: %v", err), http.StatusBadRequest)
			return
		}
		if req.Path != "" {
			go func(indexName string, req IndexRequest) {
				err = m.Build(context.Background(), indexName, req)
				if err != nil {
					http.Error(w, fmt.Sprintf("Build error: %v", err), http.StatusInternalServerError)
					return
				}
			}(indexName, req)
			w.Write([]byte(fmt.Sprintf("Indexing started for %s with index name %s", req.Path, indexName)))
			return
		}
		err = m.Build(ctx, indexName, req)
		if err != nil {
			http.Error(w, fmt.Sprintf("Build error: %v", err), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Index built successfully"))
	})
	http.HandleFunc("/{index}/search", func(w http.ResponseWriter, r *http.Request) {
		indexName := r.PathValue("index")
		if strings.TrimSpace(indexName) == "" {
			http.Error(w, "Index name required in path", http.StatusBadRequest)
			return
		}
		q := r.URL.Query().Get("q")
		if strings.TrimSpace(q) == "" {
			http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		results, err := m.Search(ctx, indexName, q)
		if err != nil {
			http.Error(w, fmt.Sprintf("Search error: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	log.Printf("HTTP server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
