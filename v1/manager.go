package v1

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

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
	m.mutex.Lock()
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
	m.mutex.Lock()
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
	defer m.mutex.Unlock()
	index, ok := m.indexes[name]
	if !ok {
		fmt.Printf("index %s not found\n", name)
		return nil, fmt.Errorf("index %s not found", name)
	}
	results, err := index.Search(ctx, NewTermQuery(q, true, 1), q)
	if err != nil {
		return nil, err
	}
	paged := Paginate(results, 1, 10)
	var data []GenericRecord
	for _, sd := range paged {
		data = append(data, index.Documents[sd.DocID])
	}
	return data, nil
}

type NewIndexRequest struct {
	ID string `json:"id"`
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
