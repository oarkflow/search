package radix

import (
	"github.com/oarkflow/search/lib"
)

var (
	InsertPool = lib.NewPool[*InsertParams](func() *InsertParams { return &InsertParams{} })
	RecordPool = lib.NewPool[*RecordInfo](func() *RecordInfo { return &RecordInfo{} })
	NodePool   = lib.NewPool[*node](func() *node { return &node{} })
)
