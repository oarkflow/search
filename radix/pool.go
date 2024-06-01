package radix

import (
	"github.com/oarkflow/search/lib"
)

var (
	InsertPool lib.Pool[*InsertParams]
	RecordPool lib.Pool[*RecordInfo]
)

func init() {
	InsertPool = lib.NewPool[*InsertParams](func() *InsertParams { return &InsertParams{} })
	RecordPool = lib.NewPool[*RecordInfo](func() *RecordInfo { return &RecordInfo{} })
}
