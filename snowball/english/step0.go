package english

import (
	"github.com/oarkflow/search/snowball/snowballword"
)

// Step 0 is to strip off apostrophes and "s".
func step0(w *snowballword.SnowballWord) bool {
	suffix, suffixRunesSize := w.FirstSuffix("'s'", "'s", "'")
	if suffix == "" {
		return false
	}
	w.RemoveLastNRunes(suffixRunesSize)
	return true
}
