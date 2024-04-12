package english

import (
	"github.com/oarkflow/search/snowball/snowballword"
)

// Step 1a is normalization of various special "s"-endings.
func step1a(w *snowballword.SnowballWord) bool {

	suffix, suffixRunesSize := w.FirstSuffix("sses", "ied", "ies", "us", "ss", "s")
	switch suffix {

	case "sses":

		// Replace by ss
		w.ReplaceSuffixRunes(suffix, suffixRunesSize, []rune("ss"), true)
		return true

	case "ies", "ied":

		// Replace by i if preceded by more than one letter,
		// otherwise by ie (so ties -> tie, cries -> cri).

		var repl string
		if len(w.RS) > 4 {
			repl = "i"
		} else {
			repl = "ie"
		}
		w.ReplaceSuffixRunes(suffix, suffixRunesSize, []rune(repl), true)
		return true

	case "us", "ss":

		// Do nothing
		return false

	case "s":

		// Delete if the preceding word part contains a vowel
		// not immediately before the s (so gas and this retain
		// the s, gaps and kiwis lose it)
		//
		for i := 0; i < len(w.RS)-2; i++ {
			if isLowerVowel(w.RS[i]) {
				w.RemoveLastNRunes(suffixRunesSize)
				return true
			}
		}
	}
	return false
}
