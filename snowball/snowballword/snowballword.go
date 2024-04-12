/*
This package defines a SnowballWord struct that is used
to encapsulate most of the "state" variables we must track
when stemming a word.  The SnowballWord struct also has
a few methods common to stemming in a variety of languages.
*/
package snowballword

import (
	"fmt"
	"unicode/utf8"
)

// SnowballWord represents a word that is going to be stemmed.
type SnowballWord struct {

	// A slice of runes
	RS []rune

	// The index in RS where the R1 region begins
	R1start int

	// The index in RS where the R2 region begins
	R2start int

	// The index in RS where the RV region begins
	RVstart int
}

// Create a new SnowballWord struct
func New(in string) (word *SnowballWord) {
	word = &SnowballWord{RS: []rune(in)}
	word.R1start = len(word.RS)
	word.R2start = len(word.RS)
	word.RVstart = len(word.RS)
	return
}

// Replace a suffix and adjust R1start and R2start as needed.
// If `force` is false, check to make sure the suffix exists first.
func (w *SnowballWord) ReplaceSuffix(suffix string, suffixRunesSize int, replacement string, force bool) bool {
	var (
		doReplacement bool
	)
	if force {
		doReplacement = true
	} else {
		var foundSuffix string
		foundSuffix, _ = w.FirstSuffix(suffix)
		if foundSuffix == suffix {
			doReplacement = true
		}
	}
	if doReplacement == false {
		return false
	}
	w.ReplaceSuffixRunes(suffix, suffixRunesSize, []rune(replacement), true)
	return true
}

// Remove the last `n` runes from the SnowballWord.
func (w *SnowballWord) RemoveLastNRunes(n int) {
	w.RS = w.RS[:len(w.RS)-n]
	w.resetR1R2()
}

// Replace a suffix and adjust R1start and R2start as needed.
// If `force` is false, check to make sure the suffix exists first.
func (w *SnowballWord) ReplaceSuffixRunes(suffixRunes string, suffixRunesSize int, replacementRunes []rune, force bool) bool {

	if force || w.HasSuffixRunes(suffixRunes) {
		lenWithoutSuffix := len(w.RS) - suffixRunesSize
		w.RS = append(w.RS[:lenWithoutSuffix], replacementRunes...)

		// If R, R2, & RV are now beyond the length
		// of the word, they are set to the length
		// of the word.  Otherwise, they are left
		// as they were.
		w.resetR1R2()
		return true
	}
	return false
}

// Resets R1start and R2start to ensure they
// are within bounds of the current rune slice.
func (w *SnowballWord) resetR1R2() {
	rsLen := len(w.RS)
	if w.R1start > rsLen {
		w.R1start = rsLen
	}
	if w.R2start > rsLen {
		w.R2start = rsLen
	}
	if w.RVstart > rsLen {
		w.RVstart = rsLen
	}
}

// Return a slice of w.RS, allowing the start
// and stop to be out of bounds.
func (w *SnowballWord) slice(start, stop int) []rune {
	startMin := 0
	if start < startMin {
		start = startMin
	}
	max := len(w.RS) - 1
	if start > max {
		start = max
	}
	if stop > max {
		stop = max
	}
	return w.RS[start:stop]
}

// Returns true if `x` runes would fit into R1.
func (w *SnowballWord) FitsInR1(x int) bool {
	return w.R1start <= len(w.RS)-x
}

// Returns true if `x` runes would fit into R2.
func (w *SnowballWord) FitsInR2(x int) bool {
	return w.R2start <= len(w.RS)-x
}

// Returns true if `x` runes would fit into RV.
func (w *SnowballWord) FitsInRV(x int) bool {
	return w.RVstart <= len(w.RS)-x
}

// Return the R1 region as a slice of runes
func (w *SnowballWord) R1() []rune {
	return w.RS[w.R1start:]
}

// Return the R1 region as a string
func (w *SnowballWord) R1String() string {
	return string(w.R1())
}

// Return the R2 region as a slice of runes
func (w *SnowballWord) R2() []rune {
	return w.RS[w.R2start:]
}

// Return the R2 region as a string
func (w *SnowballWord) R2String() string {
	return string(w.R2())
}

// Return the RV region as a slice of runes
func (w *SnowballWord) RV() []rune {
	return w.RS[w.RVstart:]
}

// Return the RV region as a string
func (w *SnowballWord) RVString() string {
	return string(w.RV())
}

// Return the SnowballWord as a string
func (w *SnowballWord) String() string {
	return string(w.RS)
}

func (w *SnowballWord) DebugString() string {
	return fmt.Sprintf("{\"%s\", %d, %d, %d}", w.String(), w.R1start, w.R2start, w.RVstart)
}

// FirstPrefix Return the first prefix found or the empty string.
func (w *SnowballWord) FirstPrefix(prefixes ...string) (foundPrefix string, foundPrefixSize int) {
	found := false
	rsLen := len(w.RS)

	for _, prefix := range prefixes {
		prefixRunesSize := utf8.RuneCountInString(prefix)
		if prefixRunesSize > rsLen {
			continue
		}
		found = true
		i := 0
		nRead := 0
		for {
			r, n := utf8.DecodeRuneInString(prefix[nRead:])
			if n == 0 || r == utf8.RuneError {
				break
			}
			nRead += n
			if i > rsLen-1 || (w.RS)[i] != r {
				found = false
				break
			}
			i++
		}

		if found {
			foundPrefix = prefix
			foundPrefixSize = prefixRunesSize
			break
		}
	}
	return
}

// Return true if `w.RS[startPos:endPos]` ends with runes from `suffixRunes`.
// That is, the slice of runes between startPos and endPos have a suffix of
// suffixRunes.
func (w *SnowballWord) HasSuffixRunesIn(startPos, endPos int, suffix string) bool {
	maxLen := endPos - startPos
	suffixLen := utf8.RuneCountInString(suffix)
	if suffixLen > maxLen {
		return false
	}

	var (
		r           rune
		n           int
		i           = 0
		numMatching = 0
	)
	for {
		r, n = utf8.DecodeLastRuneInString(suffix)
		if n == 0 || r == utf8.RuneError && i >= maxLen {
			break
		}
		suffix = suffix[:len(suffix)-n]
		if w.RS[endPos-i-1] != r {
			break
		} else {
			numMatching += 1
		}
		i++
	}
	if numMatching == suffixLen {
		return true
	}
	return false
}

// HasSuffixRunes Return true if `w` ends with `suffixRunes`
func (w *SnowballWord) HasSuffixRunes(suffix string) bool {
	return w.HasSuffixRunesIn(0, len(w.RS), suffix)
}

// FirstSuffixIfIn Find the first suffix that ends at `endPos` in the word among
// those provided; then,
// check to see if it begins after startPos.  If it does, return
// it, else return the empty string and empty rune slice.  This
// may seem a counterintuitive manner to do this.  However, it
// matches what is required most of the time by the Snowball
// stemmer steps.
func (w *SnowballWord) FirstSuffixIfIn(startPos, endPos int, suffixes ...string) (suffix string, suffixRunesSize int) {
	var suffixLen int
	for i := 0; i < len(suffixes); i++ {
		suffix = suffixes[i]
		suffixLen = utf8.RuneCountInString(suffix)
		if w.HasSuffixRunesIn(0, endPos, suffix) {
			if endPos-suffixLen >= startPos {
				return suffix, suffixLen
			} else {
				return "", 0
			}
		}
	}
	//
	return "", 0
}

func (w *SnowballWord) FirstSuffixIn(startPos, endPos int, suffixes ...string) (suffix string, suffixRunesSize int) {
	for _, suffix = range suffixes {
		if w.HasSuffixRunesIn(startPos, endPos, suffix) {
			suffixRunesSize = utf8.RuneCountInString(suffix)
			return suffix, suffixRunesSize
		}
	}
	return "", 0
}

// Find the first suffix in the word among those provided; then,
// check to see if it begins after startPos.  If it does,
// remove it.
func (w *SnowballWord) RemoveFirstSuffixIfIn(startPos int, suffixes ...string) (suffix string, suffixRunesSize int) {
	suffix, suffixRunesSize = w.FirstSuffixIfIn(startPos, len(w.RS), suffixes...)
	if suffix != "" {
		w.RemoveLastNRunes(suffixRunesSize)
	}
	return
}

// Removes the first suffix found that is in `word.RS[startPos:len(word.RS)]`
func (w *SnowballWord) RemoveFirstSuffixIn(startPos int, suffixes ...string) (suffix string, suffixRunesSize int) {
	suffix, suffixRunesSize = w.FirstSuffixIn(startPos, len(w.RS), suffixes...)
	if suffix != "" {
		w.RemoveLastNRunes(suffixRunesSize)
	}
	return
}

// Removes the first suffix found
func (w *SnowballWord) RemoveFirstSuffix(suffixes ...string) (suffix string, suffixRunesSize int) {
	return w.RemoveFirstSuffixIn(0, suffixes...)
}

// Return the first suffix found or the empty string.
func (w *SnowballWord) FirstSuffix(suffixes ...string) (suffix string, suffixRunesSize int) {
	return w.FirstSuffixIfIn(0, len(w.RS), suffixes...)
}
