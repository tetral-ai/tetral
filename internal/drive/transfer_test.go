package drive

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// A transfer page keeps the longest prefix within 1,000 entries and
// 4,194,304 bytes, counting the commas between entries, and says when it
// left one out.
func TestTransferPagesStopAtTheirBounds(t *testing.T) {
	const empty = 40
	fill := protocol.ListPageMaxJSONBytes - empty
	for _, row := range []struct {
		name  string
		sizes []int
		kept  int
		cut   bool
	}{
		{"1,001 small entries", repeat(1_001, 10), protocol.ListPageMaxItems, true},
		{"1,000 small entries", repeat(1_000, 10), protocol.ListPageMaxItems, false},
		{"two entries filling the bytes exactly", []int{fill / 2, fill - fill/2 - 1}, 2, false},
		{"two entries one byte past", []int{fill / 2, fill - fill/2}, 1, true},
		{"one entry past the bytes", []int{fill + 1}, 0, true},
	} {
		entries := make([]int, len(row.sizes))
		for i := range entries {
			entries[i] = i
		}
		kept, cut := fitPage(entries, empty, func(i int) int { return row.sizes[i] })
		if len(kept) != row.kept || cut != row.cut {
			t.Errorf("%s: kept %d, cut %v; want %d, %v", row.name, len(kept), cut, row.kept, row.cut)
		}
	}
}

func repeat(n, size int) []int {
	sizes := make([]int, n)
	for i := range sizes {
		sizes[i] = size
	}
	return sizes
}
