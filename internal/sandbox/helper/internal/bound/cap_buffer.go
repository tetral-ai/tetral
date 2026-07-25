package bound

import (
	"bytes"
	"fmt"
	"unicode/utf8"
)

// DefaultStreamStoreBytes is the hard capture cap stored per command stream:
// 1 MiB, split head/tail 50/50 (headCap = tailCap = 512 KiB in NewCapBuffer).
// The visible default handed out by Snapshot is far smaller — 50 KiB or 2000
// lines — so the stored ring is the reservoir a poll draws bounded snapshots
// from, not the model-visible amount.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/task/store.go (stream mirror headLimit)
const DefaultStreamStoreBytes = 1024 * 1024

type StreamSnapshot struct {
	Text          string `json:"text"`
	TotalBytes    int64  `json:"total_bytes"`
	TotalLines    int64  `json:"total_lines"`
	ReturnedBytes int    `json:"returned_bytes"`
	Truncated     bool   `json:"truncated"`
}

type CapBuffer struct {
	headCap   int
	tailCap   int
	head      []byte
	tail      []byte
	tailStart int64

	totalBytes  int64
	totalLines  int64
	windowBytes int64
}

func NewCapBuffer(storeCap int) *CapBuffer {
	if storeCap <= 0 {
		storeCap = DefaultStreamStoreBytes
	}
	headCap := storeCap / 2
	return &CapBuffer{headCap: headCap, tailCap: storeCap - headCap}
}

func (b *CapBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.totalBytes += int64(len(p))
	b.windowBytes += int64(len(p))
	b.totalLines += int64(bytes.Count(p, []byte{'\n'}))

	if len(b.head) < b.headCap {
		n := min(len(p), b.headCap-len(b.head))
		b.head = append(b.head, p[:n]...)
	}
	if b.tailCap > 0 {
		b.tail = append(b.tail, p...)
		if len(b.tail) > b.tailCap {
			b.tail = append([]byte{}, b.tail[len(b.tail)-b.tailCap:]...)
		}
		b.tailStart = b.windowBytes - int64(len(b.tail))
	}
	return len(p), nil
}

func (b *CapBuffer) Snapshot(visibleBytes int, visibleLines int) StreamSnapshot {
	if visibleBytes <= 0 {
		visibleBytes = 50 * 1024
	}
	if visibleLines <= 0 {
		visibleLines = 2000
	}
	headBudget := visibleBytes / 2
	tailBudget := visibleBytes - headBudget
	headLineBudget := visibleLines / 2
	tailLineBudget := visibleLines - headLineBudget
	head := prefixBounded(b.head, headBudget, headLineBudget)
	tail, tailOffset := suffixBounded(b.tail, tailBudget, tailLineBudget)
	tailAbsoluteStart := b.tailStart + int64(tailOffset)
	if overlap := int64(len(head)) - tailAbsoluteStart; overlap > 0 {
		drop := min(len(tail), int(overlap))
		tail = tail[drop:]
	}
	omitted := b.windowBytes - int64(len(head)) - int64(len(tail))
	if omitted < 0 {
		omitted = 0
	}
	textBytes := append([]byte{}, head...)
	truncated := omitted > 0
	if truncated {
		textBytes = append(textBytes, []byte(fmt.Sprintf("\n[... %d bytes truncated ...]\n", omitted))...)
	}
	textBytes = append(textBytes, tail...)
	return StreamSnapshot{
		Text:          string(textBytes),
		TotalBytes:    b.totalBytes,
		TotalLines:    b.totalLines,
		ReturnedBytes: len(head) + len(tail),
		Truncated:     truncated,
	}
}

func (b *CapBuffer) DrainSnapshot(visibleBytes int, visibleLines int) StreamSnapshot {
	snapshot := b.Snapshot(visibleBytes, visibleLines)
	b.head = nil
	b.tail = nil
	b.tailStart = 0
	b.windowBytes = 0
	return snapshot
}

func (b *CapBuffer) WindowBytes() int64 {
	return b.windowBytes
}

func prefixBounded(value []byte, maxBytes int, maxLines int) []byte {
	if maxBytes <= 0 || maxLines == 0 || len(value) == 0 {
		return nil
	}
	limit := min(len(value), maxBytes)
	if maxLines > 0 {
		lines := 0
		for i := 0; i < limit; i++ {
			if value[i] == '\n' {
				lines++
				if lines >= maxLines {
					limit = i + 1
					break
				}
			}
		}
	}
	return append([]byte{}, value[:trimUTF8End(value, limit)]...)
}

func suffixBounded(value []byte, maxBytes int, maxLines int) ([]byte, int) {
	if maxBytes <= 0 || maxLines == 0 || len(value) == 0 {
		return nil, len(value)
	}
	start := len(value) - min(len(value), maxBytes)
	if maxLines > 0 {
		lines := 0
		i := len(value) - 1
		if i >= start && value[i] == '\n' {
			i--
		}
		for ; i >= start; i-- {
			if value[i] == '\n' {
				lines++
				if lines >= maxLines {
					start = i + 1
					break
				}
			}
		}
	}
	start = advanceUTF8Start(value, start)
	return append([]byte{}, value[start:]...), start
}

func trimUTF8End(value []byte, limit int) int {
	if limit > len(value) {
		limit = len(value)
	}
	for limit > 0 && !utf8.Valid(value[:limit]) {
		limit--
	}
	return limit
}

func advanceUTF8Start(value []byte, start int) int {
	if start < 0 {
		start = 0
	}
	for start < len(value) && !utf8.Valid(value[start:]) {
		start++
	}
	return start
}
