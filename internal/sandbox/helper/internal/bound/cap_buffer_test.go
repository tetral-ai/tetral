package bound

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestCapBufferSmallSnapshot(t *testing.T) {
	buffer := NewCapBuffer(1024)
	if n, err := buffer.Write([]byte("one\ntwo\n")); err != nil || n != len("one\ntwo\n") {
		t.Fatalf("Write = %d, %v", n, err)
	}
	snapshot := buffer.Snapshot(50*1024, 2000)
	if snapshot.Text != "one\ntwo\n" || snapshot.TotalBytes != int64(len("one\ntwo\n")) || snapshot.TotalLines != 2 || snapshot.Truncated {
		t.Fatalf("snapshot = %+v; want complete text", snapshot)
	}
}

func TestCapBufferSmallSnapshotWithoutTrailingNewline(t *testing.T) {
	buffer := NewCapBuffer(1024)
	if n, err := buffer.Write([]byte("ready")); err != nil || n != len("ready") {
		t.Fatalf("Write = %d, %v", n, err)
	}
	snapshot := buffer.DrainSnapshot(50*1024, 2000)
	if snapshot.Text != "ready" || snapshot.TotalBytes != int64(len("ready")) || snapshot.ReturnedBytes != len("ready") || snapshot.Truncated {
		t.Fatalf("snapshot = %+v; want complete text without trailing newline", snapshot)
	}
}

func TestCapBufferDrainThenWriteReturnsNewWindowTextWithCumulativeTotals(t *testing.T) {
	buffer := NewCapBuffer(1024)
	empty := buffer.DrainSnapshot(50*1024, 2000)
	if empty.Text != "" || empty.TotalBytes != 0 {
		t.Fatalf("empty drain = %+v", empty)
	}
	_, _ = buffer.Write([]byte("ready\n"))
	snapshot := buffer.DrainSnapshot(50*1024, 2000)
	if snapshot.Text != "ready\n" || snapshot.TotalBytes != int64(len("ready\n")) || snapshot.ReturnedBytes != len("ready\n") {
		t.Fatalf("snapshot = %+v; want new window text with cumulative totals", snapshot)
	}
}

func TestCapBufferKeepsHeadAndTail(t *testing.T) {
	buffer := NewCapBuffer(10)
	_, _ = buffer.Write([]byte("abcdefghijklmnopqrst"))
	snapshot := buffer.Snapshot(10, 2000)
	if snapshot.Text != "abcde\n[... 10 bytes truncated ...]\npqrst" || snapshot.TotalBytes != 20 || !snapshot.Truncated {
		t.Fatalf("snapshot = %+v; want head seam tail", snapshot)
	}
}

func TestCapBufferCountsHundredMiBStreamWhileBoundingStoredWindow(t *testing.T) {
	buffer := NewCapBuffer(DefaultStreamStoreBytes)
	chunk := bytes.Repeat([]byte("a"), 1024*1024)
	for i := 0; i < 100; i++ {
		n, err := buffer.Write(chunk)
		if err != nil || n != len(chunk) {
			t.Fatalf("Write chunk %d = %d, %v; want full chunk", i, n, err)
		}
	}
	snapshot := buffer.Snapshot(64*1024, 2000)
	if snapshot.TotalBytes != 100*1024*1024 || buffer.WindowBytes() != 100*1024*1024 {
		t.Fatalf("totals = snapshot %d window %d; want 100 MiB counted", snapshot.TotalBytes, buffer.WindowBytes())
	}
	if !snapshot.Truncated {
		t.Fatalf("snapshot = %+v; want truncated 100 MiB stream", snapshot)
	}
	if snapshot.ReturnedBytes > 64*1024 {
		t.Fatalf("returned_bytes = %d; want bounded by visible window", snapshot.ReturnedBytes)
	}
	if len(buffer.head) > buffer.headCap || len(buffer.tail) > buffer.tailCap {
		t.Fatalf("stored head/tail = %d/%d caps %d/%d; want bounded storage", len(buffer.head), len(buffer.tail), buffer.headCap, buffer.tailCap)
	}
}

func TestCapBufferVisibleTailBeforeStoreOverflow(t *testing.T) {
	buffer := NewCapBuffer(1024)
	_, _ = buffer.Write([]byte(strings.Repeat("a", 40) + strings.Repeat("z", 40)))
	snapshot := buffer.Snapshot(20, 2000)
	if snapshot.Text != strings.Repeat("a", 10)+"\n[... 60 bytes truncated ...]\n"+strings.Repeat("z", 10) {
		t.Fatalf("snapshot text = %q; want visible head and tail before store overflow", snapshot.Text)
	}
	if snapshot.ReturnedBytes != 20 {
		t.Fatalf("returned_bytes = %d; want stream bytes only", snapshot.ReturnedBytes)
	}
}

func TestCapBufferVisibleLineBudget(t *testing.T) {
	buffer := NewCapBuffer(1024)
	_, _ = buffer.Write([]byte("h1\nh2\nh3\nh4\nh5\n"))
	snapshot := buffer.Snapshot(1024, 4)
	if snapshot.Text != "h1\nh2\n\n[... 3 bytes truncated ...]\nh4\nh5\n" {
		t.Fatalf("line-budget text = %q", snapshot.Text)
	}
}

func TestCapBufferTailLineBudgetKeepsNewlineTerminatedLines(t *testing.T) {
	buffer := NewCapBuffer(12)
	_, _ = buffer.Write([]byte("h1\nh2\nh3\nh4\nh5\n"))
	snapshot := buffer.Snapshot(12, 4)
	if snapshot.Text != "h1\nh2\n\n[... 3 bytes truncated ...]\nh4\nh5\n" {
		t.Fatalf("tail line-budget text = %q", snapshot.Text)
	}
}

func TestCapBufferOneVisibleLineUsesTailOnly(t *testing.T) {
	buffer := NewCapBuffer(1024)
	_, _ = buffer.Write([]byte("one\ntwo\nthree\n"))
	snapshot := buffer.Snapshot(1024, 1)
	if snapshot.Text != "\n[... 8 bytes truncated ...]\nthree\n" {
		t.Fatalf("one-line snapshot text = %q", snapshot.Text)
	}
}

func TestCapBufferDrainKeepsCumulativeTotals(t *testing.T) {
	buffer := NewCapBuffer(1024)
	_, _ = buffer.Write([]byte("one\n"))
	first := buffer.DrainSnapshot(1024, 2000)
	if first.Text != "one\n" || first.TotalBytes != 4 || first.TotalLines != 1 {
		t.Fatalf("first drain = %+v", first)
	}
	_, _ = buffer.Write([]byte("two\n"))
	second := buffer.DrainSnapshot(1024, 2000)
	if second.Text != "two\n" || second.TotalBytes != 8 || second.TotalLines != 2 || second.Truncated {
		t.Fatalf("second drain = %+v; want new bytes with cumulative totals", second)
	}
}

func TestCapBufferSnapshotsRespectUTF8Boundaries(t *testing.T) {
	buffer := NewCapBuffer(6)
	_, _ = buffer.Write([]byte("αβγ"))
	snapshot := buffer.Snapshot(6, 2000)
	if !utf8.ValidString(snapshot.Text) {
		t.Fatalf("snapshot text is invalid UTF-8: %q", snapshot.Text)
	}
	if !strings.Contains(snapshot.Text, "α") || !strings.Contains(snapshot.Text, "γ") {
		t.Fatalf("snapshot text = %q; want valid boundary-preserving head and tail", snapshot.Text)
	}
}
