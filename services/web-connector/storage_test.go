package webconnector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/blob"
)

func TestSnapshotStorePersistsCompleteHeaderAndLoadsTruncatedUTF8SafeDocument(t *testing.T) {
	objects := blob.NewFakeBlobStore()
	fetchedAt := time.Date(2026, 7, 16, 12, 34, 56, 123000000, time.UTC)
	store := NewSnapshotStore(objects, func(dst []byte) (int, error) { clear(dst); return len(dst), nil }, func() time.Time { return fetchedAt })
	content := strings.Repeat("a", maxSnapshotBytes-1) + "é" + strings.Repeat("b", 64)
	page := Page{URL: "https://example.com/", PublishedTime: "Wed, 15 Jul 2026 18:48:48 GMT", Content: content, TargetHTTPStatus: 206, Tokens: 321}
	scope := Scope{"ws", "ses", "thr"}
	ref, written, err := store.StorePage(context.Background(), scope, page)
	if err != nil {
		t.Fatal(err)
	}
	if ref.ID != "r_aaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("ref = %q", ref.ID)
	}
	if written <= 0 {
		t.Fatalf("written = %d", written)
	}
	docRaw, ok := objects.Bytes(docKey(scope, ref.ID))
	if !ok {
		t.Fatal("stored document missing")
	}
	headerRaw, body, found := bytes.Cut(docRaw, []byte{'\n'})
	if !found {
		t.Fatal("stored document has no header delimiter")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(headerRaw, &fields); err != nil {
		t.Fatal(err)
	}
	wantFields := []string{"url", "title", "published_time", "fetched_at", "target_http_status", "total_lines", "source_incomplete", "backend_tokens"}
	if len(fields) != len(wantFields) {
		t.Fatalf("header fields=%v", fields)
	}
	for _, name := range wantFields {
		if _, ok := fields[name]; !ok {
			t.Errorf("header field %q missing", name)
		}
	}
	var header documentHeader
	if err := json.Unmarshal(headerRaw, &header); err != nil {
		t.Fatal(err)
	}
	storedLines := strings.Split(string(body), "\n")
	if header.URL != page.URL || header.Title != nil || header.PublishedTime == nil || *header.PublishedTime != page.PublishedTime {
		t.Fatalf("identity header=%+v", header)
	}
	if header.FetchedAt != fetchedAt.Format(time.RFC3339Nano) || header.TargetHTTPStatus != 206 || header.TotalLines != int32(len(storedLines)) || !header.SourceIncomplete || header.BackendTokens != 321 {
		t.Fatalf("storage header=%+v lines=%d", header, len(storedLines))
	}
	if !utf8.Valid(body) {
		t.Fatal("stored body is not valid UTF-8")
	}
	unwrapped := strings.ReplaceAll(string(body), "\n", "")
	if len(unwrapped) != maxSnapshotBytes-1 || unwrapped != strings.Repeat("a", maxSnapshotBytes-1) {
		t.Fatalf("stored truncated content bytes=%d", len(unwrapped))
	}
	if ref.TotalLines != int32(len(storedLines)) {
		t.Fatalf("ref total lines=%d stored lines=%d", ref.TotalLines, len(storedLines))
	}
	for _, line := range storedLines {
		if !utf8.ValidString(line) || len([]byte(line)) > MaxStoredLineBytes {
			t.Fatalf("invalid stored line bytes=%d", len([]byte(line)))
		}
	}
	loaded, err := store.LoadPage(context.Background(), scope, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.URL != page.URL || loaded.Title != "" || loaded.PublishedTime != page.PublishedTime || !loaded.FetchedAt.Equal(fetchedAt) {
		t.Fatalf("loaded identity url=%q title=%q published=%q fetched=%s", loaded.URL, loaded.Title, loaded.PublishedTime, loaded.FetchedAt)
	}
	if loaded.TargetHTTPStatus != 206 || loaded.Tokens != 321 || !loaded.SourceIncomplete || loaded.Content != string(body) || len(loaded.Lines) != len(storedLines) {
		t.Fatalf("loaded status=%d tokens=%d incomplete=%t content_bytes=%d lines=%d", loaded.TargetHTTPStatus, loaded.Tokens, loaded.SourceIncomplete, len(loaded.Content), len(loaded.Lines))
	}
}

func TestCanonicalInputHashIsStableForMapOrderAndSensitiveToArrayOrder(t *testing.T) {
	t.Parallel()
	a := map[string]any{"z": []any{"a", "b"}, "a": map[string]any{"y": 2, "x": 1}}
	b := map[string]any{"a": map[string]any{"x": 1, "y": 2}, "z": []any{"a", "b"}}
	c := map[string]any{"z": []any{"b", "a"}, "a": map[string]any{"x": 1, "y": 2}}
	if CanonicalInputHash(a) != CanonicalInputHash(b) {
		t.Fatal("map order changed hash")
	}
	if CanonicalInputHash(a) == CanonicalInputHash(c) {
		t.Fatal("array order did not change hash")
	}
}

func TestCanonicalInputHashOmitsNullAndEmptyArraysButPreservesScalarValues(t *testing.T) {
	t.Parallel()
	base := map[string]any{"q": "", "items": []any{"a"}}
	variant := map[string]any{"q": "", "items": []any{"a"}, "missing": nil, "empty": []any{}}
	changed := map[string]any{"q": "different", "items": []any{"a"}}
	if CanonicalInputHash(base) != CanonicalInputHash(variant) {
		t.Fatal("null or empty array changed hash")
	}
	if CanonicalInputHash(base) == CanonicalInputHash(changed) {
		t.Fatal("changed scalar did not change hash")
	}
}

func TestSnapshotNormalizationOrdersTruncationCRLFSplittingWrappingAndEmptyBody(t *testing.T) {
	t.Parallel()
	physical := strings.Repeat("é", MaxStoredLineBytes/2+1)
	lines, incomplete := normalizeContent(physical + "\r\nlast\r")
	if incomplete {
		t.Fatal("small content marked incomplete")
	}
	if len(lines) != 3 || lines[0]+lines[1] != physical || lines[2] != "last" {
		t.Fatalf("lines=%q", lines)
	}
	for _, line := range lines {
		if len([]byte(line)) > MaxStoredLineBytes || !utf8.ValidString(line) {
			t.Fatalf("invalid line size/utf8: %d", len([]byte(line)))
		}
	}
	empty, _ := normalizeContent("")
	if len(empty) != 1 || empty[0] != "" {
		t.Fatalf("empty=%#v", empty)
	}
	oversized := strings.Repeat("a", maxSnapshotBytes-1) + "é"
	truncated, wasIncomplete := normalizeContent(oversized)
	if !wasIncomplete {
		t.Fatal("oversized content not marked incomplete")
	}
	if !utf8.ValidString(strings.Join(truncated, "\n")) {
		t.Fatal("truncation split UTF-8")
	}
}

func TestEveryStoredLineIsAddressableAndEmptyDocumentHasAValidFirstWindow(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	store := NewSnapshotStore(objects, zeroRandom, time.Now)
	scope := Scope{"ws", "ses", "thr"}
	ref, _, err := store.StorePage(context.Background(), scope, Page{URL: "https://example.com/", Content: strings.Repeat("x", MaxStoredLineBytes+1) + "\nlast"})
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.LoadPage(context.Background(), scope, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	for index, want := range page.Lines {
		window, windowErr := formatWindow(ref.ID, page, int32(index+1))
		if windowErr != nil {
			t.Fatalf("lineno=%d: %v", index+1, windowErr)
		}
		_, body, found := strings.Cut(window.text, "\n\n")
		if !found || strings.Split(body, "\n")[0] != want {
			t.Fatalf("lineno=%d first rendered line=%q want=%q", index+1, strings.Split(body, "\n")[0], want)
		}
	}

	emptyRef, _, err := store.StorePage(context.Background(), Scope{"ws", "ses", "empty"}, Page{URL: "https://example.com/empty", Content: ""})
	if err != nil {
		t.Fatal(err)
	}
	emptyPage, err := store.LoadPage(context.Background(), Scope{"ws", "ses", "empty"}, emptyRef.ID)
	if err != nil {
		t.Fatal(err)
	}
	emptyWindow, err := formatWindow(emptyRef.ID, emptyPage, 1)
	if err != nil {
		t.Fatal(err)
	}
	if emptyWindow.ref.LineStart != 1 || emptyWindow.ref.LineEnd != 1 || emptyWindow.ref.TotalLines != 1 || !strings.Contains(emptyWindow.text, "lines 1-1 of 1") {
		t.Fatalf("empty window=%+v", emptyWindow)
	}
}

func TestWindowContinuationAdvancesByWholeLinesToFinalWindow(t *testing.T) {
	t.Parallel()
	lines := make([]string, maxWindowLines+2)
	for i := range lines {
		lines[i] = "line"
	}
	page := Page{URL: "https://example.com/", Lines: lines}
	first, err := formatWindow("r_example", page, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !first.truncated || first.next == nil || *first.next != maxWindowLines+1 {
		t.Fatalf("first=%+v", first)
	}
	second, err := formatWindow("r_example", page, *first.next)
	if err != nil {
		t.Fatal(err)
	}
	if second.truncated || second.next != nil || second.ref.LineStart <= first.ref.LineEnd {
		t.Fatalf("second=%+v", second)
	}
}

func TestWindowByteCapNeverSplitsAStoredLine(t *testing.T) {
	t.Parallel()
	lines := make([]string, 13)
	for i := range lines {
		lines[i] = strings.Repeat("x", MaxStoredLineBytes)
	}
	window, err := formatWindow("r_example", Page{URL: "https://example.com/", Lines: lines}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if window.ref.LineEnd != 12 || window.next == nil || *window.next != 13 {
		t.Fatalf("window=%+v", window)
	}
	if len([]byte(strings.Split(window.text, "\n\n")[1])) > maxWindowBytes {
		t.Fatal("window body exceeded byte cap")
	}
}

func TestFindUsesRE2CaseFlagsCapsMatchesAndCompletesWithZeroMatches(t *testing.T) {
	t.Parallel()
	if _, err := formatFind("r_example", `(a)\1`, Page{Lines: []string{"aa"}}); err == nil {
		t.Fatal("backreference compiled")
	}
	text, err := formatFind("r_example", "(?i)alpha", Page{Lines: []string{"Alpha", "none", "ALPHA"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "2 matches") || !strings.Contains(text, "L1: Alpha") {
		t.Fatalf("text=%q", text)
	}
	zero, err := formatFind("r_example", "missing", Page{Lines: []string{"value"}})
	if err != nil {
		t.Fatal(err)
	}
	if zero != "[r_example] 0 matches for pattern missing in 1 lines" {
		t.Fatalf("zero=%q", zero)
	}
	many := make([]string, maxFindMatches+1)
	for i := range many {
		many[i] = "hit"
	}
	capped, err := formatFind("r_example", "hit", Page{Lines: many})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(capped, "showing first 250 of 251") {
		t.Fatalf("capped=%q", capped)
	}
}

func TestSnapshotWriteFailureLeavesNoPartialObjects(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	objects.SetPutHook(func(_ context.Context, key string) error {
		if strings.HasSuffix(key, ".doc") {
			return errors.New("fixture write failure")
		}
		return nil
	})
	store := NewSnapshotStore(objects, zeroRandom, time.Now)
	_, _, err := store.StorePage(context.Background(), Scope{"ws", "ses", "thr"}, Page{URL: "https://example.com/", Content: "body"})
	if err == nil {
		t.Fatal("StorePage succeeded")
	}
	if objects.Len() != 0 {
		t.Fatalf("objects=%d; partial metadata remained", objects.Len())
	}
	if len(objects.Deletes()) != 0 {
		t.Fatalf("deletes=%v; a one-object snapshot write has nothing to clean", objects.Deletes())
	}
}

func TestSnapshotWritesAreCreateOnlyAndNeverReplaceExistingBytes(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	store := NewSnapshotStore(objects, zeroRandom, time.Now)
	scope := Scope{"ws", "ses", "thr"}
	first, _, err := store.StorePage(context.Background(), scope, Page{URL: "https://example.com/", Content: "first"})
	if err != nil {
		t.Fatal(err)
	}
	before, ok := objects.Bytes(docKey(scope, first.ID))
	if !ok {
		t.Fatal("first document missing")
	}
	if _, _, err = store.StorePage(context.Background(), scope, Page{URL: "https://example.com/", Content: "second"}); err == nil {
		t.Fatal("colliding reference unexpectedly replaced data")
	}
	after, _ := objects.Bytes(docKey(scope, first.ID))
	if string(before) != string(after) {
		t.Fatal("stored snapshot changed")
	}
}
