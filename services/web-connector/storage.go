package webconnector

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/blob"
)

type RandomRead func([]byte) (int, error)

type SnapshotStore struct {
	blobs  blob.BlobStore
	random RandomRead
	now    func() time.Time
}

type compareAndSwapBlobStore interface {
	CompareAndSwap(ctx context.Context, key string, expectedETag string, content io.Reader, size int64) error
}

type metadataRecord struct {
	URL      string  `json:"url"`
	Title    *string `json:"title"`
	Date     *string `json:"date"`
	MintedAt string  `json:"minted_at"`
}

type documentHeader struct {
	URL              string  `json:"url"`
	Title            *string `json:"title"`
	PublishedTime    *string `json:"published_time"`
	FetchedAt        string  `json:"fetched_at"`
	TargetHTTPStatus int32   `json:"target_http_status"`
	TotalLines       int32   `json:"total_lines"`
	SourceIncomplete bool    `json:"source_incomplete"`
	BackendTokens    int64   `json:"backend_tokens"`
}

func NewSnapshotStore(blobs blob.BlobStore, random RandomRead, now func() time.Time) *SnapshotStore {
	if random == nil {
		random = rand.Read
	}
	if now == nil {
		now = time.Now
	}
	return &SnapshotStore{blobs: blobs, random: random, now: now}
}

func (s *SnapshotStore) StoreStub(ctx context.Context, scope Scope, hit SearchHit) (Ref, int64, error) {
	for attempts := 0; attempts < 8; attempts++ {
		id, err := s.newRef()
		if err != nil {
			return Ref{}, 0, err
		}
		record := metadataRecord{URL: hit.URL, Title: optionalString(hit.Title), Date: optionalString(hit.Date), MintedAt: s.now().UTC().Format(time.RFC3339Nano)}
		raw, err := json.Marshal(record)
		if err != nil {
			return Ref{}, 0, err
		}
		err = putBytes(ctx, s.blobs, metaKey(scope, id), raw)
		if isDuplicate(err) {
			continue
		}
		if err != nil {
			return Ref{}, 0, err
		}
		return Ref{ID: id, URL: hit.URL, Title: hit.Title}, int64(len(raw)), nil
	}
	return Ref{}, 0, fmt.Errorf("reference allocation exhausted")
}

func (s *SnapshotStore) StorePage(ctx context.Context, scope Scope, page Page) (Ref, int64, error) {
	lines, incomplete := normalizeContent(page.Content)
	page.SourceIncomplete = page.SourceIncomplete || incomplete
	if page.FetchedAt.IsZero() {
		page.FetchedAt = s.now().UTC()
	}
	for attempts := 0; attempts < 8; attempts++ {
		id, err := s.newRef()
		if err != nil {
			return Ref{}, 0, err
		}
		ref, written, err := s.storePageAt(ctx, scope, id, page, lines, false)
		if isDuplicate(err) {
			continue
		}
		return ref, written, err
	}
	return Ref{}, 0, fmt.Errorf("reference allocation exhausted")
}

func (s *SnapshotStore) StorePageForRef(ctx context.Context, scope Scope, id string, page Page) (Ref, int64, error) {
	lines, incomplete := normalizeContent(page.Content)
	page.SourceIncomplete = page.SourceIncomplete || incomplete
	if page.FetchedAt.IsZero() {
		page.FetchedAt = s.now().UTC()
	}
	return s.storePageAt(ctx, scope, id, page, lines, true)
}

func (s *SnapshotStore) storePageAt(ctx context.Context, scope Scope, id string, page Page, lines []string, loadDuplicate bool) (Ref, int64, error) {
	header := documentHeader{URL: page.URL, Title: optionalString(page.Title), PublishedTime: optionalString(page.PublishedTime), FetchedAt: page.FetchedAt.UTC().Format(time.RFC3339Nano), TargetHTTPStatus: page.TargetHTTPStatus, TotalLines: int32(len(lines)), SourceIncomplete: page.SourceIncomplete, BackendTokens: page.Tokens}
	headerRaw, err := json.Marshal(header)
	if err != nil {
		return Ref{}, 0, err
	}
	docRaw := append(append(headerRaw, '\n'), []byte(strings.Join(lines, "\n"))...)
	if err = putBytes(ctx, s.blobs, docKey(scope, id), docRaw); err != nil {
		if isDuplicate(err) && loadDuplicate {
			loaded, loadErr := s.LoadPage(ctx, scope, id)
			if loadErr == nil {
				return Ref{ID: id, URL: loaded.URL, Title: loaded.Title, TotalLines: int32(len(loaded.Lines))}, 0, nil
			}
		}
		return Ref{}, 0, err
	}
	return Ref{ID: id, URL: page.URL, Title: page.Title, LineStart: 1, LineEnd: int32(len(lines)), TotalLines: int32(len(lines))}, int64(len(docRaw)), nil
}

func (s *SnapshotStore) LoadPage(ctx context.Context, scope Scope, id string) (Page, error) {
	raw, err := getBytes(ctx, s.blobs, docKey(scope, id))
	if err != nil {
		return Page{}, err
	}
	headerPart, body, found := bytes.Cut(raw, []byte{'\n'})
	if !found {
		return Page{}, fmt.Errorf("invalid stored document")
	}
	var header documentHeader
	if json.Unmarshal(headerPart, &header) != nil {
		return Page{}, fmt.Errorf("invalid stored document")
	}
	fetchedAt, err := time.Parse(time.RFC3339Nano, header.FetchedAt)
	if err != nil {
		return Page{}, fmt.Errorf("invalid stored document")
	}
	lines := strings.Split(string(body), "\n")
	if len(lines) == 0 {
		lines = []string{""}
	}
	return Page{URL: header.URL, Title: valueString(header.Title), PublishedTime: valueString(header.PublishedTime), Content: string(body), TargetHTTPStatus: header.TargetHTTPStatus, Tokens: header.BackendTokens, SourceIncomplete: header.SourceIncomplete, Lines: lines, FetchedAt: fetchedAt}, nil
}

func (s *SnapshotStore) LoadMeta(ctx context.Context, scope Scope, id string) (SearchHit, error) {
	raw, err := getBytes(ctx, s.blobs, metaKey(scope, id))
	if err != nil {
		return SearchHit{}, err
	}
	var record metadataRecord
	if json.Unmarshal(raw, &record) != nil {
		return SearchHit{}, fmt.Errorf("invalid stored metadata")
	}
	return SearchHit{URL: record.URL, Title: valueString(record.Title), Date: valueString(record.Date)}, nil
}

func (s *SnapshotStore) PutJob(ctx context.Context, scope Scope, eventID string, raw []byte) error {
	return putBytes(ctx, s.blobs, jobKey(scope, eventID), raw)
}

func (s *SnapshotStore) GetJob(ctx context.Context, scope Scope, eventID string) ([]byte, error) {
	return getBytes(ctx, s.blobs, jobKey(scope, eventID))
}

func (s *SnapshotStore) GetJobVersion(ctx context.Context, scope Scope, eventID string) ([]byte, string, error) {
	key := jobKey(scope, eventID)
	metadata, err := s.blobs.HeadObject(ctx, key)
	if err != nil {
		return nil, "", err
	}
	raw, err := getBytes(ctx, s.blobs, key)
	if err != nil {
		return nil, "", err
	}
	return raw, metadata.ETag, nil
}

func (s *SnapshotStore) CompareAndSwapJob(ctx context.Context, scope Scope, eventID, expectedETag string, raw []byte) error {
	store, ok := s.blobs.(compareAndSwapBlobStore)
	if !ok {
		return errors.New("web job store does not support compare-and-swap")
	}
	return store.CompareAndSwap(ctx, jobKey(scope, eventID), expectedETag, bytes.NewReader(raw), int64(len(raw)))
}

func (s *SnapshotStore) DeleteKeys(ctx context.Context, keys []string) {
	for _, key := range keys {
		_ = s.blobs.Delete(ctx, key)
	}
}

func (s *SnapshotStore) newRef() (string, error) {
	raw := make([]byte, 16)
	n, err := s.random(raw)
	if err != nil {
		return "", err
	}
	if n != len(raw) {
		return "", io.ErrUnexpectedEOF
	}
	return "r_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)), nil
}

func normalizeContent(content string) ([]string, bool) {
	raw := []byte(content)
	incomplete := false
	if len(raw) > maxSnapshotBytes {
		raw = raw[:maxSnapshotBytes]
		incomplete = true
		for len(raw) > 0 && !utf8.Valid(raw) {
			raw = raw[:len(raw)-1]
		}
	}
	base := strings.Split(string(raw), "\n")
	if len(base) == 0 {
		base = []string{""}
	}
	lines := make([]string, 0, len(base))
	for _, line := range base {
		line = strings.TrimSuffix(line, "\r")
		for len([]byte(line)) > MaxStoredLineBytes {
			cut := MaxStoredLineBytes
			data := []byte(line)
			for cut > 0 && !utf8.Valid(data[:cut]) {
				cut--
			}
			lines = append(lines, string(data[:cut]))
			line = string(data[cut:])
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		lines = []string{""}
	}
	return lines, incomplete
}

func CanonicalInputHash(value any) string {
	raw, _ := json.Marshal(normalizeCanonical(value))
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func normalizeCanonical(value any) any {
	if value == nil {
		return nil
	}
	rv := reflect.ValueOf(value)
	if (rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface) && rv.IsNil() {
		return nil
	}
	switch typed := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for key, item := range typed {
			normalized := normalizeCanonical(item)
			if normalized == nil || isEmptyCanonicalArray(normalized) {
				continue
			}
			out[key] = normalized
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = normalizeCanonical(item)
		}
		return out
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = normalizeCanonical(rv.Index(i).Interface())
		}
		return out
	}
	return value
}
func isEmptyCanonicalArray(value any) bool {
	rv := reflect.ValueOf(value)
	return (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) && rv.Len() == 0
}
func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	v := value
	return &v
}
func valueString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func prefix(scope Scope) string {
	return scope.WorkspaceID + "/" + scope.SessionID + "/" + scope.ThreadID + "/"
}
func metaKey(scope Scope, id string) string { return prefix(scope) + id + ".meta" }
func docKey(scope Scope, id string) string  { return prefix(scope) + id + ".doc" }
func jobKey(scope Scope, id string) string  { return prefix(scope) + "jobs/" + id + ".job" }
func putBytes(ctx context.Context, store blob.BlobStore, key string, raw []byte) error {
	return store.Put(ctx, key, bytes.NewReader(raw), int64(len(raw)))
}
func getBytes(ctx context.Context, store blob.BlobStore, key string) ([]byte, error) {
	reader, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}
func isDuplicate(err error) bool { var target *blob.DuplicateKeyError; return errors.As(err, &target) }
func isNotFound(err error) bool  { var target *blob.NotFoundError; return errors.As(err, &target) }
