package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload/workloadtest"
)

// statementTracer observes one pool's statements: it holds the first
// statement whose SQL contains a pause's match until the pause is released,
// delays each statement by delay nanoseconds before it is sent, and passes
// each statement's SQL and duration to record when it is set.
type statementTracer struct {
	pause  atomic.Pointer[statementPause]
	delay  atomic.Int64
	record atomic.Pointer[func(sql string, took time.Duration)]
}

type statementStartKey struct{}

type statementStart struct {
	sql string
	at  time.Time
}

// statementPause holds one statement until release closes; reached closes
// when the statement arrives.
type statementPause struct {
	match   string
	reached chan struct{}
	release chan struct{}
}

// pauseOn holds the next statement whose SQL contains match.
func (s *statementTracer) pauseOn(match string) *statementPause {
	pause := &statementPause{match: match, reached: make(chan struct{}), release: make(chan struct{})}
	s.pause.Store(pause)
	return pause
}

func (s *statementTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if pause := s.pause.Load(); pause != nil && strings.Contains(data.SQL, pause.match) && s.pause.CompareAndSwap(pause, nil) {
		close(pause.reached)
		select {
		case <-pause.release:
		case <-ctx.Done():
		}
	}
	if delay := s.delay.Load(); delay > 0 {
		time.Sleep(time.Duration(delay))
	}
	return context.WithValue(ctx, statementStartKey{}, statementStart{sql: data.SQL, at: time.Now()})
}

func (s *statementTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if record, start := s.record.Load(), ctx.Value(statementStartKey{}); record != nil && start != nil {
		(*record)(start.(statementStart).sql, time.Since(start.(statementStart).at))
	}
}

// largeBlobSink keeps blobs of at most 64 MiB in the fake store. A larger
// one, which the tests send as zero bytes, is read and dropped at Put and
// read back as zero bytes, so a 500,000,000-byte file streams through the
// hub both ways without being held in memory.
type largeBlobSink struct {
	*blob.FakeBlobStore
	dropped *sync.Map
}

func (s largeBlobSink) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	if size <= 64<<20 {
		return s.FakeBlobStore.Put(ctx, key, content, size)
	}
	_, err := io.Copy(io.Discard, content)
	s.dropped.Store(key, size)
	return err
}

func (s largeBlobSink) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if _, dropped := s.dropped.Load(key); dropped {
		return io.NopCloser(io.LimitReader(zeros{}, length)), nil
	}
	return s.FakeBlobStore.GetRange(ctx, key, offset, length)
}

func digest(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// transfer is the drive transfer client of one binding on a stream's
// connection.
type transfer struct {
	st        *cliStream
	bindingID string
}

func (st *cliStream) transfer(bindingID string) *transfer {
	return &transfer{st: st, bindingID: bindingID}
}

// invalid returns the client with its requests marked as outside the
// contract on purpose.
func (x *transfer) invalid() *transfer {
	return &transfer{st: x.st.invalid(), bindingID: x.bindingID}
}

// query is the common query of the binding with extra added.
func (x *transfer) query(extra url.Values) string {
	query := url.Values{"protocol_version": {"1"}, "connection_id": {x.st.connectionID}, "binding_id": {x.bindingID}}
	for name, values := range extra {
		query[name] = values
	}
	return "?" + query.Encode()
}

func (x *transfer) do(method, target string, header http.Header, body []byte) enginetest.Response {
	x.st.f.t.Helper()
	response, err := x.st.cli.Request(x.st.f.ctx, method, target, header, body)
	if err != nil {
		x.st.f.t.Fatal(err)
	}
	return response
}

// files lists driveID's files after after, from the start when after is "".
func (x *transfer) files(driveID, after string) enginetest.Response {
	extra := url.Values{}
	if after != "" {
		extra.Set("after_path", after)
	}
	return x.do(http.MethodGet, "/v1/client/drives/"+driveID+"/files"+x.query(extra), nil, nil)
}

// changes lists driveID's change records after the cursor; afterIndex < 0
// leaves after_index out.
func (x *transfer) changes(driveID string, afterSeq, afterIndex int64) enginetest.Response {
	extra := url.Values{"after_seq": {strconv.FormatInt(afterSeq, 10)}}
	if afterIndex >= 0 {
		extra.Set("after_index", strconv.FormatInt(afterIndex, 10))
	}
	return x.do(http.MethodGet, "/v1/client/drives/"+driveID+"/changes"+x.query(extra), nil, nil)
}

// put uploads content to driveID under its own hash.
func (x *transfer) put(driveID string, content []byte) enginetest.Response {
	return x.putAs(driveID, digest(content), content)
}

// putAs uploads content to driveID under hash sha.
func (x *transfer) putAs(driveID, sha string, content []byte) enginetest.Response {
	return x.do(http.MethodPut, "/v1/client/drives/"+driveID+"/content/"+sha+x.query(nil), http.Header{"Content-Type": {"application/octet-stream"}}, content)
}

// download reads content sha of driveID with rangeHeader, none when "".
func (x *transfer) download(driveID, sha, rangeHeader string) enginetest.Response {
	header := http.Header{}
	if rangeHeader != "" {
		header.Set("Range", rangeHeader)
	}
	return x.do(http.MethodGet, "/v1/client/drives/"+driveID+"/content/"+sha+x.query(nil), header, nil)
}

// commit sends a commit of changes, each a change object's JSON, to driveID.
func (x *transfer) commit(driveID string, changes ...string) enginetest.Response {
	body := `{"protocol_version":1,"connection_id":` + quoteJSON(x.st.connectionID) + `,"binding_id":` + quoteJSON(x.bindingID) +
		`,"changes":[` + strings.Join(changes, ",") + `]}`
	return x.do(http.MethodPost, "/v1/client/drives/"+driveID+"/commits", http.Header{"Content-Type": {"application/json"}}, []byte(body))
}

// write is a write change of path to content over base, a must-not-exist
// base when base is "".
func write(path, base string, content []byte) string {
	baseJSON := "null"
	if base != "" {
		baseJSON = quoteJSON(base)
	}
	return `{"path":` + quoteJSON(path) + `,"change":"write","base_sha256":` + baseJSON + `,"content_sha256":"` + digest(content) + `","executable":false}`
}

// remove is a delete change of path over base.
func remove(path, base string) string {
	return `{"path":` + quoteJSON(path) + `,"change":"delete","base_sha256":` + quoteJSON(base) + `}`
}

// requireRefused checks that response is status with the client error code.
func requireRefusal(t *testing.T, what string, response enginetest.Response, status int, code protocol.ErrorCode) protocol.ErrorBody {
	t.Helper()
	failure, err := response.Error()
	if response.Status != status || err != nil || failure.Code != code {
		t.Fatalf("%s = %d %s; want %d %s", what, response.Status, response.Body, status, code)
	}
	return failure
}

// committed decodes a commit's 200 answer.
func committed(t testing.TB, response enginetest.Response) protocol.CommitResponse {
	t.Helper()
	if response.Status != http.StatusOK {
		t.Fatalf("commit = %d %s; want 200", response.Status, response.Body)
	}
	answer, err := protocol.DecodeCommitResponse(response.Body, protocol.CommitResponseContext{})
	if err != nil {
		t.Fatalf("commit answer %s: %v", response.Body, err)
	}
	return answer
}

// listedFiles decodes a list files 200 answer.
func listedFiles(t *testing.T, response enginetest.Response) protocol.ListFilesResponse {
	t.Helper()
	if response.Status != http.StatusOK {
		t.Fatalf("list files = %d %s; want 200", response.Status, response.Body)
	}
	page, err := protocol.DecodeListFilesResponse(response.Body, protocol.ListFilesResponseContext{})
	if err != nil {
		t.Fatalf("list files answer: %v", err)
	}
	return page
}

// listedChanges decodes a list changes 200 answer.
func listedChanges(t *testing.T, response enginetest.Response) protocol.ListChangesResponse {
	t.Helper()
	if response.Status != http.StatusOK {
		t.Fatalf("list changes = %d %s; want 200", response.Status, response.Body)
	}
	page, err := protocol.DecodeListChangesResponse(response.Body, protocol.ListChangesResponseContext{})
	if err != nil {
		t.Fatalf("list changes answer: %v", err)
	}
	return page
}

// apiWrite writes content to path of driveID through drives.files.upload
// with mustNotExist or the expected hash base, and returns the status and
// body.
func (f *machineTools) apiWrite(driveID, path string, content []byte, base string) (int, []byte) {
	f.t.Helper()
	f.developer(http.MethodGet, "/v1/drives/"+driveID, "")
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	_ = form.WriteField("path", path)
	if base == "" {
		_ = form.WriteField("must_not_exist", "true")
	} else {
		_ = form.WriteField("expected_content_sha256", base)
	}
	part, _ := form.CreateFormFile("file", "file")
	_, _ = part.Write(content)
	_ = form.Close()
	request := httptest.NewRequest(http.MethodPost, "/v1/drives/"+driveID+"/files?beta=true", &body).WithContext(f.ctx)
	request.Header.Set("Content-Type", form.FormDataContentType())
	request.Header.Set("X-Api-Key", "machine-tools-key")
	recorder := httptest.NewRecorder()
	f.api.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.Bytes()
}

// driveFiles lists driveID's current files through drives.files.list, as
// path to content hash.
func (f *machineTools) driveFiles(driveID string) map[string]string {
	f.t.Helper()
	files := map[string]string{}
	for page := ""; ; {
		target := "/v1/drives/" + driveID + "/files?limit=100"
		if page != "" {
			target += "&page=" + url.QueryEscape(page)
		}
		status, body := f.developer(http.MethodGet, target, "")
		var answer struct {
			Data []struct {
				Path          string `json:"path"`
				ContentSHA256 string `json:"content_sha256"`
			} `json:"data"`
			NextPage *string `json:"next_page"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &answer) != nil {
			f.t.Fatalf("drives.files.list = %d %s", status, body)
		}
		for _, file := range answer.Data {
			files[file.Path] = file.ContentSHA256
		}
		if answer.NextPage == nil {
			return files
		}
		page = *answer.NextPage
	}
}

// driveChanged reads the next event, which must be drive.changed of driveID
// with seq.
func (st *cliStream) driveChanged(driveID string, seq int64) {
	st.f.t.Helper()
	event := st.next()
	payload, ok := event.Envelope.Payload.(protocol.DriveChangedPayload)
	if !ok || payload.DriveID != driveID || payload.Seq != seq {
		st.f.t.Fatalf("event = %s; want drive.changed of %s at seq %d", event.Data, driveID, seq)
	}
}

// boundStream is a prepared user computer of a Session whose drives are
// bound, with its stream and its binding's transfer client.
type boundStream struct {
	computerID string
	stream     *cliStream
	transfer   *transfer
}

// preparedWithDrives prepares a user computer named name for s, whose
// drives are already bound, with capabilities; the session.prepare must
// carry drives.
func (s *toolSession) preparedWithDrives(name string, capabilities protocol.Capabilities, drives []protocol.DriveBinding) boundStream {
	s.f.t.Helper()
	cli := s.f.userInstance(name)
	stream := s.f.connect(cli, capabilities)
	computerID := s.userComputer(cli)
	s.f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := computer.IssuePreparation(s.f.ctx, tx, s.f.ws, s.id, computerID)
		return err
	})
	prepare := stream.nextPrepare(drives)
	stream.report(prepare, "")
	return boundStream{computerID: computerID, stream: stream, transfer: stream.transfer(prepare.Envelope.Payload.(protocol.SessionPreparePayload).BindingID)}
}

// blockedBody is a request body whose first read waits until release
// closes: an answer that arrives while it waits came before any body byte.
type blockedBody struct{ release chan struct{} }

func (b blockedBody) Read([]byte) (int, error) {
	<-b.release
	return 0, io.ErrUnexpectedEOF
}

// putUnsent sends a PUT of driveID's content sha declaring length whose body
// sends no byte, and returns the answer.
func (x *transfer) putUnsent(driveID, sha string, length int64) enginetest.Response {
	x.st.f.t.Helper()
	body := blockedBody{release: make(chan struct{})}
	defer close(body.release)
	response, err := x.st.cli.Open(x.st.f.ctx, http.MethodPut, "/v1/client/drives/"+driveID+"/content/"+sha+x.query(nil),
		http.Header{"Content-Type": {"application/octet-stream"}}, body, length)
	if err != nil {
		x.st.f.t.Fatalf("PUT declaring %d bytes: %v; want an answer before the body", length, err)
	}
	defer func() { _ = response.Body.Close() }()
	data, _ := io.ReadAll(response.Body)
	return enginetest.Response{Status: response.StatusCode, Header: response.Header, Body: data}
}

// manyWrites is n write changes of distinct paths to content.
func manyWrites(n int, content []byte) []string {
	changes := make([]string, n)
	for i := range changes {
		changes[i] = write("many/"+strconv.Itoa(i)+".txt", "", content)
	}
	return changes
}

// Every drive transfer request is answered by the first check of the
// contract's order that fails: shape, connection, binding, drive access,
// then size and the upload budget; requests a check refuses take nothing
// from the budget, which a developer's uploads share, and a 403 ends only
// its request. A closing binding still
// commits until its closed report, a commit body over 16 MiB is refused
// before its connection is checked while one of exactly 16 MiB reaches that
// check, and authentication comes before every check. No refusal writes a
// session.error or adds a change record.
func TestDriveTransferAnswersTheFirstFailingCheck(t *testing.T) {
	f := newMachineTools(t, withUploadBudget(drive.MinUploadCount))
	s := f.session()
	s.idle()
	team, notes, outside := f.createDrive(), f.createDrive(), f.createDrive()
	s.bind(team, "read_write")
	s.bind(notes, "read_only")
	b := s.preparedWithDrives("checks", everyTool, sorted(rw(team), ro(notes)))
	x := b.transfer
	content := []byte("twelve bytes")
	stale := &transfer{st: &cliStream{f: f, cli: b.stream.cli, connectionID: "conn_stale"}, bindingID: x.bindingID}
	unknown := b.stream.transfer("bind_unknown")
	octet := http.Header{"Content-Type": {"application/octet-stream"}}
	marked, markedStale := x.invalid(), stale.invalid()

	// Every refusal of an upload or a commit is logged with its code, and
	// none adds a change record.
	const changeRecords = `SELECT count(*) FROM drive_changes WHERE drive_id = ANY($1)`
	records := f.count(changeRecords, []string{team, notes})
	var logged []string
	for _, row := range []struct {
		name     string
		answer   func() enginetest.Response
		status   int
		code     protocol.ErrorCode
		answered bool
	}{
		{"a list without binding_id", func() enginetest.Response {
			return marked.do(http.MethodGet, "/v1/client/drives/"+team+"/files?protocol_version=1&connection_id="+x.st.connectionID, nil, nil)
		}, http.StatusBadRequest, protocol.ErrorInvalidMessage, false},
		{"a read_only PUT of text/plain", func() enginetest.Response {
			return marked.do(http.MethodPut, "/v1/client/drives/"+notes+"/content/"+digest(content)+x.query(nil), http.Header{"Content-Type": {"text/plain"}}, content)
		}, http.StatusBadRequest, protocol.ErrorInvalidMessage, false},
		{"a read_only PUT without Content-Length", func() enginetest.Response {
			response, err := x.st.cli.Invalid().Open(f.ctx, http.MethodPut, "/v1/client/drives/"+notes+"/content/"+digest(content)+x.query(nil), octet, bytes.NewReader(content), -1)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			data, _ := io.ReadAll(response.Body)
			return enginetest.Response{Status: response.StatusCode, Body: data}
		}, http.StatusForbidden, protocol.ErrorDriveAccessDenied, false},
		{"a read_only PUT", func() enginetest.Response { return x.put(notes, content) }, http.StatusForbidden, protocol.ErrorDriveAccessDenied, false},
		{"a read_only commit of 10,001 changes", func() enginetest.Response { return marked.commit(notes, manyWrites(10_001, content)...) },
			http.StatusForbidden, protocol.ErrorDriveAccessDenied, false},
		{"a read_only commit", func() enginetest.Response { return x.commit(notes, write("a.txt", "", content)) },
			http.StatusForbidden, protocol.ErrorDriveAccessDenied, false},
		{"a list of a drive outside the drive set", func() enginetest.Response { return x.files(outside, "") },
			http.StatusForbidden, protocol.ErrorDriveAccessDenied, false},
		{"a list of a read_only drive", func() enginetest.Response { return x.files(notes, "") }, http.StatusOK, "", true},
		{"a download with two ranges", func() enginetest.Response { return markedStale.download(team, digest(content), "bytes=0-1,4-5") },
			http.StatusRequestedRangeNotSatisfiable, protocol.ErrorRangeNotSatisfiable, false},
		{"a download with a reversed range", func() enginetest.Response { return markedStale.download(team, digest(content), "bytes=5-3") },
			http.StatusBadRequest, protocol.ErrorInvalidMessage, false},
		{"a commit without changes", func() enginetest.Response { return markedStale.commit(team) }, http.StatusBadRequest, protocol.ErrorInvalidMessage, false},
		{"a commit naming a path twice", func() enginetest.Response {
			return markedStale.commit(team, write("a.txt", "", content), remove("a.txt", digest(content)))
		}, http.StatusBadRequest, protocol.ErrorInvalidMessage, false},
		{"a commit carrying a writer", func() enginetest.Response {
			return marked.do(http.MethodPost, "/v1/client/drives/"+team+"/commits", http.Header{"Content-Type": {"application/json"}},
				[]byte(`{"protocol_version":1,"connection_id":"`+x.st.connectionID+`","binding_id":"`+x.bindingID+`","writer":{"type":"api_actor","api_key_id":"key_1"},"changes":[`+write("a.txt", "", content)+`]}`))
		}, http.StatusBadRequest, protocol.ErrorInvalidMessage, false},
		{"a commit body of 16 MiB and one byte on a stale connection", func() enginetest.Response {
			body := `{"protocol_version":1,"connection_id":"conn_stale","binding_id":"` + x.bindingID + `","changes":[` + write("a.txt", "", content) + `],"pad":"`
			body += strings.Repeat("a", protocol.CommitRequestMaxJSONBytes+1-len(body)-2) + `"}`
			return marked.do(http.MethodPost, "/v1/client/drives/"+team+"/commits", http.Header{"Content-Type": {"application/json"}}, []byte(body))
		}, http.StatusRequestEntityTooLarge, protocol.ErrorRequestTooLarge, false},
		{"a commit body of exactly 16 MiB on a stale connection", func() enginetest.Response {
			body := `{"protocol_version":1,"connection_id":"conn_stale","binding_id":"` + x.bindingID + `","changes":[` + write("a.txt", "", content) + `]`
			body += strings.Repeat(" ", protocol.CommitRequestMaxJSONBytes-len(body)-1) + `}`
			return x.do(http.MethodPost, "/v1/client/drives/"+team+"/commits", http.Header{"Content-Type": {"application/json"}}, []byte(body))
		}, http.StatusConflict, protocol.ErrorStaleConnection, false},
		{"a list on a stale connection", func() enginetest.Response { return stale.files(team, "") }, http.StatusConflict, protocol.ErrorStaleConnection, false},
		{"a PUT on a stale connection", func() enginetest.Response {
			return markedStale.putUnsent(team, digest(content), protocol.FileMaxBytes+1)
		},
			http.StatusConflict, protocol.ErrorStaleConnection, false},
		{"a list of an unknown binding", func() enginetest.Response { return unknown.files(team, "") }, http.StatusConflict, protocol.ErrorStaleBinding, false},
		{"a commit of 10,001 changes", func() enginetest.Response { return marked.commit(team, manyWrites(10_001, content)...) },
			http.StatusRequestEntityTooLarge, protocol.ErrorRequestTooLarge, false},
		{"a PUT without Content-Length", func() enginetest.Response {
			response, err := x.st.cli.Invalid().Open(f.ctx, http.MethodPut, "/v1/client/drives/"+team+"/content/"+digest(content)+x.query(nil), octet, bytes.NewReader(content), -1)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			data, _ := io.ReadAll(response.Body)
			return enginetest.Response{Status: response.StatusCode, Body: data}
		}, http.StatusBadRequest, protocol.ErrorInvalidMessage, false},
		{"a PUT declaring 500,000,001 bytes", func() enginetest.Response { return marked.putUnsent(team, digest(content), protocol.FileMaxBytes+1) },
			http.StatusRequestEntityTooLarge, protocol.ErrorFileTooLarge, false},
		{"a PUT within the budget", func() enginetest.Response { return x.put(team, content) }, http.StatusOK, "", true},
	} {
		response := row.answer()
		if row.answered {
			if response.Status != row.status {
				t.Fatalf("%s = %d %s; want %d", row.name, response.Status, response.Body, row.status)
			}
			continue
		}
		refusal := requireRefusal(t, row.name, response, row.status, row.code)
		if strings.Contains(row.name, "PUT") || strings.Contains(row.name, "commit") {
			// A schema refusal's message can quote the request and is not
			// recorded; a limit refusal's is Engine's own text.
			cause := refusal.Message
			if strings.HasPrefix(cause, "protocol: ") && row.code == protocol.ErrorInvalidMessage {
				cause = "the request does not match its schema"
			}
			logged = append(logged, string(row.code)+": "+cause)
		}
	}
	if after := f.count(changeRecords, []string{team, notes}); after != records {
		t.Fatalf("change records = %d after the refusals; want the %d before them", after, records)
	}
	limited := x.put(team, []byte("over the budget"))
	refusal := requireRefusal(t, "a PUT past the budget", limited, http.StatusTooManyRequests, protocol.ErrorRateLimited)
	logged = append(logged, string(protocol.ErrorRateLimited)+": "+refusal.Message)
	// The budget is the Workspace's: the machine's upload spent the
	// developer's too.
	if status, body := f.apiWrite(team, "developer.txt", []byte("over the budget"), ""); status != http.StatusTooManyRequests || !strings.Contains(string(body), `"type":"rate_limit_error"`) {
		t.Fatalf("drives.files.upload past the budget the machine spent = %d %s; want 429 rate_limit_error", status, body)
	}
	var refusals []string
	for _, record := range workloadtest.Find(workloadtest.Records(t, []byte(f.hubLogs.String())), "drive.transfer_refused") {
		if record["error.message_safe"] == nil || record["error.message"] == "" {
			t.Fatalf("drive.transfer_refused %v lacks its safe message or its cause", record)
		}
		refusals = append(refusals, fmt.Sprint(record["error.code"], ": ", record["error.message"]))
	}
	if !slices.Equal(refusals, logged) {
		t.Fatalf("drive.transfer_refused records %q; want one per refused upload and commit, with the answered code and its cause, %q", refusals, logged)
	}
	if wait, err := strconv.Atoi(limited.Header.Get("Retry-After")); err != nil || wait < 1 || wait > 60 {
		t.Fatalf("Retry-After = %q; want 1 to 60 seconds", limited.Header.Get("Retry-After"))
	}

	// A closing binding commits until its closed report is stored.
	s.requireRemoved(b.computerID, http.StatusAccepted, "closing")
	closing, _ := b.stream.closeInstruction(s.id)
	if answer := committed(t, x.commit(team, write("closing.txt", "", content))); answer.Seq.Null || !answer.Seq.Present {
		t.Fatalf("a closing binding's commit = %+v; want a new seq", answer)
	}
	b.stream.closed(closing, "closed_checks", nil)
	requireRefusal(t, "a commit after the closed report", x.commit(team, write("closed.txt", "", content)), http.StatusConflict, protocol.ErrorStaleBinding)

	f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := instance.RevokeCredential(f.ctx, tx, f.ws, x.st.cli.InstanceID, x.st.cli.CredentialID)
		return err
	})
	requireRefusal(t, "a revoked credential's PUT declaring 500,000,001 bytes", marked.putUnsent(team, digest(content), protocol.FileMaxBytes+1),
		http.StatusUnauthorized, protocol.ErrorCredentialRevoked)
	if errs := f.sessionErrors(s.id); len(errs) != 0 {
		t.Fatalf("session errors %v; want none from a refused transfer", errs)
	}
}

// acceptedVersion is an accepted result's version_id, "" when it is null.
func acceptedVersion(t testing.TB, what string, result protocol.CommitResult) string {
	t.Helper()
	accepted, ok := result.(protocol.AcceptedResult)
	if !ok {
		t.Fatalf("%s = %+v; want accepted", what, result)
	}
	return accepted.VersionID.Value
}

// rejectedAs checks that result is rejected for reason and returns it.
func rejectedAs(t *testing.T, what string, result protocol.CommitResult, reason string) protocol.RejectedResult {
	t.Helper()
	rejected, ok := result.(protocol.RejectedResult)
	if !ok || rejected.Reason != reason {
		t.Fatalf("%s = %+v; want rejected %s", what, result, reason)
	}
	return rejected
}

// Content uploaded and not yet committed is in no other binding's file list
// or changes. Two uploads and one commit of both files give both change
// records the commit's seq, every stream whose binding holds the drive, the
// writer's own and another Session's included, receives drive.changed with
// it, and a developer
// downloads the committed file. A record carries its version's executable
// bit, which a developer's upload over the file keeps. That upload and a
// developer's restore reach the bindings as a machine's commit does. A
// commit's records keep the order of its changes, at the next seq.
func TestHubCommitGivesOneSeqToItsChangesAndNotifiesEveryMount(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	writer := s.preparedWithDrives("writer", everyTool, []protocol.DriveBinding{rw(team)})
	other := f.session()
	other.idle()
	other.bind(team, "read_write")
	reader := other.preparedWithDrives("reader", everyTool, []protocol.DriveBinding{rw(team)})
	a, b := []byte("first file"), []byte("second file")
	for _, content := range [][]byte{a, b} {
		if response := writer.transfer.put(team, content); response.Status != http.StatusOK {
			t.Fatalf("PUT = %d %s; want 200", response.Status, response.Body)
		}
	}
	if files, changes := listedFiles(t, reader.transfer.files(team, "")), listedChanges(t, reader.transfer.changes(team, 0, -1)); len(files.Files) != 0 || len(changes.Records) != 0 {
		t.Fatalf("another binding lists %+v and %+v before the commit; want nothing", files.Files, changes.Records)
	}
	answer := committed(t, writer.transfer.commit(team, strings.Replace(write("a.txt", "", a), `"executable":false`, `"executable":true`, 1), write("b.txt", "", b)))
	seq := answer.Seq.Value
	if !answer.Seq.Present || answer.Seq.Null || len(answer.Results) != 2 {
		t.Fatalf("commit = %+v; want one seq and two results", answer)
	}
	for i, result := range answer.Results {
		acceptedVersion(t, "result "+strconv.Itoa(i), result)
	}
	page := listedChanges(t, reader.transfer.changes(team, 0, -1))
	if len(page.Records) != 2 || page.Records[0].Seq != seq || page.Records[1].Seq != seq || page.Records[0].Path != "a.txt" || page.Records[1].Path != "b.txt" ||
		!page.Records[0].Executable || page.Records[1].Executable {
		t.Fatalf("change records = %+v; want a.txt, executable, and b.txt at seq %d", page.Records, seq)
	}
	reader.stream.driveChanged(team, seq)
	writer.stream.driveChanged(team, seq)
	if status, body := f.developer(http.MethodGet, "/v1/drives/"+team+"/files/content?path=a.txt", ""); status != http.StatusOK || !bytes.Equal(body, a) {
		t.Fatalf("drives.files.content of a.txt = %d %q; want the committed content", status, body)
	}

	replaced := []byte("replaced by a developer")
	if status, body := f.apiWrite(team, "a.txt", replaced, digest(a)); status != http.StatusCreated {
		t.Fatalf("drives.files.upload over a.txt = %d %s; want 201", status, body)
	}
	after := listedChanges(t, reader.transfer.changes(team, seq, -1))
	if len(after.Records) != 1 || after.Records[0].Path != "a.txt" || after.Records[0].Operation != "modified" ||
		after.Records[0].ContentSHA256.Value != digest(replaced) || !after.Records[0].Executable {
		t.Fatalf("change records after the upload = %+v; want a.txt modified to the uploaded content, still executable", after.Records)
	}
	reader.stream.driveChanged(team, after.Records[0].Seq)
	writer.stream.driveChanged(team, after.Records[0].Seq)

	var versions struct {
		Data []struct {
			CreatedAt string `json:"created_at"`
		}
	}
	if status, body := f.developer(http.MethodGet, "/v1/drives/"+team+"/files/versions?path=b.txt", ""); status != http.StatusOK || json.Unmarshal(body, &versions) != nil || len(versions.Data) != 1 {
		t.Fatalf("versions of b.txt = %d %s; want the committed one", status, body)
	}
	var restored struct {
		Seq      int64
		Restored int
	}
	if status, body := f.developer(http.MethodPost, "/v1/drives/"+team+"/restore", `{"as_of":"`+versions.Data[0].CreatedAt+`"}`); status != http.StatusOK ||
		json.Unmarshal(body, &restored) != nil || restored.Restored != 1 {
		t.Fatalf("drives.restore = %d %s; want a.txt restored", status, body)
	}
	back := listedChanges(t, reader.transfer.changes(team, after.Records[0].Seq, -1))
	if len(back.Records) != 1 || back.Records[0].Seq != restored.Seq || back.Records[0].Path != "a.txt" || back.Records[0].ContentSHA256.Value != digest(a) {
		t.Fatalf("change records after the restore = %+v; want a.txt back at its committed content at seq %d", back.Records, restored.Seq)
	}
	reader.stream.driveChanged(team, restored.Seq)
	writer.stream.driveChanged(team, restored.Seq)

	// A commit's records are indexed from 0 in the order of its changes, at
	// the seq after the drive's last.
	c := []byte("third file")
	if response := writer.transfer.put(team, c); response.Status != http.StatusOK {
		t.Fatalf("PUT = %d %s; want 200", response.Status, response.Body)
	}
	committed(t, writer.transfer.commit(team, remove("b.txt", digest(b)), write("c.txt", "", c), write("a.txt", digest(a), c)))
	ordered := listedChanges(t, reader.transfer.changes(team, restored.Seq, -1)).Records
	if len(ordered) != 3 || ordered[0].Seq != restored.Seq+1 ||
		ordered[0].Index != 0 || ordered[0].Path != "b.txt" || ordered[0].Operation != "deleted" ||
		ordered[1].Index != 1 || ordered[1].Path != "c.txt" || ordered[1].Operation != "created" ||
		ordered[2].Index != 2 || ordered[2].Path != "a.txt" || ordered[2].Operation != "modified" {
		t.Fatalf("change records = %+v; want b.txt deleted, c.txt created and a.txt modified, indexed 0 to 2 at seq %d", ordered, restored.Seq+1)
	}
}

// A PUT whose body does not hash to its URL keeps nothing and is logged with
// its code, and a commit
// naming content its drive holds no version of is refused with that hash,
// also when another drive holds it, until the content is uploaded to the
// drive itself.
func TestHubCommitNeedsItsContentUploadedToItsOwnDrive(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team, other := f.createDrive(), f.createDrive()
	s.bind(team, "read_write")
	s.bind(other, "read_write")
	x := s.preparedWithDrives("writer", everyTool, sorted(rw(team), rw(other))).transfer

	named, sent := []byte("named content"), []byte("sent content")
	requireRefusal(t, "a PUT whose body differs from its hash", x.putAs(team, digest(named), sent), http.StatusBadRequest, protocol.ErrorContentHashMismatch)
	if n := f.count(`SELECT count(*) FROM drive_contents WHERE content_sha256 = ANY($1)`, []string{digest(named), digest(sent)}); n != 0 {
		t.Fatalf("contents kept after the mismatch = %d; want 0", n)
	}
	if refused := workloadtest.Find(workloadtest.Records(t, []byte(f.hubLogs.String())), "drive.transfer_refused"); len(refused) != 1 ||
		refused[0]["error.code"] != string(protocol.ErrorContentHashMismatch) {
		t.Fatalf("drive.transfer_refused records %v; want the mismatch alone", refused)
	}
	missing := requireRefusal(t, "a commit of content never stored", x.commit(team, write("named.txt", "", named)), http.StatusConflict, protocol.ErrorContentMissing)
	if got := missing.MissingSHA256.Value; len(got) != 1 || got[0] != digest(named) {
		t.Fatalf("missing_sha256 = %v; want [%s]", got, digest(named))
	}

	held := []byte("held by the other drive")
	if response := x.put(other, held); response.Status != http.StatusOK {
		t.Fatalf("PUT to the other drive = %d %s", response.Status, response.Body)
	}
	acceptedVersion(t, "the other drive's write", committed(t, x.commit(other, write("held.txt", "", held))).Results[0])
	missing = requireRefusal(t, "a commit of content only another drive holds", x.commit(team, write("held.txt", "", held)), http.StatusConflict, protocol.ErrorContentMissing)
	if got := missing.MissingSHA256.Value; len(got) != 1 || got[0] != digest(held) {
		t.Fatalf("missing_sha256 = %v; want [%s]", got, digest(held))
	}
	requireRefusal(t, "a download of content only another drive holds", x.download(team, digest(held), ""), http.StatusNotFound, protocol.ErrorContentNotFound)
	if response := x.put(team, held); response.Status != http.StatusOK {
		t.Fatalf("PUT to the drive itself = %d %s", response.Status, response.Body)
	}
	acceptedVersion(t, "the write after the upload", committed(t, x.commit(team, write("held.txt", "", held))).Results[0])
}

// The hub's commits are machine commits by the binding's Session and
// computer, decided change by change: a stale base loses to the version that
// won, which the result names with its writer; a commit sent again changes
// nothing and has no seq; case-folded paths follow the path rule; and the
// drive's max_bytes rejects exactly the writes that cross it after the
// commit's earlier changes. No decision and no repeated commit writes a
// session.error.
func TestHubCommitDecidesEachChange(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	b := s.preparedWithDrives("writer", everyTool, []protocol.DriveBinding{rw(team)})
	x := b.transfer
	upload := func(contents ...[]byte) {
		t.Helper()
		for _, content := range contents {
			if response := x.put(team, content); response.Status != http.StatusOK {
				t.Fatalf("PUT = %d %s; want 200", response.Status, response.Body)
			}
		}
	}
	versions := func() int { return f.count(`SELECT count(*) FROM drive_versions WHERE drive_id=$1`, team) }

	// Every version the hub writes has the binding's Session and computer.
	v1, v2, v3 := []byte("version one"), []byte("version two"), []byte("version three")
	upload(v1, v3)
	first := acceptedVersion(t, "the first write", committed(t, x.commit(team, write("shared.txt", "", v1))).Results[0])
	if row := f.row(`SELECT writer_kind,writer_id,writer_computer_id FROM drive_versions WHERE version_id=$1`, first); row["writer_kind"] != "session_actor" ||
		row["writer_id"] != s.id || row["writer_computer_id"] != b.computerID {
		t.Fatalf("hub version writer = %v; want session_actor %s on %s", row, s.id, b.computerID)
	}

	// Two writers with one base: the developer API wins, the hub's change
	// conflicts and names the winner; the commit's other change still
	// takes the commit's seq.
	status, body := f.apiWrite(team, "shared.txt", v2, digest(v1))
	var won struct {
		VersionID string `json:"version_id"`
	}
	if status != http.StatusCreated || json.Unmarshal(body, &won) != nil {
		t.Fatalf("drives.files.upload = %d %s; want 201", status, body)
	}
	apiWriter := f.row(`SELECT writer_kind,writer_id FROM drive_versions WHERE version_id=$1`, won.VersionID)
	upload([]byte("fresh"))
	answer := committed(t, x.commit(team, write("fresh.txt", "", []byte("fresh")), write("shared.txt", digest(v1), v3)))
	fresh := acceptedVersion(t, "the commit's other change", answer.Results[0])
	conflict := rejectedAs(t, "the stale base", answer.Results[1], "conflict")
	if current := conflict.Current.Value; !conflict.Current.Set || current.VersionID != won.VersionID || current.ContentSHA256.Value != digest(v2) ||
		current.Writer.Type != apiWriter["writer_kind"] || current.Writer.APIKeyID != apiWriter["writer_id"] {
		t.Fatalf("conflict current = %+v; want %s by %v", conflict.Current, won.VersionID, apiWriter)
	}
	if seq := f.count(`SELECT seq FROM drive_versions WHERE version_id=$1`, fresh); answer.Seq.Null || int64(seq) != answer.Seq.Value {
		t.Fatalf("the accepted change's seq = %d; want the commit's %+v", seq, answer.Seq)
	}
	// A deletion with the same stale base loses too, and the drive keeps
	// the version that won.
	kept := versions()
	stale := rejectedAs(t, "the deletion with a stale base", committed(t, x.commit(team, remove("shared.txt", digest(v1)))).Results[0], "conflict")
	if current := stale.Current.Value; !stale.Current.Set || current.VersionID != won.VersionID || versions() != kept {
		t.Fatalf("stale deletion's current = %+v with %d versions; want %s and the %d versions before it", stale.Current, versions(), won.VersionID, kept)
	}

	// A commit sent again after its answer was lost.
	gone := []byte("to be deleted")
	upload(gone)
	committed(t, x.commit(team, write("gone.txt", "", gone)))
	again := []string{write("again.txt", "", v1), remove("gone.txt", digest(gone))}
	sent := committed(t, x.commit(team, again...))
	deletion := acceptedVersion(t, "the first sending's deletion", sent.Results[1])
	before := versions()
	resent := committed(t, x.commit(team, again...))
	if !resent.Seq.Null || versions() != before || acceptedVersion(t, "the resent write", resent.Results[0]) != acceptedVersion(t, "the first write", sent.Results[0]) ||
		acceptedVersion(t, "the resent deletion", resent.Results[1]) != deletion {
		t.Fatalf("resent commit = %+v after %+v, versions %d -> %d; want the same versions, no seq, nothing added", resent, sent, before, versions())
	}

	// Paths that differ only in case.
	upload([]byte("report"))
	committed(t, x.commit(team, write("report.md", "", []byte("report"))))
	for i, result := range committed(t, x.commit(team, remove("report.md", digest([]byte("report"))), write("Report.md", "", []byte("report")))).Results {
		acceptedVersion(t, "the case rename's change "+strconv.Itoa(i), result)
	}
	if rule := rejectedAs(t, "a write folding onto Report.md", committed(t, x.commit(team, write("REPORT.md", "", []byte("report")))).Results[0], "path_rule"); rule.Rule.Value == "" {
		t.Fatalf("path_rule result = %+v; want the rule text", rule)
	}

	// max_bytes, with a deletion earlier in the commit freeing its usage.
	usage := int64(f.count(`SELECT usage_bytes FROM drives WHERE drive_id=$1`, team))
	sized := func(n int, fill string) []byte { return []byte(strings.Repeat(fill, n)) }
	a50, b40, c20, d5 := sized(50, "a"), sized(40, "b"), sized(20, "c"), sized(5, "d")
	old := sized(40, "o")
	upload(a50, b40, c20, d5, old)
	committed(t, x.commit(team, write("old.txt", "", old)))
	f.exec(`UPDATE drives SET max_bytes=$1 WHERE drive_id=$2`, usage+40+60, team)
	results := committed(t, x.commit(team, remove("old.txt", digest(old)), write("a.txt", "", a50), write("b.txt", "", b40), write("c.txt", "", c20), write("d.txt", "", d5))).Results
	for i, want := range []string{"accepted", "accepted", "accepted", "limit", "accepted"} {
		if want == "accepted" {
			acceptedVersion(t, "max_bytes change "+strconv.Itoa(i), results[i])
		} else {
			rejectedAs(t, "max_bytes change "+strconv.Itoa(i), results[i], want)
		}
	}
	// Below usage: deletions and shrinking writes are accepted, a growing
	// write is not.
	a10, b45 := sized(10, "A"), sized(45, "B")
	upload(a10, b45)
	f.exec(`UPDATE drives SET max_bytes=$1 WHERE drive_id=$2`, usage+50, team)
	results = committed(t, x.commit(team, remove("d.txt", digest(d5)), write("a.txt", digest(a50), a10), write("b.txt", digest(b40), b45))).Results
	acceptedVersion(t, "the deletion below max_bytes", results[0])
	acceptedVersion(t, "the shrinking write below max_bytes", results[1])
	rejectedAs(t, "the growing write below max_bytes", results[2], "limit")
	if errs := f.sessionErrors(s.id); len(errs) != 0 {
		t.Fatalf("session errors %v; want none from a decided or repeated commit", errs)
	}
}

// uploaded uploads each content to driveID through x.
func (x *transfer) uploaded(driveID string, contents ...[]byte) {
	x.st.f.t.Helper()
	for _, content := range contents {
		if response := x.put(driveID, content); response.Status != http.StatusOK {
			x.st.f.t.Fatalf("PUT = %d %s; want 200", response.Status, response.Body)
		}
	}
}

// filled commits n files many/<i>.txt of content to driveID and returns
// the commit's seq.
func (x *transfer) filled(driveID string, n int, content []byte) int64 {
	x.st.f.t.Helper()
	x.uploaded(driveID, content)
	answer := committed(x.st.f.t, x.commit(driveID, manyWrites(n, content)...))
	if answer.Seq.Null || !answer.Seq.Present {
		x.st.f.t.Fatalf("fill commit = %+v; want a seq", answer.Seq)
	}
	return answer.Seq.Value
}

// A list without after_path starts at the drive's last commit, read before
// the page, so a commit landing while the page is read is after its
// start_seq; list pages hold at most 1,000 entries in byte order of path and
// say when more follow.
func TestHubListPagesHoldAThousandEntries(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	x := s.preparedWithDrives("lister", everyTool, []protocol.DriveBinding{rw(team)}).transfer
	seq := x.filled(team, 1_001, []byte("one of many"))

	first := listedFiles(t, x.files(team, ""))
	if len(first.Files) != 1_000 || !first.HasMore || !first.StartSeq.Set || first.StartSeq.Value != seq {
		t.Fatalf("first files page = %d entries, has_more %v, start_seq %+v; want 1000, true, %d", len(first.Files), first.HasMore, first.StartSeq, seq)
	}
	for i := 1; i < len(first.Files); i++ {
		if first.Files[i-1].Path >= first.Files[i].Path {
			t.Fatalf("files %q then %q; want byte order", first.Files[i-1].Path, first.Files[i].Path)
		}
	}
	last := listedFiles(t, x.files(team, first.Files[999].Path))
	if len(last.Files) != 1 || last.HasMore || last.StartSeq.Set || last.Files[0].Path <= first.Files[999].Path {
		t.Fatalf("second files page = %+v; want the one remaining file, no start_seq, no more", last)
	}

	changes := listedChanges(t, x.changes(team, 0, -1))
	if len(changes.Records) != 1_000 || !changes.HasMore {
		t.Fatalf("first changes page = %d records, has_more %v; want 1000, true", len(changes.Records), changes.HasMore)
	}
	end := changes.Records[999]
	rest := listedChanges(t, x.changes(team, end.Seq, end.Index))
	if len(rest.Records) != 1 || rest.HasMore || rest.Records[0].Index != end.Index+1 {
		t.Fatalf("second changes page = %+v; want the one record after %d/%d", rest, end.Seq, end.Index)
	}
	if after := listedChanges(t, x.changes(team, seq, -1)); len(after.Records) != 0 || after.HasMore {
		t.Fatalf("changes after the whole commit %d = %+v; want none", seq, after)
	}

	listing := f.hubSQL.pauseOn("path>$3 AND path LIKE $4")
	type answer struct {
		response enginetest.Response
		err      error
	}
	listed := make(chan answer, 1)
	go func() {
		response, err := x.st.cli.Request(f.ctx, http.MethodGet, "/v1/client/drives/"+team+"/files"+x.query(nil), nil, nil)
		listed <- answer{response, err}
	}()
	<-listing.reached
	landed := f.apiCommit(team, "landed.txt")
	close(listing.release)
	got := <-listed
	if got.err != nil {
		t.Fatal(got.err)
	}
	if page := listedFiles(t, got.response); !page.StartSeq.Set || page.StartSeq.Value != seq || landed != seq+1 {
		t.Fatalf("files page read while commit %d landed has start_seq %+v; want %d", landed, page.StartSeq, seq)
	}
}

// A fresh mount that lists the files from the start and then the changes
// from the page's start_seq, while commits keep landing, ends with the
// drive's file table.
func TestHubListsCatchUpWithCommitsLandingMeanwhile(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	writer := s.preparedWithDrives("writer", everyTool, []protocol.DriveBinding{rw(team)})
	mount := s.preparedWithDrives("mount", everyTool, []protocol.DriveBinding{rw(team)}).transfer
	contents := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta")}
	writer.transfer.uploaded(team, contents...)
	seeded := writer.transfer.filled(team, 1_500, contents[0])
	if seeded <= 0 {
		t.Fatal("no seed commit")
	}

	// The writer keeps its own table to send correct bases.
	current := map[string]string{}
	for i := range 1_500 {
		current["many/"+strconv.Itoa(i)+".txt"] = digest(contents[0])
	}
	stop, done := make(chan struct{}), make(chan error, 1)
	commits := atomic.Int64{}
	go func() {
		random := rand.New(rand.NewPCG(24, 11)) //nolint:gosec // G404: a fixed seed makes the commit sequence repeatable.
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			var changes []string
			touched := map[string]bool{}
			for range 1 + random.IntN(4) {
				path := "many/" + strconv.Itoa(random.IntN(1_700)) + ".txt"
				if touched[path] {
					continue
				}
				touched[path] = true
				content := contents[random.IntN(len(contents))]
				switch base, exists := current[path]; {
				case !exists:
					changes = append(changes, write(path, "", content))
					current[path] = digest(content)
				case random.IntN(3) == 0:
					changes = append(changes, remove(path, base))
					delete(current, path)
				case base != digest(content):
					changes = append(changes, write(path, base, content))
					current[path] = digest(content)
				}
			}
			if len(changes) == 0 {
				continue
			}
			body := `{"protocol_version":1,"connection_id":` + quoteJSON(writer.stream.connectionID) + `,"binding_id":` + quoteJSON(writer.transfer.bindingID) +
				`,"changes":[` + strings.Join(changes, ",") + `]}`
			response, err := writer.stream.cli.Request(f.ctx, http.MethodPost, "/v1/client/drives/"+team+"/commits", http.Header{"Content-Type": {"application/json"}}, []byte(body))
			if err == nil && response.Status != http.StatusOK {
				err = fmt.Errorf("commit = %d %s", response.Status, response.Body)
			}
			if err == nil {
				var answer protocol.CommitResponse
				if answer, err = protocol.DecodeCommitResponse(response.Body, protocol.CommitResponseContext{}); err == nil {
					for i, result := range answer.Results {
						if _, ok := result.(protocol.AcceptedResult); !ok {
							err = fmt.Errorf("change %s = %+v; want accepted", changes[i], result)
						}
					}
				}
			}
			if err != nil {
				done <- err
				return
			}
			commits.Add(1)
		}
	}()
	waitCommits := func(n int64) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for commits.Load() < n {
			if time.Now().After(deadline) {
				t.Fatalf("the writer made %d commits; want %d", commits.Load(), n)
			}
			time.Sleep(time.Millisecond)
		}
	}

	table := map[string]string{}
	page := listedFiles(t, mount.files(team, ""))
	start := page.StartSeq.Value
	for {
		for _, file := range page.Files {
			table[file.Path] = file.ContentSHA256
		}
		if !page.HasMore {
			break
		}
		waitCommits(commits.Load() + 5)
		page = listedFiles(t, mount.files(team, page.Files[len(page.Files)-1].Path))
	}
	afterSeq, afterIndex := start, int64(-1)
	catchUp := func() {
		t.Helper()
		for {
			changes := listedChanges(t, mount.changes(team, afterSeq, afterIndex))
			for _, record := range changes.Records {
				if record.ContentSHA256.Null {
					delete(table, record.Path)
				} else {
					table[record.Path] = record.ContentSHA256.Value
				}
				afterSeq, afterIndex = record.Seq, record.Index
			}
			if !changes.HasMore {
				return
			}
		}
	}
	waitCommits(commits.Load() + 5)
	catchUp()
	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	catchUp()
	if want := f.driveFiles(team); !maps.Equal(table, want) {
		t.Fatalf("the mount's table has %d files, drives.files.list %d; want them equal", len(table), len(want))
	}
	if commits.Load() < 10 {
		t.Fatalf("only %d commits landed during the listing", commits.Load())
	}
}

// A download serves the content a version of its drive names, whole or as
// one RFC 9110 range, to a read_write and a read_only binding; a hash the
// drive's versions do not name is not found.
func TestHubDownloadServesOneRange(t *testing.T) {
	f := newMachineTools(t)
	s, other := f.session(), f.session()
	s.idle()
	other.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	other.bind(team, "read_only")
	x := s.preparedWithDrives("reader", everyTool, []protocol.DriveBinding{rw(team)}).transfer
	readOnly := other.preparedWithDrives("read_only reader", everyTool, []protocol.DriveBinding{ro(team)}).transfer
	content, empty := []byte("twelve bytes"), []byte{}
	x.uploaded(team, content, empty)
	committed(t, x.commit(team, write("twelve.txt", "", content), write("empty.txt", "", empty)))

	for _, row := range []struct {
		content      []byte
		rangeHeader  string
		status       int
		contentRange string
		body         string
		code         protocol.ErrorCode
		// outside marks a Range the contract does not let the CLI send.
		outside bool
	}{
		{content, "", http.StatusOK, "", "twelve bytes", "", false},
		{content, "bytes=11-11", http.StatusPartialContent, "bytes 11-11/12", "s", "", false},
		{content, "bytes=4-999", http.StatusPartialContent, "bytes 4-11/12", "ve bytes", "", false},
		{content, "bytes=4-", http.StatusPartialContent, "bytes 4-11/12", "ve bytes", "", false},
		{content, "bytes=-5", http.StatusPartialContent, "bytes 7-11/12", "bytes", "", false},
		{content, "bytes=-20", http.StatusPartialContent, "bytes 0-11/12", "twelve bytes", "", false},
		{content, "bytes=12-", http.StatusRequestedRangeNotSatisfiable, "", "", protocol.ErrorRangeNotSatisfiable, false},
		{content, "bytes=-0", http.StatusRequestedRangeNotSatisfiable, "", "", protocol.ErrorRangeNotSatisfiable, false},
		{content, "bytes=0-1,4-5", http.StatusRequestedRangeNotSatisfiable, "", "", protocol.ErrorRangeNotSatisfiable, true},
		{empty, "bytes=0-", http.StatusRequestedRangeNotSatisfiable, "", "", protocol.ErrorRangeNotSatisfiable, false},
		{content, "bytes=5-3", http.StatusBadRequest, "", "", protocol.ErrorInvalidMessage, true},
		{[]byte("never stored"), "", http.StatusNotFound, "", "", protocol.ErrorContentNotFound, false},
	} {
		what := "download of " + strconv.Itoa(len(row.content)) + " bytes with Range " + quoteJSON(row.rangeHeader)
		reader := x
		if row.outside {
			reader = x.invalid()
		}
		response := reader.download(team, digest(row.content), row.rangeHeader)
		if row.code != "" {
			requireRefusal(t, what, response, row.status, row.code)
			continue
		}
		if response.Status != row.status || response.Header.Get("Content-Range") != row.contentRange || string(response.Body) != row.body {
			t.Fatalf("%s = %d %q %q; want %d %q %q", what, response.Status, response.Header.Get("Content-Range"), response.Body, row.status, row.contentRange, row.body)
		}
	}
	if response := readOnly.download(team, digest(content), ""); response.Status != http.StatusOK || string(response.Body) != "twelve bytes" {
		t.Fatalf("download through the read_only binding = %d %s; want 200 with the content", response.Status, response.Body)
	}
}

// A download that its reader stops reading for over 10 seconds, with the
// server's writes blocked, completes once reading resumes.
func TestHubDownloadOutlastsAPausedReader(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	x := s.preparedWithDrives("reader", everyTool, []protocol.DriveBinding{rw(team)}).transfer
	content := bytes.Repeat([]byte("paused download "), 3<<20) // 48 MiB, past the socket buffers
	x.uploaded(team, content)
	committed(t, x.commit(team, write("large.bin", "", content)))

	response, err := x.st.cli.Open(f.ctx, http.MethodGet, "/v1/client/drives/"+team+"/content/"+digest(content)+x.query(nil), nil, nil, 0)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("download = %v, %v; want 200", response, err)
	}
	defer func() { _ = response.Body.Close() }()
	hash := sha256.New()
	if _, err := io.CopyN(hash, response.Body, 1<<20); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10*time.Second + 500*time.Millisecond)
	if _, err := io.Copy(hash, response.Body); err != nil {
		t.Fatalf("download after the pause: %v", err)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != digest(content) {
		t.Fatalf("downloaded %s; want %s", got, digest(content))
	}
}

// A file rewritten past the kept old versions loses its oldest version but
// not that version's change record, so a catch-up from below it lists it
// and ends at the current content; a cursor below a record older than 30
// days has expired.
func TestHubChangeListsKeepRecordsUntilTheyAge(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	x := s.preparedWithDrives("reader", everyTool, []protocol.DriveBinding{rw(team)}).transfer
	base := ""
	var last []byte
	for i := range 12 {
		last = []byte("rewrite " + strconv.Itoa(i))
		x.uploaded(team, last)
		committed(t, x.commit(team, write("notes.md", base, last)))
		base = digest(last)
	}
	records := listedChanges(t, x.changes(team, 0, -1)).Records
	if len(records) != 12 || records[11].ContentSHA256.Value != digest(last) {
		t.Fatalf("change records = %+v; want 12 ending at the current content", records)
	}
	if n := f.count(`SELECT count(*) FROM drive_versions WHERE drive_id=$1 AND version_id=$2`, team, records[0].VersionID); n != 0 {
		t.Fatalf("the first version is still kept; want it removed by the rewrites")
	}

	f.exec(`UPDATE drive_changes SET created_at=now()-interval '31 days' WHERE drive_id=$1 AND seq=$2`, team, records[0].Seq)
	requireRefusal(t, "a cursor below a record older than 30 days", x.changes(team, 0, -1), http.StatusGone, protocol.ErrorDriveCursorExpired)
	if rest := listedChanges(t, x.changes(team, records[0].Seq, -1)); len(rest.Records) != 11 {
		t.Fatalf("changes after the aged record = %d; want 11", len(rest.Records))
	}
}

// zeros reads zero bytes.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// A content upload of exactly 500,000,000 bytes is stored, committed, listed
// with its size and downloaded whole, and a commit of 10,000 changes is
// accepted whole under one seq, its headers within the CLI's 10-second wait
// after the request body.
func TestHubAcceptsTheLargestUploadAndCommit(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	x := s.preparedWithDrives("writer", everyTool, []protocol.DriveBinding{rw(team)}).transfer

	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(zeros{}, protocol.FileMaxBytes)); err != nil {
		t.Fatal(err)
	}
	sha := hex.EncodeToString(hash.Sum(nil))
	response, err := x.st.cli.Open(f.ctx, http.MethodPut, "/v1/client/drives/"+team+"/content/"+sha+x.query(nil),
		http.Header{"Content-Type": {"application/octet-stream"}}, io.LimitReader(zeros{}, protocol.FileMaxBytes), protocol.FileMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	var stored struct {
		ContentSHA256 string `json:"content_sha256"`
		SizeBytes     int64  `json:"size_bytes"`
	}
	if response.StatusCode != http.StatusOK || json.Unmarshal(body, &stored) != nil || stored.ContentSHA256 != sha || stored.SizeBytes != protocol.FileMaxBytes {
		t.Fatalf("PUT of %d bytes = %d %s; want 200 with its hash and size", protocol.FileMaxBytes, response.StatusCode, body)
	}
	committed(t, x.commit(team, `{"path":"max.bin","change":"write","base_sha256":null,"content_sha256":"`+sha+`","executable":false}`))
	var listed struct {
		Data []struct {
			SizeBytes int64 `json:"size_bytes"`
		}
	}
	if status, body := f.developer(http.MethodGet, "/v1/drives/"+team+"/files?path_prefix=max.bin", ""); status != http.StatusOK ||
		json.Unmarshal(body, &listed) != nil || len(listed.Data) != 1 || listed.Data[0].SizeBytes != protocol.FileMaxBytes {
		t.Fatalf("drives.files.list of max.bin = %d %s; want it with size_bytes %d", status, body, protocol.FileMaxBytes)
	}
	download, err := x.st.cli.Open(f.ctx, http.MethodGet, "/v1/client/drives/"+team+"/content/"+sha+x.query(nil), nil, nil, 0)
	if err != nil || download.StatusCode != http.StatusOK {
		t.Fatalf("download = %v, %v; want 200", download, err)
	}
	read := sha256.New()
	n, err := io.Copy(read, download.Body)
	_ = download.Body.Close()
	if err != nil || n != protocol.FileMaxBytes || hex.EncodeToString(read.Sum(nil)) != sha {
		t.Fatalf("downloaded %d bytes, %v; want all %d with the content's hash", n, err, protocol.FileMaxBytes)
	}

	content := []byte("one of ten thousand")
	x.uploaded(team, content)
	commit := `{"protocol_version":1,"connection_id":` + quoteJSON(x.st.connectionID) + `,"binding_id":` + quoteJSON(x.bindingID) +
		`,"changes":[` + strings.Join(manyWrites(protocol.CommitMaxChanges, content), ",") + `]}`
	request := &timedBody{Reader: strings.NewReader(commit)}
	response, err = x.st.cli.Open(f.ctx, http.MethodPost, "/v1/client/drives/"+team+"/commits", http.Header{"Content-Type": {"application/json"}}, request, int64(len(commit)))
	if err != nil {
		t.Fatal(err)
	}
	wait := time.Since(request.done)
	body, _ = io.ReadAll(response.Body)
	_ = response.Body.Close()
	answer, err := protocol.DecodeCommitResponse(body, protocol.CommitResponseContext{})
	if response.StatusCode != http.StatusOK || err != nil || !answer.Seq.Present || answer.Seq.Null {
		t.Fatalf("commit of %d changes = %d %.200s; want 200 with a seq", protocol.CommitMaxChanges, response.StatusCode, body)
	}
	if wait > 10*time.Second {
		t.Fatalf("the commit's headers arrived %s after its body; want within the CLI's 10-second wait", wait)
	}
	seq := answer.Seq.Value
	if n := f.count(`SELECT count(*) FROM drive_changes WHERE drive_id=$1 AND seq=$2`, team, seq); n != protocol.CommitMaxChanges {
		t.Fatalf("change records at seq %d = %d; want %d", seq, n, protocol.CommitMaxChanges)
	}
}

// lastSeq is driveID's last_seq.
func (f *machineTools) lastSeq(driveID string) int64 {
	f.t.Helper()
	return int64(f.count(`SELECT last_seq FROM drives WHERE drive_id=$1`, driveID))
}

// apiCommit writes a new file path to driveID through the developer API and
// returns the drive's last_seq after it.
func (f *machineTools) apiCommit(driveID, path string) int64 {
	f.t.Helper()
	if status, body := f.apiWrite(driveID, path, []byte("written through the api: "+path), ""); status != http.StatusCreated {
		f.t.Fatalf("drives.files.upload %s = %d %s; want 201", path, status, body)
	}
	return f.lastSeq(driveID)
}

// finish posts the terminal report of execute.
func (st *cliStream) finish(execute enginetest.Event, messageID string) {
	st.f.t.Helper()
	task := execute.Envelope.Payload.(protocol.ExecutePayload).TaskID
	report := execution(st.connectionID, messageID, execute, "terminal", 1, execEnvelope("success", task, "ok"))
	st.held(enginetest.CaseEnded, execute, report)
	st.acked(report, messageID)
}

// executeOf reads the next event, which must be the tool.execute of use.
func (st *cliStream) executeOf(use string) enginetest.Event {
	st.f.t.Helper()
	event, payload := st.execute()
	if payload.ToolUseEventID != use {
		st.f.t.Fatalf("tool.execute of %s; want %s", payload.ToolUseEventID, use)
	}
	return event
}

// executeAfterRepeat is executeOf after at most one more drive.changed of
// driveID at seq. A stream that starts after a commit sends the drive's
// notice at its start, and once more when the commit's own notice reaches
// the pod after that start; either way before the instructions that follow
// the commit.
func (st *cliStream) executeAfterRepeat(driveID string, seq int64, use string) enginetest.Event {
	st.f.t.Helper()
	event := st.next()
	if repeat, ok := event.Envelope.Payload.(protocol.DriveChangedPayload); ok && repeat.DriveID == driveID && repeat.Seq == seq {
		event = st.next()
	}
	if payload, ok := event.Envelope.Payload.(protocol.ExecutePayload); !ok || payload.ToolUseEventID != use {
		st.f.t.Fatalf("event = %s; want tool.execute of %s", event.Data, use)
	}
	return event
}

// A developer API commit reaches a stream whose ready declared drives.mount
// and whose binding holds the drive, read_write or read_only, and no stream
// without the mount or the drive; the read_only binding lists the change.
func TestDriveChangedReachesOnlyMountsOfTheDrive(t *testing.T) {
	f := newMachineTools(t)
	s, other, reader := f.session(), f.session(), f.session()
	s.idle()
	other.idle()
	reader.idle()
	team, elsewhere := f.createDrive(), f.createDrive()
	s.bind(team, "read_write")
	other.bind(elsewhere, "read_write")
	reader.bind(team, "read_only")
	noMount := everyTool
	noMount.Drives.Mount = false
	mount := s.preparedWithDrives("mount", everyTool, []protocol.DriveBinding{rw(team)})
	unmounted := s.preparedWithDrives("unmounted", noMount, []protocol.DriveBinding{rw(team)})
	unrelated := other.preparedWithDrives("unrelated", everyTool, []protocol.DriveBinding{rw(elsewhere)})
	readOnly := reader.preparedWithDrives("read_only", everyTool, []protocol.DriveBinding{ro(team)})

	seq := f.apiCommit(team, "from-the-api.txt")
	mount.stream.driveChanged(team, seq)
	readOnly.stream.driveChanged(team, seq)
	if page := listedChanges(t, readOnly.transfer.changes(team, 0, -1)); len(page.Records) != 1 || page.Records[0].Path != "from-the-api.txt" || page.Records[0].Seq != seq {
		t.Fatalf("changes through the read_only binding = %+v; want the commit's", page.Records)
	}
	// The unrelated stream's next notice is its own drive's: it got none of
	// team's, and the unmounted stream gets none at all.
	unrelated.stream.driveChanged(elsewhere, f.apiCommit(elsewhere, "its-own.txt"))
	unmounted.stream.quiet(300 * time.Millisecond)
}

// On one stream, a commit and then a tool.execute admitted after it arrive
// as drive.changed with the commit's seq before the instruction, also when
// the stream moves to the other pod between them.
func TestDriveChangedComesBeforeLaterInstructions(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	b := s.preparedWithDrives("laptop", everyTool, []protocol.DriveBinding{rw(team)})
	s.running()
	stream := b.stream
	for run := range 200 {
		seq := f.apiCommit(team, "order/"+strconv.Itoa(run)+".txt")
		if run >= 100 {
			stream = f.resume(run%2, stream.cli, stream.connectionID)
		}
		use, _ := s.admitted(b.computerID, "make")
		stream.driveChanged(team, seq)
		execute := stream.executeOf
		if run >= 100 {
			execute = func(use string) enginetest.Event { return stream.executeAfterRepeat(team, seq, use) }
		}
		stream.finish(execute(use), "report_"+strconv.Itoa(run))
	}
}

// With the pod's routing of a drive commit's notice held, a later
// instruction's notice waits behind it: the stream carries the instruction
// before the commit, the drive.changed, then the instruction after it.
func TestHeldDriveNoticeHoldsLaterInstructions(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	b := s.preparedWithDrives("laptop", everyTool, []protocol.DriveBinding{rw(team)})
	s.running()

	first, _ := s.admitted(b.computerID, "make first")
	i0 := b.stream.executeOf(first)
	routing := f.hubSQL.pauseOn("drives,mount")
	seq := f.apiCommit(team, "between.txt")
	<-routing.reached
	second, _ := s.admitted(b.computerID, "make second")
	b.stream.quiet(300 * time.Millisecond)
	close(routing.release)
	b.stream.driveChanged(team, seq)
	b.stream.executeOf(second)
	b.stream.finish(i0, "report_first")
}

// A stream that starts while its pod still holds a drive commit's notice
// sends, before an instruction committed after that commit, drive.changed
// with that commit's seq.
func TestStartingStreamSendsItsDrivesBeforeItsInstructions(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	b := s.preparedWithDrives("laptop", everyTool, []protocol.DriveBinding{rw(team)})
	s.running()

	routing := f.hubSQL.pauseOn("drives,mount")
	seq := f.apiCommit(team, "before-the-start.txt")
	<-routing.reached
	use, _ := s.admitted(b.computerID, "make")
	resumed := f.resume(0, b.stream.cli, b.stream.connectionID)
	resumed.driveChanged(team, seq)
	resumed.executeOf(use)
	close(routing.release)
}

// After the pod's LISTEN connection is lost while instructions and a drive
// commit land, every stream holding the drive sends drive.changed with the
// commit's seq before any of those instructions once LISTEN is back; a
// bound drive without commits sends none.
func TestLostListenResendsEveryDriveBeforeInstructions(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team, empty := f.createDrive(), f.createDrive()
	s.bind(team, "read_write")
	s.bind(empty, "read_write")
	drives := sorted(rw(team), rw(empty))
	laptop := s.preparedWithDrives("laptop", everyTool, drives)
	desktop := s.preparedWithDrives("desktop", everyTool, drives)
	s.running()

	podListen := "LISTEN " + instance.ConnectionNoticeChannel("computer-hub-0")
	for deadline := time.Now().Add(10 * time.Second); f.count(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND query=$1`, podListen) != 1; {
		if time.Now().After(deadline) {
			t.Fatalf("no one connection whose last query is %s", podListen)
		}
		time.Sleep(10 * time.Millisecond)
	}
	reconnect := f.hubSQL.pauseOn("LISTEN " + drive.NoticeChannel)
	f.exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND query=$1`, podListen)
	<-reconnect.reached
	first, _ := s.admitted(laptop.computerID, "make first")
	seq := f.apiCommit(team, "while-listen-is-down.txt")
	second, _ := s.admitted(laptop.computerID, "make second")
	close(reconnect.release)

	laptop.stream.driveChanged(team, seq)
	desktop.stream.driveChanged(team, seq)
	laptop.stream.executeOf(first)
	laptop.stream.executeOf(second)
	desktop.stream.quiet(300 * time.Millisecond)
}

// A drive removed from a Session stays in its prepared binding's drive set
// until the binding's applied report is stored: a PUT to it is accepted
// before the report and refused after it.
func TestRemovedDriveIsWritableUntilItsAppliedReport(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team, notes := f.createDrive(), f.createDrive()
	s.bind(team, "read_write")
	s.bind(notes, "read_write")
	b := s.preparedWithDrives("laptop", everyTool, sorted(rw(team), rw(notes)))
	s.unbind(notes)
	removal := b.stream.drivesInstruction(b.transfer.bindingID, []protocol.DriveBinding{rw(team)})
	if response := b.transfer.put(notes, []byte("before the report")); response.Status != http.StatusOK {
		t.Fatalf("PUT before the applied report = %d %s; want 200", response.Status, response.Body)
	}
	b.stream.applied(removal, "applied_removal")
	requireRefusal(t, "a PUT after the applied report", b.transfer.put(notes, []byte("after the report")), http.StatusForbidden, protocol.ErrorDriveAccessDenied)
}

// timedBody is a request body that records when its last byte was read.
type timedBody struct {
	io.Reader
	done time.Time
}

func (b *timedBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF && b.done.IsZero() {
		b.done = time.Now()
	}
	return n, err
}

// medianSelect is the median of n SELECT 1 round trips through pool.
func medianSelect(ctx context.Context, tb testing.TB, pool *sql.DB, n int) time.Duration {
	tb.Helper()
	took := make([]time.Duration, n)
	for i := range took {
		start := time.Now()
		if _, err := pool.ExecContext(ctx, `SELECT 1`); err != nil {
			tb.Fatal(err)
		}
		took[i] = time.Since(start)
	}
	slices.Sort(took)
	return took[n/2]
}

// BenchmarkDriveCommitLatency commits 10,000 changes through the hub with
// every hub statement taking 2 ms, then 7 ms, in the database round trip:
// the tracer delays each statement by the latency less the median SELECT 1
// round trip. It fails when a change is not accepted or the answer's
// headers arrive more than the CLI's 10-second wait after the request body,
// and reports the statements, their database time, the span from the first
// statement to the last and that wait. go test runs it only when -bench
// names it; the timeouts leave room to measure a commit far slower than
// that wait:
//
//	go test -run '^$' -bench DriveCommitLatency -benchtime 1x -timeout 40m ./integration/
func BenchmarkDriveCommitLatency(b *testing.B) {
	for _, latency := range []time.Duration{2 * time.Millisecond, 7 * time.Millisecond} {
		b.Run(latency.String(), func(b *testing.B) {
			f := newMachineTools(b, withTimeout(30*time.Minute))
			s := f.session()
			s.idle()
			team := f.createDrive()
			s.bind(team, "read_write")
			x := s.preparedWithDrives("writer", everyTool, []protocol.DriveBinding{rw(team)}).transfer
			content := []byte("one of ten thousand")
			x.uploaded(team, content)

			base := medianSelect(f.ctx, b, f.hubPool, 201)
			delay := max(latency-base, 0)
			f.hubSQL.delay.Store(int64(delay))
			calibrated := medianSelect(f.ctx, b, f.hubPool, 201)
			b.Logf("calibration: SELECT 1 median %s without delay, %s with a %s delay per statement", base, calibrated, delay)
			b.ResetTimer()
			for n := range b.N {
				changes := make([]string, protocol.CommitMaxChanges)
				for i := range changes {
					changes[i] = write("latency/"+strconv.Itoa(n)+"/"+strconv.Itoa(i)+".txt", "", content)
				}
				body := `{"protocol_version":1,"connection_id":` + quoteJSON(x.st.connectionID) + `,"binding_id":` + quoteJSON(x.bindingID) +
					`,"changes":[` + strings.Join(changes, ",") + `]}`
				commitUnderLatency(b, f, x, team, body)
			}
		})
	}
}

// commitUnderLatency posts body as one commit to driveID, reports and logs
// its statements and timings, and fails unless every change is accepted
// with the headers within 10 seconds of the request body's end.
func commitUnderLatency(b *testing.B, f *machineTools, x *transfer, driveID, body string) {
	b.Helper()
	var mu sync.Mutex
	var statements int
	var inDatabase time.Duration
	var first, last time.Time
	kinds := map[string]int{}
	record := func(sql string, took time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now := time.Now()
		if first.IsZero() {
			first = now.Add(-took)
		}
		last = now
		statements++
		inDatabase += took
		kinds[strings.Fields(sql)[0]]++
	}
	request := &timedBody{Reader: strings.NewReader(body)}
	f.hubSQL.record.Store(&record)
	started := time.Now()
	response, err := x.st.cli.Open(f.ctx, http.MethodPost, "/v1/client/drives/"+driveID+"/commits", http.Header{"Content-Type": {"application/json"}}, request, int64(len(body)))
	headers := time.Now()
	f.hubSQL.record.Store(nil)
	if err != nil {
		b.Fatalf("commit after %s: %v", headers.Sub(started), err)
	}
	answer, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	wait := headers.Sub(request.done)
	mu.Lock()
	b.ReportMetric(float64(statements), "statements/op")
	b.ReportMetric(inDatabase.Seconds(), "db-s/op")
	b.ReportMetric(last.Sub(first).Seconds(), "statement-span-s/op")
	b.ReportMetric(wait.Seconds(), "headers-after-body-s/op")
	b.Logf("statements: %d (%v), %s in the database, first to last %s", statements, kinds, inDatabase, last.Sub(first))
	mu.Unlock()
	b.Logf("outcome: %d after %s, headers %s after the request body", response.StatusCode, headers.Sub(started), wait)
	result, err := protocol.DecodeCommitResponse(answer, protocol.CommitResponseContext{})
	if response.StatusCode != http.StatusOK || err != nil {
		b.Fatalf("commit = %d %.200s; want 200", response.StatusCode, answer)
	}
	for i, change := range result.Results {
		acceptedVersion(b, "change "+strconv.Itoa(i), change)
	}
	if wait > 10*time.Second {
		b.Fatalf("headers arrived %s after the request body; want within the CLI's 10-second wait", wait)
	}
}
