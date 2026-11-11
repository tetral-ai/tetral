package reference

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// driveCall is one drive transfer request on its way through the check
// order of `protocol/README.md`, "Drive transfer", "Common rules".
type driveCall struct {
	endpoint wire.DriveTransferEndpoint
	entry    requestLog
	// tooLarge is set for a commit whose body passed its limit.
	tooLarge bool
	// shape is what step 1 found wrong, read from the request alone.
	shape error
	// write is set for the endpoints that need read_write access.
	write bool

	conn     *connection
	binding  *binding
	drive    *drive
	injected *fault
}

// driveParams reads a drive transfer request's path parameters, and from its
// query the members that names lists, as one JSON object of strings for
// package wire to decode.
func driveParams(request *http.Request, names ...string) (path, query []byte) {
	pathValues := map[string]string{"drive_id": request.PathValue("drive_id")}
	if hash := request.PathValue("sha256"); hash != "" {
		pathValues["sha256"] = hash
	}
	values, queryValues := request.URL.Query(), map[string]string{}
	for _, name := range names {
		if values.Has(name) {
			queryValues[name] = values.Get(name)
		}
	}
	path, _ = json.Marshal(pathValues)
	query, _ = json.Marshal(queryValues)
	return path, query
}

// newDriveCall starts a call's log entry from the request's parameters.
func newDriveCall(request *http.Request, endpoint wire.DriveTransferEndpoint, connectionID, bindingID wire.ID) *driveCall {
	return &driveCall{endpoint: endpoint, entry: requestLog{
		Method: request.Method, Endpoint: string(endpoint), Traceparent: request.Header.Get("traceparent"),
		ConnectionID: string(connectionID), BindingID: string(bindingID), DriveID: request.PathValue("drive_id"),
		ContentSha256: request.PathValue("sha256"), Range: request.Header.Get("Range"),
	}}
}

// admitDrive runs a drive transfer request through a `hold_request`,
// authentication, a commit's body limit, step 1 (shape), an error answer
// that the answer control set, step 2 (connection), step 3 (binding) and
// step 4 (drive access); decide then runs step 5 and what the endpoint
// needs of the store, all under mu. The first refusal answers the request
// and admitDrive returns false.
func (server *Server) admitDrive(w http.ResponseWriter, request *http.Request, call *driveCall, decide func() *refusal) bool {
	name, entry := string(call.endpoint), &call.entry
	server.mu.Lock()
	if !server.holdRequest(request, name) {
		return false
	}
	entry.At = server.clock.Now()
	entry.CredentialID = server.credentialOfKey(bearer(request))
	refusal := func() *refusal {
		cred, denied := server.authenticate(bearer(request), "", "", false)
		switch {
		case denied != nil:
			return denied
		case call.tooLarge:
			return refuse("request_too_large", "the commit's body is over its limit")
		case call.shape != nil:
			return refuse("invalid_message", call.shape.Error())
		}
		call.injected = server.takeFault(name, "", false)
		if call.injected != nil && call.injected.Action == "error" {
			return &refusal{code: call.injected.Code, message: "injected by the answer control", status: call.injected.Status}
		}
		conn := server.connections[entry.ConnectionID]
		if conn == nil || conn.instance.id != cred.InstanceID || server.live(cred.InstanceID) != conn {
			return refuse("stale_connection", "connection_id is not the instance's live connection")
		}
		bound := conn.bindings[entry.BindingID]
		if bound == nil || !bound.usable() {
			return refuse("stale_binding", "binding_id names no prepared or closing binding of the connection")
		}
		if !bound.access(entry.DriveID, call.write) {
			return refuse("drive_access_denied", "the binding's drive set does not give this access to the drive")
		}
		call.conn, call.binding, call.drive = conn, bound, server.driveOf(entry.DriveID)
		return decide()
	}()
	server.mu.Unlock()
	if refusal != nil {
		server.refused(w, entry, refusal)
		return false
	}
	return true
}

// logDrive enters an admitted call in the request log and, for a `delay` or
// `withhold` of the answer control, holds its answer.
func (server *Server) logDrive(call *driveCall, status int) *heldAnswer {
	server.mu.Lock()
	defer server.mu.Unlock()
	call.entry.Status = status
	server.log.requests = append(server.log.requests, call.entry)
	injected := call.injected
	if injected == nil || (injected.Action != "delay" && injected.Action != "withhold") {
		return nil
	}
	hold := &heldAnswer{Endpoint: string(call.endpoint), release: make(chan struct{})}
	if injected.Action == "delay" {
		hold.Until = server.clock.Now().Add(time.Duration(injected.DelayMs) * time.Millisecond)
	}
	server.held = append(server.held, hold)
	return hold
}

// answerDrive answers an admitted call with a JSON message, which check has
// decoded with package wire against its request.
func (server *Server) answerDrive(w http.ResponseWriter, request *http.Request, call *driveCall, message any, check func(body []byte) error) {
	body, err := wire.Encode(message)
	if err == nil {
		err = check(body)
	}
	if err != nil {
		server.unimplemented(w, call, "the answer fails its schema: "+err.Error())
		return
	}
	server.deliver(w, request, answer{status: http.StatusOK, body: body}, call.injected, server.logDrive(call, http.StatusOK))
}

// unimplemented answers 500 to a request that the store cannot answer
// within the contract, and records it for the state control.
func (server *Server) unimplemented(w http.ResponseWriter, call *driveCall, reason string) {
	server.mu.Lock()
	server.fail(fmt.Sprintf("%s of drive %s: %s", call.endpoint, call.entry.DriveID, reason))
	call.entry.Status = http.StatusInternalServerError
	server.log.requests = append(server.log.requests, call.entry)
	server.mu.Unlock()
	http.Error(w, "the reference control server does not implement this: "+reason, http.StatusInternalServerError)
}

func (server *Server) listFiles(w http.ResponseWriter, request *http.Request) {
	path, encoded := driveParams(request, "protocol_version", "connection_id", "binding_id", "after_path")
	_, shape := wire.Decode[wire.DrivePathParams](path)
	query, err := wire.Decode[wire.ListFilesQuery](encoded)
	call := newDriveCall(request, wire.ListFiles, query.ConnectionID, query.BindingID)
	call.entry.Query, call.shape = queryLog(encoded), firstError(shape, err)
	var page wire.ListFilesResponse
	if !server.admitDrive(w, request, call, func() *refusal {
		if !query.AfterPath.Set {
			page.StartSeq = wire.Some(call.drive.seq)
		}
		page.Files = []wire.FileEntry{}
		for _, file := range call.drive.files() {
			if query.AfterPath.Set && file.path <= query.AfterPath.Value {
				continue
			}
			if len(page.Files) == wire.ListPageMaxItems {
				page.HasMore = true
				break
			}
			page.Files = append(page.Files, wire.FileEntry{
				Path: wire.DrivePath(file.path), VersionID: wire.ID(file.id), ContentSha256: wire.Sha256(file.sha256),
				SizeBytes: file.size, Executable: file.executable,
			})
		}
		return nil
	}) {
		return
	}
	server.answerDrive(w, request, call, page, func(body []byte) error {
		_, err := wire.DecodeAnswer[wire.ListFilesQuery, wire.ListFilesResponse](body, query)
		return err
	})
}

func (server *Server) listChanges(w http.ResponseWriter, request *http.Request) {
	path, encoded := driveParams(request, "protocol_version", "connection_id", "binding_id", "after_seq", "after_index")
	_, shape := wire.Decode[wire.DrivePathParams](path)
	query, err := wire.Decode[wire.ListChangesQuery](encoded)
	call := newDriveCall(request, wire.ListChanges, query.ConnectionID, query.BindingID)
	call.entry.Query, call.shape = queryLog(encoded), firstError(shape, err)
	// Without after_index the page starts after the whole commit.
	after := func(record wire.ChangeRecord) bool {
		seq := query.AfterSeq.Value()
		if !query.AfterIndex.Set {
			return record.Seq > seq
		}
		return record.Seq > seq || (record.Seq == seq && record.Index > query.AfterIndex.Value.Value())
	}
	page := wire.ListChangesResponse{Records: []wire.ChangeRecord{}}
	if !server.admitDrive(w, request, call, func() *refusal {
		if expired := call.drive.expired; expired != nil && after(*expired) {
			return refuse("drive_cursor_expired", "the cursor is below the drive's newest removed record")
		}
		for _, record := range call.drive.records {
			if !after(record) {
				continue
			}
			if len(page.Records) == wire.ListPageMaxItems {
				page.HasMore = true
				break
			}
			page.Records = append(page.Records, record)
		}
		return nil
	}) {
		return
	}
	server.answerDrive(w, request, call, page, func(body []byte) error {
		_, err := wire.DecodeAnswer[wire.ListChangesQuery, wire.ListChangesResponse](body, query)
		return err
	})
}

func (server *Server) commit(w http.ResponseWriter, request *http.Request) {
	path, _ := driveParams(request)
	_, shape := wire.Decode[wire.DrivePathParams](path)
	reader := server.throttled(request, string(wire.Commit))
	body, _ := io.ReadAll(io.LimitReader(reader, wire.CommitRequestMaxJSONBytes+1))
	uncounted, err := wire.Decode[wire.UncountedCommitRequest](body)
	call := newDriveCall(request, wire.Commit, uncounted.ConnectionID, uncounted.BindingID)
	digest := sha256.Sum256(body)
	call.entry.BodySHA256, call.entry.BodyBytes = hex.EncodeToString(digest[:]), len(body)
	call.tooLarge, call.shape, call.write = len(body) > wire.CommitRequestMaxJSONBytes, firstError(shape, err), true
	var response wire.CommitResponse
	if !server.admitDrive(w, request, call, func() *refusal {
		if len(uncounted.Changes) > wire.CommitMaxChanges {
			return refuse("request_too_large", "the commit holds more than 10,000 changes")
		}
		writer := wire.DriveWriter{Actor: &wire.SessionActor{
			SessionID: wire.ID(call.binding.sessionID), ComputerID: wire.ID(computerID(call.conn.instance.id, call.binding.sessionID)),
		}}
		var missing []wire.Sha256
		if response, missing = server.commitMachine(call.drive, writer, uncounted.Changes); len(missing) > 0 {
			return &refusal{code: "content_missing", message: "the drive does not hold content that a write names", missing: missing}
		}
		server.log.received = append(server.log.received, receivedMessage{
			Endpoint: string(wire.Commit), ConnectionID: call.entry.ConnectionID, Traceparent: call.entry.Traceparent,
			Body: json.RawMessage(canonical(body)),
		})
		return nil
	}) {
		return
	}
	server.answerDrive(w, request, call, response, func(body []byte) error {
		_, err := wire.DecodeAnswer[wire.CommitRequest, wire.CommitResponse](body, uncounted.Counted())
		return err
	})
}

// computerID names the computer of a Session on an instance, the writer of
// the versions its commits make.
func computerID(instanceID, sessionID string) string {
	return "computer_" + instanceID + "_" + sessionID
}

func (server *Server) uploadContent(w http.ResponseWriter, request *http.Request) {
	path, encoded := driveParams(request, "protocol_version", "connection_id", "binding_id")
	params, shape := wire.Decode[wire.ContentPathParams](path)
	query, err := wire.Decode[wire.CommonQuery](encoded)
	call := newDriveCall(request, wire.UploadContent, query.ConnectionID, query.BindingID)
	call.entry.Query, call.write = queryLog(encoded), true
	// A missing Content-Length and one over the file limit are refused in
	// step 5; any other fault of the headers is one of shape.
	declared := request.Header.Get("Content-Length")
	headerValues := map[string]string{"Content-Type": request.Header.Get("Content-Type"), "Content-Length": declared}
	if declared == "" {
		headerValues["Content-Length"] = "0"
	}
	encodedHeaders, _ := json.Marshal(headerValues)
	headers, headerErr := wire.Decode[wire.UploadContentRequestHeaders](encodedHeaders)
	var refused *wire.DecodeError
	tooLarge := errors.As(headerErr, &refused) && refused.Kind == wire.Rule
	if tooLarge {
		headerErr = nil
	}
	call.shape = firstError(shape, err, headerErr)
	if !server.admitDrive(w, request, call, func() *refusal {
		switch {
		case declared == "":
			return refuse("invalid_message", "the upload has no Content-Length")
		case tooLarge:
			return refuse("file_too_large", "a file holds at most 500,000,000 bytes")
		}
		if seconds := server.budget.take(headers.ContentLength.Value(), server.clock.Now()); seconds > 0 {
			return &refusal{code: "rate_limited", message: "the workspace's upload budget is spent", retryAfter: seconds}
		}
		return nil
	}) {
		return
	}
	content := bytes.NewBuffer(make([]byte, 0, headers.ContentLength.Value()))
	digest := sha256.New()
	if _, err := io.Copy(io.MultiWriter(content, digest), server.throttled(request, string(wire.UploadContent))); err != nil {
		// The client went away inside the body: there is nobody to answer.
		return
	}
	call.entry.BodySHA256, call.entry.BodyBytes = hex.EncodeToString(digest.Sum(nil)), content.Len()
	if call.entry.BodySHA256 != string(params.Sha256) {
		server.refused(w, &call.entry, refuse("content_hash_mismatch", "the content's SHA-256 is not the one the URL names"))
		return
	}
	server.mu.Lock()
	if _, held := call.drive.contents[call.entry.BodySHA256]; !held {
		call.drive.contents[call.entry.BodySHA256] = driveContent{size: int64(content.Len()), data: content.Bytes()}
	}
	server.mu.Unlock()
	response := wire.UploadContentResponse{ContentSha256: params.Sha256, SizeBytes: int64(content.Len())}
	server.answerDrive(w, request, call, response, func(body []byte) error {
		_, err := wire.DecodeAnswer[wire.UploadRequest, wire.UploadContentResponse](body, wire.UploadRequest{Path: params, Headers: headers})
		return err
	})
}

func (server *Server) downloadContent(w http.ResponseWriter, request *http.Request) {
	path, encoded := driveParams(request, "protocol_version", "connection_id", "binding_id")
	params, shape := wire.Decode[wire.ContentPathParams](path)
	query, err := wire.Decode[wire.CommonQuery](encoded)
	call := newDriveCall(request, wire.DownloadContent, query.ConnectionID, query.BindingID)
	call.entry.Query = queryLog(encoded)
	// A Range of more than one range is well formed and never satisfiable.
	var ranges []wire.ByteRange
	asked, hasRange := request.Header.Get("Range"), request.Header.Get("Range") != ""
	var rangeErr error
	if hasRange {
		var wellFormed bool
		ranges, wellFormed = wire.RangeHeader(asked).Ranges()
		backwards := slices.ContainsFunc(ranges, func(one wire.ByteRange) bool {
			return one.First >= 0 && one.Last >= 0 && one.First > one.Last
		})
		if !wellFormed || backwards {
			rangeErr = fmt.Errorf("the Range header %q is not a list of byte ranges", asked)
		}
	}
	call.shape = firstError(shape, err, rangeErr)
	var content driveContent
	first, last := int64(0), int64(-1)
	if !server.admitDrive(w, request, call, func() *refusal {
		if !call.drive.keeps(string(params.Sha256)) {
			return refuse("content_not_found", "no kept version of the drive names this content")
		}
		content = call.drive.contents[string(params.Sha256)]
		last = content.size - 1
		if !hasRange {
			return nil
		}
		satisfiable := len(ranges) == 1
		if satisfiable {
			first, last, satisfiable = ranges[0].Answer(content.size)
		}
		if !satisfiable {
			return refuse("range_not_satisfiable", "the Range selects no byte of the content, or holds more than one range")
		}
		return nil
	}) {
		return
	}
	status := http.StatusOK
	if hasRange {
		status = http.StatusPartialContent
		contentRange := fmt.Sprintf("bytes %d-%d/%d", first, last, content.size)
		encodedHeaders, _ := json.Marshal(map[string]string{"Content-Range": contentRange})
		if _, err := wire.Decode[wire.PartialContentResponseHeaders](encodedHeaders); err != nil {
			server.unimplemented(w, call, "the Content-Range fails its schema: "+err.Error())
			return
		}
		w.Header().Set("Content-Range", contentRange)
	}
	hold := server.logDrive(call, status)
	if hold != nil {
		select {
		case <-hold.release:
		case <-request.Context().Done():
			server.mu.Lock()
			server.releaseHeld(func(candidate *heldAnswer) bool { return candidate == hold })
			server.mu.Unlock()
			return
		}
	}
	if call.injected != nil && call.injected.Action == "drop" {
		cut(w)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(last-first+1))
	w.WriteHeader(status)
	server.writeContent(w, request, content.reader(first, last-first+1))
}

// writeContent writes a download's content, at the pace of a `throttle` that
// the answer control set for downloads: bytes_per_second for each second of
// the clock since the answer began, so that a clock held still stalls it.
func (server *Server) writeContent(w http.ResponseWriter, request *http.Request, content io.Reader) {
	server.mu.Lock()
	throttle := server.takeFault(string(wire.DownloadContent), "", true)
	var progress *throttledRead
	if throttle != nil {
		progress = &throttledRead{Endpoint: string(wire.DownloadContent)}
		server.throttledReads = append(server.throttledReads, progress)
	}
	start := server.clock.Now()
	server.mu.Unlock()
	if throttle == nil {
		_, _ = io.Copy(w, content)
		return
	}
	flusher, _ := w.(http.Flusher)
	flusher.Flush()
	written := int64(0)
	buffer := make([]byte, 64<<10)
	for {
		changed := server.clock.Changed()
		room := server.clock.Now().Sub(start).Milliseconds()*throttle.BytesPerSecond/1000 - written
		if room <= 0 {
			select {
			case <-changed:
				continue
			case <-request.Context().Done():
				return
			}
		}
		count, err := content.Read(buffer[:min(int64(len(buffer)), room)])
		if count > 0 {
			if _, failed := w.Write(buffer[:count]); failed != nil {
				return
			}
			flusher.Flush()
			written += int64(count)
			server.mu.Lock()
			progress.Read = written
			server.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

// throttled is the request's body, read at the pace of a `throttle` that the
// answer control set for the endpoint.
func (server *Server) throttled(request *http.Request, endpoint string) io.Reader {
	server.mu.Lock()
	defer server.mu.Unlock()
	throttle := server.takeFault(endpoint, "", true)
	if throttle == nil {
		return request.Body
	}
	read := &throttledRead{Endpoint: endpoint}
	server.throttledReads = append(server.throttledReads, read)
	return &throttledBody{server: server, request: request, start: server.clock.Now(), rate: throttle.BytesPerSecond, read: read}
}

// driveSync accepts a drive sync report: its binding is the connection's and
// of the report's Session, and its `task_id`, when it has one, names a task
// that was issued for that Session on any connection of the instance.
func (server *Server) driveSync(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "drive-sync", maxBytes: wire.DriveSyncReportJSONBytes, replayable: true,
		decode: func(body []byte) (string, string, any, error) {
			report, err := wire.Decode[wire.DriveSyncReport](body)
			return string(report.ConnectionID), string(report.MessageID), &report, err
		},
		apply: func(conn *connection, value any, body []byte) (answer, *refusal) {
			report := value.(*wire.DriveSyncReport)
			if report.DriveSync.Downloading.Set || report.DriveSync.DownloadingTotal.Set || report.DriveSync.Unmounted.Set {
				return answer{}, refuse("invalid_message", "a drive sync report holds no downloading or unmounted")
			}
			var members struct {
				DriveSync json.RawMessage `json:"drive_sync"`
			}
			_ = json.Unmarshal(body, &members)
			if compactLen(members.DriveSync) > wire.DriveSyncJSONBytes {
				return answer{}, refuse("invalid_message", "drive_sync is over its limit")
			}
			bound := conn.bindings[string(report.BindingID)]
			if bound == nil || bound.sessionID != string(report.SessionID) {
				return answer{}, refuse("unknown_instruction", "the report names no binding of this connection")
			}
			if report.TaskID.Set && !server.issuedTask(conn.instance, bound.sessionID, string(report.TaskID.Value)) {
				return answer{}, refuse("unknown_instruction", "task_id names no task issued for the Session")
			}
			return acknowledge(conn, string(report.MessageID)), nil
		},
	})
}

// issuedTask says whether a tool instruction of the Session with taskID was
// issued on any connection of the instance.
func (server *Server) issuedTask(owner *instance, sessionID, taskID string) bool {
	for _, conn := range server.connections {
		if conn.instance != owner {
			continue
		}
		for _, instruction := range conn.issued {
			if instruction.sessionID == sessionID && instruction.taskID == taskID {
				return true
			}
		}
	}
	return false
}

// queryLog is a decoded query as the request log holds it.
func queryLog(encoded []byte) map[string]string {
	var query map[string]string
	_ = json.Unmarshal(encoded, &query)
	return query
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
