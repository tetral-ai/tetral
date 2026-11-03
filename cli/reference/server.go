package reference

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// stream is one SSE stream of a connection.
type stream struct {
	conn          *connection
	number        int
	started       time.Time
	frames        chan []byte
	closed        chan struct{}
	closeOnce     sync.Once
	nextHeartbeat time.Time
	log           *serverLog
}

// streamBuffer bounds the events waiting to be written on one stream.
const streamBuffer = 1 << 16

func (st *stream) send(data []byte, messageID, kind string) {
	select {
	case <-st.closed:
		return
	default:
	}
	select {
	case st.frames <- data:
		st.log.events = append(st.log.events, sentEvent{
			ConnectionID: st.conn.id, Stream: st.number, MessageID: messageID, Type: kind,
		})
	default:
		st.close()
	}
}

func (st *stream) close() { st.closeOnce.Do(func() { close(st.closed) }) }

func (st *stream) isClosed() bool {
	select {
	case <-st.closed:
		return true
	default:
		return false
	}
}

// openStream makes a new stream the connection's current one; the stream
// it replaces closes.
func (server *Server) openStream(conn *connection) *stream {
	if conn.stream != nil {
		conn.stream.close()
	}
	conn.streams++
	st := &stream{
		conn: conn, number: conn.streams, started: server.clock.Now(),
		frames: make(chan []byte, streamBuffer), closed: make(chan struct{}),
		log: &server.log,
	}
	conn.stream = st
	return st
}

func (server *Server) event(conn *connection, kind string, payload any) ([]byte, string) {
	messageID := server.newID("msg")
	encoded, _ := json.Marshal(payload)
	data, _ := json.Marshal(envelope{
		ProtocolVersion: wire.ProtocolVersion, MessageID: messageID, ConnectionID: conn.id,
		Type: kind, Payload: encoded,
	})
	return data, messageID
}

// sendHeartbeat sends the stream's next heartbeat and schedules the one
// after it HeartbeatInterval after `at`, the time this one was due.
func (server *Server) sendHeartbeat(st *stream, at time.Time) {
	conn := st.conn
	conn.heartbeatSeq++
	data, messageID := server.event(conn, "heartbeat", wire.HeartbeatPayload{HeartbeatSeq: conn.heartbeatSeq})
	st.send(data, messageID, "heartbeat")
	st.nextHeartbeat = at.Add(HeartbeatInterval)
}

// due does, before the clock control answers, everything that the clock's
// new time makes due: each stream's lease end, maximum age and heartbeat,
// and each delayed answer. So once the clock control has answered, the state
// control shows every effect of the advance and nothing that is not yet due.
func (server *Server) due() {
	for _, conn := range server.connections {
		if conn.stream != nil {
			server.tick(conn.stream)
		}
	}
	now := server.clock.Now()
	server.releaseHeld(func(hold *heldAnswer) bool { return !hold.Until.IsZero() && !now.Before(hold.Until) })
}

// tick does, in the order of their times, everything due on a stream at the
// clock's time: each heartbeat whose time has come, its maximum age and the
// end of its connection's lease. Of two at the same time, the lease end goes
// first, then the maximum age.
func (server *Server) tick(st *stream) {
	conn := st.conn
	if st.isClosed() {
		return
	}
	if conn.ended || conn.stream != st {
		st.close()
		return
	}
	const (
		leaseEnd = iota
		streamAge
		heartbeat
	)
	now := server.clock.Now()
	maxAge := st.started.Add(time.Duration(server.config.MaxStreamAgeMs) * time.Millisecond)
	for {
		next, due := conn.leaseUntil, leaseEnd
		if maxAge.Before(next) {
			next, due = maxAge, streamAge
		}
		if conn.readyAccepted && !st.nextHeartbeat.IsZero() && st.nextHeartbeat.Before(next) {
			next, due = st.nextHeartbeat, heartbeat
		}
		if now.Before(next) {
			return
		}
		switch due {
		case leaseEnd:
			server.endConnection(conn)
			return
		case streamAge:
			data, messageID := server.event(conn, "disconnect", map[string]string{"scope": "stream"})
			st.send(data, messageID, "disconnect")
			st.close()
			conn.stream = nil
			return
		default:
			server.sendHeartbeat(st, next)
		}
	}
}

func bearer(request *http.Request) string {
	key, _ := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
	return key
}

// events serves GET /v1/client/events: it authenticates, applies the
// predecessor rules and streams the connection's events.
func (server *Server) events(w http.ResponseWriter, request *http.Request) {
	values := request.URL.Query()
	query := map[string]string{}
	for _, name := range []string{"protocol_version", "instance_id", "credential_id", "previous_connection_id", "resume"} {
		if values.Has(name) {
			query[name] = values.Get(name)
		}
	}
	encoded, _ := json.Marshal(query)
	entry := requestLog{Method: "GET", Endpoint: "events", Query: query}
	server.mu.Lock()
	entry.At = server.clock.Now()
	decoded, err := wire.Decode[wire.EventsQuery](encoded)
	if err != nil {
		server.mu.Unlock()
		server.refused(w, &entry, refuse("invalid_message", "the query fails its schema"))
		return
	}
	cred, refusal := server.authenticate(bearer(request), string(decoded.InstanceID), string(decoded.CredentialID), true)
	if refusal != nil {
		server.mu.Unlock()
		server.refused(w, &entry, refusal)
		return
	}
	padTo := 0
	if len(server.eventsModes) > 0 {
		mode := server.eventsModes[0]
		server.eventsModes = server.eventsModes[1:]
		switch mode.name {
		case "delay_connected":
			server.mu.Unlock()
			if !server.waitUntil(request.Context(), mode.until) {
				return
			}
			server.mu.Lock()
		case "pad_connected":
			padTo = mode.dataBytes
		default:
			entry.Status, entry.Code = http.StatusOK, mode.name
			server.log.requests = append(server.log.requests, entry)
			server.mu.Unlock()
			server.silent(w, request, mode.name)
			return
		}
	}
	conn, resumed, refusal := server.connect(cred, string(decoded.PreviousConnectionID.Value), decoded.Resume.Value == "true")
	if refusal != nil {
		server.mu.Unlock()
		server.refused(w, &entry, refusal)
		return
	}
	st := server.openStream(conn)
	data, messageID := server.event(conn, "connected", wire.ConnectedPayload{Resumed: resumed})
	// Spaces after the object keep it one valid JSON value.
	data = append(data, bytes.Repeat([]byte(" "), max(padTo-len(data), 0))...)
	st.send(data, messageID, "connected")
	if resumed {
		if conn.readyAccepted {
			server.sendHeartbeat(st, server.clock.Now())
		}
		server.resend(conn)
		server.renewAgain(conn)
	}
	entry.ConnectionID, entry.Status = conn.id, http.StatusOK
	server.log.requests = append(server.log.requests, entry)
	server.mu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	flush := func() {
		if flusher != nil {
			flusher.Flush()
		}
	}
	flush()
	write := func(frame []byte) bool {
		_, err := io.WriteString(w, "data: "+string(frame)+"\n\n")
		flush()
		return err == nil
	}
	for {
		select {
		case frame := <-st.frames:
			if !write(frame) {
				server.lost(st)
				return
			}
		case <-st.closed:
			for {
				select {
				case frame := <-st.frames:
					write(frame)
				default:
					return
				}
			}
		case <-request.Context().Done():
			server.lost(st)
			return
		}
	}
}

// lost handles a stream whose client went away: the connection keeps its
// lease and may resume.
func (server *Server) lost(st *stream) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if st.conn.stream == st {
		st.conn.stream = nil
	}
	st.close()
}

// silent serves an events request that the events control set: it holds
// its headers, sends headers and no event, or sends comments only, until
// the client gives up.
func (server *Server) silent(w http.ResponseWriter, request *http.Request, mode string) {
	if mode == "hold_headers" {
		<-request.Context().Done()
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	for {
		// Taken before the write, so that an advance during the write still
		// ends this wait.
		changed := server.clock.Changed()
		if mode == "comments_only" {
			if _, err := io.WriteString(w, ": comment\n\n"); err != nil {
				return
			}
		}
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-changed:
		case <-request.Context().Done():
			return
		}
	}
}

// waitUntil waits until the clock reaches until, or gives false when the
// client goes away first.
func (server *Server) waitUntil(ctx context.Context, until time.Time) bool {
	for {
		// Taken before the time is read, so that an advance in between still
		// ends this wait.
		changed := server.clock.Changed()
		if !server.clock.Now().Before(until) {
			return true
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

// answer is the response to an upstream request.
type answer struct {
	status  int
	body    []byte
	noStore bool
}

func acknowledge(conn *connection, messageID string) answer {
	body, _ := wire.Encode(wire.Acknowledgement{ConnectionID: wire.ID(conn.id), MessageID: wire.ID(messageID), Accepted: true})
	return answer{status: http.StatusOK, body: body}
}

// endpoint is one upstream endpoint: how its body is read and what an
// accepted message does.
type endpoint struct {
	name     string
	maxBytes int
	// replayable is false for the fetches whose retry is a new fetch.
	replayable bool
	decode     func(body []byte) (connectionID, messageID string, value any, err error)
	apply      func(conn *connection, value any, body []byte) (answer, *refusal)
}

// serve runs one upstream request, in this order: the body's read, at the
// pace of a `throttle` that the answer control set, authentication against
// the credential table, the body's size limit, decoding with package wire,
// an error answer that the answer control set, the connection check, the
// replay ledger, the endpoint's own rules and the recording of the accepted
// message; then deliver writes the answer, changed or held by the answer
// control's other faults. The first refusal answers with its code, and every
// request, refused or not, enters the request log.
func (server *Server) serve(w http.ResponseWriter, request *http.Request, spec endpoint) {
	reader := io.Reader(request.Body)
	server.mu.Lock()
	if throttle := server.takeFault(spec.name, "", true); throttle != nil {
		read := &throttledRead{Endpoint: spec.name}
		server.throttled = append(server.throttled, read)
		reader = &throttledBody{server: server, request: request, start: server.clock.Now(), rate: throttle.BytesPerSecond, read: read}
	}
	server.mu.Unlock()
	body, _ := io.ReadAll(io.LimitReader(reader, int64(spec.maxBytes)+1))
	digest := sha256.Sum256(body)
	entry := requestLog{
		Method: "POST", Endpoint: spec.name, Traceparent: request.Header.Get("traceparent"),
		BodySHA256: hex.EncodeToString(digest[:]),
	}
	server.mu.Lock()
	if hold := server.takeHold(spec.name); hold != nil {
		held := &heldAnswer{Endpoint: spec.name, release: make(chan struct{})}
		server.held = append(server.held, held)
		server.mu.Unlock()
		select {
		case <-held.release:
		case <-request.Context().Done():
			server.mu.Lock()
			server.releaseHeld(func(candidate *heldAnswer) bool { return candidate == held })
			server.mu.Unlock()
			return
		}
		server.mu.Lock()
	}
	entry.At = server.clock.Now()
	entry.CredentialID = server.credentialOfKey(bearer(request))
	cred, denied := server.authenticate(bearer(request), "", "", false)
	if denied != nil {
		server.mu.Unlock()
		server.refused(w, &entry, denied)
		return
	}
	if len(body) > spec.maxBytes {
		server.mu.Unlock()
		server.refused(w, &entry, refuse("request_too_large", "the body is over its limit"))
		return
	}
	connectionID, messageID, value, err := spec.decode(body)
	entry.ConnectionID, entry.MessageID = connectionID, messageID
	if err != nil {
		server.mu.Unlock()
		server.refused(w, &entry, refuse("invalid_message", err.Error()))
		return
	}
	injected := server.takeFault(spec.name, messageID, false)
	if injected != nil && injected.Action == "error" {
		server.mu.Unlock()
		server.refused(w, &entry, &refusal{code: injected.Code, message: "injected by the answer control", status: injected.Status})
		return
	}
	conn := server.connections[connectionID]
	if conn == nil || conn.instance.id != cred.InstanceID || server.live(cred.InstanceID) != conn {
		server.mu.Unlock()
		server.refused(w, &entry, refuse("stale_connection", "connection_id is not the instance's live connection"))
		return
	}
	var result answer
	replayed := false
	if spec.replayable {
		if recorded, conflict := conn.replay(spec.name, messageID, body); conflict != nil {
			server.mu.Unlock()
			server.refused(w, &entry, conflict)
			return
		} else if recorded != nil {
			result, replayed = *recorded, true
		}
	}
	if !replayed {
		var rejected *refusal
		result, rejected = spec.apply(conn, value, body)
		if rejected != nil {
			server.mu.Unlock()
			server.refused(w, &entry, rejected)
			return
		}
		if spec.replayable {
			conn.record(spec.name, messageID, body, result)
		}
		server.log.received = append(server.log.received, receivedMessage{
			Endpoint: spec.name, ConnectionID: connectionID, MessageID: messageID,
			Traceparent: entry.Traceparent, Body: json.RawMessage(canonical(body)),
		})
	}
	entry.Status = result.status
	server.log.requests = append(server.log.requests, entry)
	var hold *heldAnswer
	if injected != nil && (injected.Action == "delay" || injected.Action == "withhold") {
		hold = &heldAnswer{Endpoint: spec.name, MessageID: messageID, release: make(chan struct{})}
		if injected.Action == "delay" {
			hold.Until = server.clock.Now().Add(time.Duration(injected.DelayMs) * time.Millisecond)
		}
		server.held = append(server.held, hold)
	}
	server.mu.Unlock()
	server.deliver(w, request, result, injected, hold)
}

// throttledRead is how much of a request's body a `throttle` has read.
type throttledRead struct {
	Endpoint string `json:"endpoint"`
	Read     int64  `json:"read"`
}

// throttledBody reads a request's body at rate bytes for each second of the
// clock since start, waiting for the clock while it may read nothing.
type throttledBody struct {
	server  *Server
	request *http.Request
	start   time.Time
	rate    int64
	read    *throttledRead
}

func (body *throttledBody) Read(buffer []byte) (int, error) {
	clock := body.server.clock
	for {
		changed := clock.Changed()
		allowed := clock.Now().Sub(body.start).Milliseconds() * body.rate / 1000
		body.server.mu.Lock()
		room := allowed - body.read.Read
		body.server.mu.Unlock()
		if room > 0 {
			count, err := body.request.Body.Read(buffer[:min(int64(len(buffer)), room)])
			body.server.mu.Lock()
			body.read.Read += int64(count)
			body.server.mu.Unlock()
			return count, err
		}
		select {
		case <-changed:
		case <-body.request.Context().Done():
			return 0, body.request.Context().Err()
		}
	}
}

// heldAnswer is an answer that a delay or withhold fault holds back.
type heldAnswer struct {
	Endpoint  string `json:"endpoint"`
	MessageID string `json:"message_id"`
	// Until is when a delayed answer goes out; a withheld answer has none
	// and goes out when the answer control releases it.
	Until   time.Time `json:"until,omitzero"`
	release chan struct{}
}

// releaseHeld lets out the held answers that pick selects.
func (server *Server) releaseHeld(pick func(*heldAnswer) bool) {
	kept := server.held[:0]
	for _, hold := range server.held {
		if pick(hold) {
			close(hold.release)
		} else {
			kept = append(kept, hold)
		}
	}
	server.held = kept
}

// deliver writes an answer, changed or held as a fault says.
func (server *Server) deliver(w http.ResponseWriter, request *http.Request, result answer, injected *fault, hold *heldAnswer) {
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
	if injected != nil {
		switch injected.Action {
		case "drop":
			if hijacker, ok := w.(http.Hijacker); ok {
				if raw, _, err := hijacker.Hijack(); err == nil {
					_ = raw.Close()
					return
				}
			}
			panic(http.ErrAbortHandler)
		case "wrong_identity":
			result.body = wrongIdentity(result.body)
		case "without_accepted":
			result.body = withoutAccepted(result.body)
		case "schema_fail":
			result.body = []byte("{}")
			if len(injected.Body) > 0 {
				result.body = injected.Body
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if result.noStore {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(result.status)
	_, _ = w.Write(result.body)
}

// wrongIdentity is an answer whose message_id names another message.
func wrongIdentity(body []byte) []byte {
	var members map[string]any
	if json.Unmarshal(body, &members) != nil {
		return body
	}
	if id, ok := members["message_id"].(string); ok {
		members["message_id"] = "other-" + id
	} else {
		members["message_id"] = "other"
	}
	changed, _ := json.Marshal(members)
	return changed
}

// withoutAccepted is an ACK without its `accepted` member.
func withoutAccepted(body []byte) []byte {
	var members map[string]any
	if json.Unmarshal(body, &members) != nil {
		return body
	}
	delete(members, "accepted")
	changed, _ := json.Marshal(members)
	return changed
}

// refused writes an error response, with the status the client error table
// gives its code unless the refusal names another, and logs it.
func (server *Server) refused(w http.ResponseWriter, entry *requestLog, refusal *refusal) {
	code := wire.ErrorCode(refusal.code)
	body := wire.ErrorBody{Code: code, Message: refusal.message}
	if refusal.code == "previous_connection_active" {
		body.RetryAfterMs = wire.Some(max(refusal.retry.Milliseconds(), 0))
	}
	encoded, _ := wire.Encode(body)
	status := refusal.status
	if status == 0 {
		status = code.Status()
	}
	entry.Status, entry.Code = status, refusal.code
	server.mu.Lock()
	server.log.requests = append(server.log.requests, *entry)
	server.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

func (server *Server) ready(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "ready", maxBytes: wire.MessageMaxBytes, replayable: true,
		decode: func(body []byte) (string, string, any, error) {
			ready, err := wire.Decode[wire.Ready](body)
			return string(ready.ConnectionID), string(ready.MessageID), &ready, err
		},
		apply: func(conn *connection, value any, _ []byte) (answer, *refusal) {
			ready := value.(*wire.Ready)
			if conn.readyRefused && conn.readyMessageID == string(ready.MessageID) {
				return answer{}, refuse("cli_upgrade_required", "the CLI is older than the minimum supported version")
			}
			if conn.readyMessageID != "" {
				return answer{}, refuse("invalid_message", "the connection's ready was already accepted or refused")
			}
			if server.config.MinCLIVersion != "" && older(string(ready.CLIVersion), server.config.MinCLIVersion) {
				conn.readyMessageID, conn.readyRefused = string(ready.MessageID), true
				return answer{}, refuse("cli_upgrade_required", "the CLI is older than the minimum supported version")
			}
			valid, refusal := server.grant(conn)
			if refusal != nil {
				return answer{}, refusal
			}
			conn.readyMessageID, conn.readyAccepted = string(ready.MessageID), true
			conn.leaseUntil = server.clock.Now().Add(valid)
			if conn.stream != nil {
				conn.stream.nextHeartbeat = server.clock.Now().Add(HeartbeatInterval)
			}
			for _, spec := range server.earlyDispatch[conn.instance.id] {
				_, _ = server.issue(conn, spec, true)
			}
			delete(server.earlyDispatch, conn.instance.id)
			server.release(conn)
			return grantAnswer(conn, string(ready.MessageID), valid), nil
		},
	})
}

func grantAnswer(conn *connection, messageID string, valid time.Duration) answer {
	body, _ := wire.Encode(wire.GrantAcknowledgement{
		ConnectionID: wire.ID(conn.id), MessageID: wire.ID(messageID), Accepted: true, ValidForMs: valid.Milliseconds(),
	})
	return answer{status: http.StatusOK, body: body}
}

func (server *Server) heartbeat(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "heartbeat", maxBytes: wire.MessageMaxBytes, replayable: true,
		decode: func(body []byte) (string, string, any, error) {
			heartbeat, err := wire.Decode[wire.Heartbeat](body)
			return string(heartbeat.ConnectionID), string(heartbeat.MessageID), &heartbeat, err
		},
		apply: func(conn *connection, value any, _ []byte) (answer, *refusal) {
			heartbeat := value.(*wire.Heartbeat)
			if !conn.readyAccepted {
				return answer{}, refuse("invalid_message", "a heartbeat before the connection's ready was accepted")
			}
			if heartbeat.HeartbeatSeq <= conn.acceptedHeartbeatSeq {
				return answer{}, refuse("invalid_message", "heartbeat_seq is not above the last accepted heartbeat")
			}
			valid, refusal := server.grant(conn)
			if refusal != nil {
				return answer{}, refusal
			}
			conn.acceptedHeartbeatSeq = heartbeat.HeartbeatSeq
			conn.leaseUntil = server.clock.Now().Add(valid)
			return grantAnswer(conn, string(heartbeat.MessageID), valid), nil
		},
	})
}

func (server *Server) sessionStatus(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "session-status", maxBytes: wire.MessageMaxBytes, replayable: true,
		decode: func(body []byte) (string, string, any, error) {
			report, err := wire.Decode[wire.SessionStatus](body)
			return string(report.ConnectionID), string(report.MessageID), &report, err
		},
		apply: func(conn *connection, value any, _ []byte) (answer, *refusal) {
			report := value.(*wire.SessionStatus)
			instruction := conn.issued[string(report.RequestMessageID)]
			drives := conn.answersSentDrives(report)
			answers := drives ||
				instruction != nil && instruction.sessionID == string(report.SessionID) &&
					instruction.bindingID == string(report.BindingID) &&
					((instruction.kind == "session.prepare" && (report.Status == "prepared" || report.Status == "failed")) ||
						(instruction.kind == "session.close" && report.Status == "closed"))
			if !answers {
				return answer{}, refuse("unknown_instruction", "the report answers no instruction issued on this connection")
			}
			if !drives {
				instruction.reported = true
			}
			if !conn.firstReportStored {
				conn.firstReportStored = true
				server.release(conn)
			}
			return acknowledge(conn, string(report.MessageID)), nil
		},
	})
}

func (server *Server) protocolError(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "protocol-error", maxBytes: wire.MessageMaxBytes, replayable: true,
		decode: func(body []byte) (string, string, any, error) {
			report, err := wire.Decode[wire.ProtocolErrorReport](body)
			return string(report.ConnectionID), string(report.MessageID), &report, err
		},
		apply: func(conn *connection, value any, _ []byte) (answer, *refusal) {
			report := value.(*wire.ProtocolErrorReport)
			return acknowledge(conn, string(report.MessageID)), nil
		},
	})
}
