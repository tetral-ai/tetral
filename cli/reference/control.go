package reference

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// fault changes how the server answers the next matching upstream request.
type fault struct {
	// Endpoint is an upstream endpoint's name, such as `ready`; MessageID,
	// when set, matches only that message.
	Endpoint  string `json:"endpoint"`
	MessageID string `json:"message_id"`
	// Action is `delay` (accept, then hold the answer until the clock has
	// advanced by DelayMs), `drop` (accept, then close the connection
	// without an answer), `withhold` (accept, then hold the answer until the
	// answer control releases it), `wrong_identity` (answer for another
	// message_id), `without_accepted` (answer the ACK without `accepted`),
	// `schema_fail` (answer `200` with Body, `{}` by default), `hold_request`
	// (hold the request before it is authenticated until the answer control
	// releases it, then serve it as it would be served then), `error`
	// (answer Code with Status, by default the status the client error table
	// gives Code, accepting nothing; an answer its endpoint cannot give
	// needs OutsideContract) or `throttle` (read the body at
	// BytesPerSecond of the clock, so that a clock held still stalls it; it
	// matches an endpoint only, since it applies before the body is read).
	//
	// The drive transfer endpoints are named as package wire names them
	// (`list_files`, `list_changes`, `upload_content`, `commit`,
	// `download_content`) and take the faults that fit a request without
	// message_id and an answer that is no ACK; a `throttle` of
	// `download_content` paces the content it writes.
	Action  string `json:"action"`
	DelayMs int64  `json:"delay_ms"`
	// BytesPerSecond is how many bytes of the body a `throttle` reads for
	// each second the clock advances after the request arrives.
	BytesPerSecond int64           `json:"bytes_per_second"`
	Code           string          `json:"code"`
	Status         int             `json:"status"`
	Body           json.RawMessage `json:"body"`
	// RetryAfterS is the `Retry-After` of an `error` with `rate_limited`,
	// in seconds, and MissingSha256 the `missing_sha256` of one with
	// `content_missing`.
	RetryAfterS   int64    `json:"retry_after_s"`
	MissingSha256 []string `json:"missing_sha256"`
	// OutsideContract marks an `error` the protocol does not let the
	// endpoint give; without it the control refuses such an answer.
	OutsideContract bool `json:"outside_contract"`
	// Count is how many matching requests the fault changes; 0 means one.
	Count int `json:"count"`
}

// refusal is the answer of an `error` fault.
func (injected *fault) refusal() *refusal {
	missing := make([]wire.Sha256, len(injected.MissingSha256))
	for index, hash := range injected.MissingSha256 {
		missing[index] = wire.Sha256(hash)
	}
	return &refusal{
		code: injected.Code, message: "injected by the answer control", status: injected.Status,
		missing: missing, retryAfter: injected.RetryAfterS, outsideContract: injected.OutsideContract,
	}
}

// inContract checks the refusal as a client checks an error answer of
// endpoint: the body, the status of its code, on a drive transfer endpoint
// the codes that endpoint gives, and the `Retry-After` that comes with a
// `429`. The protocol defines no body for a `5xx`, so any passes.
func (refusal *refusal) inContract(endpoint string) error {
	status, body := refusal.encoded()
	if status >= 500 {
		return nil
	}
	transfer := wire.DriveTransferEndpoint(endpoint)
	if !slices.Contains(wire.DriveTransferEndpoints, transfer) {
		_, err := wire.DecodeClientError(status, body)
		return err
	}
	if _, err := wire.DecodeDriveTransferError(transfer, status, body); err != nil {
		return err
	}
	if refusal.code == "rate_limited" {
		headers, _ := json.Marshal(map[string]string{"Retry-After": strconv.FormatInt(refusal.retryAfter, 10)})
		_, err := wire.Decode[wire.RateLimitedResponseHeaders](headers)
		return err
	}
	return nil
}

// takeFault takes the next fault for a message of an endpoint, or with
// throttle the next `throttle` of the endpoint, which no other take sees.
func (server *Server) takeFault(endpoint, messageID string, throttle bool) *fault {
	for index, candidate := range server.faults {
		if candidate.Endpoint != endpoint || (candidate.MessageID != "" && candidate.MessageID != messageID) ||
			(candidate.Action == "throttle") != throttle || candidate.Action == "hold_request" {
			continue
		}
		candidate.Count--
		if candidate.Count <= 0 {
			server.faults = slices.Delete(server.faults, index, index+1)
		}
		return candidate
	}
	return nil
}

// takeHold takes the next `hold_request` of an endpoint.
func (server *Server) takeHold(endpoint string) *fault {
	for index, candidate := range server.faults {
		if candidate.Endpoint != endpoint || candidate.Action != "hold_request" {
			continue
		}
		candidate.Count--
		if candidate.Count <= 0 {
			server.faults = slices.Delete(server.faults, index, index+1)
		}
		return candidate
	}
	return nil
}

// serverLog is what the state control reports besides the live state.
type serverLog struct {
	requests []requestLog
	received []receivedMessage
	events   []sentEvent
}

type requestLog struct {
	At           time.Time         `json:"at"`
	Method       string            `json:"method"`
	Endpoint     string            `json:"endpoint"`
	Query        map[string]string `json:"query,omitempty"`
	ConnectionID string            `json:"connection_id,omitempty"`
	MessageID    string            `json:"message_id,omitempty"`
	// BindingID, DriveID, ContentSha256 and Range are a drive transfer
	// request's: its binding, the drive and content hash of its path, and
	// its Range header.
	BindingID     string `json:"binding_id,omitempty"`
	DriveID       string `json:"drive_id,omitempty"`
	ContentSha256 string `json:"content_sha256,omitempty"`
	Range         string `json:"range,omitempty"`
	// CredentialID is the credential that the request's key names, in any
	// state, so a test can tell which key a request carried.
	CredentialID string `json:"credential_id,omitempty"`
	Traceparent  string `json:"traceparent,omitempty"`
	// BodySHA256 is the lowercase hexadecimal SHA-256 of an upstream
	// message's body as read, so a test can tell a byte-identical retry.
	BodySHA256 string `json:"body_sha256,omitempty"`
	// BodyBytes is the size of an upstream message's body as read, at most
	// one byte over its endpoint's limit.
	BodyBytes int    `json:"body_bytes,omitempty"`
	Status    int    `json:"status"`
	Code      string `json:"code,omitempty"`
}

// receivedMessage is an upstream message the server accepted.
type receivedMessage struct {
	Endpoint     string `json:"endpoint"`
	ConnectionID string `json:"connection_id"`
	MessageID    string `json:"message_id"`
	Traceparent  string `json:"traceparent,omitempty"`
	// DriveID is the drive of an accepted commit.
	DriveID string          `json:"drive_id,omitempty"`
	Body    json.RawMessage `json:"body"`
}

// sentEvent is a downstream event the server wrote on a stream.
type sentEvent struct {
	ConnectionID string `json:"connection_id"`
	Stream       int    `json:"stream"`
	MessageID    string `json:"message_id"`
	Type         string `json:"type"`
	// data is the event as written, which the sent_event control gives: the
	// state control leaves it out, since a test reads the state often.
	data []byte
}

// ControlHandler serves the fault-injection controls, `POST
// /control/<name>` with a JSON body. Nothing of the protocol reaches them.
func (server *Server) ControlHandler() http.Handler {
	controls := map[string]func(json.RawMessage) (any, error){
		"credential":                server.controlCredential,
		"prepare":                   server.controlInstruction("session.prepare"),
		"close":                     server.controlInstruction("session.close"),
		"drives":                    server.controlInstruction("session.drives"),
		"execute":                   server.controlInstruction("tool.execute"),
		"stdin":                     server.controlInstruction("tool.stdin"),
		"cancel":                    server.controlInstruction("tool.cancel"),
		"mcp_call":                  server.controlInstruction("mcp.call"),
		"send":                      server.controlSend,
		"events":                    server.controlEvents,
		"answer":                    server.controlAnswer,
		"dispatch_before_ready_ack": server.controlEarlyDispatch,
		"sent_event":                server.controlSentEvent,
		"break_stream":              server.controlBreakStream,
		"clock":                     server.controlClock,
		"drive_commit":              server.controlDriveCommit,
		"drive_limits":              server.controlDriveLimits,
		"drive_remove_version":      server.controlDriveRemoveVersion,
		"drive_expire":              server.controlDriveExpire,
		"drive_budget":              server.controlDriveBudget,
		"drive_state":               server.controlDriveState,
		"state":                     func(json.RawMessage) (any, error) { return server.snapshot(), nil },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /control/{name}", func(w http.ResponseWriter, request *http.Request) {
		control, known := controls[request.PathValue("name")]
		if !known {
			http.Error(w, "unknown control", http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(strings.TrimSpace(string(body))) == 0 {
			body = []byte("{}")
		}
		result, err := control(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
	return mux
}

func decodeControl(body json.RawMessage, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// controlCredential adds, revokes or expires a test credential. Adding a
// pending credential with Renew offers it on the instance's connection.
func (server *Server) controlCredential(body json.RawMessage) (any, error) {
	var input struct {
		Action       string     `json:"action"`
		Credential   Credential `json:"credential"`
		CredentialID string     `json:"credential_id"`
		Renew        bool       `json:"renew"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	switch input.Action {
	case "add":
		if err := server.addCredential(input.Credential); err != nil {
			return nil, err
		}
		if input.Renew {
			if input.Credential.State != "pending" {
				return nil, errors.New("only a pending credential is offered for renewal")
			}
			server.offerRenewal(server.credentials[input.Credential.CredentialID])
		}
	case "revoke", "expire":
		found := server.credentials[input.CredentialID]
		if found == nil {
			return nil, fmt.Errorf("no credential %q", input.CredentialID)
		}
		found.State = map[string]string{"revoke": "revoked", "expire": "expired"}[input.Action]
	default:
		return nil, fmt.Errorf("unknown credential action %q", input.Action)
	}
	return map[string]any{}, nil
}

// controlInstruction issues a Session or tool instruction on an instance's
// live connection, under the staging limit. Its payload must pass package
// wire's decoder unless outside_contract marks it as invalid on purpose.
func (server *Server) controlInstruction(kind string) func(json.RawMessage) (any, error) {
	return func(body json.RawMessage) (any, error) {
		var input struct {
			InstanceID      string                     `json:"instance_id"`
			Payload         json.RawMessage            `json:"payload"`
			MessageID       string                     `json:"message_id"`
			Traceparent     string                     `json:"traceparent"`
			Extra           map[string]json.RawMessage `json:"extra"`
			OutsideContract bool                       `json:"outside_contract"`
		}
		if err := decodeControl(body, &input); err != nil {
			return nil, err
		}
		server.mu.Lock()
		defer server.mu.Unlock()
		conn := server.live(input.InstanceID)
		if conn == nil {
			return nil, fmt.Errorf("instance %q has no live connection", input.InstanceID)
		}
		instruction, err := server.issue(conn, instructionSpec{
			Type: kind, Payload: input.Payload, MessageID: input.MessageID, Traceparent: input.Traceparent,
			Extra: input.Extra, OutsideContract: input.OutsideContract,
		}, false)
		if err != nil {
			return nil, err
		}
		_, sent := conn.issued[instruction.messageID]
		return map[string]any{"connection_id": conn.id, "message_id": instruction.messageID, "queued": !sent}, nil
	}
}

// controlSend writes an event on an instance's current stream as given, or
// writes again an instruction issued on the stream's connection, sent or
// still queued. Nothing is recorded as issued. The event must pass package
// wire's decoder unless outside_contract marks it as invalid on purpose, a
// malformed one included. With count, the same event is written that many
// times, each once the stream's buffer has room.
func (server *Server) controlSend(body json.RawMessage) (any, error) {
	var input struct {
		InstanceID      string          `json:"instance_id"`
		Event           json.RawMessage `json:"event"`
		Data            *string         `json:"data"`
		Resend          string          `json:"resend"`
		Count           int             `json:"count"`
		OutsideContract bool            `json:"outside_contract"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	if input.Resend == "" && !input.OutsideContract {
		given := []byte(input.Event)
		if input.Data != nil {
			given = []byte(*input.Data)
		}
		if _, err := wire.Decode[wire.Downstream](given); err != nil {
			return nil, err
		}
	}
	if input.Count > 1 {
		return server.sendRepeated(input.InstanceID, input.Event, input.Count)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	conn := server.live(input.InstanceID)
	if conn == nil || conn.stream == nil {
		return nil, fmt.Errorf("instance %q has no open stream", input.InstanceID)
	}
	var data []byte
	switch {
	case input.Resend != "":
		instruction := conn.issued[input.Resend]
		if instruction == nil {
			return nil, fmt.Errorf("no issued instruction %q", input.Resend)
		}
		data = instruction.envelope
	case input.Data != nil:
		data = []byte(*input.Data)
	case len(input.Event) > 0:
		encoded, err := json.Marshal(input.Event)
		if err != nil {
			return nil, err
		}
		data = encoded
	default:
		return nil, errors.New("send needs event, data or resend")
	}
	var fields struct {
		MessageID string `json:"message_id"`
		Type      string `json:"type"`
	}
	_ = json.Unmarshal(data, &fields)
	conn.stream.send(data, fields.MessageID, fields.Type)
	return map[string]any{"connection_id": conn.id}, nil
}

// sendRepeated writes event count times on the instance's current stream,
// waiting for room in its buffer instead of closing it.
func (server *Server) sendRepeated(instanceID string, event json.RawMessage, count int) (any, error) {
	if len(event) == 0 {
		return nil, errors.New("send with count needs event")
	}
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	var fields struct {
		MessageID string `json:"message_id"`
		Type      string `json:"type"`
	}
	_ = json.Unmarshal(data, &fields)
	for sent := 0; sent < count; {
		server.mu.Lock()
		conn := server.live(instanceID)
		if conn == nil || conn.stream == nil || conn.stream.isClosed() {
			server.mu.Unlock()
			return nil, fmt.Errorf("instance %q has no open stream", instanceID)
		}
		room := len(conn.stream.frames) < cap(conn.stream.frames)
		if room {
			conn.stream.send(data, fields.MessageID, fields.Type)
			sent++
		}
		server.mu.Unlock()
		if !room {
			time.Sleep(time.Millisecond)
		}
	}
	return map[string]any{"sent": count}, nil
}

// eventsMode is how one events request is answered instead of at once.
type eventsMode struct {
	name string
	// until is the clock's time before which a `delay_connected` request
	// does not connect.
	until time.Time
	// dataBytes is the size that a `pad_connected` request's `connected`
	// is padded to.
	dataBytes int
}

// controlEvents makes the next events requests hold their headers
// (`hold_headers`), send headers and no event (`headers_only`), send
// comments only (`comments_only`), connect only once the clock has
// advanced by delay_ms from this control (`delay_connected`), or send a
// `connected` whose JSON is padded with spaces to data_bytes
// (`pad_connected`).
func (server *Server) controlEvents(body json.RawMessage) (any, error) {
	var input struct {
		Mode      string `json:"mode"`
		Count     int    `json:"count"`
		DelayMs   int64  `json:"delay_ms"`
		DataBytes int    `json:"data_bytes"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	switch {
	case input.Mode == "delay_connected" && input.DelayMs <= 0:
		return nil, errors.New("delay_connected needs delay_ms above 0")
	case input.Mode == "pad_connected" && input.DataBytes <= 0:
		return nil, errors.New("pad_connected needs data_bytes above 0")
	case !slices.Contains([]string{"hold_headers", "headers_only", "comments_only", "delay_connected", "pad_connected"}, input.Mode):
		return nil, fmt.Errorf("unknown events mode %q", input.Mode)
	}
	// Counted from this control, so that a request arriving after an
	// advance still connects at the time the control named.
	mode := eventsMode{
		name: input.Mode, until: server.clock.Now().Add(time.Duration(input.DelayMs) * time.Millisecond),
		dataBytes: input.DataBytes,
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	for range max(input.Count, 1) {
		server.eventsModes = append(server.eventsModes, mode)
	}
	return map[string]any{}, nil
}

// controlAnswer sets a fault for the next matching upstream requests, or
// with action `release` releases the withheld answers, those of one
// endpoint or one message_id when the control names them, so that a test
// can release answers in another order than their requests.
func (server *Server) controlAnswer(body json.RawMessage) (any, error) {
	var input fault
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	switch input.Action {
	case "release":
		server.releaseHeld(func(hold *heldAnswer) bool {
			return hold.Until.IsZero() && (input.Endpoint == "" || hold.Endpoint == input.Endpoint) &&
				(input.MessageID == "" || hold.MessageID == input.MessageID)
		})
		return map[string]any{}, nil
	case "drop", "withhold", "wrong_identity", "without_accepted", "schema_fail":
	case "hold_request":
		if input.MessageID != "" {
			return nil, errors.New("a hold_request matches an endpoint only, since it applies before the body is decoded")
		}
	case "delay":
		if input.DelayMs <= 0 {
			return nil, errors.New("a delay needs delay_ms above 0")
		}
	case "throttle":
		if input.BytesPerSecond <= 0 || input.MessageID != "" {
			return nil, errors.New("a throttle needs bytes_per_second above 0 and no message_id")
		}
	case "error":
		switch {
		case input.Code == "":
			return nil, errors.New("an error answer needs a code")
		case input.Status == 0 && wire.ErrorCode(input.Code).Status() == 0:
			return nil, fmt.Errorf("an error answer with %q, a code outside the client error table, needs a status", input.Code)
		case input.Status != 0 && (input.Status < 400 || input.Status > 599):
			return nil, fmt.Errorf("an error answer's status %d is not between 400 and 599", input.Status)
		case input.Endpoint == "":
		case !input.OutsideContract:
			if err := input.refusal().inContract(input.Endpoint); err != nil {
				return nil, fmt.Errorf("the error answer is outside the protocol and not marked outside_contract: %w", err)
			}
		}
	default:
		return nil, fmt.Errorf("unknown answer action %q", input.Action)
	}
	if input.Endpoint == "" {
		return nil, errors.New("an answer fault needs an endpoint")
	}
	if slices.Contains(wire.DriveTransferEndpoints, wire.DriveTransferEndpoint(input.Endpoint)) {
		download := input.Endpoint == string(wire.DownloadContent)
		list := input.Endpoint == string(wire.ListFiles) || input.Endpoint == string(wire.ListChanges)
		switch {
		case input.MessageID != "":
			return nil, errors.New("a drive transfer request carries no message_id")
		case input.Action == "wrong_identity" || input.Action == "without_accepted":
			return nil, errors.New("a drive transfer answer is no ACK")
		case download && input.Action == "schema_fail":
			return nil, errors.New("a download's answer is raw content and has no schema")
		case list && input.Action == "throttle":
			return nil, errors.New("a list request has no body to throttle")
		}
	}
	server.faults = append(server.faults, &input)
	return map[string]any{}, nil
}

// controlEarlyDispatch makes the next accepted ready of an instance's
// connection send these Session instructions before its ACK is answered,
// past the staging limit. Each must pass package wire's decoder unless it
// is marked outside_contract.
func (server *Server) controlEarlyDispatch(body json.RawMessage) (any, error) {
	var input struct {
		InstanceID   string            `json:"instance_id"`
		Instructions []instructionSpec `json:"instructions"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	for _, spec := range input.Instructions {
		// The connection and, without one given, the message get their
		// identities only when the ready is accepted.
		if _, _, err := instructionEnvelope("conn_pending", cmp.Or(spec.MessageID, "msg_pending"), spec); err != nil {
			return nil, err
		}
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	server.earlyDispatch[input.InstanceID] = append(server.earlyDispatch[input.InstanceID], input.Instructions...)
	return map[string]any{}, nil
}

// controlBreakStream closes an instance's current stream; its connection
// keeps its lease.
func (server *Server) controlBreakStream(body json.RawMessage) (any, error) {
	var input struct {
		InstanceID string `json:"instance_id"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	conn := server.live(input.InstanceID)
	if conn == nil || conn.stream == nil {
		return nil, fmt.Errorf("instance %q has no open stream", input.InstanceID)
	}
	conn.stream.close()
	conn.stream = nil
	return map[string]any{"connection_id": conn.id}, nil
}

// controlSentEvent gives the data of one event the server wrote, by its
// place in the state's events.
func (server *Server) controlSentEvent(body json.RawMessage) (any, error) {
	var input struct {
		Index int `json:"index"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if input.Index < 0 || input.Index >= len(server.log.events) {
		return nil, fmt.Errorf("the server wrote %d events", len(server.log.events))
	}
	return map[string]any{"data": string(server.log.events[input.Index].data)}, nil
}

// controlClock advances the server's clock and does what the new time
// makes due before it answers (due).
func (server *Server) controlClock(body json.RawMessage) (any, error) {
	var input struct {
		AdvanceMs int64 `json:"advance_ms"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	if input.AdvanceMs < 0 {
		return nil, errors.New("the clock only moves forward")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	now := server.clock.Advance(time.Duration(input.AdvanceMs) * time.Millisecond)
	server.due()
	return map[string]any{"now": now}, nil
}

// Snapshot is what the state control reports: the leases, bindings,
// credentials and held answers as they stand, and every request, accepted
// message and event so far.
type Snapshot struct {
	Now         time.Time           `json:"now"`
	Credentials []CredentialState   `json:"credentials"`
	Connections []ConnectionState   `json:"connections"`
	Tickets     map[string][]string `json:"tickets"`
	Held        []heldAnswer        `json:"held"`
	Throttled   []throttledRead     `json:"throttled"`
	Requests    []requestLog        `json:"requests"`
	Received    []receivedMessage   `json:"received"`
	Events      []sentEvent         `json:"events"`
	ToolLists   []ToolListState     `json:"tool_lists"`
	// Failures are the requests and messages that the server does not
	// implement or could not make within the contract; a test that finds one
	// fails.
	Failures []string `json:"failures"`
}

type CredentialState struct {
	InstanceID   string    `json:"instance_id"`
	CredentialID string    `json:"credential_id"`
	State        string    `json:"state"`
	ExpiresAt    time.Time `json:"expires_at"`
	Fetched      bool      `json:"fetched"`
	RenewalsSent int       `json:"renewals_sent"`
	Reuses       int       `json:"reuses"`
}

type ConnectionState struct {
	ConnectionID      string             `json:"connection_id"`
	InstanceID        string             `json:"instance_id"`
	CredentialID      string             `json:"credential_id"`
	Live              bool               `json:"live"`
	LeaseUntil        time.Time          `json:"lease_until"`
	StreamOpen        bool               `json:"stream_open"`
	Streams           int                `json:"streams"`
	ReadyMessageID    string             `json:"ready_message_id,omitempty"`
	ReadyAccepted     bool               `json:"ready_accepted"`
	MountsDrives      bool               `json:"mounts_drives"`
	HeartbeatSeq      int64              `json:"heartbeat_seq"`
	AcceptedSeq       int64              `json:"accepted_heartbeat_seq"`
	FirstReportStored bool               `json:"first_report_stored"`
	Queued            int                `json:"queued"`
	Issued            []InstructionState `json:"issued"`
	Bindings          []BindingState     `json:"bindings"`
}

type InstructionState struct {
	MessageID      string `json:"message_id"`
	Type           string `json:"type"`
	SessionID      string `json:"session_id"`
	BindingID      string `json:"binding_id"`
	ToolUseEventID string `json:"tool_use_event_id,omitempty"`
	TaskID         string `json:"task_id,omitempty"`
	Reported       bool   `json:"reported"`
	Sends          int    `json:"sends"`
}

type BindingState struct {
	BindingID        string `json:"binding_id"`
	SessionID        string `json:"session_id"`
	PrepareMessageID string `json:"prepare_message_id"`
	// Prepared and Closed are set once the binding's prepared and closed
	// reports are stored.
	Prepared bool `json:"prepared"`
	Closed   bool `json:"closed"`
	// DriveSets are the drive sets that authorize the binding's drive
	// transfer, oldest first: the last is the one last sent.
	DriveSets [][]wire.DriveBinding `json:"drive_sets"`
}

func (server *Server) snapshot() Snapshot {
	server.mu.Lock()
	defer server.mu.Unlock()
	state := Snapshot{
		Now: server.clock.Now(), Tickets: map[string][]string{},
		Requests: slices.Clone(server.log.requests), Received: slices.Clone(server.log.received),
		Events: slices.Clone(server.log.events), ToolLists: server.toolListStates(),
		Failures: slices.Clone(server.failures),
	}
	for _, hold := range server.held {
		state.Held = append(state.Held, *hold)
	}
	for _, read := range server.throttledReads {
		state.Throttled = append(state.Throttled, *read)
	}
	for _, cred := range server.credentials {
		state.Credentials = append(state.Credentials, CredentialState{
			InstanceID: cred.InstanceID, CredentialID: cred.CredentialID, State: cred.State, ExpiresAt: cred.ExpiresAt,
			Fetched: cred.fetched, RenewalsSent: cred.renewalSent, Reuses: cred.reuses,
		})
	}
	slices.SortFunc(state.Credentials, func(a, b CredentialState) int { return strings.Compare(a.CredentialID, b.CredentialID) })
	for _, conn := range server.connections {
		connState := ConnectionState{
			ConnectionID: conn.id, InstanceID: conn.instance.id, CredentialID: conn.credential.CredentialID,
			Live: server.live(conn.instance.id) == conn, LeaseUntil: conn.leaseUntil,
			StreamOpen: conn.stream != nil, Streams: conn.streams, ReadyMessageID: conn.readyMessageID,
			ReadyAccepted: conn.readyAccepted, MountsDrives: conn.mountsDrives,
			HeartbeatSeq: conn.heartbeatSeq, AcceptedSeq: conn.acceptedHeartbeatSeq,
			FirstReportStored: conn.firstReportStored, Queued: len(conn.queue),
		}
		for _, messageID := range conn.order {
			instruction := conn.issued[messageID]
			connState.Issued = append(connState.Issued, InstructionState{
				MessageID: messageID, Type: instruction.kind, SessionID: instruction.sessionID,
				BindingID: instruction.bindingID, ToolUseEventID: instruction.toolUseEventID, TaskID: instruction.taskID,
				Reported: instruction.reported, Sends: instruction.sends,
			})
		}
		for bindingID, prepared := range conn.bindings {
			bindingState := BindingState{
				BindingID: bindingID, SessionID: prepared.sessionID, PrepareMessageID: prepared.prepareMessageID,
				Prepared: prepared.prepared, Closed: prepared.closed,
			}
			for _, set := range prepared.sets {
				bindingState.DriveSets = append(bindingState.DriveSets, set.drives)
			}
			connState.Bindings = append(connState.Bindings, bindingState)
		}
		slices.SortFunc(connState.Bindings, func(a, b BindingState) int { return strings.Compare(a.BindingID, b.BindingID) })
		state.Connections = append(state.Connections, connState)
	}
	slices.SortFunc(state.Connections, func(a, b ConnectionState) int { return strings.Compare(a.ConnectionID, b.ConnectionID) })
	for computer, tickets := range server.tickets {
		state.Tickets[computer.instanceID+"/"+computer.sessionID] = slices.Clone(tickets)
	}
	return state
}
