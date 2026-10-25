package reference

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
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
	// `schema_fail` (answer `200` with Body, `{}` by default), `error`
	// (answer Code with Status, by default the status the client error table
	// gives Code, accepting nothing) or `throttle` (read the body at
	// BytesPerSecond of the clock, so that a clock held still stalls it; it
	// matches an endpoint only, since it applies before the body is read).
	Action  string `json:"action"`
	DelayMs int64  `json:"delay_ms"`
	// BytesPerSecond is how many bytes of the body a `throttle` reads for
	// each second the clock advances after the request arrives.
	BytesPerSecond int64           `json:"bytes_per_second"`
	Code           string          `json:"code"`
	Status         int             `json:"status"`
	Body           json.RawMessage `json:"body"`
	// Count is how many matching requests the fault changes; 0 means one.
	Count int `json:"count"`
}

// takeFault takes the next fault for a message of an endpoint, or with
// throttle the next `throttle` of the endpoint, which no other take sees.
func (server *Server) takeFault(endpoint, messageID string, throttle bool) *fault {
	for index, candidate := range server.faults {
		if candidate.Endpoint != endpoint || (candidate.MessageID != "" && candidate.MessageID != messageID) ||
			(candidate.Action == "throttle") != throttle {
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
	Traceparent  string            `json:"traceparent,omitempty"`
	// BodySHA256 is the lowercase hexadecimal SHA-256 of an upstream
	// message's body as read, so a test can tell a byte-identical retry.
	BodySHA256 string `json:"body_sha256,omitempty"`
	Status     int    `json:"status"`
	Code       string `json:"code,omitempty"`
}

// receivedMessage is an upstream message the server accepted.
type receivedMessage struct {
	Endpoint     string          `json:"endpoint"`
	ConnectionID string          `json:"connection_id"`
	MessageID    string          `json:"message_id"`
	Traceparent  string          `json:"traceparent,omitempty"`
	Body         json.RawMessage `json:"body"`
}

// sentEvent is a downstream event the server wrote on a stream.
type sentEvent struct {
	ConnectionID string `json:"connection_id"`
	Stream       int    `json:"stream"`
	MessageID    string `json:"message_id"`
	Type         string `json:"type"`
}

// ControlHandler serves the fault-injection controls, `POST
// /control/<name>` with a JSON body. Nothing of the protocol reaches them.
func (server *Server) ControlHandler() http.Handler {
	controls := map[string]func(json.RawMessage) (any, error){
		"credential":                server.controlCredential,
		"prepare":                   server.controlInstruction("session.prepare"),
		"close":                     server.controlInstruction("session.close"),
		"execute":                   server.controlInstruction("tool.execute"),
		"stdin":                     server.controlInstruction("tool.stdin"),
		"cancel":                    server.controlInstruction("tool.cancel"),
		"send":                      server.controlSend,
		"events":                    server.controlEvents,
		"answer":                    server.controlAnswer,
		"dispatch_before_ready_ack": server.controlEarlyDispatch,
		"break_stream":              server.controlBreakStream,
		"clock":                     server.controlClock,
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

// controlInstruction issues a session.prepare or session.close on an
// instance's live connection, under the staging limit.
func (server *Server) controlInstruction(kind string) func(json.RawMessage) (any, error) {
	return func(body json.RawMessage) (any, error) {
		var input struct {
			InstanceID  string                     `json:"instance_id"`
			Payload     json.RawMessage            `json:"payload"`
			MessageID   string                     `json:"message_id"`
			Traceparent string                     `json:"traceparent"`
			Extra       map[string]json.RawMessage `json:"extra"`
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
			Extra: input.Extra,
		}, false)
		if err != nil {
			return nil, err
		}
		_, sent := conn.issued[instruction.messageID]
		return map[string]any{"connection_id": conn.id, "message_id": instruction.messageID, "queued": !sent}, nil
	}
}

// controlSend writes any event on an instance's current stream as given,
// a malformed one included, or writes again an instruction issued on the
// stream's connection, sent or still queued. Nothing is recorded as issued.
func (server *Server) controlSend(body json.RawMessage) (any, error) {
	var input struct {
		InstanceID string          `json:"instance_id"`
		Event      json.RawMessage `json:"event"`
		Data       *string         `json:"data"`
		Resend     string          `json:"resend"`
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

// eventsMode is how one events request is answered instead of at once.
type eventsMode struct {
	name string
	// until is the clock's time before which a `delay_connected` request
	// does not connect.
	until time.Time
}

// controlEvents makes the next events requests hold their headers
// (`hold_headers`), send headers and no event (`headers_only`), send
// comments only (`comments_only`) or connect only once the clock has
// advanced by delay_ms from this control (`delay_connected`).
func (server *Server) controlEvents(body json.RawMessage) (any, error) {
	var input struct {
		Mode    string `json:"mode"`
		Count   int    `json:"count"`
		DelayMs int64  `json:"delay_ms"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	switch {
	case input.Mode == "delay_connected" && input.DelayMs <= 0:
		return nil, errors.New("delay_connected needs delay_ms above 0")
	case input.Mode != "delay_connected" && !slices.Contains([]string{"hold_headers", "headers_only", "comments_only"}, input.Mode):
		return nil, fmt.Errorf("unknown events mode %q", input.Mode)
	}
	// Counted from this control, so that a request arriving after an
	// advance still connects at the time the control named.
	mode := eventsMode{name: input.Mode, until: server.clock.Now().Add(time.Duration(input.DelayMs) * time.Millisecond)}
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
		}
	default:
		return nil, fmt.Errorf("unknown answer action %q", input.Action)
	}
	if input.Endpoint == "" {
		return nil, errors.New("an answer fault needs an endpoint")
	}
	server.faults = append(server.faults, &input)
	return map[string]any{}, nil
}

// controlEarlyDispatch makes the next accepted ready of an instance's
// connection send these Session instructions before its ACK is answered,
// past the staging limit.
func (server *Server) controlEarlyDispatch(body json.RawMessage) (any, error) {
	var input struct {
		InstanceID   string            `json:"instance_id"`
		Instructions []instructionSpec `json:"instructions"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
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
}

func (server *Server) snapshot() Snapshot {
	server.mu.Lock()
	defer server.mu.Unlock()
	state := Snapshot{
		Now: server.clock.Now(), Tickets: map[string][]string{},
		Requests: slices.Clone(server.log.requests), Received: slices.Clone(server.log.received),
		Events: slices.Clone(server.log.events),
	}
	for _, hold := range server.held {
		state.Held = append(state.Held, *hold)
	}
	for _, read := range server.throttled {
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
			ReadyAccepted: conn.readyAccepted, HeartbeatSeq: conn.heartbeatSeq, AcceptedSeq: conn.acceptedHeartbeatSeq,
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
			connState.Bindings = append(connState.Bindings, BindingState{
				BindingID: bindingID, SessionID: prepared.sessionID, PrepareMessageID: prepared.prepareMessageID,
			})
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
