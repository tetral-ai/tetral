package reference

import (
	"bytes"
	"encoding/json"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// ledgerEntry is the recorded result of one accepted upstream message: a
// retry with the same content gets the same answer, other content
// `409 message_conflict`.
type ledgerEntry struct {
	endpoint string
	content  []byte
	answer   answer
}

// canonical is a message's content for the replay comparison: its JSON with
// sorted member names and numbers as written.
func canonical(body []byte) []byte {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var tree any
	if err := decoder.Decode(&tree); err != nil {
		return body
	}
	sorted, err := json.Marshal(tree)
	if err != nil {
		return body
	}
	return sorted
}

// replay finds the recorded answer to a retry, or the conflict of a reused
// message_id.
func (conn *connection) replay(endpoint, messageID string, body []byte) (*answer, *refusal) {
	entry, found := conn.ledger[messageID]
	if !found {
		return nil, nil
	}
	if entry.endpoint != endpoint || !bytes.Equal(entry.content, canonical(body)) {
		return nil, refuse("message_conflict", "the message_id was used on this connection with different content")
	}
	recorded := entry.answer
	return &recorded, nil
}

func (conn *connection) record(endpoint, messageID string, body []byte, result answer) {
	conn.ledger[messageID] = &ledgerEntry{endpoint: endpoint, content: canonical(body), answer: result}
}

// issuedInstruction is a Session instruction issued on a connection.
type issuedInstruction struct {
	messageID string
	kind      string
	sessionID string
	bindingID string
	envelope  []byte
	reported  bool
	sends     int
	// prepares is the binding a session.prepare names; it exists on the
	// connection only once the prepare is issued, so a queued prepare grants
	// no Git ticket.
	prepares *binding
}

// binding is a binding that a session.prepare issued on a connection names.
type binding struct {
	sessionID        string
	prepareMessageID string
	// cloudWithRepositories is set for a cloud computer with repositories,
	// the only preparation that may fetch a git ticket.
	cloudWithRepositories bool
}

// instructionSpec is a Session instruction a control asks for.
type instructionSpec struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MessageID   string          `json:"message_id"`
	Traceparent string          `json:"traceparent"`
}

// issue issues a Session instruction on conn. The payload is sent as
// given, so a test can issue one that fails its schema; its identities are
// read only to accept the reports that answer it. Unless bypass is set the
// instruction is queued while the connection's ready is not accepted, and
// beyond the staging limit until the first Session report is stored.
func (server *Server) issue(conn *connection, spec instructionSpec, bypass bool) (*issuedInstruction, error) {
	if spec.Type != "session.prepare" && spec.Type != "session.close" {
		return nil, errUnsupported(spec.Type)
	}
	var payload bytes.Buffer
	if err := json.Compact(&payload, spec.Payload); err != nil {
		return nil, err
	}
	var fields struct {
		SessionID    any   `json:"session_id"`
		BindingID    any   `json:"binding_id"`
		CreateCwd    any   `json:"create_cwd"`
		Repositories []any `json:"repositories"`
	}
	_ = json.Unmarshal(payload.Bytes(), &fields)
	messageID := spec.MessageID
	if messageID == "" {
		messageID = server.newID("msg")
	}
	encoded, err := json.Marshal(envelope{
		ProtocolVersion: wire.ProtocolVersion, MessageID: messageID, ConnectionID: conn.id,
		Type: spec.Type, Payload: payload.Bytes(), Traceparent: spec.Traceparent,
	})
	if err != nil {
		return nil, err
	}
	sessionID, _ := fields.SessionID.(string)
	bindingID, _ := fields.BindingID.(string)
	instruction := &issuedInstruction{messageID: messageID, kind: spec.Type, sessionID: sessionID, bindingID: bindingID, envelope: encoded}
	if spec.Type == "session.prepare" {
		createCwd, _ := fields.CreateCwd.(bool)
		instruction.prepares = &binding{
			sessionID: sessionID, prepareMessageID: messageID,
			cloudWithRepositories: createCwd && len(fields.Repositories) > 0,
		}
	}
	held := !conn.readyAccepted || (!conn.firstReportStored && conn.sessionIssued >= StagingLimit)
	if held && !bypass {
		conn.queue = append(conn.queue, instruction)
		return instruction, nil
	}
	server.dispatch(conn, instruction)
	return instruction, nil
}

// envelope is a downstream event as the server writes it.
type envelope struct {
	ProtocolVersion int             `json:"protocol_version"`
	MessageID       string          `json:"message_id"`
	ConnectionID    string          `json:"connection_id"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Traceparent     string          `json:"traceparent,omitempty"`
}

// dispatch records an instruction as issued and writes it on the current
// stream; without one it waits for the resume that resends it.
func (server *Server) dispatch(conn *connection, instruction *issuedInstruction) {
	conn.issued[instruction.messageID] = instruction
	conn.order = append(conn.order, instruction.messageID)
	if instruction.prepares != nil {
		conn.bindings[instruction.bindingID] = instruction.prepares
	}
	if !conn.firstReportStored {
		conn.sessionIssued++
	}
	if conn.stream != nil {
		conn.stream.send(instruction.envelope, instruction.messageID, instruction.kind)
		instruction.sends++
	}
}

// release dispatches the queued instructions that the connection's state
// now allows, in the order they were asked for.
func (server *Server) release(conn *connection) {
	for len(conn.queue) > 0 && conn.readyAccepted && (conn.firstReportStored || conn.sessionIssued < StagingLimit) {
		next := conn.queue[0]
		conn.queue = conn.queue[1:]
		server.dispatch(conn, next)
	}
}

// resend writes, on a resumed stream, every issued instruction without a
// stored report.
func (server *Server) resend(conn *connection) {
	for _, messageID := range conn.order {
		instruction := conn.issued[messageID]
		if !instruction.reported {
			conn.stream.send(instruction.envelope, instruction.messageID, instruction.kind)
			instruction.sends++
		}
	}
}

type errUnsupported string

func (e errUnsupported) Error() string { return "unsupported instruction type " + string(e) }
