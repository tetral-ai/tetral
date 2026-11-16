package enginetest

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// The CLI-direction cases the default script and the Session reports play.
const (
	CaseReady        = "the_cli_sends_ready_on_a_new_connection"
	CaseHeartbeat    = "the_cli_answers_each_heartbeat"
	CasePrepared     = "the_cli_reports_a_preparation_it_completed"
	CaseClosed       = "the_cli_reports_a_close"
	CaseApplied      = "the_cli_reports_a_drive_set_it_applied"
	CaseEnded        = "the_cli_reports_a_command_that_ended"
	CaseRanThenEnded = "the_cli_reports_a_command_that_runs_then_ends"
)

var behaviourCases = sync.OnceValues(LoadBehaviourCases)

// HoldReaction holds reports, the request bodies a test sends as the CLI's
// reaction to trigger on connectionID, to the CLI-direction case name: the
// trigger is the case's event, and the reports are its requests, one each
// and in its order. It returns the path of each request.
func HoldReaction(name, connectionID string, trigger Event, reports ...[]byte) ([]string, error) {
	file, err := behaviourCases()
	if err != nil {
		return nil, err
	}
	for _, c := range file.Cases {
		if c.Name != name || c.Direction != "cli" {
			continue
		}
		bindings := NewBindings(file.Contents)
		bindings.Connection = connectionID
		var paths []string
		for i, step := range c.Steps {
			switch {
			case step.Expect == nil || (i == 0) != (step.Expect.Event != nil) || (i > 0 && step.Expect.Request == nil):
				return nil, fmt.Errorf("case %s: step %d is not a trigger followed by requests", name, i+1)
			case i == 0:
				event, err := DecodeJSON(trigger.Data)
				if err != nil {
					return nil, err
				}
				if err := bindings.Match(step.Expect.Event, event); err != nil {
					return nil, fmt.Errorf("case %s: the event %s is not its trigger: %w", name, abbreviate(trigger.Data), err)
				}
			case i > len(reports):
				return nil, fmt.Errorf("case %s has %d requests; the test sends %d", name, len(c.Steps)-1, len(reports))
			default:
				report, err := DecodeJSON(reports[i-1])
				if err != nil {
					return nil, err
				}
				if err := bindings.Match(step.Expect.Request.Body, report); err != nil {
					return nil, fmt.Errorf("case %s: request %d, %s, is not the case's: %w", name, i, abbreviate(reports[i-1]), err)
				}
				paths = append(paths, step.Expect.Request.Path)
			}
		}
		if len(reports) != len(paths) {
			return nil, fmt.Errorf("case %s has %d requests; the test sends %d", name, len(paths), len(reports))
		}
		return paths, nil
	}
	return nil, fmt.Errorf("no CLI-direction case %s", name)
}

// Machine is a scripted machine on top of a CLI. Its default script opens
// the stream, sends ready and answers each heartbeat; a test sends every
// other report itself, through React, which holds the report to the
// CLI-direction case that states it as the real CLI's behaviour. A test
// that needs a step late, twice, changed or missing replaces Ready or
// Heartbeat, or sends through CLI directly.
type Machine struct {
	CLI          *CLI
	Capabilities protocol.Capabilities
	// Ready answers connected on a new connection and Heartbeat answers a
	// heartbeat. A nil Heartbeat hands heartbeats to Events unanswered.
	Ready     func(m *Machine, connected Event) error
	Heartbeat func(m *Machine, beat Event) error
	// Events carries every event the script did not consume, and is closed
	// when the stream ends.
	Events       chan Event
	ConnectionID string

	ctx    context.Context
	stream *Stream
}

// NewMachine returns a machine of cli that declares capabilities.
func NewMachine(cli *CLI, capabilities protocol.Capabilities) *Machine {
	return &Machine{CLI: cli, Capabilities: capabilities, Ready: (*Machine).ready, Heartbeat: (*Machine).heartbeat}
}

// Connect opens a stream as a new connection and runs Ready.
func (m *Machine) Connect(ctx context.Context) error {
	m.CLI.t().Helper()
	connected, err := m.open(ctx, m.CLI.EventsQuery("", nil))
	if err != nil {
		return err
	}
	m.ConnectionID = connected.Envelope.ConnectionID
	if err := m.Ready(m, connected); err != nil {
		return err
	}
	m.pump()
	return nil
}

// Resume closes the stream and opens the connection's next one.
func (m *Machine) Resume(ctx context.Context) error {
	m.CLI.t().Helper()
	m.Close()
	resume := true
	connected, err := m.open(ctx, m.CLI.EventsQuery(m.ConnectionID, &resume))
	if err != nil {
		return err
	}
	if payload, ok := connected.Envelope.Payload.(protocol.ConnectedPayload); !ok || !payload.Resumed || connected.Envelope.ConnectionID != m.ConnectionID {
		return fmt.Errorf("event %s; want connected on %s with resumed", connected.Data, m.ConnectionID)
	}
	m.pump()
	return nil
}

// Close closes the stream; the connection keeps its lease.
func (m *Machine) Close() {
	if m.stream != nil {
		_ = m.stream.Close()
	}
}

func (m *Machine) open(ctx context.Context, query url.Values) (Event, error) {
	stream, response, err := m.CLI.OpenEvents(ctx, query)
	if err != nil {
		return Event{}, err
	}
	if stream == nil {
		return Event{}, fmt.Errorf("events = %d %s", response.Status, response.Body)
	}
	m.ctx, m.stream = ctx, stream
	connected, err := stream.Next()
	if err != nil {
		return Event{}, err
	}
	if _, ok := connected.Envelope.Payload.(protocol.ConnectedPayload); !ok {
		return Event{}, fmt.Errorf("event %s; want connected", connected.Data)
	}
	return connected, nil
}

// pump reads the stream until it ends, answering heartbeats.
func (m *Machine) pump() {
	stream, events := m.stream, make(chan Event, 64)
	m.Events = events
	go func() {
		defer close(events)
		for {
			event, err := stream.Next()
			if err != nil {
				return
			}
			if _, beat := event.Envelope.Payload.(protocol.HeartbeatPayload); beat && m.Heartbeat != nil {
				if err := m.Heartbeat(m, event); err != nil && m.ctx.Err() == nil {
					m.CLI.t().Errorf("enginetest: heartbeat: %v", err)
				}
				continue
			}
			events <- event
		}
	}()
}

// React sends reports as the CLI's reaction to trigger, held to the
// CLI-direction case name, and returns their answers.
func (m *Machine) React(name string, trigger Event, reports ...[]byte) ([]Response, error) {
	m.CLI.t().Helper()
	paths, err := HoldReaction(name, m.ConnectionID, trigger, reports...)
	if err != nil {
		return nil, err
	}
	answers := make([]Response, len(reports))
	for i, report := range reports {
		if answers[i], err = m.CLI.Post(m.ctx, paths[i], report); err != nil {
			return nil, err
		}
	}
	return answers, nil
}

func (m *Machine) ready(connected Event) error {
	ready := protocol.Ready{ConnectionID: m.ConnectionID, MessageID: "ready_" + m.ConnectionID, OS: "linux", Arch: "x86_64",
		CLIVersion: "1.0.0", Capabilities: m.Capabilities}
	answers, err := m.React(CaseReady, connected, protocol.Encode(ready))
	if err != nil {
		return err
	}
	_, err = answers[0].Grant()
	return err
}

func (m *Machine) heartbeat(beat Event) error {
	payload := beat.Envelope.Payload.(protocol.HeartbeatPayload)
	answers, err := m.React(CaseHeartbeat, beat, protocol.Encode(protocol.Heartbeat{ConnectionID: m.ConnectionID,
		MessageID: "hb_" + beat.Envelope.MessageID, RequestMessageID: beat.Envelope.MessageID, HeartbeatSeq: payload.HeartbeatSeq}))
	if err != nil {
		return err
	}
	_, err = answers[0].Grant()
	return err
}

// SessionReport is the report of status answering a Session instruction,
// with cwd for a prepared one.
func SessionReport(connectionID string, instruction Event, status string) protocol.SessionStatus {
	report := protocol.SessionStatus{ConnectionID: connectionID, MessageID: "status_" + instruction.Envelope.MessageID,
		RequestMessageID: instruction.Envelope.MessageID, Status: status}
	switch payload := instruction.Envelope.Payload.(type) {
	case protocol.SessionPreparePayload:
		report.SessionID, report.BindingID, report.Cwd = payload.SessionID, payload.BindingID, protocol.Some(payload.Cwd)
	case protocol.SessionClosePayload:
		report.SessionID, report.BindingID = payload.SessionID, payload.BindingID
	case protocol.SessionDrivesPayload:
		report.SessionID, report.BindingID = payload.SessionID, payload.BindingID
	}
	return report
}

// Report sends the Session report the real CLI sends for instruction once
// it has done what the instruction asks: prepared, closed or applied.
func (m *Machine) Report(instruction Event) (Response, error) {
	m.CLI.t().Helper()
	name, status := CasePrepared, "prepared"
	switch instruction.Envelope.Payload.(type) {
	case protocol.SessionClosePayload:
		name, status = CaseClosed, "closed"
	case protocol.SessionDrivesPayload:
		name, status = CaseApplied, "applied"
	}
	answers, err := m.React(name, instruction, protocol.Encode(SessionReport(m.ConnectionID, instruction, status)))
	if err != nil {
		return Response{}, err
	}
	return answers[0], nil
}
