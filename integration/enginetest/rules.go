package enginetest

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// T is the part of *testing.T a CLI reports to.
type T interface {
	Helper()
	Errorf(format string, args ...any)
}

// checker is what a CLI and its copies share: the test that a message
// outside the contract fails, and what the stream rules need of the
// messages before: which connections declared drive mounting, each
// binding's drive set and the drives that have a commit.
type checker struct {
	t T

	mu          sync.Mutex
	connections map[string]*connectionState
	committed   map[string]bool
}

type connectionState struct {
	mount    bool
	bindings map[string]*bindingState
}

// bindingState follows a binding's drive set (README, "Drive binding
// changes"), leaving out every drive Engine may already have removed: the
// rules ask only for what Engine certainly owes.
type bindingState struct {
	prepare []string
	changes map[string][]string
	// set is nil until Engine acknowledged the prepared report.
	set map[string]bool
}

func (c *checker) report(format string, args ...any) {
	c.t.Helper()
	c.t.Errorf("enginetest: "+format, args...)
}

func (c *checker) connection(id string) *connectionState {
	state := c.connections[id]
	if state == nil {
		state = &connectionState{bindings: map[string]*bindingState{}}
		c.connections[id] = state
	}
	return state
}

func (s *connectionState) binding(id string) *bindingState {
	state := s.bindings[id]
	if state == nil {
		state = &bindingState{changes: map[string][]string{}}
		s.bindings[id] = state
	}
	return state
}

func driveIDs(bindings []protocol.DriveBinding) []string {
	ids := make([]string, len(bindings))
	for i, binding := range bindings {
		ids[i] = binding.DriveID
	}
	return ids
}

// instructed records what an event tells of drive sets and commits.
func (c *checker) instructed(envelope protocol.Downstream) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch payload := envelope.Payload.(type) {
	case protocol.SessionPreparePayload:
		c.connection(envelope.ConnectionID).binding(payload.BindingID).prepare = driveIDs(payload.Drives)
	case protocol.SessionDrivesPayload:
		binding := c.connection(envelope.ConnectionID).binding(payload.BindingID)
		binding.changes[envelope.MessageID] = driveIDs(payload.Drives)
		if binding.set != nil {
			for _, drive := range binding.changes[envelope.MessageID] {
				binding.set[drive] = true
			}
		}
	case protocol.DriveChangedPayload:
		c.committed[payload.DriveID] = true
	}
}

// reporting records a Session report before it is sent: from then on
// Engine may have removed what the report lets it remove.
func (c *checker) reporting(status protocol.SessionStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	connection := c.connection(status.ConnectionID)
	switch status.Status {
	case "closed":
		delete(connection.bindings, status.BindingID)
	case "applied":
		binding := connection.binding(status.BindingID)
		drives, found := binding.changes[status.RequestMessageID]
		if !found || binding.set == nil {
			return
		}
		for drive := range binding.set {
			if !slices.Contains(drives, drive) {
				delete(binding.set, drive)
			}
		}
	}
}

// accepted records an accepted ready's drive mounting.
func (c *checker) accepted(ready protocol.Ready) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connection(ready.ConnectionID).mount = ready.Capabilities.Drives.Mount
}

// prepared records an acknowledged prepared report.
func (c *checker) prepared(status protocol.SessionStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if status.Status != "prepared" {
		return
	}
	binding, found := c.connection(status.ConnectionID).bindings[status.BindingID]
	if !found || binding.set != nil {
		return
	}
	binding.set = map[string]bool{}
	for _, drive := range binding.prepare {
		binding.set[drive] = true
	}
}

func (c *checker) commit(drive string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.committed[drive] = true
}

// owed is the drives of connectionID's drive sets that have a commit, when
// its ready declared drive mounting: the notices a stream's start owes.
func (c *checker) owed(connectionID string) map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	owed := map[string]bool{}
	connection, found := c.connections[connectionID]
	if !found || !connection.mount {
		return owed
	}
	for _, binding := range connection.bindings {
		for drive := range binding.set {
			if c.committed[drive] {
				owed[drive] = true
			}
		}
	}
	return owed
}

// streamRules holds one stream to the stream rules of README.md.
type streamRules struct {
	checks     *checker
	events     int
	connection string
	ids        map[string]bool
	// owed is the start notices not yet received; nil once the first
	// instruction arrived.
	owed map[string]bool
}

func (c *checker) stream() *streamRules {
	return &streamRules{checks: c, ids: map[string]bool{}}
}

func (r *streamRules) report(format string, args ...any) {
	r.checks.t.Helper()
	r.checks.report("event %d of the stream: %s", r.events, fmt.Sprintf(format, args...))
}

// malformed reports an event that is not one data line.
func (r *streamRules) malformed(event Event) {
	r.checks.t.Helper()
	r.events++
	r.report("not one data line: %q", event.Lines)
}

// undecodable reports an event whose data line the contract refuses.
func (r *streamRules) undecodable(event Event, err error) {
	r.checks.t.Helper()
	r.events++
	r.report("outside the contract: %v: %s", err, abbreviate(event.Data))
}

// event holds a decoded event to the rules that span events.
func (r *streamRules) event(event Event) {
	r.checks.t.Helper()
	r.events++
	envelope := event.Envelope
	if err := validate(definition{connectionFile, "Downstream"}, event.Data); err != nil {
		r.report("outside the contract: %v: %s", err, abbreviate(event.Data))
	}
	kind := envelope.Payload.MessageType()
	if r.events == 1 {
		if kind != "connected" {
			r.report("the first event is %s, not connected", kind)
		}
		r.connection = envelope.ConnectionID
		r.owed = r.checks.owed(r.connection)
	}
	if r.ids[envelope.MessageID] {
		r.report("message %s is on the stream twice", envelope.MessageID)
	}
	r.ids[envelope.MessageID] = true
	switch {
	case kind == "drive.changed":
		delete(r.owed, envelope.Payload.(protocol.DriveChangedPayload).DriveID)
	case r.owed != nil && (strings.HasPrefix(kind, "session.") || strings.HasPrefix(kind, "tool.") || kind == "mcp.call"):
		still := r.checks.owed(r.connection)
		for drive := range r.owed {
			if still[drive] {
				r.report("%s comes before the stream's drive.changed of drive %s", kind, drive)
			}
		}
		r.owed = nil
	}
	r.checks.instructed(envelope)
}

func abbreviate(data []byte) string {
	const most = 512
	if len(data) <= most {
		return string(data)
	}
	return fmt.Sprintf("%s… (%d bytes)", data[:most], len(data))
}
