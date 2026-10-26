package computerhub

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// A stream writes Session instructions in passes. A pass runs when the
// stream starts and whenever a connection notice or the listener's return
// wakes it. Its transaction reads the connection without a lock, then locks
// the instance, the connection's credential and the connection FOR SHARE, in
// the order of every writer of those rows (internal/instance), so a
// revocation, a deregistration, an activation or a resume commits wholly
// before or after the pass chooses what to write:
//
//  1. an ended connection gets its disconnect and ends the stream; a
//     connection whose hub_pod or stream_count names another stream ends the
//     stream without one;
//  2. it chooses nothing while the lease has passed, the credential is
//     revoked or expired, or the instance is deregistered, nor before the
//     ready is accepted;
//  3. otherwise it chooses credential.renew once on this stream for each
//     pending renewal of the connection (instance.PendingRenewals), under the
//     message_id renewMessageID gives, the same on every stream;
//  4. then, in issue order, each unreported instruction whose issue_seq is
//     at most the stream's bound and not yet written on this stream; until
//     the first Session report is stored only the first
//     instance.SessionInstructionCap of them. It records the first write of
//     each instruction an earlier pass wrote (instance.MarkWritten).
//
// The pass writes what it chose only after its transaction commits, so no
// row lock is held while bytes go to the stream, and a pass that wrote
// instructions is followed at once by one that records their first write.
// Before each instruction it writes, and before that following pass, the
// stream writes a heartbeat that fell due (beat), so a long write keeps the
// lease.
// While it writes, a connection notice that finds the connection ended or
// held by another stream, or the stream's close, cuts the write (watch).
//
// The bound is the highest issue_seq a notice released. A stream start and
// a listener return release every instruction the connection row counts, so
// an instruction never waits for a hint that was lost; an instruction issued
// later waits for its own notice.

// streamState is what one stream keeps across passes. written holds every
// message the stream wrote, so none is written twice on it; unmarked holds
// the instructions among them whose first write (instance.MarkWritten) has
// not committed yet, which the next pass records without writing them again.
type streamState struct {
	stream   *clientStream
	writer   sseWriter
	written  map[string]bool
	unmarked map[string]bool
	accepted bool
	// retryBase is the wait before the stream's next pass after a failed
	// one, before its jitter; 0 once a pass commits.
	retryBase time.Duration
	// beat writes the stream's heartbeat when one is due, and reports
	// whether the stream may go on.
	beat func() bool
}

type passOutcome int

const (
	passWritten passOutcome = iota
	// passWrote is a committed pass that wrote instructions, whose first
	// write the next pass records.
	passWrote
	passFailed
	passEnded
	passMoved
	passBroken
)

// A failed pass runs again after passRetryFirst, the wait doubling after
// each further failure up to passRetryMax and multiplied by 1+r, r uniform
// in 0 to 0.2, so that streams failing together during a database outage
// spread their retries; a pass that commits returns it to passRetryFirst.
// passDeadline bounds a pass's transaction, so a held lock does not keep its
// pooled connection; its writes are bounded only by streamStallTimeout.
const (
	passRetryFirst = time.Second
	passRetryMax   = 30 * time.Second
	passDeadline   = 10 * time.Second
)

// retryAfter returns the wait before the stream's pass runs again after a
// failed one, with jitter r.
func (state *streamState) retryAfter(r float64) time.Duration {
	if state.retryBase == 0 {
		state.retryBase = passRetryFirst
	} else {
		state.retryBase = min(2*state.retryBase, passRetryMax)
	}
	return time.Duration(float64(state.retryBase) * (1 + r))
}

// firstWrite is an instruction whose first write to any stream a pass
// recorded.
type firstWrite struct {
	instruction instance.Instruction
	queued      time.Duration
}

// choice is what a pass's transaction found and chose to write.
type choice struct {
	ended        bool
	disconnect   protocol.ErrorCode
	moved        bool
	renewals     []string
	instructions []instance.Instruction
	marked       []string
	firsts       []firstWrite
}

// deliver runs one pass of state's stream.
func (s *Streams) deliver(ctx context.Context, state *streamState) passOutcome {
	stream := state.stream
	// A notice before the transaction is seen by it; only a later one is
	// left for watch.
	select {
	case <-stream.notified:
	default:
	}
	bound, all := stream.take()
	var chosen choice
	txCtx, cancel := context.WithTimeout(ctx, passDeadline)
	err := s.database.WithWorkspaceTx(txCtx, string(stream.workspace), "computer_hub.instruction_write", func(tx *dbconnect.Tx) error {
		chosen = choice{}
		connection, current, credential, err := shareRows(txCtx, tx, stream.workspace, stream.connection)
		if err != nil {
			return err
		}
		if connection.EndedAt != nil {
			chosen.ended = true
			chosen.disconnect, _ = disconnectCode(connection)
			return nil
		}
		if connection.HubPod != s.podName || connection.StreamCount != stream.count {
			chosen.moved = true
			return nil
		}
		if connection.CredentialID != credential.ID {
			return errCredentialMoved
		}
		if all {
			bound = max(bound, connection.LastIssueSeq)
			stream.raise(bound)
		}
		state.accepted = state.accepted || connection.ReadyAcceptedAt != nil
		now, err := instance.Now(txCtx, tx)
		if err != nil || !writable(connection, current, credential, now) {
			return err
		}
		renewals, err := instance.PendingRenewals(txCtx, tx, stream.workspace, connection.InstanceID, stream.connection, now)
		if err != nil {
			return err
		}
		for _, credentialID := range renewals {
			if !state.written[renewMessageID(credentialID)] {
				chosen.renewals = append(chosen.renewals, credentialID)
			}
		}
		upTo := bound
		if connection.FirstSessionReportAt == nil {
			upTo = min(upTo, instance.SessionInstructionCap)
		}
		instructions, err := instance.UnreportedInstructions(txCtx, tx, stream.workspace, stream.connection, upTo)
		if err != nil {
			return err
		}
		for _, instruction := range instructions {
			if !state.written[instruction.MessageID] {
				chosen.instructions = append(chosen.instructions, instruction)
				continue
			}
			if !state.unmarked[instruction.MessageID] {
				continue
			}
			first, queued, err := instance.MarkWritten(txCtx, tx, stream.workspace, stream.connection, instruction.MessageID)
			if err != nil {
				return err
			}
			chosen.marked = append(chosen.marked, instruction.MessageID)
			if first {
				chosen.firsts = append(chosen.firsts, firstWrite{instruction: instruction, queued: queued})
			}
		}
		return nil
	})
	cancel()
	if err != nil {
		// The failed statement is observed under the pass's operation; the
		// stream keeps its bound and runs the pass again (retryAfter).
		if all {
			stream.raiseAll()
		}
		return passFailed
	}
	for _, messageID := range chosen.marked {
		delete(state.unmarked, messageID)
	}
	for _, first := range chosen.firsts {
		s.metrics.instructionDelivered(first.queued)
		_, span := workload.StartRemoteChildSpan(ctx, "instruction.deliver", first.instruction.TraceContext)
		span.SetString("message.id", first.instruction.MessageID)
		span.SetString("connection.id", stream.connection)
		span.SetString("message.type", first.instruction.Type)
		span.End()
	}
	switch {
	case chosen.ended:
		if chosen.disconnect != "" {
			_ = state.writer.write(id.New("msg_"), protocol.DisconnectPayload{Scope: "connection", Retryable: protocol.Some(false), Code: protocol.Some(string(chosen.disconnect))})
		}
		return passEnded
	case chosen.moved:
		return passMoved
	case len(chosen.renewals) == 0 && len(chosen.instructions) == 0:
		return passWritten
	}
	defer s.watch(ctx, state)()
	for _, credentialID := range chosen.renewals {
		if state.writer.write(renewMessageID(credentialID), protocol.CredentialRenewPayload{CredentialID: credentialID}) != nil {
			return passBroken
		}
		state.written[renewMessageID(credentialID)] = true
	}
	for _, instruction := range chosen.instructions {
		if !state.beat() || state.writer.writeEnvelope(instruction.Envelope) != nil {
			return passBroken
		}
		state.written[instruction.MessageID] = true
		state.unmarked[instruction.MessageID] = true
	}
	if len(chosen.instructions) > 0 {
		return passWrote
	}
	return passWritten
}

// watch cuts the writes of state's stream once the stream is closed or a
// connection notice finds its connection ended or held by another stream,
// until the returned function stops it.
func (s *Streams) watch(ctx context.Context, state *streamState) func() {
	ctx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-ctx.Done():
				return
			case <-state.stream.done:
				state.writer.cut()
				return
			case <-state.stream.notified:
				if s.endedOrMoved(ctx, state.stream) {
					state.writer.cut()
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-finished
	}
}

// endedOrMoved reports whether stream's connection, read without a lock, has
// ended or is held by another stream.
func (s *Streams) endedOrMoved(ctx context.Context, stream *clientStream) bool {
	var connection instance.Connection
	err := s.database.WithWorkspaceReadOnlyTx(ctx, string(stream.workspace), "computer_hub.stream_watch", func(tx *dbconnect.Tx) error {
		var err error
		connection, err = instance.ReadConnection(ctx, tx, stream.workspace, stream.connection)
		return err
	})
	return err == nil && (connection.EndedAt != nil || connection.HubPod != s.podName || connection.StreamCount != stream.count)
}

// errCredentialMoved is a connection whose credential changed between a
// transaction's unlocked read of the connection and its locks; only a
// resume, which also moves the stream, changes it, so the next pass finds the
// stream moved.
var errCredentialMoved = errors.New("computer-hub: the connection's credential changed during the transaction")

// shareRows reads connectionID without a lock, then locks its instance, its
// credential and the connection FOR SHARE in that order, and returns them as
// locked.
func shareRows(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) (instance.Connection, instance.Instance, instance.Credential, error) {
	unlocked, err := instance.ReadConnection(ctx, tx, ws, connectionID)
	if err != nil {
		return instance.Connection{}, instance.Instance{}, instance.Credential{}, err
	}
	current, err := instance.ShareInstance(ctx, tx, ws, unlocked.InstanceID)
	if err != nil {
		return instance.Connection{}, instance.Instance{}, instance.Credential{}, err
	}
	credential, err := instance.ShareCredential(ctx, tx, ws, unlocked.InstanceID, unlocked.CredentialID)
	if err != nil {
		return instance.Connection{}, instance.Instance{}, instance.Credential{}, err
	}
	connection, err := instance.ShareConnection(ctx, tx, ws, connectionID)
	return connection, current, credential, err
}

// writable reports whether a pass may write on connection at now, the
// database clock: the lease is later than now, the ready is accepted, the
// instance is registered and the credential is neither revoked nor expired.
func writable(connection instance.Connection, current instance.Instance, credential instance.Credential, now time.Time) bool {
	if !connection.Live(now) || connection.ReadyAcceptedAt == nil || current.RegistrationState == instance.RegistrationDeregistered {
		return false
	}
	switch instance.CredentialState(credential, now) {
	case instance.CredentialRevoked, instance.CredentialExpired:
		return false
	}
	return true
}

// renewMessageID is the message_id of credential.renew for credentialID. A
// resumed stream writes it again under the same ID, so a CLI that handled it
// on an earlier stream of the connection recognizes it.
func renewMessageID(credentialID string) string { return "renew_" + credentialID }
