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
// wakes it, in one transaction that reads its connection without a lock,
// then locks the instance, the connection's credential and the connection
// FOR SHARE, in the order of every writer of those rows (internal/instance),
// so a revocation, a deregistration, an activation or a resume commits
// wholly before or after the pass:
//
//  1. an ended connection writes its disconnect and ends the stream; a
//     connection whose hub_pod or stream_count names another stream ends the
//     stream without one;
//  2. it writes nothing while the lease has passed, the credential is revoked
//     or expired, or the instance is deregistered, nor before the ready is
//     accepted;
//  3. otherwise it writes credential.renew once on this stream for each
//     pending renewal of the connection (instance.PendingRenewals), under the
//     message_id renewMessageID gives, the same on every stream;
//  4. then, in issue order, each unreported instruction whose issue_seq is
//     at most the stream's bound and not yet written on this stream; until
//     the first Session report is stored only the first
//     instance.SessionInstructionCap of them.
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
}

type passOutcome int

const (
	passWritten passOutcome = iota
	passFailed
	passEnded
	passMoved
	passBroken
)

// A failed pass runs again after passRetryFirst, the wait doubling after
// each further failure up to passRetryMax and multiplied by 1+r, r uniform
// in 0 to 0.2, so that streams failing together during a database outage
// spread their retries; a pass that commits returns it to passRetryFirst.
// passDeadline bounds one pass, its transaction and its writes, so neither
// a held lock nor a reader that stops reading keeps the pass's row locks
// and pooled connection.
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

// firstWrite is an instruction this pass wrote to a stream for the first
// time on any stream.
type firstWrite struct {
	instruction instance.Instruction
	queued      time.Duration
}

// deliver runs one pass of state's stream within passDeadline.
func (s *Streams) deliver(ctx context.Context, state *streamState) passOutcome {
	ctx, cancel := context.WithTimeout(ctx, passDeadline)
	defer cancel()
	deadline, _ := ctx.Deadline()
	state.writer.until = deadline
	defer func() { state.writer.until = time.Time{} }()
	stream := state.stream
	bound, all := stream.take()
	outcome := passWritten
	var (
		firsts []firstWrite
		marked []string
	)
	err := s.database.WithWorkspaceTx(ctx, string(stream.workspace), "computer_hub.instruction_write", func(tx *dbconnect.Tx) error {
		outcome, firsts, marked = passWritten, firsts[:0], marked[:0]
		connection, current, credential, err := shareRows(ctx, tx, stream.workspace, stream.connection)
		if err != nil {
			return err
		}
		if connection.EndedAt != nil {
			if code, ok := disconnectCode(connection); ok {
				_ = state.writer.write(id.New("msg_"), protocol.DisconnectPayload{Scope: "connection", Retryable: protocol.Some(false), Code: protocol.Some(string(code))})
			}
			outcome = passEnded
			return nil
		}
		if connection.HubPod != s.podName || connection.StreamCount != stream.count {
			outcome = passMoved
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
		now, err := instance.Now(ctx, tx)
		if err != nil || !writable(connection, current, credential, now) {
			return err
		}
		renewals, err := instance.PendingRenewals(ctx, tx, stream.workspace, connection.InstanceID, stream.connection, now)
		if err != nil {
			return err
		}
		for _, credentialID := range renewals {
			messageID := renewMessageID(credentialID)
			if state.written[messageID] {
				continue
			}
			if state.writer.write(messageID, protocol.CredentialRenewPayload{CredentialID: credentialID}) != nil {
				outcome = passBroken
				return nil
			}
			state.written[messageID] = true
		}
		upTo := bound
		if connection.FirstSessionReportAt == nil {
			upTo = min(upTo, instance.SessionInstructionCap)
		}
		instructions, err := instance.UnreportedInstructions(ctx, tx, stream.workspace, stream.connection, upTo)
		if err != nil {
			return err
		}
		for _, instruction := range instructions {
			if !state.written[instruction.MessageID] {
				if state.writer.writeEnvelope(instruction.Envelope) != nil {
					outcome = passBroken
					return nil
				}
				state.written[instruction.MessageID] = true
				state.unmarked[instruction.MessageID] = true
			}
			if !state.unmarked[instruction.MessageID] {
				continue
			}
			first, queued, err := instance.MarkWritten(ctx, tx, stream.workspace, stream.connection, instruction.MessageID)
			if err != nil {
				return err
			}
			marked = append(marked, instruction.MessageID)
			if first {
				firsts = append(firsts, firstWrite{instruction: instruction, queued: queued})
			}
		}
		return nil
	})
	if err == nil {
		for _, messageID := range marked {
			delete(state.unmarked, messageID)
		}
	}
	if err != nil {
		// The failed statement is observed under the pass's operation; the
		// stream keeps its bound and runs the pass again (retryAfter).
		if all {
			stream.raiseAll()
		}
		return passFailed
	}
	for _, first := range firsts {
		s.metrics.instructionDelivered(first.queued)
		_, span := workload.StartRemoteChildSpan(ctx, "instruction.deliver", first.instruction.TraceContext)
		span.SetString("message.id", first.instruction.MessageID)
		span.SetString("connection.id", stream.connection)
		span.SetString("message.type", first.instruction.Type)
		span.End()
	}
	return outcome
}

// errCredentialMoved is a connection whose credential changed between the
// pass's unlocked read and its locks; only a resume, which also moves the
// stream, changes it, so the next pass finds the stream moved.
var errCredentialMoved = errors.New("computer-hub: the connection's credential changed during the pass")

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
