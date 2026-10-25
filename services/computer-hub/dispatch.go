package computerhub

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
)

// A stream writes Session instructions in passes. A pass runs when the
// stream starts and whenever a connection notice or the listener's return
// wakes it, in one transaction that holds the connection row FOR SHARE, so a
// revocation or a resume commits wholly before or after the pass:
//
//  1. an ended connection writes its disconnect and ends the stream; a
//     connection whose hub_pod or stream_count names another stream ends the
//     stream without one;
//  2. it writes nothing while the lease has passed, the credential is revoked
//     or expired, or the instance is deregistered, nor before the ready is
//     accepted;
//  3. otherwise it writes, in issue order, each unreported instruction whose
//     issue_seq is at most the stream's bound and not yet written on this
//     stream; until the first Session report is stored only the first
//     instance.SessionInstructionCap of them.
//
// The bound is the highest issue_seq a notice released. A stream start and
// a listener return release every instruction the connection row counts, so
// an instruction never waits for a hint that was lost; an instruction issued
// later waits for its own notice.

// streamState is what one stream keeps across passes.
type streamState struct {
	stream   *clientStream
	writer   sseWriter
	written  map[string]bool
	accepted bool
}

type passOutcome int

const (
	passWritten passOutcome = iota
	passFailed
	passEnded
	passMoved
	passBroken
)

// passRetryDelay is the wait before a failed pass runs again.
const passRetryDelay = time.Second

// firstWrite is an instruction this pass wrote to a stream for the first
// time on any stream.
type firstWrite struct {
	instruction instance.Instruction
	queued      time.Duration
}

// deliver runs one pass of state's stream.
func (s *Streams) deliver(ctx context.Context, state *streamState) passOutcome {
	stream := state.stream
	bound, all := stream.take()
	outcome := passWritten
	var firsts []firstWrite
	err := s.database.WithWorkspaceTx(ctx, string(stream.workspace), "computer_hub.instruction_write", func(tx *dbconnect.Tx) error {
		outcome, firsts = passWritten, firsts[:0]
		connection, err := instance.ShareConnection(ctx, tx, stream.workspace, stream.connection)
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
		if all {
			bound = max(bound, connection.LastIssueSeq)
			stream.raise(bound)
		}
		state.accepted = state.accepted || connection.ReadyAcceptedAt != nil
		if writable, err := s.writable(ctx, tx, connection); err != nil || !writable {
			return err
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
			if state.written[instruction.MessageID] {
				continue
			}
			if state.writer.writeEnvelope(instruction.Envelope) != nil {
				outcome = passBroken
				return nil
			}
			state.written[instruction.MessageID] = true
			first, queued, err := instance.MarkWritten(ctx, tx, stream.workspace, stream.connection, instruction.MessageID)
			if err != nil {
				return err
			}
			if first {
				firsts = append(firsts, firstWrite{instruction: instruction, queued: queued})
			}
		}
		return nil
	})
	if err != nil {
		// The failed statement is observed under the pass's operation; the
		// stream keeps its bound and runs the pass again after
		// passRetryDelay.
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

// writable reports whether a pass may write Session instructions on
// connection: its lease is later than the database clock, its credential is
// neither revoked nor expired, its instance is registered and its ready is
// accepted. It reads the credential and the instance FOR SHARE.
func (s *Streams) writable(ctx context.Context, tx *dbconnect.Tx, connection instance.Connection) (bool, error) {
	now, err := instance.Now(ctx, tx)
	if err != nil || !connection.Live(now) || connection.ReadyAcceptedAt == nil {
		return false, err
	}
	current, err := instance.ShareInstance(ctx, tx, connection.WorkspaceID, connection.InstanceID)
	if err != nil || current.RegistrationState == instance.RegistrationDeregistered {
		return false, err
	}
	credential, err := instance.ShareCredential(ctx, tx, connection.WorkspaceID, connection.InstanceID, connection.CredentialID)
	if err != nil {
		return false, err
	}
	switch instance.CredentialState(credential, now) {
	case instance.CredentialRevoked, instance.CredentialExpired:
		return false, nil
	}
	return true, nil
}
