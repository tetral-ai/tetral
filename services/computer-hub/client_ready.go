package computerhub

import (
	"context"
	"net/http"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/hubflow"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
)

// readyOutcome is what a ready did: accepted or refused for the first time,
// or nothing new (a replay).
type readyOutcome int

const (
	readyReplayed readyOutcome = iota
	readyAccepted
	readyRefused
)

// ready answers POST /v1/client/ready on the live connection. A replay
// returns the stored answer; a second ready under a new message_id answers
// 400 invalid_message. A CLI older than the minimum version is refused with
// 403 cli_upgrade_required, recorded on the connection, whose lease is not
// renewed. Otherwise hubflow.AcceptReady stores the ready with a lease of the
// grant and the ACK carries the grant in whole milliseconds.
func (h *clientHandler) ready(w http.ResponseWriter, r *http.Request) {
	received := time.Now()
	ctx, id := r.Context(), identityOf(r)
	body, err := readClientBody(r)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	ready, err := protocol.DecodeReady(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	var (
		answer  clientAnswer
		outcome readyOutcome
	)
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_ready", func(tx *dbconnect.Tx) error {
		answer, outcome = clientAnswer{}, readyReplayed
		granted, err := authorize(ctx, tx, id, false, true)
		if err != nil {
			return err
		}
		connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, ready.ConnectionID, granted.now, true)
		if err != nil {
			return err
		}
		if _, found, err := replayOf(ctx, tx, connection, instance.RouteReady, ready.MessageID, fingerprint); found || err != nil {
			answer = readyAnswer(connection)
			return err
		}
		if connection.ReadyAnswered() {
			return refuse(protocol.ErrorInvalidMessage, "the connection's ready was already answered")
		}
		if instance.VersionBelow(ready.CLIVersion, h.MinCLIVersion) {
			message := "CLI " + ready.CLIVersion + " is older than the minimum supported version " + h.MinCLIVersion
			answer, outcome = errorAnswer(protocol.ErrorCLIUpgradeRequired, message), readyRefused
			return instance.RefuseReady(ctx, tx, id.workspace, connection, instance.ReadyRefusal{
				MessageID: ready.MessageID, Fingerprint: fingerprint, Code: instance.RefusedCLIUpgradeRequired, Message: message, CLIVersion: ready.CLIVersion,
			})
		}
		leaseExpiresAt, validForMS, err := grantFor(ctx, tx, connection, granted.now)
		if err != nil {
			return err
		}
		answer, outcome = grantAcknowledge(connection.ID, ready.MessageID, validForMS), readyAccepted
		return hubflow.AcceptReady(ctx, tx, id.workspace, connection, instance.ReadyAcceptance{
			MessageID: ready.MessageID, Fingerprint: fingerprint, AcceptedAt: granted.now, LeaseExpiresAt: leaseExpiresAt, ValidForMS: validForMS,
			OS: ready.OS, Arch: ready.Arch, CLIVersion: ready.CLIVersion, Capabilities: protocol.Encode(ready.Capabilities),
		})
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	if ready.Startup.Set && outcome != readyReplayed {
		recordCLIColdStart(ctx, received, ready.Startup.Value, outcome)
	}
	writeClientAnswer(w, answer)
}

// readyAnswer is the answer the connection's recorded ready got.
func readyAnswer(connection instance.Connection) clientAnswer {
	if connection.ReadyValidForMS != nil {
		return grantAcknowledge(connection.ID, *connection.ReadyMessageID, *connection.ReadyValidForMS)
	}
	return errorAnswer(protocol.ErrorCode(*connection.RefusedCode), *connection.RefusedMessage)
}

// recordCLIColdStart records the CLI cold start a first answered ready
// reports: the CLI's config, connect and ready phases and register, the hub's
// time from receiving the ready to answering it. A refused ready ends the
// cold start with the refusal's code and observes nothing.
func recordCLIColdStart(ctx context.Context, received time.Time, startup protocol.ReadyStartup, outcome readyOutcome) {
	coldStart := workload.BeginReportedColdStart(ctx, "cli", "new", received, []workload.ReportedPhase{
		{Name: "config", Duration: time.Duration(startup.ConfigMS) * time.Millisecond},
		{Name: "connect", Duration: time.Duration(startup.ConnectMS) * time.Millisecond},
		{Name: "ready", Duration: time.Duration(startup.ReadyMS) * time.Millisecond},
	}, "register")
	if outcome == readyRefused {
		coldStart.Fail(ctx, instance.RefusedCLIUpgradeRequired)
		return
	}
	coldStart.Complete(ctx)
}
