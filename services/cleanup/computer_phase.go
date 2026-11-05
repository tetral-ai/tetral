package tetralcleanup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/hubflow"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	computerPhaseBudget   = 45 * time.Second
	computerPhasePageSize = 256

	// Causes of cleanup_controller_requests_expired_total and
	// controller_requests.expired.
	RequestCauseClaimExpired   = "claim_expired"
	RequestCausePendingExpired = "pending_expired"
)

// cleanupActor is the audit actor of the credentials the computer phase
// revokes: Engine itself, through Cleanup.
var cleanupActor = instance.Actor{CredentialKind: "engine", CredentialID: ServiceName}

// ComputerPhase ends the connections whose lease passed and times out
// Controller requests: a claimed request its instance has not fulfilled
// within ClaimTimeout of its latest claim returns to pending, and a request
// pending for PendingExpiry expires. It finds due rows across Workspaces
// through Cleanup's two discovery functions and handles each in its own
// Workspace transaction, whose writer locks the row and rechecks it, so a
// lease renewed or a request claimed or fulfilled after discovery is left
// alone. It takes pages of 256 until none is due, a page fails, or its 45 s
// context ends.
type ComputerPhase struct {
	Client        *dbconnect.Client
	ClaimTimeout  time.Duration
	PendingExpiry time.Duration
	Logger        *slog.Logger
}

// ComputerResult counts what one run of the phase ended.
type ComputerResult struct {
	ConnectionsEnded int
	ClaimsExpired    int
	PendingExpired   int
}

type dueRow struct{ workspaceID, id string }

// Run runs the phase once. The cutoffs are the database clock at its start:
// that time for leases, minus ClaimTimeout for claims and minus
// PendingExpiry for pending requests.
func (p ComputerPhase) Run(ctx context.Context) (ComputerResult, error) {
	ctx, cancel := context.WithTimeout(ctx, computerPhaseBudget)
	defer cancel()
	var result ComputerResult
	var now time.Time
	if err := p.Client.QueryRow(ctx, "tetralcleanup.computer_phase_clock", `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return result, fmt.Errorf("read the computer phase clock: %w", err)
	}
	err := p.endExpiredConnections(ctx, now, &result)
	err = errors.Join(err, p.timeOutRequests(ctx, now.Add(-p.ClaimTimeout), now.Add(-p.PendingExpiry), &result))
	if result.ClaimsExpired > 0 || result.PendingExpired > 0 {
		p.logger().InfoContext(ctx, "controller_requests.expired",
			slog.String("operation", "cleanup.computers"),
			slog.String("component", ServiceName),
			slog.Int("controller_requests.claim_expired", result.ClaimsExpired),
			slog.Int("controller_requests.pending_expired", result.PendingExpired))
	}
	return result, err
}

func (p ComputerPhase) endExpiredConnections(ctx context.Context, cutoff time.Time, result *ComputerResult) error {
	return p.pages(ctx, "tetralcleanup.discover_expired_connections",
		`SELECT workspace_id, connection_id FROM public.tetral_cleanup_expired_connections($1, $2)`, []any{cutoff},
		func(row dueRow) error {
			ended := false
			err := p.Client.WithWorkspaceTx(ctx, row.workspaceID, "tetralcleanup.end_expired_connection", func(tx *dbconnect.Tx) error {
				var err error
				ended, err = hubflow.EndExpiredConnection(ctx, tx, workspace.ID(row.workspaceID), row.id)
				return err
			})
			if err == nil && ended {
				result.ConnectionsEnded++
			}
			return err
		})
}

func (p ComputerPhase) timeOutRequests(ctx context.Context, claimedBefore, pendingBefore time.Time, result *ComputerResult) error {
	return p.pages(ctx, "tetralcleanup.discover_due_requests",
		`SELECT workspace_id, request_id FROM public.tetral_cleanup_due_requests($1, $2, $3)`, []any{claimedBefore, pendingBefore},
		func(row dueRow) error {
			var (
				returned           bool
				revoked, revokedOf string
				ended              computer.Ended
			)
			err := p.Client.WithWorkspaceTx(ctx, row.workspaceID, "tetralcleanup.time_out_request", func(tx *dbconnect.Tx) error {
				ws := workspace.ID(row.workspaceID)
				_, locks, err := hubflow.LockRequest(ctx, tx, ws, row.id)
				if err != nil {
					return err
				}
				if revoked, returned, err = hubflow.ReturnTimedOutClaim(ctx, tx, ws, row.id, claimedBefore, locks); err != nil {
					return err
				}
				if !returned {
					if ended, err = computer.Expire(ctx, tx, ws, row.id, pendingBefore); err != nil {
						return err
					}
					revoked = ended.Revoked
				}
				if revoked == "" {
					return nil
				}
				credential, err := instance.ReadCredential(ctx, tx, ws, revoked)
				revokedOf = credential.InstanceID
				return err
			})
			if err != nil {
				return err
			}
			switch {
			case returned:
				result.ClaimsExpired++
			case !ended.At.IsZero():
				result.PendingExpired++
				computer.RecordRequestEnded(ctx, ended)
			}
			if revoked != "" {
				instance.AuditCredentialOperation(ctx, p.logger(), cleanupActor, revokedOf, revoked, instance.AuditRevoke)
			}
			return nil
		})
}

// pages runs handle on every row discovery returns, a page of 256 at a time,
// until a page is short, a page has failures or ctx ends. Handled rows are
// no longer due, so each page starts from the oldest still due.
func (p ComputerPhase) pages(ctx context.Context, operation, query string, args []any, handle func(dueRow) error) error {
	for ctx.Err() == nil {
		var page []dueRow
		err := p.Client.WithTx(ctx, operation, &sql.TxOptions{ReadOnly: true}, func(tx *dbconnect.Tx) error {
			page = page[:0]
			rows, err := tx.Query(ctx, query, append(args, computerPhasePageSize)...)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var row dueRow
				if err := rows.Scan(&row.workspaceID, &row.id); err != nil {
					return err
				}
				page = append(page, row)
			}
			return rows.Err()
		})
		if err != nil {
			return err
		}
		var errs []error
		for _, row := range page {
			if ctx.Err() != nil {
				break
			}
			errs = append(errs, handle(row))
		}
		if err := errors.Join(errs...); err != nil || len(page) < computerPhasePageSize {
			return err
		}
	}
	return ctx.Err()
}

func (p ComputerPhase) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}
