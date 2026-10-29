package drive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	// MinUploadCount is the least a request counts against the upload budget,
	// and so the least per-minute amount a service may be configured with.
	MinUploadCount = 65_536
	// DefaultUploadBudgetBytesPerMinute is the per-minute amount when a
	// service's setting is unset.
	DefaultUploadBudgetBytesPerMinute = 500_000_000
)

// UploadBudgetPoint is the pressure point of upload budget refusals; its one
// cause is budget.
const UploadBudgetPoint = "drive_upload_budget"

// UploadBudget is every Workspace's budget of upload bytes per minute. Its
// state is in drive_upload_budgets, so every process that takes from it draws
// from one budget per Workspace.
type UploadBudget struct {
	db        *dbconnect.Client
	perMinute int64
	pressure  *workload.PressurePoint
	clock     func() time.Time
}

// NewUploadBudget registers the drive_upload_budget pressure point on
// registry.
func NewUploadBudget(db *dbconnect.Client, perMinute int64, registry *workload.Registry) (*UploadBudget, error) {
	if perMinute < MinUploadCount {
		return nil, fmt.Errorf("drive: upload budget of %d bytes per minute is below %d", perMinute, MinUploadCount)
	}
	return &UploadBudget{db: db, perMinute: perMinute, pressure: workload.NewPressurePoint(registry, UploadBudgetPoint, "budget")}, nil
}

// WithClock replaces the database clock, and the pressure point's, for tests.
func (b *UploadBudget) WithClock(clock func() time.Time) *UploadBudget {
	b.clock = clock
	b.pressure.WithClock(clock)
	return b
}

// UploadRefusedError is a refused take. It unwraps to the pressure point's
// Rejection, so an observation that ends with it counts outcome rejected.
type UploadRefusedError struct {
	RetryAfterSeconds int
	rejection         error
}

func (e *UploadRefusedError) Error() string {
	return fmt.Sprintf("drive: upload budget refused the request; retry after %d seconds", e.RetryAfterSeconds)
}

func (e *UploadRefusedError) Unwrap() error { return e.rejection }

// Take counts a request with the given Content-Length, or -1 when it has none,
// and takes that count from the Workspace's budget, or returns
// UploadRefusedError and takes nothing. A taken count is never given back.
func (b *UploadBudget) Take(ctx context.Context, ws workspace.ID, contentLength int64) error {
	count := b.perMinute
	if contentLength >= 0 {
		count = max(MinUploadCount, min(contentLength, b.perMinute))
	}
	var now *time.Time
	if b.clock != nil {
		at := b.clock()
		now = &at
	}
	retryAfter, err := b.take(ctx, ws, count, now)
	if err != nil {
		return err
	}
	if retryAfter == 0 {
		b.pressure.Admit(ctx)
		return nil
	}
	return &UploadRefusedError{RetryAfterSeconds: retryAfter, rejection: b.pressure.Reject(ctx, "budget")}
}

// available is the budget at $4, or at the database clock when $4 is null: the
// stored tokens plus the refill since refilled_at, a negative elapsed time
// counting as none, and never more than the per-minute amount $3.
const available = `least($3::numeric, b.tokens + greatest(0, extract(epoch FROM coalesce($4::timestamptz, clock_timestamp()) - b.refilled_at)) * $3::numeric / 60)`

// take returns 0 when it took count, or the whole seconds, from 1 to 60, until
// the budget holds count. A Workspace's first take finds no row and a full
// budget; a later one locks the row through the conflict, so concurrent takes
// of one Workspace are decided one after the other.
func (b *UploadBudget) take(ctx context.Context, ws workspace.ID, count int64, now *time.Time) (int, error) {
	retryAfter := 0
	err := b.db.WithWorkspaceTx(ctx, string(ws), "drive.upload_budget_take", func(tx *dbconnect.Tx) error {
		var tokens string
		err := tx.QueryRow(ctx, `INSERT INTO drive_upload_budgets AS b (workspace_id,tokens,refilled_at) VALUES ($1,$3::numeric-$2,coalesce($4::timestamptz,clock_timestamp()))
			ON CONFLICT (workspace_id) DO UPDATE SET tokens=`+available+`-$2,refilled_at=coalesce($4::timestamptz,clock_timestamp())
			WHERE `+available+`>=$2 RETURNING tokens`, string(ws), count, b.perMinute, now).Scan(&tokens)
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// The conflict left the row locked, so this reads the state the
		// refusal was decided on.
		return tx.QueryRow(ctx, `SELECT greatest(1, ceil(($2 - `+available+`) * 60 / $3::numeric))::int FROM drive_upload_budgets b WHERE workspace_id=$1`,
			string(ws), count, b.perMinute, now).Scan(&retryAfter)
	})
	return retryAfter, err
}
