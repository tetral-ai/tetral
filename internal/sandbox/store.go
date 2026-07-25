package sandbox

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type Store interface {
	CreateSandbox(ctx context.Context, sandbox *Sandbox) error
	SaveProviderHandle(ctx context.Context, ws workspace.ID, sandboxID string, handle ProviderHandle, updatedAt time.Time) (*Sandbox, error)
	MarkActive(ctx context.Context, ws workspace.ID, sandboxID string, updatedAt time.Time) (*Sandbox, error)
	MarkStatusRefreshed(ctx context.Context, ws workspace.ID, sandboxID string, refreshedAt time.Time) (*Sandbox, error)
	RefreshSandboxState(ctx context.Context, ws workspace.ID, sandboxID string, status Status, refreshedAt time.Time) (*Sandbox, error)
	InspectStaleStartup(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time) (StaleStartupInspection, error)
	RefreshStaleStartup(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time, update StaleStartupRefreshUpdate) (*Sandbox, error)
	ClaimStaleCreating(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time, update StartupFailureUpdate) (*Sandbox, error)
	ClaimDueStartupCleanup(ctx context.Context, ws workspace.ID, sandboxID string, now time.Time, leaseDuration time.Duration, maxAttempts int) (*StartupCleanupClaim, error)
	MarkStartupCleanupAttempt(ctx context.Context, ws workspace.ID, sandboxID string, update CleanupAttemptUpdate) (*Sandbox, error)
	FindLiveBySessionID(ctx context.Context, ws workspace.ID, sessionID string) (*Sandbox, error)
	FindLatestBySessionID(ctx context.Context, ws workspace.ID, sessionID string) (*Sandbox, error)
	MarkReleasing(ctx context.Context, ws workspace.ID, sandboxID string, updatedAt time.Time) (*Sandbox, error)
	MarkArchived(ctx context.Context, ws workspace.ID, sandboxID string, archivedAt time.Time) (*Sandbox, error)
	MarkReleasedForDeleteProviderMissing(ctx context.Context, ws workspace.ID, sandboxID string, releasedAt time.Time) (*Sandbox, error)
	MarkReleased(ctx context.Context, ws workspace.ID, sandboxID string, releasedAt time.Time) (*Sandbox, error)
	MarkFailed(ctx context.Context, ws workspace.ID, sandboxID string, failedAt time.Time) (*Sandbox, error)
	MarkStartupFailed(ctx context.Context, ws workspace.ID, sandboxID string, update StartupFailureUpdate) (*Sandbox, error)
}

type StartupFailureUpdate struct {
	FailedAt              time.Time
	StartupFailureReason  string
	CleanupStatus         CleanupStatus
	CleanupMethod         string
	CleanupErrorKind      string
	CleanupRetryable      bool
	CleanupAttempted      bool
	CleanupNextAttemptAt  *time.Time
	AdoptedProviderHandle *ProviderHandle
}

type StaleStartupInspection struct {
	Sandbox        *Sandbox
	SessionDeleted bool
}

type StaleStartupRefreshUpdate struct {
	RefreshedAt           time.Time
	Status                Status
	AdoptedProviderHandle *ProviderHandle
}

type CleanupAttemptUpdate struct {
	AttemptedAt          time.Time
	CleanupLeaseToken    string
	CleanupStatus        CleanupStatus
	CleanupMethod        string
	CleanupErrorKind     string
	CleanupRetryable     bool
	CleanupNextAttemptAt *time.Time
}

type StartupCleanupClaim struct {
	Sandbox                 *Sandbox
	ProviderAttemptReserved bool
}

// PostgreSQLTransaction is the minimal committed transaction capability
// needed to persist sandbox rows inside a caller-owned PostgreSQL transaction.
type PostgreSQLTransaction interface {
	Exec(ctx context.Context, query string, args ...any) (interface {
		RowsAffected() (int64, error)
	}, error)
	QueryRowScanner(ctx context.Context, query string, args ...any) interface {
		Scan(dest ...any) error
	}
}
