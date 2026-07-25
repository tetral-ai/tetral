package session

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type Store interface {
	WithWorkspaceTx(ctx context.Context, ws workspace.ID, fn func(Transaction) error) error
	WithWorkspaceTxAndCleanup(ctx context.Context, ws workspace.ID, fn func(Transaction) error, onCommitFailure func()) error
	WithRuntimeMutationLock(ctx context.Context, ws workspace.ID, sessionID string, fn func() error) error
	Get(ctx context.Context, ws workspace.ID, sessionID string) (*Session, error)
	List(ctx context.Context, ws workspace.ID, options ListOptions) (*StoreListResult, error)
	ListSessionProviderAuth(ctx context.Context, ws workspace.ID, sessionID string) (ProviderSelectors, error)
	ListThreads(ctx context.Context, ws workspace.ID, sessionID string, options ThreadListOptions) (*StoreThreadListResult, error)
	GetThread(ctx context.Context, ws workspace.ID, sessionID string, threadID string) (*Thread, error)
	ArchiveThread(ctx context.Context, ws workspace.ID, sessionID string, threadID string, archivedAt time.Time) (*Thread, error)
	ListResources(ctx context.Context, ws workspace.ID, sessionID string, options ResourceListOptions) (*StoreResourceListResult, error)
}

type Transaction interface {
	Exec(ctx context.Context, query string, args ...any) (ExecResult, error)
	QueryRows(ctx context.Context, query string, args ...any) (QueryRows, error)
	QueryRowScanner(ctx context.Context, query string, args ...any) RowScanner

	CreateSession(ctx context.Context, sess *Session) error
	CreatePrimaryThread(ctx context.Context, thread *Thread) error
	CreateSessionPreparation(ctx context.Context, preparation SessionPreparationAdmission) error
	GetSession(ctx context.Context, sessionID string) (*Session, error)
	LockSession(ctx context.Context, sessionID string) (*Session, error)
	LockSessionForDelete(ctx context.Context, sessionID string) (*Session, error)
	ListSessions(ctx context.Context, options ListOptions) ([]*Session, bool, error)
	RequireSessionUsableForMutation(ctx context.Context, sessionID string) error
	PrepareSessionResourceMutation(ctx context.Context, sessionID string, now time.Time) error
	UpdateSession(ctx context.Context, sessionID string, update UpdateSession) (*Session, error)
	GetProviderCredentialForAdmission(ctx context.Context, credentialID string, boundVaultIDs []string) (*ProviderCredentialForAdmission, error)
	UpsertSessionProviderAuth(ctx context.Context, selector SessionProviderAuthAdmission) error
	ArchiveSession(ctx context.Context, sessionID string, archivedAt time.Time) (*Session, error)
	DeleteSession(ctx context.Context, sessionID string) error

	CreateResource(ctx context.Context, resource *Resource) error
	ListResources(ctx context.Context, sessionID string, options ResourceListOptions) ([]*Resource, bool, error)
	GetResource(ctx context.Context, sessionID string, resourceID string) (*Resource, error)
	UpdateGitHubRepositoryToken(ctx context.Context, sessionID string, resourceID string, encryptedToken []byte, updatedAt time.Time) (*Resource, error)
	RequestResourceDelete(ctx context.Context, sessionID string, resourceID string, requestedAt time.Time) (*Resource, error)
	DetachResource(ctx context.Context, sessionID string, resourceID string, detachedAt time.Time) (*Resource, error)
	ReattachResource(ctx context.Context, sessionID string, resourceID string, updatedAt time.Time) (*Resource, error)
}

type ExecResult interface {
	RowsAffected() (int64, error)
}

type QueryRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type RowScanner = interface {
	Scan(dest ...any) error
}

type txExecutor interface {
	Exec(ctx context.Context, query string, args ...any) (ExecResult, error)
	QueryRows(ctx context.Context, query string, args ...any) (QueryRows, error)
	QueryRowScanner(ctx context.Context, query string, args ...any) RowScanner
}

type ListOrder string

const (
	ListOrderAscending  ListOrder = "asc"
	ListOrderDescending ListOrder = "desc"
)

type ListOptions struct {
	Limit           int
	Page            string
	IncludeArchived bool
	AgentID         string
	AgentVersion    int
	MemoryStoreID   string
	DeploymentID    string
	Statuses        []Status
	CreatedAtGT     *time.Time
	CreatedAtGTE    *time.Time
	CreatedAtLT     *time.Time
	CreatedAtLTE    *time.Time
	Order           ListOrder

	cursorCreatedAt time.Time
	cursorID        string
}

type StoreListResult struct {
	Data     []*Session
	NextPage *string
}

type ResourceListOptions struct {
	Limit int
	Page  string

	cursorSequence int64
	cursorID       string
}

type StoreResourceListResult struct {
	Data     []*Resource
	NextPage *string
}

type ThreadListOptions struct {
	Limit int
	Page  string

	cursorSequence int64
	cursorID       string
}

type StoreThreadListResult struct {
	Data     []*Thread
	NextPage *string
}

type UpdateSession struct {
	Title              *string
	TitlePresent       bool
	MetadataPatch      map[string]*string
	MetadataPresent    bool
	ApprovalMode       *ApprovalMode
	RuntimeAgentConfig *RuntimeAgentConfig
	UpdatedAt          time.Time
}

type SessionProviderAuthAdmission struct {
	SessionID    string
	ProviderID   string
	VaultID      string
	CredentialID string
	AccessMode   string
	UpdatedAt    time.Time
}

type ProviderCredentialForAdmission struct {
	ID         string
	VaultID    string
	AuthType   string
	ProviderID string
	AccessMode string
	Archived   bool
	Revoked    bool
}

type SessionPreparationAdmission struct {
	SessionID            string
	EnvironmentID        string
	PreparationAttemptID string
	SandboxID            string
	CreatedAt            time.Time
}
