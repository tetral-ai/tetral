package dbconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/storage"
)

type Client struct {
	db         *sql.DB
	provider   Provider
	descriptor Descriptor

	closeOnce sync.Once
	closeErr  error
}

var notificationChannelPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func newClient(db *sql.DB, provider Provider, descriptor Descriptor) *Client {
	return &Client{db: db, provider: provider, descriptor: descriptor}
}

// NewClientForTesting wraps an already-open test database in a Runtime
// Client without changing ownership of the underlying database handle.
func NewClientForTesting(db *sql.DB) *Client {
	return newClient(db, ProviderPlainDSN, Descriptor{Host: "<testing>", Database: "<testing>"})
}

type Row struct {
	row    *sql.Row
	client *Client
	op     string
	err    error
}

func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	err := r.row.Scan(dest...)
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if r.client == nil {
		return err
	}
	return r.client.classifyRuntimeError(r.op, err)
}

type Rows struct {
	rows   *sql.Rows
	client *Client
	op     string
}

func (r *Rows) Next() bool {
	return r.rows.Next()
}

func (r *Rows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *Rows) Err() error {
	err := r.rows.Err()
	if err == nil {
		return nil
	}
	if r.client == nil {
		return err
	}
	return r.client.classifyRuntimeError(r.op, err)
}

func (r *Rows) Close() error {
	return r.rows.Close()
}

type Tx struct {
	tx        *sql.Tx
	client    *Client
	operation string
}

// NewTxForTesting wraps an existing test transaction so legacy tests
// can exercise Runtime Client transaction callbacks while controlling
// commit and rollback themselves.
func NewTxForTesting(sqlTx *sql.Tx, client *Client, operation string) *Tx {
	return &Tx{tx: sqlTx, client: client, operation: operation}
}

func (tx *Tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := tx.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, tx.client.classifyRuntimeError(tx.operation, err)
	}
	return result, nil
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.Exec(ctx, query, args...)
}

func (tx *Tx) Query(ctx context.Context, query string, args ...any) (*Rows, error) {
	rows, err := tx.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, tx.client.classifyRuntimeError(tx.operation, err)
	}
	return &Rows{rows: rows, client: tx.client, op: tx.operation}, nil
}

func (tx *Tx) QueryRows(ctx context.Context, query string, args ...any) (interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}, error) {
	return tx.Query(ctx, query, args...)
}

func (tx *Tx) QueryRow(ctx context.Context, query string, args ...any) *Row {
	return &Row{row: tx.tx.QueryRowContext(ctx, query, args...), client: tx.client, op: tx.operation}
}

func (tx *Tx) QueryRowScanner(ctx context.Context, query string, args ...any) interface {
	Scan(dest ...any) error
} {
	return tx.QueryRow(ctx, query, args...)
}

func (c *Client) InitializeSchema(ctx context.Context) error {
	if err := storage.InitializePostgreSQLSchema(ctx, c.db); err != nil {
		return diagnostic(c.provider, c.descriptor, PhaseInitializeSchema, KindSchemaInitializationFailed, "", "schema initialization failed", err)
	}
	return nil
}

func (c *Client) MigrateSchema(ctx context.Context) error {
	if err := storage.MigrateSchema(ctx, c.db); err != nil {
		return diagnostic(c.provider, c.descriptor, PhaseMigrateSchema, KindSchemaMigrationFailed, "", "schema migration failed", err)
	}
	return nil
}

func (c *Client) VerifySchema(ctx context.Context) error {
	if err := storage.VerifySchema(ctx, c.db); err != nil {
		return diagnostic(c.provider, c.descriptor, PhaseVerifySchema, KindSchemaVerificationFailed, "", "schema verification failed", err)
	}
	return nil
}

func (c *Client) VerifyRuntimeRole(ctx context.Context) error {
	if err := storage.VerifyRuntimeRole(ctx, c.db); err != nil {
		var roleErr *storage.RuntimeRoleError
		if errors.As(err, &roleErr) {
			return diagnostic(c.provider, c.descriptor, PhaseVerifyRuntimeRole, KindRuntimeRoleInvalid, "", "runtime role invalid", err)
		}
		return err
	}
	return nil
}

func (c *Client) Exec(ctx context.Context, operation string, query string, args ...any) (sql.Result, error) {
	if err := c.validateOperation(operation); err != nil {
		return nil, err
	}
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, c.classifyRuntimeError(operation, err)
	}
	return result, nil
}

func (c *Client) Query(ctx context.Context, operation string, query string, args ...any) (*Rows, error) {
	if err := c.validateOperation(operation); err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, c.classifyRuntimeError(operation, err)
	}
	return &Rows{rows: rows, client: c, op: operation}, nil
}

func (c *Client) QueryRow(ctx context.Context, operation string, query string, args ...any) *Row {
	if err := c.validateOperation(operation); err != nil {
		return &Row{err: err}
	}
	return &Row{row: c.db.QueryRowContext(ctx, query, args...), client: c, op: operation}
}

// Listen holds one dedicated PostgreSQL connection until ctx is cancelled or
// the connection fails. Notifications are hints only; callers must re-read
// their durable source after onReady and onNotification.
func (c *Client) Listen(ctx context.Context, operation string, channel string, onReady func(), onNotification func(string)) error {
	if err := c.validateOperation(operation); err != nil {
		return err
	}
	if !notificationChannelPattern.MatchString(channel) {
		return errors.New("dbconnect: invalid notification channel")
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "LISTEN "+channel); err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	if onReady != nil {
		onReady()
	}
	err = conn.Raw(func(raw any) error {
		stdlibConn, ok := raw.(*stdlib.Conn)
		if !ok {
			return errors.New("dbconnect: notification listener requires the pgx stdlib driver")
		}
		for {
			notification, waitErr := stdlibConn.Conn().WaitForNotification(ctx)
			if waitErr != nil {
				return waitErr
			}
			if onNotification != nil {
				onNotification(notification.Payload)
			}
		}
	})
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return c.classifyRuntimeError(operation, err)
}

func (c *Client) WithTx(ctx context.Context, operation string, opts *sql.TxOptions, fn func(*Tx) error) error {
	if err := c.validateOperation(operation); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, opts)
	if err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	wrapped := &Tx{tx: tx, client: c, operation: operation}
	if err := fn(wrapped); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	committed = true
	return nil
}

// WithAdvisoryLock runs fn while holding a PostgreSQL advisory lock on a
// dedicated connection. The lock is outside any SQL transaction so callers can
// serialize multi-step workflows without holding a transaction across remote IO.
func (c *Client) WithAdvisoryLock(ctx context.Context, operation string, category int32, resource int32, fn func() error) error {
	if err := c.validateOperation(operation); err != nil {
		return err
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1, $2)", category, resource); err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	unlocked := false
	defer func() {
		if !unlocked {
			_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1, $2)", category, resource)
		}
	}()
	if err := fn(); err != nil {
		return err
	}
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1, $2)", category, resource); err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	unlocked = true
	return nil
}

// TryWithAdvisoryLock runs fn while holding a PostgreSQL advisory lock on a
// dedicated connection when the lock is immediately available. It returns
// false without running fn when another connection already holds the lock.
func (c *Client) TryWithAdvisoryLock(ctx context.Context, operation string, category int32, resource int32, fn func() error) (bool, error) {
	if err := c.validateOperation(operation); err != nil {
		return false, err
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return false, c.classifyRuntimeError(operation, err)
	}
	defer func() { _ = conn.Close() }()
	var locked bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1, $2)", category, resource).Scan(&locked); err != nil {
		return false, c.classifyRuntimeError(operation, err)
	}
	if !locked {
		return false, nil
	}
	unlocked := false
	defer func() {
		if !unlocked {
			_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1, $2)", category, resource)
		}
	}()
	if err := fn(); err != nil {
		return true, err
	}
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1, $2)", category, resource); err != nil {
		return true, c.classifyRuntimeError(operation, err)
	}
	unlocked = true
	return true, nil
}

func (c *Client) WithWorkspaceTx(ctx context.Context, workspaceID string, operation string, fn func(*Tx) error) error {
	return c.withWorkspaceTx(ctx, workspaceID, operation, nil, fn, nil)
}

func (c *Client) WithWorkspaceReadOnlyTx(ctx context.Context, workspaceID string, operation string, fn func(*Tx) error) error {
	return c.withWorkspaceTx(ctx, workspaceID, operation, &sql.TxOptions{ReadOnly: true}, fn, nil)
}

func (c *Client) WithWorkspaceTxAndCleanup(ctx context.Context, workspaceID string, operation string, fn func(*Tx) error, onCommitFailure func()) error {
	return c.withWorkspaceTx(ctx, workspaceID, operation, nil, fn, onCommitFailure)
}

func (c *Client) withWorkspaceTx(ctx context.Context, workspaceID string, operation string, opts *sql.TxOptions, fn func(*Tx) error, onCommitFailure func()) error {
	if workspaceID == "" {
		return errors.New("dbconnect: WithWorkspaceTx requires a non-empty workspace_id")
	}
	if err := c.validateOperation(operation); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, opts)
	if err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, "SELECT set_config('tetral.workspace_id', $1, true)", workspaceID); err != nil {
		return c.classifyRuntimeError(operation, err)
	}
	wrapped := &Tx{tx: tx, client: c, operation: operation}
	if err := fn(wrapped); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		if onCommitFailure != nil {
			onCommitFailure()
		}
		return c.classifyRuntimeError(operation, err)
	}
	committed = true
	return nil
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.db.PingContext(ctx); err != nil {
		return diagnostic(c.provider, c.descriptor, PhasePing, classifyOpenOrPingKind(err), "", "ping failed", err)
	}
	return nil
}

func (c *Client) Stats() sql.DBStats {
	if c == nil || c.db == nil {
		return sql.DBStats{}
	}
	return c.db.Stats()
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.db.Close()
	})
	return c.closeErr
}

func (c *Client) validateOperation(operation string) error {
	if err := validateOperationLabel(operation); err != nil {
		return diagnostic(c.provider, c.descriptor, PhaseRuntimeQuery, KindInternalError, "", "invalid operation label", err)
	}
	return nil
}

func (c *Client) classifyRuntimeError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return diagnostic(c.provider, c.descriptor, PhaseRuntimeQuery, KindCanceled, operation, "runtime query canceled", err)
	case errors.Is(err, context.DeadlineExceeded):
		return diagnostic(c.provider, c.descriptor, PhaseRuntimeQuery, KindTimeout, operation, "runtime query timed out", err)
	case errors.Is(err, driver.ErrBadConn):
		return diagnostic(c.provider, c.descriptor, PhaseRuntimeQuery, KindEndpointUnreachable, operation, "runtime connection failed", err)
	default:
		return err
	}
}
