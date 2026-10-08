package dbconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestWithAdvisoryLockUnlockIgnoresRequestCancellationAfterSuccessfulCallback(t *testing.T) {
	state := newAdvisoryLockFakeState()
	db := openAdvisoryLockFakeDB(t, state)
	client := NewClientForTesting(db)
	ctx, cancel := context.WithCancel(context.Background())

	err := client.WithAdvisoryLock(ctx, "session.runtime_mutation_lock", 7, 42, func() error {
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("WithAdvisoryLock: %v", err)
	}
	if state.lockCalls != 1 || state.unlockCalls != 1 {
		t.Fatalf("lock/unlock calls = %d/%d; want 1/1", state.lockCalls, state.unlockCalls)
	}
	if state.unlockSawCanceledContext {
		t.Fatal("explicit advisory unlock used the canceled request context")
	}
}

func TestWithAdvisoryLockReturnsUnlockDatabaseFailure(t *testing.T) {
	state := newAdvisoryLockFakeState()
	state.unlockErr = errors.New("unlock failed")
	db := openAdvisoryLockFakeDB(t, state)
	client := NewClientForTesting(db)

	err := client.WithAdvisoryLock(context.Background(), "session.runtime_mutation_lock", 7, 42, func() error {
		return nil
	})
	if !errors.Is(err, state.unlockErr) {
		t.Fatalf("WithAdvisoryLock err = %T %v; want unlockErr", err, err)
	}
	if state.unlockCalls != 2 {
		t.Fatalf("unlock calls = %d; want explicit unlock plus deferred cleanup attempt", state.unlockCalls)
	}
}

func TestTryWithAdvisoryLockSkipsCallbackWhenUnavailable(t *testing.T) {
	state := newAdvisoryLockFakeState()
	state.tryLockResult = false
	db := openAdvisoryLockFakeDB(t, state)
	client := NewClientForTesting(db)
	called := false

	locked, err := client.TryWithAdvisoryLock(context.Background(), "session.runtime_mutation_lock", 7, 42, func() error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("TryWithAdvisoryLock: %v", err)
	}
	if locked || called {
		t.Fatalf("locked/called = %v/%v; want false/false", locked, called)
	}
	if state.tryLockCalls != 1 || state.unlockCalls != 0 {
		t.Fatalf("tryLock/unlock calls = %d/%d; want 1/0", state.tryLockCalls, state.unlockCalls)
	}
}

func TestTryWithAdvisoryLockUnlockIgnoresRequestCancellationAfterSuccessfulCallback(t *testing.T) {
	state := newAdvisoryLockFakeState()
	state.tryLockResult = true
	db := openAdvisoryLockFakeDB(t, state)
	client := NewClientForTesting(db)
	ctx, cancel := context.WithCancel(context.Background())

	locked, err := client.TryWithAdvisoryLock(ctx, "session.runtime_mutation_lock", 7, 42, func() error {
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("TryWithAdvisoryLock: %v", err)
	}
	if !locked {
		t.Fatal("TryWithAdvisoryLock locked = false; want true")
	}
	if state.tryLockCalls != 1 || state.unlockCalls != 1 {
		t.Fatalf("tryLock/unlock calls = %d/%d; want 1/1", state.tryLockCalls, state.unlockCalls)
	}
	if state.unlockSawCanceledContext {
		t.Fatal("explicit advisory unlock used the canceled request context")
	}
}

func TestTryWithSessionLockClosesConnectionWhenUnlockFails(t *testing.T) {
	for _, test := range []struct {
		name      string
		unlockErr error
		closes    int
		opens     int
	}{
		{name: "unlocked connection returns to the pool", closes: 0, opens: 1},
		{name: "failed unlock closes the physical connection", unlockErr: errors.New("unlock failed"), closes: 1, opens: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := newAdvisoryLockFakeState()
			state.tryLockResult = true
			state.sessionUnlockErr = test.unlockErr
			db := openAdvisoryLockFakeDB(t, state)
			db.SetMaxIdleConns(1)
			client := NewClientForTesting(db)
			called := false

			locked, err := client.TryWithSessionLock(context.Background(), "cleanup.schedule_election", "tetral.cleanup.scheduler", func(*SessionLockConn) error {
				called = true
				return nil
			})
			if !locked || !called || !errors.Is(err, test.unlockErr) || (test.unlockErr == nil) != (err == nil) {
				t.Fatalf("locked/called/err = %t/%t/%v; want true/true/%v", locked, called, err, test.unlockErr)
			}
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.sessionUnlockCalls != 1 || state.closes != test.closes || state.opens != test.opens {
				t.Fatalf("unlock/closes/opens = %d/%d/%d; want 1/%d/%d", state.sessionUnlockCalls, state.closes, state.opens, test.closes, test.opens)
			}
		})
	}
}

const advisoryLockFakeDriverName = "tetral_advisory_lock_fake"

var advisoryLockFakeDriverOnce sync.Once

type advisoryLockFakeState struct {
	mu                       sync.Mutex
	lockCalls                int
	tryLockCalls             int
	unlockCalls              int
	tryLockResult            bool
	unlockSawCanceledContext bool
	unlockErr                error
	sessionUnlockCalls       int
	sessionUnlockErr         error
	opens                    int
	closes                   int
}

func newAdvisoryLockFakeState() *advisoryLockFakeState {
	return &advisoryLockFakeState{}
}

func openAdvisoryLockFakeDB(t *testing.T, state *advisoryLockFakeState) *sql.DB {
	t.Helper()
	advisoryLockFakeDriverOnce.Do(func() {
		sql.Register(advisoryLockFakeDriverName, advisoryLockFakeDriver{})
	})
	key := advisoryLockFakeRegisterState(state)
	db, err := sql.Open(advisoryLockFakeDriverName, key)
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() {
		advisoryLockFakeDeleteState(key)
		_ = db.Close()
	})
	return db
}

var advisoryLockFakeStates = struct {
	sync.Mutex
	next int
	data map[string]*advisoryLockFakeState
}{data: map[string]*advisoryLockFakeState{}}

func advisoryLockFakeRegisterState(state *advisoryLockFakeState) string {
	advisoryLockFakeStates.Lock()
	defer advisoryLockFakeStates.Unlock()
	advisoryLockFakeStates.next++
	key := strings.Builder{}
	key.WriteString("state-")
	key.WriteString(strconv.Itoa(advisoryLockFakeStates.next))
	name := key.String()
	advisoryLockFakeStates.data[name] = state
	return name
}

func advisoryLockFakeDeleteState(key string) {
	advisoryLockFakeStates.Lock()
	defer advisoryLockFakeStates.Unlock()
	delete(advisoryLockFakeStates.data, key)
}

type advisoryLockFakeDriver struct{}

func (advisoryLockFakeDriver) Open(name string) (driver.Conn, error) {
	advisoryLockFakeStates.Lock()
	state := advisoryLockFakeStates.data[name]
	advisoryLockFakeStates.Unlock()
	if state == nil {
		return nil, errors.New("missing fake state")
	}
	state.mu.Lock()
	state.opens++
	state.mu.Unlock()
	return advisoryLockFakeConn{state: state}, nil
}

type advisoryLockFakeConn struct {
	state *advisoryLockFakeState
}

func (c advisoryLockFakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented")
}

func (c advisoryLockFakeConn) Close() error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.closes++
	return nil
}

func (c advisoryLockFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not implemented")
}

func (c advisoryLockFakeConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	switch query {
	case "SELECT pg_advisory_lock($1, $2)":
		c.state.lockCalls++
		return driver.RowsAffected(1), ctx.Err()
	case "SELECT pg_advisory_unlock($1, $2)":
		c.state.unlockCalls++
		if ctx.Err() != nil {
			c.state.unlockSawCanceledContext = true
			return nil, ctx.Err()
		}
		if c.state.unlockErr != nil {
			return nil, c.state.unlockErr
		}
		return driver.RowsAffected(1), nil
	default:
		return nil, errors.New("unexpected query")
	}
}

func (c advisoryLockFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if query == "SELECT pg_catalog.pg_advisory_unlock(pg_catalog.hashtextextended($1, 0))" {
		c.state.sessionUnlockCalls++
		if c.state.sessionUnlockErr != nil {
			return nil, c.state.sessionUnlockErr
		}
		return &advisoryLockFakeRows{values: []driver.Value{true}}, nil
	}
	c.state.tryLockCalls++
	return &advisoryLockFakeRows{values: []driver.Value{c.state.tryLockResult}}, nil
}

type advisoryLockFakeRows struct {
	values []driver.Value
	read   bool
}

func (advisoryLockFakeRows) Columns() []string { return []string{"pg_try_advisory_lock"} }
func (advisoryLockFakeRows) Close() error      { return nil }
func (r *advisoryLockFakeRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	copy(dest, r.values)
	return nil
}
