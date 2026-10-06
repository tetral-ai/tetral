package integration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSubagentOutputCaptureObserverStopsAtQueryBoundary(t *testing.T) {
	queryFailure := errors.New("capture query failed")
	stageFailure := errors.New("capture staging failed")
	for _, test := range []struct {
		name       string
		capture    bool
		queryError error
		stageError error
		cancel     bool
	}{
		{name: "no_capture"},
		{name: "query_error", queryError: queryFailure},
		{name: "capture_staged", capture: true},
		{name: "staging_error", capture: true, stageError: stageFailure},
		{name: "cancelled_write_error", cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseQuery := func() { releaseOnce.Do(func() { close(release) }) }
			writeTimeout := fmt.Errorf("write failed: %w", &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded})
			var polls, stages int
			query := func(ctx context.Context, statement string) (driver.Rows, error) {
				if strings.Contains(statement, "SELECT finish_idle_write_id") {
					polls++
					close(entered)
					select {
					case <-ctx.Done():
						return nil, writeTimeout
					case <-release:
					}
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if test.queryError != nil {
						return nil, test.queryError
					}
					rows := &captureObserverRows{columns: []string{"finish_idle_write_id", "capture_generation"}}
					if test.capture {
						rows.values = []driver.Value{"rwrite_observer_capture", int64(1)}
					}
					return rows, nil
				}
				if strings.Contains(statement, "SELECT state") {
					return &captureObserverRows{columns: []string{"state"}, values: []driver.Value{"pending"}}, nil
				}
				return nil, fmt.Errorf("unexpected capture query: %s", statement)
			}
			connection := &captureObserverConnection{query: query, exec: func(statement string) (driver.Result, error) {
				if !strings.Contains(statement, "SET state='staged'") {
					return nil, fmt.Errorf("unexpected capture update: %s", statement)
				}
				stages++
				return driver.RowsAffected(1), test.stageError
			}}
			db := sql.OpenDB(captureObserverConnector{connection: connection})
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close observer database: %v", err)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			stop := make(chan struct{})
			type result struct {
				settled bool
				err     error
			}
			finished := make(chan result, 1)
			done := make(chan struct{})
			// Release and join before the borrowed database closes, including Fatal exits.
			t.Cleanup(func() {
				cancel()
				releaseQuery()
				<-done
			})
			go func() {
				defer close(done)
				settled, err := settleNextSubagentOutputCaptureForTest(ctx, db, "sesn_observer_capture", stop)
				finished <- result{settled: settled, err: err}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("observer did not enter its capture query")
			}
			if test.cancel {
				cancel()
			} else {
				close(stop)
				if err := ctx.Err(); err != nil {
					t.Fatalf("graceful stop canceled the in-flight query: %v", err)
				}
				releaseQuery()
			}
			got := <-finished
			<-done
			wantError := test.queryError
			if test.stageError != nil {
				wantError = test.stageError
			}
			if test.cancel {
				wantError = writeTimeout
			}
			if !errors.Is(got.err, wantError) || got.settled != test.capture {
				t.Fatalf("observer = settled:%t err:%v; want settled:%t err:%v", got.settled, got.err, test.capture, wantError)
			}
			wantStages := 0
			if test.capture {
				wantStages = 1
			}
			if polls != 1 || stages != wantStages {
				t.Fatalf("observer polls/stages = %d/%d; want 1/%d", polls, stages, wantStages)
			}
		})
	}
}

// The scripted SQL driver controls only the fixture observer's query boundary;
// the existing PostgreSQL/Bun callers prove real child-close and capture custody.
type captureObserverConnector struct{ connection *captureObserverConnection }

func (c captureObserverConnector) Connect(context.Context) (driver.Conn, error) {
	return c.connection, nil
}
func (c captureObserverConnector) Driver() driver.Driver { return captureObserverDriver{} }

type captureObserverDriver struct{}

func (captureObserverDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("observer test requires its connector")
}

type captureObserverConnection struct {
	query func(context.Context, string) (driver.Rows, error)
	exec  func(string) (driver.Result, error)
}

func (*captureObserverConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("observer test uses direct queries")
}
func (*captureObserverConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("observer test has no transaction")
}
func (*captureObserverConnection) Close() error { return nil }
func (c *captureObserverConnection) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	return c.query(ctx, query)
}
func (c *captureObserverConnection) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	return c.exec(query)
}

type captureObserverRows struct {
	columns []string
	values  []driver.Value
}

func (r *captureObserverRows) Columns() []string { return r.columns }
func (*captureObserverRows) Close() error        { return nil }
func (r *captureObserverRows) Next(values []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	copy(values, r.values)
	r.values = nil
	return nil
}
