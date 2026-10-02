package dbconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestListenRejectsUnsupportedDriverBeforeReadiness(t *testing.T) {
	connector := &notificationUnsupportedDriver{}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	ready, notifications := 0, 0
	err := newTestClient(db).Listen(t.Context(), "notification.unsupported", "fixture_notifications", func() { ready++ }, func(string) { notifications++ })
	if err == nil || !strings.Contains(err.Error(), "notification listener requires the pgx stdlib driver") {
		t.Fatalf("unsupported driver result: %v", err)
	}
	if ready != 0 || notifications != 0 || connector.statements != 0 {
		t.Fatalf("unsupported driver announced readiness or executed LISTEN: ready=%d notifications=%d statements=%d", ready, notifications, connector.statements)
	}
}

// This driver deliberately accepts LISTEN, exposing the previous false-ready
// ordering. It has no native pgx notification reader.
type notificationUnsupportedDriver struct{ statements int }

func (d *notificationUnsupportedDriver) Connect(context.Context) (driver.Conn, error) {
	return notificationUnsupportedConnection{owner: d}, nil
}
func (d *notificationUnsupportedDriver) Driver() driver.Driver { return d }
func (d *notificationUnsupportedDriver) Open(string) (driver.Conn, error) {
	return notificationUnsupportedConnection{owner: d}, nil
}

type notificationUnsupportedConnection struct {
	owner *notificationUnsupportedDriver
}

func (notificationUnsupportedConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture does not prepare statements")
}
func (notificationUnsupportedConnection) Close() error { return nil }
func (notificationUnsupportedConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture does not begin transactions")
}
func (c notificationUnsupportedConnection) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.owner.statements++
	return driver.RowsAffected(0), nil
}
