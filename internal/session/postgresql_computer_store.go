package session

import (
	"context"
	"errors"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/instance"
)

func (t *postgresqlTransaction) CheckComputerEntries(ctx context.Context, sessionID string, entries []computer.Entry) ([]computer.Checked, error) {
	tx, err := t.rawDBTx()
	if err != nil {
		return nil, err
	}
	return computer.CheckEntries(ctx, tx, t.workspaceID, sessionID, entries)
}

func (t *postgresqlTransaction) AddComputers(ctx context.Context, sessionID string, checked []computer.Checked) ([]string, error) {
	tx, err := t.rawDBTx()
	if err != nil {
		return nil, err
	}
	return computer.AddComputers(ctx, tx, t.workspaceID, sessionID, checked)
}

func (t *postgresqlTransaction) GetComputer(ctx context.Context, sessionID string, computerID string) (computer.Object, error) {
	tx, err := t.rawDBTx()
	if err != nil {
		return computer.Object{}, err
	}
	object, err := computer.Get(ctx, tx, t.workspaceID, sessionID, computerID)
	if errors.Is(err, computer.ErrNotFound) {
		return computer.Object{}, &NotFoundError{Message: "computer not found"}
	}
	return object, err
}

func (t *postgresqlTransaction) FindComputerIdempotencyRecord(ctx context.Context, scope instance.IdempotencyScope) (computer.IdempotencyRecord, bool, error) {
	tx, err := t.rawDBTx()
	if err != nil {
		return computer.IdempotencyRecord{}, false, err
	}
	return computer.FindIdempotencyRecord(ctx, tx, t.workspaceID, scope)
}

func (t *postgresqlTransaction) SaveComputerIdempotencyRecord(ctx context.Context, scope instance.IdempotencyScope, record computer.IdempotencyRecord) error {
	tx, err := t.rawDBTx()
	if err != nil {
		return err
	}
	return computer.SaveIdempotencyRecord(ctx, tx, t.workspaceID, scope, record)
}

// attachComputers fills the attached computers of sessions with one read.
func (t *postgresqlTransaction) attachComputers(ctx context.Context, sessions []*Session) error {
	tx, err := t.rawDBTx()
	if err != nil {
		return err
	}
	ids := make([]string, len(sessions))
	for i, sess := range sessions {
		ids[i] = sess.ID
	}
	listed, err := computer.ListAttached(ctx, tx, t.workspaceID, ids)
	if err != nil {
		return err
	}
	for _, sess := range sessions {
		sess.Computers = listed[sess.ID]
	}
	return nil
}
