package computer

import (
	"context"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// AdmissionRouting is runtimecontrol.ComputerRouting for one admission
// transaction. It keeps the request it recorded and the preparations it
// issued, whose cold-start phases the caller records once the transaction
// commits; a retried transaction starts from a new value.
type AdmissionRouting struct {
	Created *Request
	Issued  []Binding
}

func (r *AdmissionRouting) EnsureRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (string, bool, error) {
	request, fresh, err := EnsureRequest(ctx, tx, ws, sessionID, computerID)
	if err != nil {
		return "", false, err
	}
	if fresh {
		r.Created = &request
	}
	return request.ID, fresh, nil
}

func (r *AdmissionRouting) IssuePreparation(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (bool, error) {
	binding, err := IssuePreparation(ctx, tx, ws, sessionID, computerID)
	return r.issued(binding), err
}

func (r *AdmissionRouting) PrepareAgain(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (bool, error) {
	binding, err := prepareAgain(ctx, tx, ws, sessionID, computerID)
	return r.issued(binding), err
}

func (r *AdmissionRouting) issued(binding *Binding) bool {
	if binding == nil {
		return false
	}
	r.Issued = append(r.Issued, *binding)
	return true
}
