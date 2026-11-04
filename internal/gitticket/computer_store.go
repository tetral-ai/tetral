package gitticket

import (
	"context"
	"crypto/rand"
	"database/sql"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// StatusRevoked is a computer ticket an issue of a newer one replaced.
const StatusRevoked = "revoked"

// IssueComputerTicket creates a live ticket for computerID of sessionID in
// tx, revoking every earlier live ticket of that computer, and returns the
// token, of which only the SHA-256 is stored.
func IssueComputerTicket(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (string, error) {
	token, err := GenerateToken(rand.Reader)
	if err != nil {
		return "", err
	}
	hash, err := HashToken(token)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_git_tickets SET state='revoked',revoked_at=clock_timestamp()
		WHERE workspace_id=$1 AND computer_id=$2 AND state='live'`, string(ws), computerID); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO computer_git_tickets (workspace_id,session_id,computer_id,ticket_id,token_hash,state) VALUES ($1,$2,$3,$4,$5,'live')`,
		string(ws), sessionID, computerID, id.New("gitt_"), hash); err != nil {
		return "", err
	}
	return token, nil
}

// PostgreSQLComputerTicketStore finds computer tickets by hash for git-proxy.
type PostgreSQLComputerTicketStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLComputerTicketStore(client *dbconnect.Client) *PostgreSQLComputerTicketStore {
	return &PostgreSQLComputerTicketStore{client: client}
}

// FindByTokenHash reads the computer ticket whose hash is tokenHash, through
// the git_ticket_lookup policy, as a Ticket with its ComputerID.
func (s *PostgreSQLComputerTicketStore) FindByTokenHash(ctx context.Context, tokenHash []byte) (*Ticket, error) {
	if err := ValidateHash(tokenHash); err != nil {
		return nil, &ValidationError{Message: "git ticket token_hash is invalid"}
	}
	var ticket *Ticket
	err := s.client.WithTx(ctx, "gitticket.find_computer_ticket", &sql.TxOptions{ReadOnly: true}, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.git_ticket_lookup', 'true', true)"); err != nil {
			return err
		}
		var workspaceID string
		found := Ticket{}
		if err := tx.QueryRow(ctx, `SELECT workspace_id,session_id,computer_id,ticket_id,token_hash,state,created_at FROM computer_git_tickets WHERE token_hash=$1`,
			tokenHash).Scan(&workspaceID, &found.SessionID, &found.ComputerID, &found.TicketID, &found.TokenHash, &found.Status, &found.CreatedAt); err != nil {
			return err
		}
		found.WorkspaceID = workspace.ID(workspaceID)
		found.CreatedAt = found.CreatedAt.UTC()
		ticket = &found
		return nil
	})
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "git ticket not found"}
	}
	return ticket, err
}
