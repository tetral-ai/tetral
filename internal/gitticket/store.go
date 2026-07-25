package gitticket

import (
	"context"
	"database/sql"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type Ticket struct {
	WorkspaceID workspace.ID
	SessionID   string
	TicketID    string
	TokenHash   []byte
	Status      string
	CreatedAt   time.Time
	RotatedAt   *time.Time
}

type PostgreSQLGitTicketStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLStore(client *dbconnect.Client) *PostgreSQLGitTicketStore {
	return &PostgreSQLGitTicketStore{client: client}
}

func (s *PostgreSQLGitTicketStore) CreatePending(ctx context.Context, ws workspace.ID, sessionID string, ticketID string, tokenHash []byte, now time.Time) (*Ticket, error) {
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "git ticket store is required"}
	}
	if err := validateIdentity(ws, sessionID, ticketID); err != nil {
		return nil, err
	}
	if err := ValidateHash(tokenHash); err != nil {
		return nil, &ValidationError{Message: "git ticket token_hash is invalid"}
	}
	if now.IsZero() {
		now = storage.Now()
	} else {
		now = now.UTC()
	}
	var created *Ticket
	err := s.client.WithWorkspaceTx(ctx, string(ws), "gitticket.create_pending", func(tx *dbconnect.Tx) error {
		ticket, err := scanTicket(tx.QueryRow(ctx,
			`INSERT INTO session_git_tickets (
				workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at
			) VALUES ($1, $2, $3, $4, 'pending', $5, NULL)
			RETURNING workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at`,
			string(ws), sessionID, ticketID, append([]byte(nil), tokenHash...), now,
		))
		if err != nil {
			return err
		}
		created = ticket
		return nil
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func (s *PostgreSQLGitTicketStore) ActivatePending(ctx context.Context, ws workspace.ID, sessionID string, ticketID string, now time.Time) (*Ticket, error) {
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "git ticket store is required"}
	}
	if err := validateIdentity(ws, sessionID, ticketID); err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = storage.Now()
	} else {
		now = now.UTC()
	}
	var activated *Ticket
	err := s.client.WithWorkspaceTx(ctx, string(ws), "gitticket.activate_pending", func(tx *dbconnect.Tx) error {
		var status string
		if err := tx.QueryRow(ctx,
			`SELECT status
			   FROM session_git_tickets
			  WHERE workspace_id = $1 AND session_id = $2 AND ticket_id = $3
			  FOR UPDATE`, string(ws), sessionID, ticketID).Scan(&status); err != nil {
			return err
		}
		if status == StatusLive {
			var err error
			activated, err = scanTicket(tx.QueryRow(ctx,
				`SELECT workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at
				   FROM session_git_tickets
				  WHERE workspace_id = $1 AND session_id = $2 AND ticket_id = $3`, string(ws), sessionID, ticketID))
			return err
		}
		if status != StatusPending {
			return &ValidationError{Message: "git ticket is not pending"}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_git_tickets
			    SET status = 'rotated', rotated_at = $1
			  WHERE workspace_id = $2 AND session_id = $3 AND status = 'live'`,
			now, string(ws), sessionID); err != nil {
			return err
		}
		ticket, err := scanTicket(tx.QueryRow(ctx,
			`UPDATE session_git_tickets
			    SET status = 'live', rotated_at = NULL
			  WHERE workspace_id = $1 AND session_id = $2 AND ticket_id = $3 AND status = 'pending'
			RETURNING workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at`,
			string(ws), sessionID, ticketID))
		if err != nil {
			return err
		}
		activated = ticket
		return nil
	})
	return activated, err
}

func (s *PostgreSQLGitTicketStore) FindByTokenHash(ctx context.Context, tokenHash []byte) (*Ticket, error) {
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "git ticket store is required"}
	}
	if err := ValidateHash(tokenHash); err != nil {
		return nil, &ValidationError{Message: "git ticket token_hash is invalid"}
	}
	var ticket *Ticket
	err := s.client.WithTx(ctx, "gitticket.find_by_token_hash", &sql.TxOptions{ReadOnly: true}, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.git_ticket_lookup', 'true', true)"); err != nil {
			return err
		}
		found, err := scanTicket(tx.QueryRow(ctx,
			`SELECT workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at
			   FROM session_git_tickets
			  WHERE token_hash = $1`,
			append([]byte(nil), tokenHash...),
		))
		if err != nil {
			return err
		}
		ticket = found
		return nil
	})
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "git ticket not found"}
	}
	return ticket, err
}

func (s *PostgreSQLGitTicketStore) FindBySessionTokenHash(ctx context.Context, ws workspace.ID, sessionID string, tokenHash []byte) (*Ticket, error) {
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "git ticket store is required"}
	}
	if ws == "" || sessionID == "" {
		return nil, &ValidationError{Message: "git ticket scope is required"}
	}
	if err := ValidateHash(tokenHash); err != nil {
		return nil, &ValidationError{Message: "git ticket token_hash is invalid"}
	}
	var ticket *Ticket
	err := s.client.WithWorkspaceTx(ctx, string(ws), "gitticket.find_by_session_token_hash", func(tx *dbconnect.Tx) error {
		found, err := scanTicket(tx.QueryRow(ctx,
			`SELECT workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at
			   FROM session_git_tickets
			  WHERE workspace_id = $1 AND session_id = $2 AND token_hash = $3`,
			string(ws), sessionID, append([]byte(nil), tokenHash...),
		))
		if err != nil {
			return err
		}
		ticket = found
		return nil
	})
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "git ticket not found"}
	}
	return ticket, err
}

func validateIdentity(ws workspace.ID, sessionID string, ticketID string) error {
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if ticketID == "" {
		return &ValidationError{Message: "ticket_id is required"}
	}
	return nil
}

func scanTicket(row interface{ Scan(dest ...any) error }) (*Ticket, error) {
	var (
		workspaceID string
		sessionID   string
		ticketID    string
		tokenHash   []byte
		status      string
		createdAt   time.Time
		rotatedAt   sql.NullTime
	)
	if err := row.Scan(&workspaceID, &sessionID, &ticketID, &tokenHash, &status, &createdAt, &rotatedAt); err != nil {
		return nil, err
	}
	created := createdAt.UTC()
	var rotated *time.Time
	if rotatedAt.Valid {
		parsed := rotatedAt.Time.UTC()
		rotated = &parsed
	}
	return &Ticket{
		WorkspaceID: workspace.ID(workspaceID),
		SessionID:   sessionID,
		TicketID:    ticketID,
		TokenHash:   append([]byte(nil), tokenHash...),
		Status:      status,
		CreatedAt:   created,
		RotatedAt:   rotated,
	}, nil
}
