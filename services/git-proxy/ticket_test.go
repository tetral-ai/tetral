package gitproxy

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestTicketValidatorLiveAndRotatedGrace(t *testing.T) {
	now := time.Date(2026, 7, 3, 18, 0, 0, 0, time.UTC)
	liveToken, liveHash := deterministicTicket(t, 1)
	rotatedToken, rotatedHash := deterministicTicket(t, 2)
	expiredToken, expiredHash := deterministicTicket(t, 3)
	pendingToken, pendingHash := deterministicTicket(t, 6)
	rotatedAt := now.Add(-time.Minute)
	expiredAt := now.Add(-10 * time.Minute)
	store := fakeTicketStore{tickets: map[string]*gitticket.Ticket{
		string(liveHash): {
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_live",
			TicketID:    "gittkt_live",
			TokenHash:   liveHash,
			Status:      gitticket.StatusLive,
		},
		string(rotatedHash): {
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_rotated",
			TicketID:    "gittkt_rotated",
			TokenHash:   rotatedHash,
			Status:      gitticket.StatusRotated,
			RotatedAt:   &rotatedAt,
		},
		string(expiredHash): {
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_expired",
			TicketID:    "gittkt_expired",
			TokenHash:   expiredHash,
			Status:      gitticket.StatusRotated,
			RotatedAt:   &expiredAt,
		},
		string(pendingHash): {
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_pending",
			TicketID:    "gittkt_pending",
			TokenHash:   pendingHash,
			Status:      gitticket.StatusPending,
		},
	}}
	validator := TicketValidator{
		Store:         store,
		Now:           func() time.Time { return now },
		RotationGrace: 5 * time.Minute,
	}

	for _, token := range []string{liveToken, rotatedToken} {
		if _, err := validator.Validate(context.Background(), token); err != nil {
			t.Fatalf("Validate(%q): %v", token, err)
		}
	}
	for _, token := range []string{"short", pendingToken, expiredToken, deterministicMissingTicket(t)} {
		if _, err := validator.Validate(context.Background(), token); !errors.Is(err, ErrTicketUnauthorized) {
			t.Fatalf("Validate(%q) = %v; want ErrTicketUnauthorized", token, err)
		}
	}
}

func TestTicketValidatorRejectsHashMismatch(t *testing.T) {
	token, hash := deterministicTicket(t, 4)
	_, otherHash := deterministicTicket(t, 5)
	store := fakeTicketStore{tickets: map[string]*gitticket.Ticket{
		string(hash): {
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_hash_mismatch",
			TicketID:    "gittkt_hash_mismatch",
			TokenHash:   otherHash,
			Status:      gitticket.StatusLive,
		},
	}}
	validator := TicketValidator{Store: store}

	if _, err := validator.Validate(context.Background(), token); !errors.Is(err, ErrTicketUnauthorized) {
		t.Fatalf("Validate hash mismatch = %v; want ErrTicketUnauthorized", err)
	}
}

func deterministicTicket(t *testing.T, fill byte) (string, []byte) {
	t.Helper()
	token, err := gitticket.GenerateToken(bytes.NewReader(bytes.Repeat([]byte{fill}, gitticket.TokenBytes)))
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	hash, err := gitticket.HashToken(token)
	if err != nil {
		t.Fatalf("HashToken: %v", err)
	}
	return token, hash
}

func deterministicMissingTicket(t *testing.T) string {
	t.Helper()
	token, _ := deterministicTicket(t, 200)
	return token
}

type fakeTicketStore struct {
	tickets map[string]*gitticket.Ticket
}

func (s fakeTicketStore) FindByTokenHash(_ context.Context, tokenHash []byte) (*gitticket.Ticket, error) {
	if ticket, ok := s.tickets[string(tokenHash)]; ok {
		copyTicket := *ticket
		copyTicket.TokenHash = append([]byte(nil), ticket.TokenHash...)
		return &copyTicket, nil
	}
	return nil, &gitticket.NotFoundError{Message: "missing"}
}
