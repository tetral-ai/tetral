package gitproxy

import (
	"context"
	"crypto/subtle"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/gitticket"
)

const (
	DefaultDrainGraceSeconds          = 1800
	DefaultTicketRotationGraceSeconds = DefaultDrainGraceSeconds
)

var ErrTicketUnauthorized = errors.New("git capability ticket is unauthorized")

type TicketStore interface {
	FindByTokenHash(context.Context, []byte) (*gitticket.Ticket, error)
}

type TicketValidator struct {
	Store         TicketStore
	Now           func() time.Time
	RotationGrace time.Duration
}

func (v TicketValidator) Validate(ctx context.Context, token string) (*gitticket.Ticket, error) {
	if err := gitticket.ValidateToken(token); err != nil {
		return nil, ErrTicketUnauthorized
	}
	hash, err := gitticket.HashToken(token)
	if err != nil {
		return nil, ErrTicketUnauthorized
	}
	if v.Store == nil {
		return nil, ErrTicketUnauthorized
	}
	row, err := v.Store.FindByTokenHash(ctx, hash)
	if err != nil {
		return nil, ErrTicketUnauthorized
	}
	if subtle.ConstantTimeCompare(row.TokenHash, hash) != 1 {
		return nil, ErrTicketUnauthorized
	}
	switch row.Status {
	case gitticket.StatusLive:
		return row, nil
	case gitticket.StatusRotated:
		if row.RotatedAt != nil && !v.now().After(row.RotatedAt.Add(v.rotationGrace())) {
			return row, nil
		}
		return nil, ErrTicketUnauthorized
	default:
		return nil, ErrTicketUnauthorized
	}
}

func (v TicketValidator) now() time.Time {
	if v.Now != nil {
		return v.Now().UTC()
	}
	return storage.Now()
}

func (v TicketValidator) rotationGrace() time.Duration {
	if v.RotationGrace > 0 {
		return v.RotationGrace
	}
	return DefaultTicketRotationGraceSeconds * time.Second
}
