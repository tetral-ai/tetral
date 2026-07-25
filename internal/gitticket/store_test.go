package gitticket

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLStoreActivatesPendingOnlyAfterExternalInstall(t *testing.T) {
	ctx := context.Background()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedTicketSession(t, adminDB, workspace.DefaultID, "sesn_git_ticket")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))

	token1, hash1 := testTokenAndHash(t, 1)
	token2, hash2 := testTokenAndHash(t, 2)
	now := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)

	first, err := store.CreatePending(ctx, workspace.DefaultID, "sesn_git_ticket", "gittkt_1", hash1, now)
	if err != nil {
		t.Fatalf("CreatePending first: %v", err)
	}
	if first.Status != StatusPending || first.RotatedAt != nil || !bytes.Equal(first.TokenHash, hash1) {
		t.Fatalf("first ticket = %+v; want pending hash1 without rotated_at", first)
	}
	if _, err := store.ActivatePending(ctx, workspace.DefaultID, "sesn_git_ticket", first.TicketID, now); err != nil {
		t.Fatalf("ActivatePending first: %v", err)
	}
	second, err := store.CreatePending(ctx, workspace.DefaultID, "sesn_git_ticket", "gittkt_2", hash2, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("CreatePending second: %v", err)
	}
	stillLive, err := store.FindByTokenHash(ctx, hash1)
	if err != nil || stillLive.Status != StatusLive {
		t.Fatalf("first ticket before activation = %+v err=%v; want live", stillLive, err)
	}
	if _, err := store.ActivatePending(ctx, workspace.DefaultID, "sesn_git_ticket", second.TicketID, now.Add(time.Minute)); err != nil {
		t.Fatalf("ActivatePending second: %v", err)
	}

	rotated, err := store.FindByTokenHash(ctx, hash1)
	if err != nil {
		t.Fatalf("FindByTokenHash first: %v", err)
	}
	if rotated.Status != StatusRotated || rotated.RotatedAt == nil {
		t.Fatalf("rotated ticket = %+v; want rotated with rotated_at", rotated)
	}
	live, err := store.FindByTokenHash(ctx, hash2)
	if err != nil {
		t.Fatalf("FindByTokenHash second: %v", err)
	}
	if live.Status != StatusLive || live.RotatedAt != nil || live.SessionID != "sesn_git_ticket" {
		t.Fatalf("live ticket = %+v; want live session ticket", live)
	}

	var liveCount int
	if err := adminDB.QueryRowContext(ctx,
		`SELECT count(*) FROM session_git_tickets WHERE workspace_id = $1 AND session_id = $2 AND status = 'live'`,
		string(workspace.DefaultID), "sesn_git_ticket",
	).Scan(&liveCount); err != nil {
		t.Fatalf("count live tickets: %v", err)
	}
	if liveCount != 1 {
		t.Fatalf("live ticket count = %d; want 1", liveCount)
	}

	var persisted string
	if err := adminDB.QueryRowContext(ctx,
		`SELECT string_agg(ticket_id || ':' || encode(token_hash, 'hex'), ',' ORDER BY ticket_id)
		   FROM session_git_tickets
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspace.DefaultID), "sesn_git_ticket",
	).Scan(&persisted); err != nil {
		t.Fatalf("read persisted tickets: %v", err)
	}
	if strings.Contains(persisted, token1) || strings.Contains(persisted, token2) {
		t.Fatalf("persisted ticket rows contain a raw token: %s", persisted)
	}
}

func TestPostgreSQLStoreRejectsInvalidHashAndMissingTicket(t *testing.T) {
	ctx := context.Background()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedTicketSession(t, adminDB, workspace.DefaultID, "sesn_git_ticket")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))

	if _, err := store.CreatePending(ctx, workspace.DefaultID, "sesn_git_ticket", "gittkt_bad", []byte("short"), time.Now()); err == nil {
		t.Fatal("CreatePending accepted an invalid token hash")
	}
	if _, err := store.FindByTokenHash(ctx, bytes.Repeat([]byte{9}, TokenBytes)); err == nil {
		t.Fatal("FindByTokenHash found missing ticket")
	} else {
		var notFound *NotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("FindByTokenHash err = %T %v; want NotFoundError", err, err)
		}
	}
}

func TestPostgreSQLStoreActivatePendingRetryRejectsWrongTicket(t *testing.T) {
	ctx := context.Background()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedTicketSession(t, adminDB, workspace.DefaultID, "sesn_git_retry")
	seedTicketSession(t, adminDB, workspace.DefaultID, "sesn_git_other")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	now := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)

	_, firstHash := testTokenAndHash(t, 10)
	first, err := store.CreatePending(ctx, workspace.DefaultID, "sesn_git_retry", "gittkt_retry_first", firstHash, now)
	if err != nil {
		t.Fatalf("CreatePending first: %v", err)
	}
	if _, err := store.ActivatePending(ctx, workspace.DefaultID, "sesn_git_retry", first.TicketID, now); err != nil {
		t.Fatalf("ActivatePending first: %v", err)
	}
	_, secondHash := testTokenAndHash(t, 11)
	second, err := store.CreatePending(ctx, workspace.DefaultID, "sesn_git_retry", "gittkt_retry_second", secondHash, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("CreatePending second: %v", err)
	}
	if _, err := store.ActivatePending(ctx, workspace.DefaultID, "sesn_git_retry", second.TicketID, now.Add(time.Minute)); err != nil {
		t.Fatalf("ActivatePending second: %v", err)
	}
	if live, err := store.ActivatePending(ctx, workspace.DefaultID, "sesn_git_retry", second.TicketID, now.Add(2*time.Minute)); err != nil || live.Status != StatusLive {
		t.Fatalf("idempotent ActivatePending exact live = %+v err=%v; want live", live, err)
	}

	_, otherHash := testTokenAndHash(t, 12)
	other, err := store.CreatePending(ctx, workspace.DefaultID, "sesn_git_other", "gittkt_retry_other", otherHash, now)
	if err != nil {
		t.Fatalf("CreatePending other session: %v", err)
	}
	for name, scopedTicket := range map[string][2]string{
		"rotated":   {"sesn_git_retry", first.TicketID},
		"missing":   {"sesn_git_retry", "gittkt_retry_missing"},
		"different": {"sesn_git_retry", other.TicketID},
		"malformed": {"sesn_git_retry", ""},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := store.ActivatePending(ctx, workspace.DefaultID, scopedTicket[0], scopedTicket[1], now.Add(3*time.Minute)); err == nil {
				t.Fatal("ActivatePending accepted a non-exact retry ticket")
			}
		})
	}
	if _, err := store.FindBySessionTokenHash(ctx, workspace.DefaultID, "sesn_git_retry", otherHash); err == nil {
		t.Fatal("FindBySessionTokenHash crossed the session boundary")
	} else {
		var notFound *NotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("cross-session lookup err = %T %v; want NotFoundError", err, err)
		}
	}
}

func TestPostgreSQLSessionGitTicketsSchemaConstraints(t *testing.T) {
	ctx := context.Background()
	_, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedTicketSession(t, adminDB, workspace.DefaultID, "sesn_git_ticket")
	now := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	hash1 := bytes.Repeat([]byte{1}, TokenBytes)
	hash2 := bytes.Repeat([]byte{2}, TokenBytes)
	hash3 := bytes.Repeat([]byte{3}, TokenBytes)

	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO session_git_tickets (
			workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at
		) VALUES ($1, $2, 'gittkt_live', $3, 'live', $4, NULL)`,
		string(workspace.DefaultID), "sesn_git_ticket", hash1, now,
	); err != nil {
		t.Fatalf("insert live ticket: %v", err)
	}
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO session_git_tickets (
			workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at
		) VALUES ($1, $2, 'gittkt_second_live', $3, 'live', $4, NULL)`,
		string(workspace.DefaultID), "sesn_git_ticket", hash2, now,
	); err == nil {
		t.Fatal("schema accepted a second live ticket for the same session")
	}
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO session_git_tickets (
			workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at
		) VALUES ($1, $2, 'gittkt_short_hash', $3, 'rotated', $4, $4)`,
		string(workspace.DefaultID), "sesn_git_ticket", []byte("short"), now,
	); err == nil {
		t.Fatal("schema accepted a non-32-byte token_hash")
	}
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO session_git_tickets (
			workspace_id, session_id, ticket_id, token_hash, status, created_at, rotated_at
		) VALUES ($1, $2, 'gittkt_rotated_without_time', $3, 'rotated', $4, NULL)`,
		string(workspace.DefaultID), "sesn_git_ticket", hash3, now,
	); err == nil {
		t.Fatal("schema accepted a rotated ticket without rotated_at")
	}
}

func testTokenAndHash(t *testing.T, fill byte) (string, []byte) {
	t.Helper()
	token, err := GenerateToken(bytes.NewReader(bytes.Repeat([]byte{fill}, TokenBytes)))
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	hash, err := HashToken(token)
	if err != nil {
		t.Fatalf("HashToken: %v", err)
	}
	return token, hash
}

func seedTicketSession(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 7, 3, 9, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agents (workspace_id, id, name, created_at, updated_at)
		 VALUES ($1, 'agent_git_ticket', 'Git Ticket Agent', $2, $2)
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		string(ws), now,
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, 'agv_git_ticket_1', 'agent_git_ticket', 1, '{}', 'hash_git_ticket', $2)
		 ON CONFLICT (workspace_id, agent_id, version) DO NOTHING`,
		string(ws), now,
	); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, 'env_git_ticket', 'Git Ticket Env', '{}', $2, $2)
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		string(ws), now,
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sessions (
			workspace_id, id, type, metadata_json, status, lifecycle_state,
			agent_id, agent_version, environment_id, vault_ids_json,
			created_at, updated_at
		) VALUES (
			$1, $2, 'session', '{}', 'idle', 'admitted',
			'agent_git_ticket', 1, 'env_git_ticket', '[]',
			$3, $3
		)`,
		string(ws), sessionID, now,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}
