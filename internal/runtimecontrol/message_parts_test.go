package runtimecontrol_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func requireSQLState(t *testing.T, name string, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("%s = %v; want SQLSTATE %s", name, err, code)
	}
}

// Both serving roles that append assistant parts can insert them through the
// shared append primitive but can never update or delete a stored part. Rows
// of another workspace are invisible and cannot be inserted, and the foreign
// key binds a part to its message's exact Session, Thread and parts mode.
func TestPostgreSQLAssistantPartsAreAppendOnlyForServingRoles(t *testing.T) {
	for _, workload := range []string{"bridge", "job_runner"} {
		t.Run(workload, func(t *testing.T) {
			ctx := context.Background()
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			roles := storagetest.OpenWorkloadDB(t, admin, workload)
			client := dbconnect.NewClientForTesting(roles.DB)
			const (
				sessionID   = "sesn_parts_roles"
				threadID    = "thr_parts_roles"
				otherThread = "thr_parts_roles_other"
				messageID   = "msg_parts_roles"
			)
			sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
			sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, otherThread)
			sessionfixture.SeedBridgeAPISession(t, admin, "ws_parts_other", "sesn_parts_other", "thr_parts_other")
			if _, err := admin.ExecContext(ctx, `INSERT INTO session_messages (
				workspace_id,session_id,session_thread_id,message_id,sequence,kind,data_json,created_at,updated_at
			) VALUES ('default',$1,$2,'msg_parts_roles_user',1,'user','{"parts":[{"type":"text","text":"hi"}]}',now(),now())`, sessionID, threadID); err != nil {
				t.Fatalf("seed embedded message: %v", err)
			}
			sessionfixture.SeedAssistantMessagePartsForTest(t, admin, "default", sessionID, threadID, messageID, 2, nil, "mreq_parts_roles",
				`{"type":"text","text":"seeded"}`)
			sessionfixture.SeedAssistantMessagePartsForTest(t, admin, "ws_parts_other", "sesn_parts_other", "thr_parts_other", "msg_parts_other", 1, nil, "mreq_parts_other",
				`{"type":"text","text":"other workspace"}`)

			scope := &bridgev1.RuntimeScope{WorkspaceId: "default", SessionId: sessionID, SessionThreadId: threadID}
			if err := client.WithWorkspaceTx(ctx, "default", "runtimecontrol.test_parts_append", func(tx *dbconnect.Tx) error {
				header, found, err := runtimecontrol.LockAssistantMessageHeaderTx(ctx, tx, scope, "mreq_parts_roles")
				if err != nil || !found {
					t.Fatalf("lock header = %v/%v", found, err)
				}
				_, err = runtimecontrol.AppendAssistantMessagePartsTx(ctx, tx, runtimecontrol.AssistantPartsAppend{
					Scope: scope, ModelRequestID: "mreq_parts_roles", Header: &header, Now: time.Now().UTC(),
					Parts: []runtimecontrol.AssistantPart{{Value: map[string]any{
						"type": "tool_call", "modelToolCallId": "call_parts_roles", "toolName": "Read", "canonicalInput": map[string]any{},
					}}},
				})
				return err
			}); err != nil {
				t.Fatalf("%s append: %v", workload, err)
			}
			var index, next int64
			var kind, callID string
			if err := admin.QueryRowContext(ctx, `SELECT part.part_index, part.part_kind, part.model_tool_call_id, message.next_part_index
				FROM session_message_parts part JOIN session_messages message
				  ON message.workspace_id=part.workspace_id AND message.message_id=part.message_id
				WHERE part.workspace_id='default' AND part.message_id=$1 AND part.part_kind='tool_call'`, messageID).Scan(&index, &kind, &callID, &next); err != nil {
				t.Fatalf("read appended part: %v", err)
			}
			if index != 1 || kind != "tool_call" || callID != "call_parts_roles" || next != 2 {
				t.Fatalf("appended part index/kind/call/next = %d/%s/%s/%d; want 1/tool_call/call_parts_roles/2", index, kind, callID, next)
			}

			statement := func(name string, query string, args ...any) error {
				return client.WithWorkspaceTx(ctx, "default", "runtimecontrol.test_parts_"+name, func(tx *dbconnect.Tx) error {
					_, err := tx.Exec(ctx, query, args...)
					return err
				})
			}
			requireSQLState(t, "UPDATE part", statement("update",
				`UPDATE session_message_parts SET data_json = data_json WHERE workspace_id='default' AND message_id=$1`, messageID), "42501")
			requireSQLState(t, "DELETE part", statement("delete",
				`DELETE FROM session_message_parts WHERE workspace_id='default' AND message_id=$1`, messageID), "42501")
			insert := `INSERT INTO session_message_parts (
				workspace_id, session_id, session_thread_id, message_id, part_index, part_kind, data_json
			) VALUES ($1, $2, $3, $4, $5, 'text', '{"type":"text","text":"x"}')`
			requireSQLState(t, "INSERT into another workspace", statement("foreign_workspace",
				insert, "ws_parts_other", "sesn_parts_other", "thr_parts_other", "msg_parts_other", 1), "42501")
			requireSQLState(t, "INSERT under another Thread", statement("wrong_thread",
				insert, "default", sessionID, otherThread, messageID, 2), "23503")
			requireSQLState(t, "INSERT under an embedded message", statement("embedded_parent",
				insert, "default", sessionID, threadID, "msg_parts_roles_user", 0), "23503")
			var visible int
			if err := client.WithWorkspaceTx(ctx, "default", "runtimecontrol.test_parts_visibility", func(tx *dbconnect.Tx) error {
				return tx.QueryRow(ctx, `SELECT count(*) FROM session_message_parts WHERE workspace_id='ws_parts_other'`).Scan(&visible)
			}); err != nil || visible != 0 {
				t.Fatalf("other workspace parts visible = %d/%v; want 0", visible, err)
			}
		})
	}
}
