package eventstream_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func seedRequestEvent(t *testing.T, db *sql.DB, sessionID, threadID, eventID string, sequence int64, eventType, modelID, payload, kind string, visible bool) {
	t.Helper()
	seedEventStreamEvent(t, db, "default", sessionID, threadID, eventID, sequence, eventType, payload, "public", visible, "2026-10-01T00:00:00Z")
	projection := `{}`
	if kind != "" {
		encoded, err := json.Marshal(map[string]string{"request_kind": kind})
		if err != nil {
			t.Fatal(err)
		}
		projection = string(encoded)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE session_events SET model_request_id=NULLIF($2,''),projection_json=$3 WHERE event_id=$1`, eventID, modelID, projection); err != nil {
		t.Fatal(err)
	}
	seedEventStreamChange(t, db, "default", sessionID, threadID, eventID, 1, "public", visible)
}

func TestPostgreSQLRequestFinalMessagesAndPreviewAdmission(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_final", "thr_final")
	seedRequestEvent(t, admin, "sesn_final", "thr_final", "evt_start", 1, "span.model_request_start", "mreq_final", `{}`, "agent_provider_request", true)
	payload := `{"content":[{"type":"text","text":"alpha βeta omega\n"}]}`
	seedRequestEvent(t, admin, "sesn_final", "thr_final", "evt_message_one", 2, "agent.message", "mreq_final", payload, "", true)
	seedRequestEvent(t, admin, "sesn_final", "thr_final", "evt_permission", 3, "agent.tool_use", "mreq_final", `{"name":"Write","input":{},"evaluated_permission":"ask"}`, "", true)
	seedRequestEvent(t, admin, "sesn_final", "thr_final", "evt_message_two", 4, "agent.message", "mreq_final", `{"content":[{"type":"text","text":"second\n"}]}`, "", true)
	runtime := storagetest.OpenWorkloadDB(t, admin, "event_stream").DB
	reader := newPostgreSQLEventReader(runtime)
	scope := eventstream.ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_final"}
	descriptor, err := reader.ReadPreviewRequest(t.Context(), workspace.DefaultID, "sesn_final", "thr_final", "mreq_final", "evt_start")
	if err != nil || descriptor.StartStreamPosition <= 0 || !descriptor.IsPrimaryThread || descriptor.ThreadRole != "main" || descriptor.ThreadVisibility != "public" || descriptor.RequestKind != "agent_provider_request" || descriptor.Ended {
		t.Fatalf("descriptor=%+v err=%v", descriptor, err)
	}
	for _, threadScope := range []string{"", "thr_final"} {
		t.Run("before_end_"+threadScope, func(t *testing.T) {
			var changes []eventstream.StreamChange
			var err error
			if threadScope == "" {
				changes, err = reader.ListSessionEventChanges(t.Context(), workspace.DefaultID, "sesn_final", 0, 100)
			} else {
				changes, err = reader.ListThreadEventChanges(t.Context(), workspace.DefaultID, "sesn_final", threadScope, 0, 100)
			}
			if err != nil || len(changes) != 4 {
				t.Fatalf("changes=%d err=%v", len(changes), err)
			}
			for _, change := range changes {
				if change.Event.Type == "agent.message" {
					if !change.DeferredMessage || len(change.Event.Payload) != 0 || change.ModelRequestID != "mreq_final" || change.RequestStartEventID != "evt_start" || change.RequestStartStreamPosition != descriptor.StartStreamPosition {
						t.Fatalf("bodied/uncorrelated descriptor=%+v", change)
					}
				} else if len(change.Event.Payload) == 0 {
					t.Fatal("ordinary formal body was removed")
				}
			}
			scoped := scope
			scoped.ThreadID = threadScope
			if _, err := reader.ListRequestFinalMessages(t.Context(), scoped, "evt_not_committed_end", 0, 1); err == nil {
				t.Fatal("uncommitted End expanded")
			}
		})
	}
	history, err := reader.ListSessionEvents(t.Context(), workspace.DefaultID, "sesn_final", eventstream.ListOptions{Limit: 10, Order: "asc"})
	if err != nil || len(history.Data) != 4 || string(history.Data[1].Payload) != payload {
		t.Fatalf("committed history changed: %v", err)
	}
	seedRequestEvent(t, admin, "sesn_final", "thr_final", "evt_end", 5, "span.model_request_end", "mreq_final", `{"model_request_start_id":"evt_start","is_error":true}`, "", true)
	seedRequestEvent(t, admin, "sesn_final", "thr_final", "evt_suffix", 6, "session.status_idle", "", `{"stop_reason":"end_turn"}`, "", true)
	for _, threadScope := range []string{"", "thr_final"} {
		scoped := scope
		scoped.ThreadID = threadScope
		first, err := reader.ListRequestFinalMessages(t.Context(), scoped, "evt_end", 0, 1)
		if err != nil || len(first) != 1 || first[0].Event.ID != "evt_message_one" || first[0].Sequence != 2 || string(first[0].Event.Payload) != payload {
			t.Fatalf("first page=%+v err=%v", first, err)
		}
		second, err := reader.ListRequestFinalMessages(t.Context(), scoped, "evt_end", first[0].Sequence, 1)
		if err != nil || len(second) != 1 || second[0].Event.ID != "evt_message_two" || second[0].Sequence != 4 {
			t.Fatalf("second page=%+v err=%v", second, err)
		}
		last, err := reader.ListRequestFinalMessages(t.Context(), scoped, "evt_end", second[0].Sequence, 1)
		if err != nil || len(last) != 0 {
			t.Fatalf("last page=%+v err=%v", last, err)
		}
	}
	descriptor, err = reader.ReadPreviewRequest(t.Context(), workspace.DefaultID, "sesn_final", "thr_final", "mreq_final", "evt_start")
	if err != nil || !descriptor.Ended {
		t.Fatalf("durable End not observed: %+v %v", descriptor, err)
	}
	for name, args := range map[string][]string{"wrong_request": {"thr_final", "mreq_wrong", "evt_start"}, "wrong_start": {"thr_final", "mreq_final", "evt_message_one"}, "wrong_thread": {"thr_child", "mreq_final", "evt_start"}} {
		t.Run(name, func(t *testing.T) {
			if _, err := reader.ReadPreviewRequest(t.Context(), workspace.DefaultID, "sesn_final", args[0], args[1], args[2]); err == nil {
				t.Fatal("unrelated identity admitted")
			}
		})
	}
	for _, badScope := range []eventstream.ReadScope{{WorkspaceID: "foreign", SessionID: "sesn_final"}, {WorkspaceID: workspace.DefaultID, SessionID: "sesn_other"}, {WorkspaceID: workspace.DefaultID, SessionID: "sesn_final", ThreadID: "thr_child"}} {
		if _, err := reader.ListRequestFinalMessages(t.Context(), badScope, "evt_end", 0, 1); err == nil {
			t.Fatalf("cross-scope End admitted %+v", badScope)
		}
	}
	if _, err := reader.ListRequestFinalMessages(t.Context(), scope, "evt_permission", 0, 1); err == nil {
		t.Fatal("non-End expanded")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := reader.ListRequestFinalMessages(canceled, scope, "evt_end", 0, 1); err == nil {
		t.Fatal("canceled page query succeeded")
	}
	seedRequestEvent(t, admin, "sesn_final", "thr_child", "evt_child_start", 1, "span.model_request_start", "mreq_child", `{}`, "agent_provider_request", false)
	seedRequestEvent(t, admin, "sesn_final", "thr_child", "evt_child_message", 2, "agent.message", "mreq_child", `{"content":[{"type":"text","text":"child"}]}`, "", false)
	seedRequestEvent(t, admin, "sesn_final", "thr_child", "evt_child_end", 3, "span.model_request_end", "mreq_child", `{"model_request_start_id":"evt_child_start"}`, "", false)
	if _, err := reader.ListRequestFinalMessages(t.Context(), scope, "evt_child_end", 0, 1); err == nil {
		t.Fatal("hidden child group reached Session view")
	}
	childScope := scope
	childScope.ThreadID = "thr_child"
	child, err := reader.ListRequestFinalMessages(t.Context(), childScope, "evt_child_end", 0, 1)
	if err != nil || len(child) != 1 || child[0].Event.ID != "evt_child_message" {
		t.Fatalf("child formal group=%+v err=%v", child, err)
	}
}

func TestPostgreSQLSessionChangeLifecyclePreservesDeletion(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_delete_feed", "thr_delete_feed")
	seedRequestEvent(t, admin, "sesn_delete_feed", "", "evt_deleted_feed", 1, "session.deleted", "", `{}`, "", true)
	if _, err := admin.ExecContext(t.Context(), `UPDATE sessions SET lifecycle_state='deleted' WHERE id='sesn_delete_feed'`); err != nil {
		t.Fatal(err)
	}
	runtime := storagetest.OpenWorkloadDB(t, admin, "event_stream").DB
	reader := newPostgreSQLEventReader(runtime)
	changes, err := reader.ListSessionEventChanges(t.Context(), workspace.DefaultID, "sesn_delete_feed", 0, 100)
	if err != nil || len(changes) != 1 || changes[0].Event.Type != "session.deleted" {
		t.Fatalf("deletion feed=%+v err=%v", changes, err)
	}
	if _, err := reader.ListSessionEventChanges(t.Context(), workspace.DefaultID, "sesn_delete_feed", changes[0].StreamPosition, 100); err == nil {
		t.Fatal("deleted session remains readable after deletion cursor")
	}
	if _, err := reader.CurrentStreamPosition(t.Context(), workspace.DefaultID, "sesn_delete_feed"); err == nil {
		t.Fatal("new stream admits deleted session")
	}
	if _, err := reader.ListSessionEventChanges(t.Context(), "foreign", "sesn_delete_feed", 0, 100); err == nil {
		t.Fatal("foreign deletion visible")
	}
	if _, err := reader.ListThreadEventChanges(t.Context(), workspace.DefaultID, "sesn_delete_feed", "thr_delete_feed", 0, 100); err == nil {
		t.Fatal("deleted thread read continues")
	}
}
