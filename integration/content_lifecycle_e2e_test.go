package integration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func TestContentLifecycleLocalEndToEnd(t *testing.T) {
	requireContentLifecycleDependencies(t, true)
	for _, lost := range []bool{false, true} {
		name := "complete-text-and-tool"
		if lost {
			name = "lost-content-receipt"
		}
		t.Run(name, func(t *testing.T) {
			chain := startContentE2E(t, "durable-interleaved", lost, false)
			chain.sdk.control(t, "send", map[string]any{"sessionId": chain.session, "text": "start-content-fixture"})
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND provider_command_reference_json IS NOT NULL`, chain.session, 1)
			chain.gateway.control(t, map[string]any{"kind": "release_finish"}, "released_finish")
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='false'`, chain.session, 1)
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND execution_state='running' AND provider_command_reference_json IS NOT NULL`, chain.session, 1)
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, chain.session, 0)
			// The command's status port remains held until the durable End is observed.
			chain.provider.finish()
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, chain.session, 1)
			assertContentLifecycleHistory(t, chain.db, chain.session, contentReadCase)
			assertContentSDKTexts(t, chain, []string{"alpha", "beta", "done"})
			observed := chain.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
			assertContentNativeContext(t, observed["nativeContexts"], contentReadCase)
			assertContentE2EProvider(t, chain, observed, 2, 1)
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, chain.session, 1)
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_runtime_tool_results r JOIN session_events e ON e.workspace_id=r.workspace_id AND e.event_id=r.consumed_by_terminal_event_id WHERE r.session_id=$1 AND r.execution_state='consumed' AND r.model_tool_call_id='call-read-note' AND r.provider_command_reference_json IS NOT NULL AND e.type='agent.tool_result' AND e.payload_json::jsonb->>'tool_use_id'=r.tool_use_event_id`, chain.session, 1)
			if lost {
				chain.lost.mu.Lock()
				attempts := chain.lost.attempts
				replayMatches := chain.lost.replayMatches
				chain.lost.mu.Unlock()
				if attempts != 2 || !replayMatches {
					t.Fatalf("lost committed receipt attempts=%d want2", attempts)
				}
			}
		})
	}
	t.Run("failed-stream-keeps-committed-content", func(t *testing.T) {
		chain := startContentE2E(t, "error-after-complete-prefix", false, false)
		chain.sdk.control(t, "send", map[string]any{"sessionId": chain.session, "text": "start-content-fixture"})
		waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true'`, chain.session, 1)
		waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, chain.session, 1)
		// The existing Runtime budget permits three reschedules, each with a
		// new model request. Gateway must never replay within one request.
		assertContentSDKTexts(t, chain, []string{"alpha", "alpha", "alpha", "alpha"})
		assertContentFailedRequestParts(t, chain)
		waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='false'`, chain.session, 0)
		observed := chain.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
		assertContentE2EProvider(t, chain, observed, 4, 0)
		assertContentAdmissionFailures(t, observed["admissionFailures"], "provider_stream_error", 4)
	})
	for _, mode := range []string{"warm", "cold", "missing-object"} {
		t.Run("attachment-from-minio/"+mode, func(t *testing.T) {
			chain := startContentE2E(t, "text", false, mode == "cold")
			// Establish an actual previous turn for both warm and cold admission.
			chain.sdk.control(t, "send", map[string]any{"sessionId": chain.session, "text": "start-content-fixture"})
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, chain.session, 1)
			if mode == "cold" {
				chain.runtime.release(t, "evict-idle")
				evicted := chain.runtime.marker(t, "idle-evicted")
				var observed bool
				if json.Unmarshal(evicted["observed"], &observed) != nil || observed {
					t.Fatal("real idle cleanup did not evict residency")
				}
			}
			uploaded := chain.sdk.control(t, "upload_png", nil)
			var file struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(uploaded, &file) != nil || file.ID == "" {
				t.Fatal("SDK upload returned no file identity")
			}
			var objectKey string
			if err := chain.db.QueryRow(`SELECT o.blob_key FROM files f JOIN file_objects o ON o.workspace_id=f.workspace_id AND o.object_id=f.object_id WHERE f.file_id=$1`, file.ID).Scan(&objectKey); err != nil {
				t.Fatal(err)
			}
			reader, err := chain.objects.Get(context.Background(), objectKey)
			if err != nil {
				t.Fatal(err)
			}
			bytes, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				t.Fatal(err)
			}
			literal, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==")
			const digest = "4ff6ab670a58c14270e034e2090d9a432caa263a14e0a25785386b0c12f880b5"
			if !reflect.DeepEqual(bytes, literal) || fmt.Sprintf("%x", sha256.Sum256(bytes)) != digest {
				t.Fatal("actual MinIO bytes differ from independent PNG fixture")
			}
			if mode == "missing-object" {
				if err := chain.objects.Delete(context.Background(), objectKey); err != nil {
					t.Fatal(err)
				}
			}
			chain.sdk.control(t, "send", map[string]any{"sessionId": chain.session, "text": "start-content-fixture", "fileId": file.ID})
			waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, chain.session, 2)
			observed := chain.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
			var attachments []struct {
				RequestOrdinal int    `json:"requestOrdinal"`
				MediaType      string `json:"mediaType"`
				SizeBytes      int    `json:"sizeBytes"`
				SHA            string `json:"sha256"`
			}
			if err := json.Unmarshal(observed["nativeAttachments"], &attachments); err != nil {
				t.Fatal(err)
			}
			if mode == "missing-object" {
				if len(attachments) != 0 {
					t.Fatal("missing object produced fabricated attachment bytes")
				}
				// Attachment consumption belongs to RequestStart. Its retryable
				// byte-read failure ends that request; the normal successor
				// has no pending attachment and may complete without bytes.
				assertContentE2EProvider(t, chain, observed, 2, 0)
				assertContentSDKTexts(t, chain, []string{"alpha", "alpha"})
				assertContentAdmissionFailures(t, observed["admissionFailures"], "attachment_unavailable", 1)
				waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_start'`, chain.session, 3)
				waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true'`, chain.session, 1)
				waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error'`, chain.session, 1)
			} else {
				if len(attachments) != 1 || attachments[0].RequestOrdinal != 2 || attachments[0].MediaType != "image/png" || attachments[0].SizeBytes != len(literal) || attachments[0].SHA != digest {
					t.Fatalf("provider attachment observation differs: %+v", attachments)
				}
				assertContentE2EProvider(t, chain, observed, 2, 0)
				assertContentSDKTexts(t, chain, []string{"alpha", "alpha"})
				waitContentSQLCount(t, chain.db, `SELECT count(*) FROM session_file_attachment_consumptions WHERE session_id=$1`, chain.session, 1)
			}
		})
	}
}

func assertContentE2EProvider(t *testing.T, chain *contentE2E, observation map[string]json.RawMessage, requests int, commands int64) {
	t.Helper()
	var calls int
	var credentialStore string
	if json.Unmarshal(observation["providerCalls"], &calls) != nil || calls != requests || chain.provider.calls.Load() != commands {
		t.Fatalf("provider/command calls=%d/%d want%d/%d", calls, chain.provider.calls.Load(), requests, commands)
	}
	if json.Unmarshal(observation["credentialStore"], &credentialStore) != nil || credentialStore != "sql" {
		t.Fatal("E2E bypassed installed SQL credentials")
	}
}
func assertContentSDKTexts(t *testing.T, chain *contentE2E, want []string) {
	t.Helper()
	assertContentSDKTextHistory(t, chain, want)

	session := chain.sdk.control(t, "session", map[string]any{"sessionId": chain.session})
	var terminal struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(session, &terminal) != nil || terminal.Status != "idle" {
		t.Fatal("SDK Session did not reach idle")
	}

}
func assertContentSDKTextHistory(t *testing.T, chain *contentE2E, want []string) {
	t.Helper()
	raw := chain.sdk.control(t, "events", map[string]any{"sessionId": chain.session})
	var events []struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &events); err != nil {
		t.Fatal(err)
	}
	var texts, ids []string
	for _, event := range events {
		if event.Type == "agent.message" {
			if len(event.Content) != 1 || event.Content[0].Type != "text" {
				t.Fatal("SDK returned malformed text event")
			}
			texts = append(texts, event.Content[0].Text)
			ids = append(ids, event.ID)
		}
	}
	if !reflect.DeepEqual(texts, want) {
		t.Fatalf("SDK complete text=%v want%v; public events=%s", texts, want, raw)
	}
	rows, err := chain.db.Query(`SELECT event_id,payload_json::jsonb->'content'->0->>'text' FROM session_events WHERE session_id=$1 AND type='agent.message' ORDER BY sequence`, chain.session)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	i := 0
	seen := map[string]bool{}
	for rows.Next() {
		var eventID, text string
		if err := rows.Scan(&eventID, &text); err != nil {
			t.Fatal(err)
		}
		if i >= len(ids) || ids[i] != eventID || texts[i] != text || seen[eventID] {
			t.Fatal("SDK and independent SQL identities/texts disagree")
		}
		seen[eventID] = true
		i++
	}
	if rows.Err() != nil || i != len(want) {
		t.Fatal("SQL text census differs")
	}
}

func assertContentAdmissionFailures(t *testing.T, raw json.RawMessage, code string, want int) {
	t.Helper()
	var failures []struct {
		RequestID string `json:"requestId"`
		Code      string `json:"errorCode"`
	}
	if json.Unmarshal(raw, &failures) != nil || len(failures) != want {
		t.Fatalf("typed Gateway failures differ: %s", raw)
	}
	seen := map[string]bool{}
	for _, failure := range failures {
		if failure.RequestID == "" || seen[failure.RequestID] || failure.Code != code {
			t.Fatalf("typed failure/request identity differs: %s", raw)
		}
		seen[failure.RequestID] = true
	}
}
func assertContentFailedRequestParts(t *testing.T, chain *contentE2E) {
	t.Helper()
	rows, err := chain.db.Query(`SELECT model_request_id,count(*) FILTER (WHERE type='span.model_request_start'),count(*) FILTER (WHERE type='agent.message'),count(*) FILTER (WHERE type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true'),min(payload_json::jsonb->'content'->0->>'text') FILTER (WHERE type='agent.message') FROM session_events WHERE session_id=$1 AND type IN ('span.model_request_start','span.model_request_end','agent.message') GROUP BY model_request_id`, chain.session)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var request, text string
		var starts, messages, ends int
		if err := rows.Scan(&request, &starts, &messages, &ends, &text); err != nil {
			t.Fatal(err)
		}
		if request == "" || starts != 1 || messages != 1 || ends != 1 || text != "alpha" {
			t.Fatalf("failed request replay or fabricated content: %q %d/%d/%d %q", request, starts, messages, ends, text)
		}
		count++
	}
	if rows.Err() != nil || count != 4 {
		t.Fatal("expected original request and three independently failed reschedules")
	}
}
