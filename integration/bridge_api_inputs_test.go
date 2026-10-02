package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestPostgreSQLBridgeAPIStoreCommitInputsProjectsInterAgentMessageExactlyOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inter_agent", "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	const (
		sourceToolUseEventID = "evt_bridge_inter_agent_send"
		childThreadID        = "thr_bridge_inter_agent_child"
		content              = "hello child"
	)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", sourceToolUseEventID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_thr_bridge_inter_agent_child","message":"hello child"}}`)
	seedBridgeAPIAllowedToolRoute(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", sourceToolUseEventID)
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("inter-agent-context-test-key-32b")
	parentScope := bridgeAPIScope("sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", childThreadID)
	deliveryID := runtimecontrol.AgentMailDeliveryID(sourceToolUseEventID, childThreadID)
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	store.Clock = func() time.Time { return now }
	delivered, err := store.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: parentScope, DeliveryId: deliveryID, TargetThreadId: childThreadID,
		SourceToolUseEventId: sourceToolUseEventID, Content: content,
	})
	if err != nil {
		t.Fatalf("DeliverInterAgentMail: %v", err)
	}
	if delivered.GetCommitted() == nil {
		t.Fatalf("delivery outcome = %+v; want committed", delivered)
	}
	childScope := bridgeAPIScope("sesn_bridge_inter_agent", childThreadID, "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	runtimeInputID := runtimecontrol.CompletionRuntimeInputID(deliveryID)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.ID("default"),
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "inter-agent-vertical",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease resolved inter-agent wake = %#v/%v; want one", leased, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode resolved inter-agent wake: %v", err)
	}
	if job.RuntimeInputID != runtimeInputID ||
		job.SessionThreadID != childThreadID ||
		job.InputKind != "agent_mail" {
		t.Fatalf("resolved inter-agent job = %#v; want exact child mail wake", job)
	}
	deliveryStore := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now.Add(2 * time.Second) }
	plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || plan.AcceptAgentMail == nil || plan.StaleAccepted {
		t.Fatalf("PrepareRuntimeCommand inter-agent delivery = %#v/%v; want live Runtime command", plan, err)
	}
	if plan.AcceptAgentMail.GetContent() != content || plan.AcceptAgentMail.GetDeliveryId() != deliveryID {
		t.Fatalf("prepared inter-agent command = %#v; want exact stored mail content and delivery identity", plan.AcceptAgentMail)
	}
	request := &bridgev1.CommitInputsRequest{
		Scope: childScope, RuntimeInputId: runtimeInputID,
	}

	response, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs inter_agent_message: %v", err)
	}
	if response.GetCommitted() == nil {
		t.Fatalf("inter-agent commit outcome = %#v; want committed", response)
	}
	replay, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs inter-agent replay: %v", err)
	}
	if replay.GetCommitted() == nil {
		t.Fatalf("inter-agent replay outcome = %#v; want committed", replay)
	}
	var committedInbox, leasedQueue string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2)`,
		runtimeInputID, leased[0].ID).Scan(&committedInbox, &leasedQueue); err != nil {
		t.Fatalf("read materialized inter-agent custody: %v", err)
	}
	if committedInbox != "committed" || leasedQueue != queue.StatusLeased {
		t.Fatalf("materialized inter-agent custody = Inbox:%s Queue:%s; want committed/leased", committedInbox, leasedQueue)
	}
	if settled, err := deliveryStore.MarkRuntimeInputAccepted(context.Background(), job, plan.AttemptedBinding); err != nil || settled {
		t.Fatalf("mark inter-agent Runtime admission = settled:%t err:%v", settled, err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID).Scan(&committedInbox); err != nil {
		t.Fatalf("read admitted inter-agent custody: %v", err)
	}
	if committedInbox != "accepted" {
		t.Fatalf("admitted inter-agent Inbox status = %q; want accepted", committedInbox)
	}
	settled, found, err := deliveryStore.ReplayRuntimeDeliveryFinalization(context.Background(), job)
	if err != nil || !found || !settled.QueueLeaseSettled {
		t.Fatalf("settle inter-agent execution custody = %#v/%t/%v", settled, found, err)
	}
	loadResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: childScope,
	})
	if err != nil {
		t.Fatalf("LoadContext inter-agent message: %v", err)
	}
	var loaded bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loadResponse.GetContextJson()), &loaded); err != nil {
		t.Fatalf("decode inter-agent context: %v", err)
	}
	if len(loaded.ContextEntries) != 1 || loaded.ContextEntries[0].ContextKind != "user" || len(loaded.ContextEntries[0].Parts) != 1 ||
		testJSONPathString(t, string(loaded.ContextEntries[0].Parts[0]), "type") != "text" {
		t.Fatalf("loaded inter-agent context = %s; want one user text entry", loadResponse.GetContextJson())
	}
	var receivedEventID string
	var receivedVisibility string
	var receivedSessionVisible bool
	var receivedPayloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, visibility, session_visible, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_inter_agent'
		    AND session_thread_id = 'thr_bridge_inter_agent_child'
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $1`, deliveryID).Scan(&receivedEventID, &receivedVisibility, &receivedSessionVisible, &receivedPayloadJSON); err != nil {
		t.Fatalf("read received event: %v", err)
	}
	if receivedVisibility != "public" || !receivedSessionVisible ||
		testJSONPathString(t, receivedPayloadJSON, "source_thread_id") != "thr_bridge_inter_agent_parent" ||
		testJSONPathString(t, receivedPayloadJSON, "source_tool_use_event_id") != sourceToolUseEventID {
		t.Fatalf("received event = visibility %s sessionVisible %v payload %s; want public parent attribution", receivedVisibility, receivedSessionVisible, receivedPayloadJSON)
	}
	if !strings.Contains(receivedPayloadJSON, `"source_task_name":null`) {
		t.Fatalf("received event payload = %s; want null callable name for primary source", receivedPayloadJSON)
	}
	assertDurableInterAgentPublicContent(t, receivedPayloadJSON, content)
	var sentPayloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_inter_agent'
		    AND session_thread_id = 'thr_bridge_inter_agent_parent'
		    AND type = 'agent.thread_message_sent'
		    AND payload_json::jsonb ->> 'delivery_id' = $1`, deliveryID).Scan(&sentPayloadJSON); err != nil {
		t.Fatalf("read sent event: %v", err)
	}
	if testJSONPathString(t, sentPayloadJSON, "target_thread_id") != childThreadID ||
		testJSONPathString(t, sentPayloadJSON, "target_task_name") != "task_"+childThreadID {
		t.Fatalf("sent event payload = %s; want target child ID and callable task_name", sentPayloadJSON)
	}
	assertDurableInterAgentPublicContent(t, sentPayloadJSON, content)
	var messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_inter_agent'
		    AND session_thread_id = 'thr_bridge_inter_agent_child'
		    AND kind = 'user'
		    AND source_event_id = $1`,
		receivedEventID,
	).Scan(&messageCount); err != nil {
		t.Fatalf("read received projection count: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("received message projections = %d; want exactly one", messageCount)
	}
	var streamChangeCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_event_stream_changes
		  WHERE workspace_id = 'default'
		    AND event_id = $1`,
		receivedEventID,
	).Scan(&streamChangeCount); err != nil {
		t.Fatalf("read received stream change count: %v", err)
	}
	if streamChangeCount != 2 {
		t.Fatalf("received stream changes = %d; want admission and processing revisions", streamChangeCount)
	}

}
