package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	"github.com/tetral-ai/tetral/services/bridge/internal/outputcapture"
)

// This file owns the Bridge children protocol-family boundary.

func seedBridgeAPIChildLifecycleToolSource(
	t *testing.T,
	db *sql.DB,
	sessionID string,
	threadID string,
	eventID string,
) *bridgev1.ChildLifecycleSource {
	t.Helper()
	seedBridgeAPIEvent(
		t,
		db,
		"default",
		sessionID,
		threadID,
		eventID,
		nextBridgeAPIEventSequenceForTest(t, db, sessionID, threadID),
		"agent.tool_use",
		`{}`,
	)
	if _, err := db.ExecContext(
		context.Background(),
		`UPDATE session_events
		    SET visibility = 'public'
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND session_thread_id = $2
		    AND event_id = $3`,
		sessionID,
		threadID,
		eventID,
	); err != nil {
		t.Fatalf("make child lifecycle source public: %v", err)
	}
	return &bridgev1.ChildLifecycleSource{
		Identity: &bridgev1.ChildLifecycleSource_SourceToolUseEventId{
			SourceToolUseEventId: eventID,
		},
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadVisibilityFollowsRole(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_visibility", "thr_bridge_child_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_visibility", "bind_bridge_child_visibility", 1, "pod_uid_child_visibility")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_child_visibility", "thr_bridge_child_parent", "bind_bridge_child_visibility", 1, "pod_uid_child_visibility")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_visibility", "thr_bridge_child_parent", "evt_bridge_child_public_spawn", 1, "agent.tool_use", `{}`)
	prefixJSON := bridgeThreadContextPrefixJSON(t, "sesn_bridge_child_visibility", "msg_bridge_child_seed", "seed context", "thr_bridge_child_parent", "evt_bridge_child_public_spawn", "all")

	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_child_parent",
		ChildThreadId:           "thr_bridge_child_public",
		Role:                    "subagent",
		TaskName:                "public child",
		AgentType:               "general",
		SourceToolUseEventId:    "evt_bridge_child_public_spawn",
		ForkTurns:               "all",
		ThreadContextPrefixJson: prefixJSON,
	}); err != nil {
		t.Fatalf("CreateChildThread subagent: %v", err)
	}
	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_child_parent",
		ChildThreadId:  "thr_bridge_child_reviewer",
		Role:           "approval_reviewer",
		TaskName:       "reviewer child",
		IsTrunk:        true,
	}); err != nil {
		t.Fatalf("CreateChildThread reviewer: %v", err)
	}

	if got := bridgeThreadVisibility(t, admin, "thr_bridge_child_public"); got != "public" {
		t.Fatalf("subagent visibility = %q; want public", got)
	}
	if got := bridgeThreadVisibility(t, admin, "thr_bridge_child_reviewer"); got != "internal" {
		t.Fatalf("approval reviewer visibility = %q; want internal", got)
	}
	var reviewerIsTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_visibility'
		    AND id = 'thr_bridge_child_reviewer'`,
	).Scan(&reviewerIsTrunk); err != nil {
		t.Fatalf("read reviewer trunk flag: %v", err)
	}
	if !reviewerIsTrunk {
		t.Fatalf("approval reviewer is_trunk = false; want true")
	}
	successor, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_child_parent",
		ChildThreadId:  "thr_bridge_child_reviewer_successor",
		Role:           "approval_reviewer",
		TaskName:       "reviewer child successor",
		IsTrunk:        true,
	})
	if err != nil {
		t.Fatalf("CreateChildThread reviewer successor: %v", err)
	}
	if successor.GetChildThreadId() != "thr_bridge_child_reviewer_successor" {
		t.Fatalf("reviewer successor child id = %q; want thr_bridge_child_reviewer_successor", successor.GetChildThreadId())
	}
	var predecessorIsTrunk, successorIsTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_visibility' AND id = 'thr_bridge_child_reviewer'`,
	).Scan(&predecessorIsTrunk); err != nil {
		t.Fatalf("read reviewer predecessor trunk flag: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_visibility' AND id = 'thr_bridge_child_reviewer_successor'`,
	).Scan(&successorIsTrunk); err != nil {
		t.Fatalf("read reviewer successor trunk flag: %v", err)
	}
	if predecessorIsTrunk || !successorIsTrunk {
		t.Fatalf("reviewer succession flags = predecessor:%t successor:%t; want false/true", predecessorIsTrunk, successorIsTrunk)
	}
	var liveTrunks int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_visibility' AND parent_thread_id = 'thr_bridge_child_parent' AND role = 'approval_reviewer' AND is_trunk`,
	).Scan(&liveTrunks); err != nil {
		t.Fatalf("count live reviewer trunks: %v", err)
	}
	if liveTrunks != 1 {
		t.Fatalf("live reviewer trunks = %d; want 1", liveTrunks)
	}

	listed, err := store.ListChildThreads(context.Background(), &bridgev1.ListChildThreadsRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_child_parent",
	})
	if err != nil {
		t.Fatalf("ListChildThreads: %v", err)
	}
	if len(listed.GetThreadJson()) != 1 {
		t.Fatalf("listed child threads = %d; want only the subagent", len(listed.GetThreadJson()))
	}
	var listedThread struct {
		ID   string `json:"session_thread_id"`
		Role string `json:"role"`
	}
	if err := json.Unmarshal([]byte(listed.GetThreadJson()[0]), &listedThread); err != nil {
		t.Fatalf("decode listed child thread: %v", err)
	}
	if listedThread.ID != "thr_bridge_child_public" || listedThread.Role != "subagent" {
		t.Fatalf("listed child thread = %+v; want public subagent", listedThread)
	}
}

func TestValidateChildThreadRequestRejectsInvalidReviewerThreadContextPrefix(t *testing.T) {
	scope := bridgeAPIScope("sesn_bridge_reviewer_validation", "thr_bridge_reviewer_parent", "bind_bridge_reviewer_validation", 1, "pod_uid_reviewer_validation")
	reviewID := "arvw_bridge_reviewer_validation"
	request := &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_reviewer_parent",
		ChildThreadId:           approvalReviewerSidecarThreadID(scope, "thr_bridge_reviewer_parent", reviewID),
		Role:                    "approval_reviewer",
		AgentType:               "approval_reviewer",
		ForkTurns:               "all",
		ThreadContextPrefixJson: bridgeReviewerThreadContextPrefixJSON(t, "thr_bridge_reviewer_parent", "evt_bridge_reviewer_parent_boundary", reviewID, nil),
		ReviewerReviewId:        reviewID,
	}

	tests := []struct {
		name       string
		forkTurns  string
		prefixJSON string
	}{
		{
			name:       "zero fork turns",
			forkTurns:  "0",
			prefixJSON: `{"source_parent_thread_id":"thr_bridge_reviewer_parent","review_id":"arvw_bridge_reviewer_validation","fork_turns":"0","runtime_messages_snapshot":[]}`,
		},
		{
			name:       "trailing JSON value",
			forkTurns:  "all",
			prefixJSON: request.GetThreadContextPrefixJson() + `{}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
			candidate.ForkTurns = test.forkTurns
			candidate.ThreadContextPrefixJson = test.prefixJSON
			if err := validateChildThreadRequest(candidate, "approval_reviewer", "approval_reviewer", candidate.GetParentThreadId(), candidate.GetForkTurns()); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("validateChildThreadRequest err = %v; want InvalidArgument", err)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadEnforcesReviewerTrunkMarker(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_reviewer_trunk", "thr_bridge_reviewer_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_reviewer_trunk", "bind_bridge_reviewer_trunk", 1, "pod_uid_reviewer_trunk")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_reviewer_trunk", "thr_bridge_reviewer_parent", "bind_bridge_reviewer_trunk", 1, "pod_uid_reviewer_trunk")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_reviewer_trunk", "thr_bridge_reviewer_parent", "evt_bridge_reviewer_parent_boundary", 1, "user.message", `{"content":[{"type":"text","text":"parent"}]}`)

	trunkRequest := &bridgev1.CreateChildThreadRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_reviewer_parent",
		ChildThreadId:  "thr_bridge_reviewer_trunk",
		Role:           "approval_reviewer",
		TaskName:       "reviewer trunk",
		IsTrunk:        true,
	}
	created, err := store.CreateChildThread(context.Background(), trunkRequest)
	if err != nil {
		t.Fatalf("CreateChildThread reviewer trunk: %v", err)
	}
	if created.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("reviewer trunk ack = %s; want committed", created.GetAck().GetStatus())
	}
	replay, err := store.CreateChildThread(context.Background(), trunkRequest)
	if err != nil {
		t.Fatalf("CreateChildThread reviewer trunk replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("reviewer trunk replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	var replayedIsTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_reviewer_trunk' AND id = 'thr_bridge_reviewer_trunk'`,
	).Scan(&replayedIsTrunk); err != nil {
		t.Fatalf("read replayed reviewer trunk flag: %v", err)
	}
	if !replayedIsTrunk {
		t.Fatal("same-id reviewer trunk replay demoted the existing trunk")
	}

	secondTrunk := proto.Clone(trunkRequest).(*bridgev1.CreateChildThreadRequest)
	secondTrunk.ChildThreadId = "thr_bridge_reviewer_second_trunk"
	secondTrunk.TaskName = "second reviewer trunk"
	if _, err := store.CreateChildThread(context.Background(), secondTrunk); err != nil {
		t.Fatalf("CreateChildThread reviewer successor: %v", err)
	}
	var predecessorIsTrunk, successorIsTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_reviewer_trunk' AND id = 'thr_bridge_reviewer_trunk'`,
	).Scan(&predecessorIsTrunk); err != nil {
		t.Fatalf("read reviewer predecessor trunk flag: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_reviewer_trunk' AND id = 'thr_bridge_reviewer_second_trunk'`,
	).Scan(&successorIsTrunk); err != nil {
		t.Fatalf("read reviewer successor trunk flag: %v", err)
	}
	if predecessorIsTrunk || !successorIsTrunk {
		t.Fatalf("reviewer succession flags = predecessor:%t successor:%t; want false/true", predecessorIsTrunk, successorIsTrunk)
	}

	for _, reviewID := range []string{"arvw_bridge_reviewer_sidecar_a", "arvw_bridge_reviewer_sidecar_b"} {
		sidecar := proto.Clone(trunkRequest).(*bridgev1.CreateChildThreadRequest)
		sidecar.ChildThreadId = approvalReviewerSidecarThreadID(scope, "thr_bridge_reviewer_parent", reviewID)
		sidecar.TaskName = sidecar.ChildThreadId
		sidecar.IsTrunk = false
		sidecar.ReviewerReviewId = reviewID
		sidecar.ForkTurns = "all"
		sidecar.ThreadContextPrefixJson = bridgeReviewerThreadContextPrefixJSON(t, "thr_bridge_reviewer_parent", "evt_bridge_reviewer_parent_boundary", reviewID, nil)
		if _, err := store.CreateChildThread(context.Background(), sidecar); err != nil {
			t.Fatalf("CreateChildThread reviewer sidecar %q: %v", sidecar.ChildThreadId, err)
		}

		var isTrunk bool
		if err := admin.QueryRowContext(context.Background(),
			`SELECT is_trunk
			   FROM session_threads
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_reviewer_trunk'
			    AND id = $1`,
			sidecar.ChildThreadId,
		).Scan(&isTrunk); err != nil {
			t.Fatalf("read reviewer sidecar %q trunk flag: %v", sidecar.ChildThreadId, err)
		}
		if isTrunk {
			t.Fatalf("reviewer sidecar %q is_trunk = true; want false", sidecar.ChildThreadId)
		}
	}

	invalidSubagent := &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_reviewer_parent",
		ChildThreadId:           "thr_bridge_invalid_subagent_trunk",
		Role:                    "subagent",
		TaskName:                "invalid subagent trunk",
		AgentType:               "general",
		SourceToolUseEventId:    "evt_bridge_invalid_subagent_trunk",
		ForkTurns:               "all",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(t, "sesn_bridge_reviewer_trunk", "msg_bridge_invalid_subagent_trunk", "seed context", "thr_bridge_reviewer_parent", "evt_bridge_invalid_subagent_trunk", "all"),
		IsTrunk:                 true,
	}
	if _, err := store.CreateChildThread(context.Background(), invalidSubagent); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("subagent is_trunk err = %v; want InvalidArgument", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadConcurrentReviewerTrunkReplay(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_reviewer_replay_race", "thr_bridge_reviewer_replay_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_reviewer_replay_race", "bind_bridge_reviewer_replay_race", 1, "pod_uid_reviewer_replay_race")
	storeA := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	storeB := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_reviewer_replay_race", "thr_bridge_reviewer_replay_parent", "bind_bridge_reviewer_replay_race", 1, "pod_uid_reviewer_replay_race")
	request := &bridgev1.CreateChildThreadRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_reviewer_replay_parent",
		ChildThreadId:  "thrd_aprv_replay_race",
		Role:           "approval_reviewer",
		TaskName:       "reviewer trunk",
		IsTrunk:        true,
	}
	blocker, blockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		  FOR UPDATE`,
		"default",
		"sesn_bridge_reviewer_replay_race",
		"thr_bridge_reviewer_replay_parent",
	)

	type createResult struct {
		response *bridgev1.CreateChildThreadResponse
		err      error
	}
	results := make(chan createResult, 2)
	for _, store := range []*PostgreSQLBridgeAPIStore{storeA, storeB} {
		go func() {
			response, err := store.CreateChildThread(context.Background(), proto.Clone(request).(*bridgev1.CreateChildThreadRequest))
			results <- createResult{response: response, err: err}
		}()
	}
	waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release reviewer parent lock: %v", err)
	}

	statuses := map[bridgev1.BridgeWriteStatus]int{}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("CreateChildThread concurrent reviewer replay: %v", result.err)
		}
		statuses[result.response.GetAck().GetStatus()]++
	}
	if statuses[bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED] != 1 ||
		statuses[bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE] != 1 {
		t.Fatalf("concurrent reviewer replay statuses = %v; want one committed and one duplicate", statuses)
	}
	var isTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_reviewer_replay_race' AND id = 'thrd_aprv_replay_race'`,
	).Scan(&isTrunk); err != nil {
		t.Fatalf("read concurrent reviewer trunk flag: %v", err)
	}
	if !isTrunk {
		t.Fatal("concurrent same-id replay demoted the reviewer trunk")
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadCommitsCreatedEventAndContextPrefix(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_seed", "thr_bridge_child_seed_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_seed", "bind_bridge_child_seed", 1, "pod_uid_child_seed")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("child-seed-load-context-test-key")
	scope := bridgeAPIScope("sesn_bridge_child_seed", "thr_bridge_child_seed_parent", "bind_bridge_child_seed", 1, "pod_uid_child_seed")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_seed", "thr_bridge_child_seed_parent", "evt_bridge_child_seed_spawn", 1, "agent.tool_use", `{}`)
	prefixJSON := bridgeThreadContextPrefixJSON(t, "sesn_bridge_child_seed", "msg_bridge_child_seed_parent", "parent context", "thr_bridge_child_seed_parent", "evt_bridge_child_seed_spawn", "all")
	request := &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_child_seed_parent",
		ChildThreadId:           "thr_bridge_child_seed_worker",
		Role:                    "subagent",
		TaskName:                "seeded worker",
		AgentType:               "worker",
		SourceToolUseEventId:    "evt_bridge_child_seed_spawn",
		ForkTurns:               "all",
		ThreadContextPrefixJson: prefixJSON,
	}
	missingSource := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	missingSource.ChildThreadId = "thr_bridge_child_seed_missing_source"
	missingSource.SourceToolUseEventId = ""
	if _, err := store.CreateChildThread(context.Background(), missingSource); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing source_tool_use_event_id err = %v; want InvalidArgument", err)
	}
	invalidForkTurns := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	invalidForkTurns.ChildThreadId = "thr_bridge_child_seed_bad_fork"
	invalidForkTurns.SourceToolUseEventId = "evt_bridge_child_seed_bad_fork"
	invalidForkTurns.ForkTurns = "0"
	if _, err := store.CreateChildThread(context.Background(), invalidForkTurns); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid fork_turns err = %v; want InvalidArgument", err)
	}
	forbiddenRouting := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	forbiddenRouting.ChildThreadId = "thr_bridge_child_seed_routing"
	forbiddenRouting.ThreadContextPrefixJson = strings.Replace(
		prefixJSON,
		`"role":"user"`,
		`"role":"user","providerId":"openai","modelId":"gpt-5.5"`,
		1,
	)
	if _, err := store.CreateChildThread(context.Background(), forbiddenRouting); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("fork prefix routing metadata err = %v; want InvalidArgument", err)
	}

	response, err := store.CreateChildThread(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateChildThread: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED || response.GetChildThreadId() != "thr_bridge_child_seed_worker" {
		t.Fatalf("CreateChildThread response = %+v; want committed child id", response)
	}
	replay, err := store.CreateChildThread(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateChildThread replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetChildThreadId() != response.GetChildThreadId() {
		t.Fatalf("CreateChildThread replay = %+v; want duplicate same child", replay)
	}

	var parentThreadID string
	var role string
	var visibility string
	var statusValue string
	var agentType string
	var taskName string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT parent_thread_id, role, visibility, status, agent_type, task_name
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_seed'
		    AND id = 'thr_bridge_child_seed_worker'`).Scan(&parentThreadID, &role, &visibility, &statusValue, &agentType, &taskName); err != nil {
		t.Fatalf("read child thread: %v", err)
	}
	if parentThreadID != "thr_bridge_child_seed_parent" || role != "subagent" || visibility != "public" || statusValue != "idle" || agentType != "worker" || taskName != "seeded worker" {
		t.Fatalf("child row = parent=%q role=%q visibility=%q status=%q agentType=%q taskName=%q; want seeded public worker",
			parentThreadID, role, visibility, statusValue, agentType, taskName)
	}

	var createdEventID string
	var createdVisibility string
	var createdSessionVisible bool
	var createdPayloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, visibility, session_visible, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_seed'
		    AND session_thread_id = 'thr_bridge_child_seed_worker'
		    AND type = 'session.thread_created'`).Scan(&createdEventID, &createdVisibility, &createdSessionVisible, &createdPayloadJSON); err != nil {
		t.Fatalf("read thread_created event: %v", err)
	}
	if createdVisibility != "public" || !createdSessionVisible ||
		testJSONPathString(t, createdPayloadJSON, "session_thread_id") != "thr_bridge_child_seed_worker" ||
		testJSONPathString(t, createdPayloadJSON, "parent_thread_id") != "thr_bridge_child_seed_parent" ||
		testJSONPathString(t, createdPayloadJSON, "agent_type") != "worker" ||
		testJSONPathString(t, createdPayloadJSON, "task_name") != "seeded worker" {
		t.Fatalf("thread_created event projection = visibility %s sessionVisible %v payload %s; want public child metadata", createdVisibility, createdSessionVisible, createdPayloadJSON)
	}
	var streamThreadID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT session_thread_id
		   FROM session_event_stream_changes
		  WHERE workspace_id = 'default'
		    AND event_id = $1`,
		createdEventID,
	).Scan(&streamThreadID); err != nil {
		t.Fatalf("read thread_created stream change: %v", err)
	}
	if streamThreadID != "thr_bridge_child_seed_worker" {
		t.Fatalf("thread_created stream thread = %q; want child thread", streamThreadID)
	}

	var prefixEntriesJSON string
	var prefixBoundaryEventID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT entries_json, parent_boundary_event_id
		   FROM session_thread_context_prefixes
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_seed'
		    AND child_thread_id = 'thr_bridge_child_seed_worker'`).Scan(&prefixEntriesJSON, &prefixBoundaryEventID); err != nil {
		t.Fatalf("read thread context prefix: %v", err)
	}
	if prefixBoundaryEventID != "evt_bridge_child_seed_spawn" || !strings.Contains(prefixEntriesJSON, "parent context") {
		t.Fatalf("thread context prefix boundary=%q entries=%s; want durable parent boundary and snapshot", prefixBoundaryEventID, prefixEntriesJSON)
	}
	childContext, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_child_seed", "thr_bridge_child_seed_worker", "bind_bridge_child_seed", 1, "pod_uid_child_seed"),
		RuntimeInputId: "rin_bridge_child_seed_load",
		SequenceFrom:   1,
		SequenceTo:     1,
	})
	if err != nil {
		t.Fatalf("LoadContext child fork seed: %v", err)
	}
	var childContextPayload struct {
		Messages            []map[string]any `json:"messages"`
		ThreadContextPrefix *struct {
			ParentBoundaryEventID string           `json:"parentBoundaryEventId"`
			Entries               []map[string]any `json:"entries"`
		} `json:"threadContextPrefix"`
	}
	if err := json.Unmarshal([]byte(childContext.GetContextJson()), &childContextPayload); err != nil {
		t.Fatalf("decode child context: %v", err)
	}
	if len(childContextPayload.Messages) != 0 {
		t.Fatalf("child context messages = %s; want no sequenced fork message", childContext.GetContextJson())
	}
	if childContextPayload.ThreadContextPrefix == nil ||
		childContextPayload.ThreadContextPrefix.ParentBoundaryEventID != "evt_bridge_child_seed_spawn" ||
		len(childContextPayload.ThreadContextPrefix.Entries) != 1 ||
		childContextPayload.ThreadContextPrefix.Entries[0]["id"] != "msg_bridge_child_seed_parent" {
		t.Fatalf("child context = %s; want separate parent context prefix", childContext.GetContextJson())
	}

	emptyPrefixJSON := `{"source_parent_thread_id":"thr_bridge_child_seed_parent","parent_boundary_event_id":"evt_bridge_child_seed_empty_spawn","source_tool_use_event_id":"evt_bridge_child_seed_empty_spawn","fork_turns":"none","runtime_messages_snapshot":[]}`
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_seed", "thr_bridge_child_seed_parent", "evt_bridge_child_seed_empty_spawn", 2, "agent.tool_use", `{}`)
	emptySeedRequest := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	emptySeedRequest.ChildThreadId = "thr_bridge_child_seed_empty"
	emptySeedRequest.TaskName = "empty seed worker"
	emptySeedRequest.SourceToolUseEventId = "evt_bridge_child_seed_empty_spawn"
	emptySeedRequest.ForkTurns = "none"
	emptySeedRequest.ThreadContextPrefixJson = emptyPrefixJSON
	if _, err := store.CreateChildThread(context.Background(), emptySeedRequest); err != nil {
		t.Fatalf("CreateChildThread empty fork seed: %v", err)
	}
	emptyChildContext, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_child_seed", "thr_bridge_child_seed_empty", "bind_bridge_child_seed", 1, "pod_uid_child_seed"),
		RuntimeInputId: "rin_bridge_child_seed_empty_load",
		SequenceFrom:   1,
		SequenceTo:     1,
	})
	if err != nil {
		t.Fatalf("LoadContext empty child fork seed: %v", err)
	}
	var emptyChildContextPayload struct {
		Messages            []map[string]any `json:"messages"`
		ThreadContextPrefix *struct {
			Entries []map[string]any `json:"entries"`
		} `json:"threadContextPrefix"`
	}
	if err := json.Unmarshal([]byte(emptyChildContext.GetContextJson()), &emptyChildContextPayload); err != nil {
		t.Fatalf("decode empty child context: %v", err)
	}
	if len(emptyChildContextPayload.Messages) != 0 {
		t.Fatalf("empty child context = %s; want no sequenced fork message", emptyChildContext.GetContextJson())
	}
	if emptyChildContextPayload.ThreadContextPrefix == nil || len(emptyChildContextPayload.ThreadContextPrefix.Entries) != 0 {
		t.Fatalf("empty child context = %s; want empty separate prefix", emptyChildContext.GetContextJson())
	}

	conflict := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	conflict.ChildThreadId = "thr_bridge_child_seed_other"
	if _, err := store.CreateChildThread(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting source tool use err = %v; want AlreadyExists", err)
	}
	duplicateTask := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	duplicateTask.ChildThreadId = "thr_bridge_child_seed_duplicate_task"
	duplicateTask.SourceToolUseEventId = "evt_bridge_child_seed_other_spawn"
	duplicateTask.ThreadContextPrefixJson = bridgeThreadContextPrefixJSON(t, "sesn_bridge_child_seed", "msg_bridge_child_seed_parent", "parent context", "thr_bridge_child_seed_parent", "evt_bridge_child_seed_other_spawn", "all")
	if _, err := store.CreateChildThread(context.Background(), duplicateTask); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate task_name err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadConcurrentDuplicateTaskName(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_race", "thr_bridge_child_race_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_race", "bind_bridge_child_race", 1, "pod_uid_child_race")
	storeA := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	storeB := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_child_race", "thr_bridge_child_race_parent", "bind_bridge_child_race", 1, "pod_uid_child_race")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_race", "thr_bridge_child_race_parent", "evt_bridge_child_race_a", 1, "agent.tool_use", `{}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_race", "thr_bridge_child_race_parent", "evt_bridge_child_race_b", 2, "agent.tool_use", `{}`)
	requestA := &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_child_race_parent",
		ChildThreadId:           "thr_bridge_child_race_a",
		Role:                    "subagent",
		TaskName:                "same worker",
		AgentType:               "worker",
		SourceToolUseEventId:    "evt_bridge_child_race_a",
		ForkTurns:               "none",
		ThreadContextPrefixJson: `{"source_parent_thread_id":"thr_bridge_child_race_parent","parent_boundary_event_id":"evt_bridge_child_race_a","source_tool_use_event_id":"evt_bridge_child_race_a","fork_turns":"none","runtime_messages_snapshot":[]}`,
	}
	requestB := proto.Clone(requestA).(*bridgev1.CreateChildThreadRequest)
	requestB.ChildThreadId = "thr_bridge_child_race_b"
	requestB.SourceToolUseEventId = "evt_bridge_child_race_b"
	requestB.ThreadContextPrefixJson = `{"source_parent_thread_id":"thr_bridge_child_race_parent","parent_boundary_event_id":"evt_bridge_child_race_b","source_tool_use_event_id":"evt_bridge_child_race_b","fork_turns":"none","runtime_messages_snapshot":[]}`

	start := make(chan struct{})
	var wg sync.WaitGroup
	type createResult struct {
		response *bridgev1.CreateChildThreadResponse
		err      error
	}
	results := make(chan createResult, 2)
	for _, item := range []struct {
		store   *PostgreSQLBridgeAPIStore
		request *bridgev1.CreateChildThreadRequest
	}{
		{store: storeA, request: requestA},
		{store: storeB, request: requestB},
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := item.store.CreateChildThread(context.Background(), item.request)
			results <- createResult{response: response, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	committed := 0
	alreadyExists := 0
	for result := range results {
		if result.err != nil {
			if status.Code(result.err) == codes.AlreadyExists {
				alreadyExists++
				continue
			}
			t.Fatalf("CreateChildThread concurrent err = %v; want committed or AlreadyExists", result.err)
		}
		if result.response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
			t.Fatalf("CreateChildThread concurrent response = %+v; want committed", result.response)
		}
		committed++
	}
	if committed != 1 || alreadyExists != 1 {
		t.Fatalf("concurrent create committed=%d alreadyExists=%d; want 1/1", committed, alreadyExists)
	}
	var childCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_race'
		    AND parent_thread_id = 'thr_bridge_child_race_parent'
		    AND role = 'subagent'
		    AND task_name = 'same worker'`).Scan(&childCount); err != nil {
		t.Fatalf("count child threads: %v", err)
	}
	if childCount != 1 {
		t.Fatalf("childCount = %d; want durable task_name uniqueness", childCount)
	}
}

func TestPostgreSQLBridgeAPIStoreChildThreadStatusEventsStayThreadScoped(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_status", "thr_bridge_child_status_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_status", "bind_bridge_child_status", 1, "pod_uid_child_status")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_child_status", "prep_bridge_child_status")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_child_status", "2026-01-01T00:00:00Z")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions
		    SET status = 'running'
		  WHERE workspace_id = 'default'
		    AND id = 'sesn_bridge_child_status'`); err != nil {
		t.Fatalf("seed running public session: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'running'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND id = 'thr_bridge_child_status_parent'`); err != nil {
		t.Fatalf("seed running main thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, status_event_id, idle_since, cleanup_after,
			cleanup_enqueued_at, cleanup_claimed_at, cleanup_job_id,
			binding_id, binding_generation, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_child_status', 'running', 'evt_child_status_session_running_sentinel', NULL, NULL,
			'2026-01-01T00:00:10Z', '2026-01-01T00:00:11Z', 'qjob_child_status_cleanup_sentinel',
			'bind_child_status_runtime_sentinel', 41, '2026-01-01T00:00:05Z', '2026-01-01T00:00:12Z'
		)`); err != nil {
		t.Fatalf("seed running runtime status sentinel: %v", err)
	}
	blobStore := blob.NewFakeBlobStore()
	scanner := &recordingOutputScanner{files: []outputcapture.SandboxOutputFile{
		capturedOutputFile("/mnt/session/outputs/child-report.txt", "captured child output"),
	}}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	clockNow := time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC)
	store.Clock = func() time.Time { return clockNow }
	store.SandboxStatusFreshnessWindow = 5 * time.Minute
	store.OutputCapturer = outputcapture.NewCapturer(blobStore, scanner)
	parentScope := bridgeAPIScope("sesn_bridge_child_status", "thr_bridge_child_status_parent", "bind_bridge_child_status", 1, "pod_uid_child_status")
	childScope := bridgeAPIScope("sesn_bridge_child_status", "thr_bridge_child_status_worker", "bind_bridge_child_status", 1, "pod_uid_child_status")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_status", "thr_bridge_child_status_parent", "evt_bridge_child_status_spawn", 1, "agent.tool_use", `{}`)

	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                   parentScope,
		ParentThreadId:          "thr_bridge_child_status_parent",
		ChildThreadId:           "thr_bridge_child_status_worker",
		Role:                    "subagent",
		TaskName:                "status worker",
		AgentType:               "worker",
		SourceToolUseEventId:    "evt_bridge_child_status_spawn",
		ForkTurns:               "none",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(t, "sesn_bridge_child_status", "msg_bridge_child_status_seed", "seed", "thr_bridge_child_status_parent", "evt_bridge_child_status_spawn", "none"),
	}); err != nil {
		t.Fatalf("CreateChildThread: %v", err)
	}
	running, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          childScope,
		RuntimeWriteId: "rwrite_bridge_child_status_running",
		EventType:      "session.status_running",
		PayloadJson:    `{"type":"session.status_running"}`,
		SessionVisible: true,
	})
	if err != nil {
		t.Fatalf("WriteEvent child running: %v", err)
	}
	finishIdleRequest := &bridgev1.FinishIdleRequest{
		Scope:          childScope,
		DurableTurnId:  running.GetEventId(),
		StopReasonJson: `{"type":"end_turn"}`,
	}
	finishIdleResponse, err := store.FinishIdle(context.Background(), finishIdleRequest)
	if err != nil {
		t.Fatalf("FinishIdle child: %v", err)
	}
	if finishIdleResponse.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("FinishIdle child ack = %s; want committed", finishIdleResponse.GetAck().GetStatus())
	}
	finishIdleReplay, err := store.FinishIdle(context.Background(), finishIdleRequest)
	if err != nil {
		t.Fatalf("FinishIdle child replay: %v", err)
	}
	if finishIdleReplay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("FinishIdle child replay ack = %s; want duplicate", finishIdleReplay.GetAck().GetStatus())
	}
	if len(scanner.targets) != 1 {
		t.Fatalf("child output scanner calls after replay = %d; want 1", len(scanner.targets))
	}
	scanTarget := scanner.targets[0]
	if scanTarget.WorkspaceID != "default" ||
		scanTarget.SessionID != "sesn_bridge_child_status" ||
		scanTarget.SessionThreadID != "thr_bridge_child_status_worker" ||
		scanTarget.BindingID != "bind_bridge_child_status" ||
		scanTarget.BindingGeneration != 1 {
		t.Fatalf("child output scan target = %+v; want request workspace/session/child thread/binding", scanTarget)
	}

	var runningType string
	var runningPayload string
	var runningThreadID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT type, payload_json, session_thread_id
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = $1`,
		running.GetEventId(),
	).Scan(&runningType, &runningPayload, &runningThreadID); err != nil {
		t.Fatalf("read child running event: %v", err)
	}
	if runningType != "session.thread_status_running" ||
		runningThreadID != "thr_bridge_child_status_worker" ||
		testJSONPathString(t, runningPayload, "session_thread_id") != "thr_bridge_child_status_worker" ||
		testJSONPathString(t, runningPayload, "task_name") != "status worker" {
		t.Fatalf("child running event = type %q thread %q payload %s; want thread-scoped running", runningType, runningThreadID, runningPayload)
	}
	var idleEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND type = 'session.thread_status_idle'
		    AND runtime_write_id = $1`,
		running.GetEventId(),
	).Scan(&idleEventCount); err != nil {
		t.Fatalf("count child idle events: %v", err)
	}
	if idleEventCount != 1 {
		t.Fatalf("child idle event count after replay = %d; want 1", idleEventCount)
	}
	var idleEventID string
	var idlePayload string
	var idleVisibility string
	var idleSessionVisible bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json, visibility, session_visible
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND type = 'session.thread_status_idle'
		    AND runtime_write_id = $1`,
		running.GetEventId(),
	).Scan(&idleEventID, &idlePayload, &idleVisibility, &idleSessionVisible); err != nil {
		t.Fatalf("read child idle event: %v", err)
	}
	if idleVisibility != "public" || !idleSessionVisible ||
		testJSONPathString(t, idlePayload, "session_thread_id") != "thr_bridge_child_status_worker" ||
		testJSONPathString(t, idlePayload, "task_name") != "status worker" ||
		testJSONPathString(t, idlePayload, "stop_reason.type") != "end_turn" {
		t.Fatalf("child idle event = visibility %q sessionVisible=%v payload %s; want public session-visible thread-scoped idle", idleVisibility, idleSessionVisible, idlePayload)
	}
	var idleStreamCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_event_stream_changes c
		   JOIN session_events e
		     ON e.workspace_id = c.workspace_id
		    AND e.session_id = c.session_id
		    AND e.event_id = c.event_id
		  WHERE c.workspace_id = 'default'
		    AND c.session_id = 'sesn_bridge_child_status'
		    AND c.event_id = $1
		    AND c.session_thread_id = 'thr_bridge_child_status_worker'
		    AND c.revision = 1
		    AND c.visibility = 'public'
		    AND c.session_visible = true
		    AND c.stream_position = e.latest_stream_position`, idleEventID).Scan(&idleStreamCount); err != nil {
		t.Fatalf("count matching child idle stream changes: %v", err)
	}
	if idleStreamCount != 1 {
		t.Fatalf("matching child idle stream changes after replay = %d; want 1", idleStreamCount)
	}
	var threadStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND id = 'thr_bridge_child_status_worker'`).Scan(&threadStatus); err != nil {
		t.Fatalf("read child thread status: %v", err)
	}
	if threadStatus != "idle" {
		t.Fatalf("child thread status = %q; want idle after FinishIdle", threadStatus)
	}
	var sessionIdleEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND type = 'session.status_idle'`).Scan(&sessionIdleEventCount); err != nil {
		t.Fatalf("count session idle events: %v", err)
	}
	if sessionIdleEventCount != 0 {
		t.Fatalf("session.status_idle event count after child FinishIdle = %d; want 0", sessionIdleEventCount)
	}
	var finishIdleOperationCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND operation = 'finish_idle'
		    AND source_kind = 'turn_closeout'
		    AND idempotency_key = $1
		    AND runtime_input_id = $1
		    AND ack_status = 'committed'`, running.GetEventId()).Scan(&finishIdleOperationCount); err != nil {
		t.Fatalf("count child finish_idle operations: %v", err)
	}
	if finishIdleOperationCount != 1 {
		t.Fatalf("child finish_idle operation count after replay = %d; want 1", finishIdleOperationCount)
	}
	var outputCaptureCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_output_captures
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND source_path = '/mnt/session/outputs/child-report.txt'`).Scan(&outputCaptureCount); err != nil {
		t.Fatalf("count child output captures: %v", err)
	}
	if outputCaptureCount != 1 {
		t.Fatalf("child output capture rows after replay = %d; want 1", outputCaptureCount)
	}
	var capturedFileCount int
	var capturedObjectKey string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), max(o.blob_key)
		   FROM files f
		   JOIN file_objects o
		     ON o.workspace_id = f.workspace_id
		    AND o.object_id = f.object_id
		  WHERE f.workspace_id = 'default'
		    AND f.scope_type = 'session'
		    AND f.scope_id = 'sesn_bridge_child_status'
		    AND f.filename = 'child-report.txt'`).Scan(&capturedFileCount, &capturedObjectKey); err != nil {
		t.Fatalf("read child captured file projection: %v", err)
	}
	if capturedFileCount != 1 {
		t.Fatalf("child captured file rows after replay = %d; want 1", capturedFileCount)
	}
	if body, ok := blobStore.Bytes(capturedObjectKey); !ok || string(body) != "captured child output" {
		t.Fatalf("child captured blob = %q present=%v; want captured child output", string(body), ok)
	}
	assertBridgeAPIChildFinishIdlePreservesSessionState(t, admin, "sesn_bridge_child_status", "thr_bridge_child_status_parent", "bind_bridge_child_status", 1)
	firstCloseSource := seedBridgeAPIChildLifecycleToolSource(
		t,
		admin,
		"sesn_bridge_child_status",
		"thr_bridge_child_status_parent",
		"evt_bridge_child_status_close_first",
	)
	clockNow = time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	if _, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_child_status_worker",
		ClosedAt:      "2026-01-01T00:01:00Z",
		Source:        firstCloseSource,
	}); err != nil {
		t.Fatalf("MarkChildThreadClosed: %v", err)
	}
	if _, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_child_status_worker",
		ClosedAt:      "2026-01-01T00:01:00Z",
		Source:        firstCloseSource,
	}); err != nil {
		t.Fatalf("MarkChildThreadClosed replay: %v", err)
	}
	var closedStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND id = 'thr_bridge_child_status_worker'`).Scan(&closedStatus); err != nil {
		t.Fatalf("read closed child thread status: %v", err)
	}
	if closedStatus != "closed_for_runtime" {
		t.Fatalf("child thread status after close = %q; want closed_for_runtime", closedStatus)
	}
	var closeIdleCount int
	var closeIdlePayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), max(payload_json)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND type = 'session.thread_status_idle'
		    AND payload_json::jsonb #>> '{stop_reason,type}' = 'closed_for_runtime'`).Scan(&closeIdleCount, &closeIdlePayload); err != nil {
		t.Fatalf("read close idle event: %v", err)
	}
	if closeIdleCount != 1 || testJSONPathString(t, closeIdlePayload, "stop_reason.type") != "closed_for_runtime" {
		t.Fatalf("close idle event count/payload = %d/%s; want one closed_for_runtime idle event", closeIdleCount, closeIdlePayload)
	}
	if testJSONPathString(t, closeIdlePayload, "task_name") != "status worker" {
		t.Fatalf("close idle task_name = %s; want callable status worker", closeIdlePayload)
	}
	resumeSource := seedBridgeAPIChildLifecycleToolSource(
		t,
		admin,
		"sesn_bridge_child_status",
		"thr_bridge_child_status_parent",
		"evt_bridge_child_status_resume",
	)
	clockNow = time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)
	if _, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_child_status_worker",
		ActiveAt:      "2026-01-01T00:02:00Z",
		Source:        resumeSource,
	}); err != nil {
		t.Fatalf("MarkChildThreadActive after close: %v", err)
	}
	secondCloseSource := seedBridgeAPIChildLifecycleToolSource(
		t,
		admin,
		"sesn_bridge_child_status",
		"thr_bridge_child_status_parent",
		"evt_bridge_child_status_close_second",
	)
	clockNow = time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC)
	if _, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_child_status_worker",
		ClosedAt:      "2026-01-01T00:03:00Z",
		Source:        secondCloseSource,
	}); err != nil {
		t.Fatalf("MarkChildThreadClosed after resume: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND type = 'session.thread_status_idle'
		    AND payload_json::jsonb #>> '{stop_reason,type}' = 'closed_for_runtime'`).Scan(&closeIdleCount); err != nil {
		t.Fatalf("read close idle event count after resume: %v", err)
	}
	if closeIdleCount != 2 {
		t.Fatalf("close idle event count after resume = %d; want a new closed_for_runtime idle event", closeIdleCount)
	}
	assertBridgeAPIChildFinishIdlePreservesSessionState(t, admin, "sesn_bridge_child_status", "thr_bridge_child_status_parent", "bind_bridge_child_status", 1)
	var sessionLevelStatusEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND type IN ('session.status_running', 'session.status_idle')`).Scan(&sessionLevelStatusEventCount); err != nil {
		t.Fatalf("read session-level status count: %v", err)
	}
	if sessionLevelStatusEventCount != 0 {
		t.Fatalf("session-level status event count = %d; want only thread status events for child", sessionLevelStatusEventCount)
	}
}

func TestPostgreSQLBridgeAPIStoreMarkChildThreadClosedCascadesAcrossDescendants(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID    = "sesn_bridge_close_tree"
		mainID       = "thr_bridge_close_tree_main"
		childID      = "thr_bridge_close_tree_child"
		grandchildID = "thr_bridge_close_tree_grandchild"
		bindingID    = "bind_bridge_close_tree"
		podUID       = "pod_bridge_close_tree"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, childID, grandchildID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_close_tree")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 6, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = 5 * time.Minute
	closeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_close_tree_command")
	response, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: childID,
		ClosedAt:      "2026-01-01T00:00:05.000Z",
		Source:        closeSource,
	})
	if err != nil {
		t.Fatalf("MarkChildThreadClosed tree: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("close tree ack = %s; want committed", response.GetAck().GetStatus())
	}
	if len(response.GetDeclaration().GetReceipts()) != 2 {
		t.Fatalf("close tree receipts = %d; want one per target", len(response.GetDeclaration().GetReceipts()))
	}
	receiptTargets := map[string]string{}
	for _, receipt := range response.GetDeclaration().GetReceipts() {
		if len(receipt.GetChildLifecycle()) != 1 {
			t.Fatalf("close tree receipt %q lifecycle stamps = %d; want 1", receipt.GetSessionThreadId(), len(receipt.GetChildLifecycle()))
		}
		stamp := receipt.GetChildLifecycle()[0]
		if stamp.GetChildThreadId() != receipt.GetSessionThreadId() {
			t.Fatalf("close tree receipt target = %q/%q; want matching thread scope", receipt.GetSessionThreadId(), stamp.GetChildThreadId())
		}
		if stamp.GetEffectiveAt() != "2026-01-01T00:00:05.000Z" {
			t.Fatalf("close tree receipt effective_at = %q; want original declaration bytes", stamp.GetEffectiveAt())
		}
		receiptTargets[receipt.GetSessionThreadId()] = stamp.GetDisposition().String()
	}
	if receiptTargets[childID] != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED.String() ||
		receiptTargets[grandchildID] != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED.String() {
		t.Fatalf("close tree receipt targets = %#v; want both target-scoped closed receipts", receiptTargets)
	}
	var operationScopes []string
	operationRows, err := admin.QueryContext(context.Background(),
		`SELECT session_thread_id
		   FROM session_bridge_operations
		  WHERE workspace_id='default'
		    AND session_id=$1
		    AND operation='mark_child_thread_closed'
		  ORDER BY session_thread_id`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("read close tree declaration operations: %v", err)
	}
	defer func() { _ = operationRows.Close() }()
	for operationRows.Next() {
		var threadID string
		if err := operationRows.Scan(&threadID); err != nil {
			t.Fatalf("scan close tree declaration operation: %v", err)
		}
		operationScopes = append(operationScopes, threadID)
	}
	if err := operationRows.Err(); err != nil {
		t.Fatalf("iterate close tree declaration operations: %v", err)
	}
	if len(operationScopes) != 2 || operationScopes[0] != childID || operationScopes[1] != grandchildID {
		t.Fatalf("close tree operation scopes = %v; want [%s %s]", operationScopes, childID, grandchildID)
	}
	replay, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: childID,
		ClosedAt:      "2026-01-01T00:00:05.000Z",
		Source:        closeSource,
	})
	if err != nil {
		t.Fatalf("MarkChildThreadClosed tree replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		!proto.Equal(response.GetDeclaration(), replay.GetDeclaration()) {
		t.Fatalf("close tree replay = %+v; want duplicate with exact stored receipts", replay)
	}
	rows, err := admin.QueryContext(context.Background(),
		`SELECT id, status FROM session_threads
		  WHERE workspace_id='default' AND session_id=$1 AND id IN ($2, $3)
		  ORDER BY id`,
		sessionID, childID, grandchildID,
	)
	if err != nil {
		t.Fatalf("read closed child tree: %v", err)
	}
	defer func() { _ = rows.Close() }()
	statuses := map[string]string{}
	for rows.Next() {
		var threadID, threadStatus string
		if err := rows.Scan(&threadID, &threadStatus); err != nil {
			t.Fatalf("scan closed child tree: %v", err)
		}
		statuses[threadID] = threadStatus
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate closed child tree: %v", err)
	}
	if statuses[childID] != "closed_for_runtime" || statuses[grandchildID] != "closed_for_runtime" {
		t.Fatalf("closed child tree statuses = %#v; want both closed_for_runtime", statuses)
	}
	var closedEvents, completionMail int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT
		   count(*) FILTER (WHERE type='session.thread_status_idle' AND payload_json::jsonb #>> '{stop_reason,type}'='closed_for_runtime'),
		   count(*) FILTER (WHERE type='agent.thread_message_sent')
		 FROM session_events
		 WHERE workspace_id='default' AND session_id=$1`,
		sessionID,
	).Scan(&closedEvents, &completionMail); err != nil {
		t.Fatalf("count closed child tree events: %v", err)
	}
	if closedEvents != 2 || completionMail != 0 {
		t.Fatalf("closed child tree events/mail = %d/%d; want 2/0", closedEvents, completionMail)
	}
	store.OutputCapturer = outputcapture.NewCapturer(blob.NewFakeBlobStore(), &recordingOutputScanner{})
	if _, err := store.FinishIdle(context.Background(), &bridgev1.FinishIdleRequest{
		Scope:          bridgeAPIScope(sessionID, grandchildID, bindingID, 1, podUID),
		DurableTurnId:  "evt_bridge_close_tree_grandchild_running",
		StopReasonJson: `{"type":"end_turn"}`,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("late FinishIdle after descendant close = %v; want FailedPrecondition", err)
	}
	var lateStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads
		  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		sessionID,
		grandchildID,
	).Scan(&lateStatus); err != nil {
		t.Fatalf("read descendant status after late FinishIdle: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'`,
		sessionID,
	).Scan(&completionMail); err != nil {
		t.Fatalf("count completion mail after late FinishIdle: %v", err)
	}
	if lateStatus != "closed_for_runtime" || completionMail != 0 {
		t.Fatalf("late FinishIdle status/mail = %q/%d; want closed_for_runtime/0", lateStatus, completionMail)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'idle', closed_at = NULL
		  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID,
		childID,
	); err != nil {
		t.Fatalf("reopen child before topology-change replay: %v", err)
	}
	const laterChildID = "thr_bridge_close_tree_later_child"
	seedBridgeAPIChildThread(t, admin, "default", sessionID, childID, laterChildID)
	replayAfterTopologyChange, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: childID,
		ClosedAt:      "2026-01-01T00:00:05.000Z",
		Source:        closeSource,
	})
	if err != nil {
		t.Fatalf("MarkChildThreadClosed replay after topology change: %v", err)
	}
	if replayAfterTopologyChange.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		!proto.Equal(response.GetDeclaration(), replayAfterTopologyChange.GetDeclaration()) {
		t.Fatalf("close tree replay after topology change = %+v; want original duplicate receipt set", replayAfterTopologyChange)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'closed_for_runtime', closed_at = '2026-01-01T00:00:05Z'
		  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID,
		childID,
	); err != nil {
		t.Fatalf("restore closed parent after topology-change replay: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "sevt_bridge_close_tree_late_spawn", 2, "agent.tool_use", `{}`)
	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ParentThreadId:       childID,
		ChildThreadId:        "thr_bridge_close_tree_late_child",
		Role:                 "subagent",
		TaskName:             "late-child",
		AgentType:            "worker",
		SourceToolUseEventId: "sevt_bridge_close_tree_late_spawn",
		ForkTurns:            "none",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(
			t,
			sessionID,
			"msg_bridge_close_tree_late_seed",
			"late seed",
			childID,
			"sevt_bridge_close_tree_late_spawn",
			"none",
		),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("create below already-closed parent err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreMarkChildThreadClosedPreservesTerminalTargetsInFrozenSubtree(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID         = "sesn_bridge_close_terminal_tree"
		mainID            = "thr_bridge_close_terminal_tree_main"
		failedRootID      = "thr_bridge_close_terminal_tree_failed"
		terminatedChildID = "thr_bridge_close_terminal_tree_terminated"
		runningGrandID    = "thr_bridge_close_terminal_tree_running"
		closedSiblingID   = "thr_bridge_close_terminal_tree_closed"
		bindingID         = "bind_bridge_close_terminal_tree"
		podUID            = "pod_bridge_close_terminal_tree"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, failedRootID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, failedRootID, terminatedChildID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, terminatedChildID, runningGrandID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, failedRootID, closedSiblingID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = CASE id
		      WHEN $2 THEN 'failed'
		      WHEN $3 THEN 'terminated'
		      WHEN $4 THEN 'running'
		      WHEN $5 THEN 'closed_for_runtime'
		    END,
		        closed_at = CASE WHEN id = $5 THEN '2026-01-01T00:00:01Z'::timestamptz ELSE NULL END
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND id IN ($2, $3, $4, $5)`,
		sessionID,
		failedRootID,
		terminatedChildID,
		runningGrandID,
		closedSiblingID,
	); err != nil {
		t.Fatalf("seed mixed child subtree statuses: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 6, 0, time.UTC) }
	closeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_close_terminal_tree")
	request := &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: failedRootID,
		ClosedAt:      "2026-01-01T00:00:05.000Z",
		Source:        closeSource,
	}
	response, err := store.MarkChildThreadClosed(context.Background(), request)
	if err != nil {
		t.Fatalf("MarkChildThreadClosed mixed terminal tree: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("close mixed terminal tree ack = %s; want committed", response.GetAck().GetStatus())
	}
	wantDispositions := map[string]bridgev1.ChildLifecycleDisposition{
		failedRootID:      bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED,
		terminatedChildID: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED,
		runningGrandID:    bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED,
		closedSiblingID:   bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_CLOSED,
	}
	gotDispositions := make(map[string]bridgev1.ChildLifecycleDisposition, len(wantDispositions))
	for _, receipt := range response.GetDeclaration().GetReceipts() {
		if len(receipt.GetChildLifecycle()) != 1 {
			t.Fatalf("close mixed terminal tree receipt %q lifecycle stamps = %d; want 1", receipt.GetSessionThreadId(), len(receipt.GetChildLifecycle()))
		}
		gotDispositions[receipt.GetSessionThreadId()] = receipt.GetChildLifecycle()[0].GetDisposition()
	}
	if !reflect.DeepEqual(gotDispositions, wantDispositions) {
		t.Fatalf("close mixed terminal tree dispositions = %#v; want %#v", gotDispositions, wantDispositions)
	}
	rows, err := admin.QueryContext(context.Background(),
		`SELECT id, status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND id IN ($2, $3, $4, $5)`,
		sessionID,
		failedRootID,
		terminatedChildID,
		runningGrandID,
		closedSiblingID,
	)
	if err != nil {
		t.Fatalf("read mixed terminal tree statuses: %v", err)
	}
	defer func() { _ = rows.Close() }()
	gotStatuses := map[string]string{}
	for rows.Next() {
		var threadID, threadStatus string
		if err := rows.Scan(&threadID, &threadStatus); err != nil {
			t.Fatalf("scan mixed terminal tree status: %v", err)
		}
		gotStatuses[threadID] = threadStatus
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate mixed terminal tree statuses: %v", err)
	}
	wantStatuses := map[string]string{
		failedRootID:      "failed",
		terminatedChildID: "terminated",
		runningGrandID:    "closed_for_runtime",
		closedSiblingID:   "closed_for_runtime",
	}
	if !reflect.DeepEqual(gotStatuses, wantStatuses) {
		t.Fatalf("close mixed terminal tree statuses = %#v; want %#v", gotStatuses, wantStatuses)
	}
	var closedEvents int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND type = 'session.thread_status_idle'
		    AND payload_json::jsonb #>> '{stop_reason,type}' = 'closed_for_runtime'`,
		sessionID,
	).Scan(&closedEvents); err != nil {
		t.Fatalf("count mixed terminal tree close events: %v", err)
	}
	if closedEvents != 1 {
		t.Fatalf("mixed terminal tree close events = %d; want only the running descendant transition", closedEvents)
	}
	replay, err := store.MarkChildThreadClosed(context.Background(), request)
	if err != nil {
		t.Fatalf("MarkChildThreadClosed mixed terminal tree replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		!proto.Equal(response.GetDeclaration(), replay.GetDeclaration()) {
		t.Fatalf("close mixed terminal tree replay = %+v; want exact duplicate declaration", replay)
	}
	const escapedChildID = "thr_bridge_close_terminal_tree_escaped"
	seedBridgeAPIEvent(t, admin, "default", sessionID, failedRootID, "sevt_bridge_close_terminal_tree_late_spawn", 2, "agent.tool_use", `{}`)
	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ParentThreadId:       failedRootID,
		ChildThreadId:        escapedChildID,
		Role:                 "subagent",
		TaskName:             "escaped-child",
		AgentType:            "worker",
		SourceToolUseEventId: "sevt_bridge_close_terminal_tree_late_spawn",
		ForkTurns:            "none",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(
			t,
			sessionID,
			"msg_bridge_close_terminal_tree_late_seed",
			"late seed",
			failedRootID,
			"sevt_bridge_close_terminal_tree_late_spawn",
			"none",
		),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("create below preserved failed root err = %v; want FailedPrecondition", err)
	}
	var escapedChildCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_threads
		  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID,
		escapedChildID,
	).Scan(&escapedChildCount); err != nil {
		t.Fatalf("count child below preserved failed root: %v", err)
	}
	if escapedChildCount != 0 {
		t.Fatalf("children created below preserved failed root = %d; want 0", escapedChildCount)
	}
}

func TestPostgreSQLBridgeAPIStoreCloseAndConcurrentChildCreateSerializeAtTheParent(t *testing.T) {
	type operationResult struct {
		operation string
		err       error
	}
	for _, test := range []struct {
		name        string
		createFirst bool
	}{
		{name: "create first is included in close", createFirst: true},
		{name: "close first rejects create", createFirst: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_bridge_close_create_" + suffix
			mainID := "thr_bridge_close_create_main_" + suffix
			parentID := "thr_bridge_close_create_parent_" + suffix
			createdID := "thr_bridge_close_create_new_" + suffix
			bindingID := "bind_bridge_close_create_" + suffix
			podUID := "pod_bridge_close_create_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, mainID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, parentID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "sevt_bridge_close_create_"+suffix, 1, "agent.tool_use", `{}`)
			scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
			createRequest := &bridgev1.CreateChildThreadRequest{
				Scope:                scope,
				ParentThreadId:       parentID,
				ChildThreadId:        createdID,
				Role:                 "subagent",
				TaskName:             "concurrent-child",
				AgentType:            "worker",
				SourceToolUseEventId: "sevt_bridge_close_create_" + suffix,
				ForkTurns:            "none",
				ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(
					t,
					sessionID,
					"msg_bridge_close_create_seed_"+suffix,
					"concurrent seed",
					parentID,
					"sevt_bridge_close_create_"+suffix,
					"none",
				),
			}
			closeRequest := &bridgev1.MarkChildThreadClosedRequest{
				Scope:         scope,
				ChildThreadId: parentID,
				ClosedAt:      "2026-01-01T00:00:05Z",
				Source: seedBridgeAPIChildLifecycleToolSource(
					t,
					admin,
					sessionID,
					mainID,
					"evt_bridge_close_create_command_"+suffix,
				),
			}
			blocker, blockerPID := lockPostgreSQLFinalizationFence(t, admin,
				`SELECT id FROM session_threads
				  WHERE workspace_id=$1 AND session_id=$2 AND id=$3
				  FOR UPDATE`,
				"default",
				sessionID,
				parentID,
			)
			results := make(chan operationResult, 2)
			startCreate := func() {
				store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
				go func() {
					_, err := store.CreateChildThread(context.Background(), createRequest)
					results <- operationResult{operation: "create", err: err}
				}()
			}
			startClose := func() {
				store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
				go func() {
					_, err := store.MarkChildThreadClosed(context.Background(), closeRequest)
					results <- operationResult{operation: "close", err: err}
				}()
			}
			if test.createFirst {
				startCreate()
			} else {
				startClose()
			}
			waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
			if test.createFirst {
				startClose()
			} else {
				startCreate()
			}
			waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
			if err := blocker.Commit(); err != nil {
				t.Fatalf("release close/create parent lock: %v", err)
			}

			outcomes := map[string]error{}
			for range 2 {
				outcome := <-results
				outcomes[outcome.operation] = outcome.err
			}
			if outcomes["close"] != nil {
				t.Fatalf("concurrent close: %v", outcomes["close"])
			}
			if test.createFirst {
				if outcomes["create"] != nil {
					t.Fatalf("create-first concurrent create: %v", outcomes["create"])
				}
				var parentStatus, createdStatus string
				if err := admin.QueryRowContext(context.Background(),
					`SELECT parent.status, child.status
					   FROM session_threads parent
					   JOIN session_threads child
					     ON child.workspace_id=parent.workspace_id
					    AND child.session_id=parent.session_id
					    AND child.parent_thread_id=parent.id
					  WHERE parent.workspace_id='default'
					    AND parent.session_id=$1
					    AND parent.id=$2
					    AND child.id=$3`,
					sessionID,
					parentID,
					createdID,
				).Scan(&parentStatus, &createdStatus); err != nil {
					t.Fatalf("read create-first close result: %v", err)
				}
				if parentStatus != "closed_for_runtime" || createdStatus != "closed_for_runtime" {
					t.Fatalf("create-first statuses = %q/%q; want both closed_for_runtime", parentStatus, createdStatus)
				}
				return
			}
			if status.Code(outcomes["create"]) != codes.FailedPrecondition {
				t.Fatalf("close-first concurrent create err = %v; want FailedPrecondition", outcomes["create"])
			}
			var createdCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_threads
				  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
				sessionID,
				createdID,
			).Scan(&createdCount); err != nil {
				t.Fatalf("count close-first child: %v", err)
			}
			if createdCount != 0 {
				t.Fatalf("close-first created rows = %d; want 0", createdCount)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreMarkChildThreadActiveOnlyReopensClosedThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_child_resume_guard"
		mainID    = "thr_bridge_child_resume_guard_main"
		childID   = "thr_bridge_child_resume_guard_child"
		bindingID = "bind_bridge_child_resume_guard"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_bridge_child_resume_guard")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status = 'running' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID); err != nil {
		t.Fatalf("mark child running: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, "pod_uid_bridge_child_resume_guard")
	firstResumeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_child_resume_guard_first")
	activeResponse, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
		Scope: scope, ChildThreadId: childID, ActiveAt: "2026-01-01T00:01:00Z",
		Source: firstResumeSource,
	})
	if err != nil {
		t.Fatalf("MarkChildThreadActive running child: %v", err)
	}
	if len(activeResponse.GetDeclaration().GetReceipts()) != 1 ||
		activeResponse.GetDeclaration().GetReceipts()[0].GetSessionThreadId() != childID ||
		activeResponse.GetDeclaration().GetReceipts()[0].GetChildLifecycle()[0].GetDisposition() != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_ACTIVE {
		t.Fatalf("already-active receipt = %+v; want one target-scoped ALREADY_ACTIVE receipt", activeResponse.GetDeclaration())
	}
	var childStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID).Scan(&childStatus); err != nil {
		t.Fatalf("read running child status: %v", err)
	}
	if childStatus != "running" {
		t.Fatalf("running child status after resume = %q; want running", childStatus)
	}

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status = 'closed_for_runtime' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID); err != nil {
		t.Fatalf("mark child closed_for_runtime: %v", err)
	}
	secondResumeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_child_resume_guard_second")
	if _, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
		Scope: scope, ChildThreadId: childID, ActiveAt: "2026-01-01T00:02:00Z",
		Source: secondResumeSource,
	}); err != nil {
		t.Fatalf("MarkChildThreadActive closed child: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID).Scan(&childStatus); err != nil {
		t.Fatalf("read reopened child status: %v", err)
	}
	if childStatus != "idle" {
		t.Fatalf("closed child status after resume = %q; want idle", childStatus)
	}
}

func TestPostgreSQLBridgeAPIStoreMarkChildThreadActivePreservesTerminalThread(t *testing.T) {
	for _, testCase := range []struct {
		status      string
		disposition bridgev1.ChildLifecycleDisposition
	}{
		{status: "failed", disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED},
		{status: "terminated", disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED},
	} {
		t.Run(testCase.status, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_resume_" + testCase.status
			mainID := "thr_bridge_resume_" + testCase.status + "_main"
			childID := "thr_bridge_resume_" + testCase.status + "_child"
			bindingID := "bind_bridge_resume_" + testCase.status
			seedBridgeAPISession(t, admin, "default", sessionID, mainID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_bridge_resume_terminal")
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_threads
				    SET status = $3
				  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
				sessionID,
				childID,
				testCase.status,
			); err != nil {
				t.Fatalf("mark child %s: %v", testCase.status, err)
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			source := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_resume_"+testCase.status)
			response, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
				Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, "pod_bridge_resume_terminal"),
				ChildThreadId: childID,
				ActiveAt:      "2026-01-01T00:01:00Z",
				Source:        source,
			})
			if err != nil {
				t.Fatalf("MarkChildThreadActive %s child: %v", testCase.status, err)
			}
			stamps := response.GetDeclaration().GetReceipts()[0].GetChildLifecycle()
			if len(stamps) != 1 || stamps[0].GetDisposition() != testCase.disposition {
				t.Fatalf("resume %s disposition = %+v; want %s", testCase.status, stamps, testCase.disposition)
			}
			var childStatus string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status FROM session_threads
				  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
				sessionID,
				childID,
			).Scan(&childStatus); err != nil {
				t.Fatalf("read %s child status: %v", testCase.status, err)
			}
			if childStatus != testCase.status {
				t.Fatalf("resume %s child status = %q; want unchanged", testCase.status, childStatus)
			}
			var statusEvents int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default'
				    AND session_id = $1
				    AND session_thread_id = $2
				    AND type = 'session.thread_status_idle'`,
				sessionID,
				childID,
			).Scan(&statusEvents); err != nil {
				t.Fatalf("count resume %s status events: %v", testCase.status, err)
			}
			if statusEvents != 0 {
				t.Fatalf("resume %s status events = %d; want 0", testCase.status, statusEvents)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreReviewerLifecycleSourceRequiresReviewerThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_reviewer_lifecycle_role"
		mainID    = "thr_bridge_reviewer_lifecycle_role_main"
		reviewID  = "arvw_bridge_reviewer_lifecycle_role"
		bindingID = "bind_bridge_reviewer_lifecycle_role"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, "pod_uid_bridge_reviewer_lifecycle_role")
	childID := approvalReviewerSidecarThreadID(scope, mainID, reviewID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_bridge_reviewer_lifecycle_role")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))

	_, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         scope,
		ChildThreadId: childID,
		ClosedAt:      "2026-01-01T00:01:00Z",
		Source:        &bridgev1.ChildLifecycleSource{Identity: &bridgev1.ChildLifecycleSource_ReviewerReviewId{ReviewerReviewId: reviewID}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reviewer lifecycle source on subagent role err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreToolLifecycleSourceRequiresDirectSubagentChild(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_tool_lifecycle_owner"
		mainID    = "thr_bridge_tool_lifecycle_owner_main"
		parentID  = "thr_bridge_tool_lifecycle_owner_parent"
		targetID  = "thr_bridge_tool_lifecycle_owner_target"
		bindingID = "bind_bridge_tool_lifecycle_owner"
		podUID    = "pod_bridge_tool_lifecycle_owner"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, targetID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	source := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_tool_lifecycle_owner")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))

	_, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: targetID,
		ClosedAt:      "2026-01-01T00:01:00Z",
		Source:        source,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("tool lifecycle source on another parent's child err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreSharedChildStatusWriterPreservesClosedThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_closed_wins"
		mainID    = "thr_bridge_closed_wins_main"
		childID   = "thr_bridge_closed_wins_child"
		bindingID = "bind_bridge_closed_wins"
		podUID    = "pod_bridge_closed_wins"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status='closed_for_runtime'
		  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		sessionID,
		childID,
	); err != nil {
		t.Fatalf("close child fixture: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	childScope := bridgeAPIScope(sessionID, childID, bindingID, 1, podUID)
	if err := store.withScopeTx(context.Background(), childScope, "test.closed_wins", func(tx *dbconnect.Tx) error {
		return updateChildThreadStatusTx(
			context.Background(),
			tx,
			childScope,
			"running",
			time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC),
		)
	}); err != nil {
		t.Fatalf("shared status write after close: %v", err)
	}
	var got string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		sessionID,
		childID,
	).Scan(&got); err != nil {
		t.Fatalf("read child status: %v", err)
	}
	if got != "closed_for_runtime" {
		t.Fatalf("child status after shared writer = %q; want closed_for_runtime", got)
	}
}
