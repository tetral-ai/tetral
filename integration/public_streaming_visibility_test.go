package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

func TestPostgreSQLPublicStreamingVisibility(t *testing.T) {
	t.Run("actual-gateway-produces-only-primary-preview", publicProjectionGatewayVisibility)
	t.Run("scoped-main-child-reviewer-compaction-and-private-content", func(t *testing.T) {
		f := newPublicProjectionFixture(t, publicProjectionOptions{})
		setup := f.start(t, "", "")
		child := f.child(t, setup)
		compactionChild := f.child(t, setup)
		f.end(t, setup)
		reviewer := f.reviewer(t)
		f.open(t, "session", []string{"agent.message", "agent.thinking"}, "")
		f.open(t, "ordinary", nil, "")
		f.open(t, "main-thread", nil, f.thread(t))
		f.open(t, "child", nil, child)
		if f.metric(t, "event_stream_preview_viewers") != 1 || f.metric(t, "event_stream_preview_subscriptions") != 1 {
			t.Fatal("formal Thread or Session viewer subscribed to previews")
		}
		rp := f.start(t, "", "")
		rc := f.start(t, child, "")
		ri := f.start(t, reviewer, runtimecontrol.RequestKindApprovalReviewer)
		concurrentCompaction := f.start(t, compactionChild, runtimecontrol.RequestKindCompactionSummary)
		mp := f.text(t, rp, "alpha βeta omega\n", true)
		mc := f.text(t, rc, "public-child-text", false)
		mi := f.text(t, ri, "hidden-reviewer-text", false)
		thinking := f.thinking(t, rp)
		f.tool(t, rp, "read", `{"path":"private-tool-input-marker"}`)
		// The originals cross native NATS. Ineligible scopes are deliberate
		// receiver-negative injections, never fabricated Bridge receipts.
		for _, pair := range []struct {
			r     *publicProjectionRequest
			event string
		}{{rp, mp}, {rc, mc}, {ri, mi}} {
			f.publish(t, f.frame(pair.r, "request_open", "", "", 0, ""), f.frame(pair.r, "event_start", pair.event, "agent.message", 0, ""), f.frame(pair.r, "event_delta", pair.event, "agent.message", 1, "alpha "))
		}
		f.publish(t, f.frame(rp, "event_start", thinking, "agent.thinking", 0, ""))
		compactionPreviewID := id.New("evt_")
		f.publish(t, f.frame(concurrentCompaction, "request_open", "", "", 0, ""), f.frame(concurrentCompaction, "event_start", compactionPreviewID, "agent.message", 0, ""), f.frame(concurrentCompaction, "event_delta", compactionPreviewID, "agent.message", 1, "hidden-compaction-text"))
		f.waitEvent(t, "session", "event_delta", 1)
		result := f.fence(t, "session", rp)
		for _, event := range result.Events {
			if publicEventType(event) == "event_start" {
				body, _ := event["event"].(map[string]any)
				if publicEventID(body) != mp {
					t.Fatal("ineligible/closed event received preview start")
				}
			}
		}
		if countPublicEvents(result, "event_delta") != 1 {
			t.Fatal("non-primary preview delivered")
		}
		f.end(t, rc)
		f.end(t, ri)
		f.compact(t, concurrentCompaction, "hidden-compaction-text")
		var summary string
		if err := f.db.QueryRow(`SELECT data_json::jsonb->'parts'->0->>'text' FROM session_messages WHERE session_id=$1 AND session_thread_id=$2 AND kind='compaction'`, f.session, compactionChild).Scan(&summary); err != nil || summary != "hidden-compaction-text" {
			t.Fatal("concurrent compaction summary lacked exact durable private context")
		}
		f.end(t, rp)
		for _, viewer := range []string{"session", "ordinary", "main-thread"} {
			result := f.waitEvent(t, viewer, "span.model_request_end", 1)
			f.assertFormal(t, result, []string{"alpha βeta omega\n"})
			if viewer != "session" {
				assertPublicPreviewShapes(t, result, nil, nil)
			}
			publicProjectionNoPrivateReasoning(t, result.Events)
		}
		childResult := f.waitEvent(t, "child", "span.model_request_end", 1)
		f.assertFormal(t, childResult, []string{"public-child-text"})
		assertPublicPreviewShapes(t, childResult, nil, nil)
		// The existing child cross-post contract is asserted positively, separately
		// from the hidden child generated-text/request events.
		var crosspost string
		if err := f.db.QueryRow(`SELECT event_id FROM session_events WHERE session_id=$1 AND session_thread_id=$2 AND type='agent.thread_message_received' AND session_visible=TRUE`, f.session, child).Scan(&crosspost); err != nil {
			t.Fatal("actual child lifecycle lacked public cross-post")
		}
		ids := publicProjectionAllPages(t, f, "", "asc", false)
		if !publicProjectionContains(ids, crosspost) || publicProjectionContains(ids, mc) || publicProjectionContains(ids, mi) {
			t.Fatal("Session formal visibility violated child cross-post contract")
		}
		for _, thread := range []string{"", child, f.thread(t)} {
			publicProjectionAssertListPrivacy(t, f, thread)
		}
		var private string
		if err := f.db.QueryRow(`SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m WHERE session_id=$1 AND model_request_id=$2 AND kind='assistant'`, f.session, rp.id).Scan(&private); err != nil {
			t.Fatal(err)
		}
		var body struct {
			Parts []map[string]any `json:"parts"`
		}
		if json.Unmarshal([]byte(private), &body) != nil {
			t.Fatal("private context decode")
		}
		found := false
		for _, part := range body.Parts {
			if part["type"] == "reasoning" {
				metadata, _ := json.Marshal(part["providerMetadata"])
				if part["text"] != "private-reasoning-marker" || string(metadata) != `{"anthropic":{"signature":"private-provider-signature-marker"}}` {
					t.Fatal("durable reasoning/metadata differs from fixed vector")
				}
				found = true
			}
		}
		if !found {
			t.Fatal("content-free thinking lacked committed private reasoning proof")
		}
		frames, _ := json.Marshal(f.tap.snapshot())
		for _, marker := range []string{"private-reasoning-marker", "private-provider-signature-marker", "private-tool-input-marker"} {
			if strings.Contains(string(frames), marker) {
				t.Fatal("private context/tool input escaped into NATS preview")
			}
		}
		// Compaction uses the actual owning transaction, with an independently
		// acknowledged Start and private committed summary, after RP has ended.
		compaction := f.start(t, "", runtimecontrol.RequestKindCompactionSummary)
		fake := id.New("evt_")
		f.publish(t, f.frame(compaction, "request_open", "", "", 0, ""), f.frame(compaction, "event_start", fake, "agent.message", 0, ""), f.frame(compaction, "event_delta", fake, "agent.message", 1, "hidden-compaction-text"))
		before := countPublicEvents(f.snapshot(t, "session"), "event_delta")
		f.compact(t, compaction, "hidden-compaction-text")
		f.waitEvent(t, "session", "span.model_request_end", 2)
		if countPublicEvents(f.snapshot(t, "session"), "event_delta") != before {
			t.Fatal("compaction preview admitted")
		}
		publicProjectionAssertListPrivacy(t, f, "")
		publicLogAssertion(t, "read-only-scoped-visibility-original-child-crossposts-private-context")
	})
	t.Run("exact-descriptor-scope-correlation-and-ended-guards", func(t *testing.T) {
		f := newPublicProjectionFixture(t, publicProjectionOptions{})
		f.open(t, "authorized", []string{"agent.message"}, "")
		r := f.start(t, "", "")
		m := f.text(t, r, "alpha βeta omega\n", false)
		descriptor, err := f.baseReader.ReadPreviewRequest(f.ctx, workspace.DefaultID, f.session, r.scope.SessionThreadId, r.id, r.start)
		if err != nil || !descriptor.IsPrimaryThread || descriptor.ThreadRole != "main" || descriptor.ThreadVisibility != "public" || descriptor.RequestKind != runtimecontrol.RequestKindAgentProviderRequest || descriptor.Ended {
			t.Fatalf("actual read-only descriptor: %+v %v", descriptor, err)
		}
		for _, variant := range []string{"wrong-workspace", "wrong-session", "wrong-thread", "wrong-model", "wrong-start", "nonexistent"} {
			t.Run(variant, func(t *testing.T) {
				frame := f.frame(r, "request_open", "", "", 0, "")
				switch variant {
				case "wrong-workspace":
					frame.WorkspaceID = "foreign"
				case "wrong-session":
					frame.SessionID = id.New("sesn_")
				case "wrong-thread":
					frame.ThreadID = id.New("thread_")
				case "wrong-model":
					frame.ModelRequestID = id.New("mreq_")
				case "wrong-start":
					frame.ModelRequestStartEventID = m
				case "nonexistent":
					frame.ModelRequestStartEventID = id.New("evt_")
				}
				if _, err := f.baseReader.ReadPreviewRequest(f.ctx, workspace.ID(frame.WorkspaceID), frame.SessionID, frame.ThreadID, frame.ModelRequestID, frame.ModelRequestStartEventID); err == nil {
					t.Fatal("read-only SQL admitted wrong exact scope/correlation")
				}
				if variant != "wrong-workspace" && variant != "wrong-session" {
					f.publish(t, frame)
				}
			})
		}
		before := f.metric(t, "event_stream_preview_invalid_frames_total")
		mismatch := f.frame(r, "request_open", "", "", 0, "")
		mismatch.WorkspaceID = "foreign"
		data, _ := json.Marshal(mismatch)
		if err = f.publisher.Publish(eventwire.PreviewSubject("default", f.session), data); err != nil {
			t.Fatal(err)
		}
		if err = f.publisher.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		publicWait(t, "subject/payload mismatch rejected at real subscriber", func() bool { return f.metric(t, "event_stream_preview_invalid_frames_total") > before })
		result := f.fence(t, "authorized", r)
		if countPublicEvents(result, "event_start") != 0 {
			t.Fatal("invalid descriptor invented preview admission")
		}
		f.publish(t, f.frame(r, "request_open", "", "", 0, ""), f.frame(r, "event_start", m, "agent.message", 0, ""), f.frame(r, "event_delta", m, "agent.message", 1, "alpha "))
		f.waitEvent(t, "authorized", "event_delta", 1)
		end := f.end(t, r)
		f.waitEvent(t, "authorized", "span.model_request_end", 1)
		descriptor, err = f.baseReader.ReadPreviewRequest(f.ctx, workspace.DefaultID, f.session, r.scope.SessionThreadId, r.id, r.start)
		if err != nil || !descriptor.Ended {
			t.Fatal("exact SQL descriptor did not observe durable End")
		}
		for _, scope := range []eventstream.ReadScope{{WorkspaceID: "foreign", SessionID: f.session}, {WorkspaceID: workspace.DefaultID, SessionID: id.New("sesn_")}, {WorkspaceID: workspace.DefaultID, SessionID: f.session, ThreadID: id.New("thread_")}} {
			if _, err = f.baseReader.ListRequestFinalMessages(f.ctx, scope, end, 0, 1); err == nil {
				t.Fatal("cross-scope End expanded")
			}
		}
		publicLogAssertion(t, "exact-SQL-descriptor-and-subject-payload-rejection-healthy-control")
	})
	t.Run("unauthorized-viewers-never-subscribe", func(t *testing.T) {
		f := newPublicProjectionFixture(t, publicProjectionOptions{})
		foreign := publicProjectionForeignKey(t, f)
		for _, key := range []string{foreign, "invalid-api-key-control"} {
			code, _ := f.raw(t, http.MethodGet, "/v1/sessions/"+f.session+"/events/stream?beta=true&event_deltas[]=agent.message", key)
			if code != 401 && code != 404 {
				t.Fatalf("unauthorized stream status=%d", code)
			}
		}
		reviewer := f.reviewer(t)
		code, _ := f.raw(t, http.MethodGet, "/v1/sessions/"+f.session+"/threads/"+reviewer+"/stream?beta=true", "")
		if code != 404 {
			t.Fatalf("internal reviewer stream status=%d", code)
		}
		if f.metric(t, "event_stream_preview_viewers") != 0 || f.metric(t, "event_stream_preview_subscriptions") != 0 {
			t.Fatal("unauthorized/internal scope subscribed")
		}
		f.open(t, "positive", []string{"agent.message"}, "")
		if f.metric(t, "event_stream_preview_subscriptions") != 1 {
			t.Fatal("authorized positive control failed to subscribe")
		}
	})
	t.Run("actual-session-delete-closes-session-and-thread-viewers", func(t *testing.T) {
		f := newPublicProjectionFixture(t, publicProjectionOptions{})
		setup := f.start(t, "", "")
		child := f.child(t, setup)
		f.end(t, setup)
		f.open(t, "session", []string{"agent.message"}, "")
		f.open(t, "thread", nil, f.thread(t))
		f.open(t, "child", nil, child)
		code, _ := f.raw(t, http.MethodDelete, "/v1/sessions/"+f.session+"?beta=true", "")
		if code != 200 {
			t.Fatalf("actual Session lifecycle delete status=%d", code)
		}
		publicWait(t, "SDK viewers end after actual deletion", func() bool {
			return f.snapshot(t, "session").Ended && f.snapshot(t, "thread").Ended && f.snapshot(t, "child").Ended
		})
		publicWait(t, "deletion releases preview subscription", func() bool {
			return f.metric(t, "event_stream_preview_viewers") == 0 && f.metric(t, "event_stream_preview_subscriptions") == 0 && f.metric(t, "event_stream_preview_pending_bytes") == 0
		})
		if _, err := f.baseReader.CurrentStreamPosition(f.ctx, workspace.DefaultID, f.session); err == nil {
			t.Fatal("deleted Session remained readable")
		}
		publicLogAssertion(t, "actual-lifecycle-deletion-readability-and-cleanup")
	})
}

// This bounded variant executes the real Gateway, provider SDK, native NATS,
// Bridge and public SDK. Bridge writes deliberately replace Runtime consumption;
// the Identity root owns the separate Runtime mapping proof.
func publicProjectionGatewayVisibility(t *testing.T) {
	f := newPublicProjectionFixture(t, publicProjectionOptions{})
	setup := f.start(t, "", "")
	child, compactChild := f.child(t, setup), f.child(t, setup)
	f.end(t, setup)
	reviewer := f.reviewer(t)
	f.open(t, "primary", []string{"agent.message", "agent.thinking"}, "")
	f.open(t, "child", nil, child)
	requests := []*publicProjectionRequest{f.start(t, "", ""), f.start(t, child, ""), f.start(t, reviewer, runtimecontrol.RequestKindApprovalReviewer), f.start(t, compactChild, runtimecontrol.RequestKindCompactionSummary)}
	keyCtx, cancelKey := context.WithTimeout(f.ctx, 35*time.Second)
	defer cancelKey()
	command := exec.CommandContext(keyCtx, "bun", "scripts/platform-key.ts", "insert", "--provider", "anthropic", "--key-id", "pfk_public_projection", "--cache-scope", "projection")
	command.Dir = "../services/gateway"
	command.Env = append(os.Environ(), "TETRAL_DATABASE_URL="+storagetest.AdminDatabaseURL(t, f.db), "ENGINE_VAULT_KEY="+sdkIntegrationVaultKey)
	command.Stdin = strings.NewReader("projection-fixture-provider-key")
	if err := command.Run(); err != nil {
		t.Fatal("actual platform key CLI failed")
	}
	f.gateway = startContentGatewayChildContext(f.ctx, t, map[string]any{"scenario": "public-text", "followupScenario": "public-text", "bindingKey": "projection-test-binding-hmac-key-32", "runtimePodUid": f.scope.Binding.TargetPodUid, "databaseUrl": storagetest.RuntimeDatabaseURL(t, f.pools.OpenWorkload(t, "provider_gateway", nil)), "previewNats": publicPublisherConfig(f.broker), "holdPreviewFragments": true, "holdFinish": true, "holdEveryRequest": true})
	connection, err := grpc.NewClient(f.gateway.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := providergatewayv1.NewProviderGatewayServiceClient(connection)
	publicWait(t, "actual Gateway native publisher connected", func() bool { return publicProjectionGatewayMetric(t, f.gateway, "connected") == 1 })
	initialAttempts := publicProjectionGatewayMetric(t, f.gateway, "attempted_total")
	if initialAttempts != 0 {
		t.Fatal("Gateway offered previews before any admitted request")
	}
	var primaryIDs, childIDs []string
	for index, request := range requests {
		token, err := f.bridge.RefreshRuntimeBindingToken(f.ctx, &bridgev1.RefreshRuntimeBindingTokenRequest{Scope: request.scope})
		if err != nil || token.GetRuntimeBindingToken() == "" {
			t.Fatalf("actual Bridge binding refresh failed: %v", err)
		}
		kind := providergatewayv1.ProviderRequestKind_PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST
		role := providergatewayv1.ProviderThreadRole_PROVIDER_THREAD_ROLE_MAIN
		visibility := providergatewayv1.ProviderThreadVisibility_PROVIDER_THREAD_VISIBILITY_PUBLIC
		var storedRole, storedVisibility string
		if err := f.db.QueryRow(`SELECT role,visibility FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3`, request.scope.WorkspaceId, f.session, request.scope.SessionThreadId).Scan(&storedRole, &storedVisibility); err != nil {
			t.Fatal(err)
		}
		switch storedRole {
		case "subagent":
			role = providergatewayv1.ProviderThreadRole_PROVIDER_THREAD_ROLE_SUBAGENT
		case "approval_reviewer":
			role = providergatewayv1.ProviderThreadRole_PROVIDER_THREAD_ROLE_APPROVAL_REVIEWER
		case "main":
		default:
			t.Fatalf("unexpected legal thread role %q", storedRole)
		}
		if storedVisibility == "internal" {
			visibility = providergatewayv1.ProviderThreadVisibility_PROVIDER_THREAD_VISIBILITY_INTERNAL
		} else if storedVisibility != "public" {
			t.Fatalf("unexpected legal thread visibility %q", storedVisibility)
		}
		switch request.kind {
		case runtimecontrol.RequestKindApprovalReviewer:
			kind = providergatewayv1.ProviderRequestKind_PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
		case runtimecontrol.RequestKindCompactionSummary:
			kind = providergatewayv1.ProviderRequestKind_PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY
		}
		input := &providergatewayv1.ProviderRequest{RequestId: id.New("req_"), ModelRequestId: request.id, ModelRequestStartEventId: request.start, RequestKind: kind, WorkspaceId: request.scope.WorkspaceId, SessionId: f.session, SessionThreadId: request.scope.SessionThreadId, BindingId: request.scope.Binding.BindingId, BindingGeneration: request.scope.Binding.BindingGeneration, RuntimeProcessId: request.scope.Binding.RuntimeProcessId, RuntimeBindingToken: token.GetRuntimeBindingToken(), OutputContractVersion: 2, ThreadRole: role, ThreadVisibility: visibility, Model: &providergatewayv1.ModelRef{ProviderId: "anthropic", ModelId: "claude-opus-4-8"}, System: []*providergatewayv1.SystemSegment{{Kind: providergatewayv1.SystemSegmentKind_SYSTEM_SEGMENT_KIND_BASE, Text: "Fixed projection provider input.", CacheHint: providergatewayv1.SystemCacheHint_SYSTEM_CACHE_HINT_NONE}}, Context: []*providergatewayv1.ProviderContextEntry{{Role: providergatewayv1.ProviderContextRole_PROVIDER_CONTEXT_ROLE_USER, Content: []*providergatewayv1.ProviderContextItem{{Value: &providergatewayv1.ProviderContextItem_Text{Text: &providergatewayv1.ProviderContextText{Text: "fixed projection input"}}}}}}, Limits: &providergatewayv1.ProviderRequestLimits{TimeoutMs: 30000}}
		if kind == providergatewayv1.ProviderRequestKind_PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER {
			schema := `{"type":"object","properties":{"decision":{"type":"string"}}}`
			input.OutputSchemaJson = &schema
		}
		before := publicProjectionGatewayMetric(t, f.gateway, "attempted_total")
		streamCtx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer fixture-runtime-token"), 30*time.Second)
		t.Cleanup(cancel)
		stream, err := client.StreamProviderRequest(streamCtx, input)
		if err != nil {
			t.Fatalf("actual Gateway RPC start: %v", err)
		}
		type collected struct {
			events []*providergatewayv1.ProviderStreamEvent
			err    error
		}
		completed := make(chan collected, 1)
		go func() {
			var result collected
			for {
				event, err := stream.Recv()
				if err != nil {
					if err != io.EOF {
						result.err = err
					}
					completed <- result
					return
				}
				result.events = append(result.events, event)
			}
		}()
		publicWait(t, "admitted Gateway request reaches provider fragment barrier", func() bool {
			select {
			case result := <-completed:
				t.Fatalf("Gateway finished before controlled provider input: %v (%d frames)", result.err, len(result.events))
			default:
			}
			observation := f.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
			var waiting int
			_ = json.Unmarshal(observation["fragmentWaiting"], &waiting)
			return waiting > 0
		})
		f.releaseFragments(t, 4)
		if index == 0 {
			f.waitEvent(t, "primary", "event_delta", 1)
		}
		f.finish(t)
		var output collected
		select {
		case output = <-completed:
		case <-streamCtx.Done():
			t.Fatal("actual Gateway complete-frame stream did not join")
		}
		cancel()
		if output.err != nil {
			t.Fatalf("actual Gateway stream failed: %v", output.err)
		}
		var texts []*providergatewayv1.ProviderTextCompletePayload
		var reasoning *providergatewayv1.ProviderReasoningCompletePayload
		var thinking string
		finished := false
		for _, event := range output.events {
			if event.GetProviderError() != nil {
				t.Fatal("actual Gateway emitted a provider error")
			}
			if value := event.GetThinkingStarted(); value != nil {
				thinking = value.EventId
			}
			if value := event.GetReasoningComplete(); value != nil {
				reasoning = value
			}
			if value := event.GetTextComplete(); value != nil {
				texts = append(texts, value)
			}
			finished = finished || event.GetFinish() != nil
		}
		if !finished || len(texts) != 2 || texts[0].Text != "alpha βeta omega\n" || texts[1].Text != "second\n" || reasoning == nil || reasoning.Text != "private-reasoning-marker" || reasoning.ThinkingEventId != thinking || !strings.Contains(reasoning.ProviderMetadataJson, "private-provider-signature-marker") {
			t.Fatal("admitted Gateway output differs from independent content/reasoning controls")
		}
		publicWait(t, "Gateway offers fully leave native publisher queue", func() bool { return publicProjectionGatewayMetric(t, f.gateway, "pending_frames") == 0 })
		after := publicProjectionGatewayMetric(t, f.gateway, "attempted_total")
		if index == 0 {
			if after <= initialAttempts {
				t.Fatal("primary positive control made no production preview offers")
			}
		} else if after != before {
			t.Fatalf("ineligible request kind=%s role=%s visibility=%s made production preview offers", request.kind, storedRole, storedVisibility)
		}
		if request.kind == runtimecontrol.RequestKindCompactionSummary {
			f.compact(t, request, texts[0].Text+texts[1].Text)
			continue
		}
		f.thinkingWithID(t, request, thinking)
		var ids []string
		for n, text := range texts {
			parts := sessionfixture.BridgeTextContextDeltaForTest(text.Text).Parts
			if n == 0 {
				parts = append([]*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: reasoning.Text, ProviderMetadataJson: bridgeString(reasoning.ProviderMetadataJson)}}}}, parts...)
			}
			ids = append(ids, f.textWithID(t, request, text.EventId, text.Text, parts))
		}
		f.end(t, request)
		switch index {
		case 0:
			primaryIDs = ids
		case 1:
			childIDs = ids
		}
	}
	primary := f.waitEvent(t, "primary", "span.model_request_end", 1)
	childResult := f.waitEvent(t, "child", "span.model_request_end", 1)
	f.assertFormal(t, primary, []string{"alpha βeta omega\n", "second\n"})
	f.assertFormal(t, childResult, []string{"alpha βeta omega\n", "second\n"})
	assertPublicPreviewShapes(t, childResult, nil, nil)
	for _, pair := range []struct {
		result publicSDKSnapshot
		ids    []string
	}{{primary, primaryIDs}, {childResult, childIDs}} {
		var ids []string
		for _, event := range pair.result.Events {
			if publicEventType(event) == "agent.message" {
				ids = append(ids, publicEventID(event))
			}
		}
		if !reflect.DeepEqual(ids, pair.ids) {
			t.Fatal("formal SDK output lost actual Gateway allocated identity")
		}
		publicProjectionNoPrivateReasoning(t, pair.result.Events)
	}
	frames := f.tap.snapshot()
	if len(frames) == 0 {
		t.Fatal("primary native NATS positive control missing")
	}
	for _, frame := range frames {
		if frame.ModelRequestID != requests[0].id {
			t.Fatal("ineligible Gateway request appeared in native NATS ledger")
		}
	}
	encoded, _ := json.Marshal(frames)
	if strings.Contains(string(encoded), "private-reasoning-marker") || strings.Contains(string(encoded), "private-provider-signature-marker") {
		t.Fatal("actual Gateway preview leaked reasoning or provider metadata")
	}
	observation := f.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
	var credentialStore string
	var providerCalls, platformSelections int
	_ = json.Unmarshal(observation["credentialStore"], &credentialStore)
	_ = json.Unmarshal(observation["providerCalls"], &providerCalls)
	_ = json.Unmarshal(observation["platformSelections"], &platformSelections)
	if credentialStore != "sql" || providerCalls != 4 || platformSelections != 4 {
		t.Fatal("producer proof did not execute all admitted requests with actual SQL credential selection")
	}
	publicLogAssertion(t, "actual-Gateway-primary-positive-child-reviewer-compaction-zero-offers-same-ID-Bridge-SDK")
}

func publicProjectionGatewayMetric(t *testing.T, gateway *contentGatewayChild, metric string) float64 {
	t.Helper()
	observation := gateway.control(t, map[string]any{"kind": "observe"}, "observation")
	var metrics string
	if json.Unmarshal(observation["previewMetrics"], &metrics) != nil {
		t.Fatal("actual Gateway preview metrics unavailable")
	}
	prefix := "providergateway_preview_" + metric + " "
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, prefix) {
			value, err := strconv.ParseFloat(strings.TrimPrefix(line, prefix), 64)
			if err != nil {
				t.Fatal("invalid actual Gateway preview metric")
			}
			return value
		}
	}
	t.Fatalf("missing actual Gateway preview metric %s", metric)
	return 0
}

func publicProjectionNoPrivateReasoning(t *testing.T, events []map[string]any) {
	t.Helper()
	body, _ := json.Marshal(events)
	for _, marker := range []string{"private-reasoning-marker", "private-provider-signature-marker", "hidden-reviewer-text", "hidden-compaction-text"} {
		if strings.Contains(string(body), marker) {
			t.Fatal("private reasoning/hidden content escaped into public projection")
		}
	}
	for _, event := range events {
		if publicEventType(event) == "agent.thinking" {
			if !reflect.DeepEqual(publicKeys(event), []string{"id", "processed_at", "type"}) {
				t.Fatal("thinking notification contains private body/metadata")
			}
		}
	}
}
func publicProjectionAssertListPrivacy(t *testing.T, f *publicProjectionFixture, thread string) {
	t.Helper()
	page := ""
	for i := 0; i < 128; i++ {
		result := f.list(t, thread, "asc", page)
		publicProjectionNoPrivateReasoning(t, result.Data)
		if result.Next == nil {
			return
		}
		page = *result.Next
	}
	t.Fatal("privacy list paging budget exceeded")
}
