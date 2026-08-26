package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestPostgreSQLGatewaySemanticTimeoutReschedulesAndLaterInputContinues(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_provider_timeout_production"
		threadID  = "sthr_provider_timeout_production"
		bindingID = "bind_provider_timeout_production"
		podUID    = "pod_provider_timeout_production"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := NewPostgreSQLBridgeAPIStore(client)
	store.RuntimeBindingTokenHMACKey = []byte("provider-timeout-production-signing-key")
	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, store)
	t.Cleanup(stopBridge)
	process := startProviderTimeoutRuntime(t, admin, bridgeAddress, sessionID, threadID, bindingID, podUID)
	firstCapturePending := make(chan string, 1)
	releaseFirstCapture := make(chan struct{})
	capturesSettled := make(chan error, 1)
	go func() {
		firstWriteID, generation, err := waitForPendingOutputCapture(admin, sessionID, "")
		if err != nil {
			capturesSettled <- err
			return
		}
		firstCapturePending <- firstWriteID
		<-releaseFirstCapture
		if err := settleOutputCaptureGenerationForTest(admin, sessionID, firstWriteID, generation, "staged"); err != nil {
			capturesSettled <- err
			return
		}
		secondWriteID, secondGeneration, err := waitForPendingOutputCapture(admin, sessionID, firstWriteID)
		if err == nil {
			err = settleOutputCaptureGenerationForTest(admin, sessionID, secondWriteID, secondGeneration, "staged")
		}
		capturesSettled <- err
	}()

	events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	appendMessage := func(idempotencyKey, text string) {
		t.Helper()
		appended, err := events.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, idempotencyKey,
			sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
				Type:    sessionevent.EventTypeUserMessage,
				Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: text}},
			}}})
		if err != nil || len(appended.Data) != 1 {
			t.Fatalf("append provider timeout input = %#v/%v", appended, err)
		}
	}

	appendMessage("idem_provider_timeout_first", "survive semantic timeout exhaustion")
	deliverAttachmentRuntimeInput(t, runtimeDB, admin, process.port, sessionID, podUID)
	waitForProviderFailureFacts(t, admin, sessionID, 2, "rescheduling", process)
	select {
	case <-firstCapturePending:
	case <-time.After(10 * time.Second):
		t.Fatal("provider timeout closeout did not create output capture custody")
	}

	appendMessage("idem_provider_timeout_second", "continue after the failed turn")
	deliverAttachmentRuntimeInput(t, runtimeDB, admin, process.port, sessionID, podUID)
	close(releaseFirstCapture)
	waitForProviderFailureFacts(t, admin, sessionID, 3, "idle", process)
	result := process.close(t)
	if captureErr := <-capturesSettled; captureErr != nil {
		t.Fatalf("settle provider timeout output captures: %v", captureErr)
	}
	if result.ProviderInvocations != 3 || result.FinishIdleInvocations != 2 || result.FinishIdleResult != "committed" {
		t.Fatalf("Gateway/provider and FinishIdle invocations/result = %d/%d/%s; want 3/2/committed",
			result.ProviderInvocations, result.FinishIdleInvocations, result.FinishIdleResult)
	}
	if len(result.ProviderRequestContexts) != 3 {
		t.Fatalf("captured provider request contexts = %d; want 3", len(result.ProviderRequestContexts))
	}
	for index, providerContext := range result.ProviderRequestContexts {
		if strings.Contains(providerContext, "failed partial") {
			t.Fatalf("provider request %d retained failed partial text: %s", index+1, providerContext)
		}
	}
	if !strings.Contains(result.ProviderRequestContexts[0], "survive semantic timeout exhaustion") ||
		!strings.Contains(result.ProviderRequestContexts[1], "survive semantic timeout exhaustion") {
		t.Fatalf("semantic retry lost the original pending input: %v", result.ProviderRequestContexts)
	}
	if !strings.Contains(result.ProviderRequestContexts[2], "survive semantic timeout exhaustion") ||
		!strings.Contains(result.ProviderRequestContexts[2], "continue after the failed turn") {
		t.Fatalf("post-reschedule provider request lost durable inputs: %s", result.ProviderRequestContexts[2])
	}

	var starts, ends, reschedules, errorEnds, userMessages, assistantMessages int
	var finalEndPayloads string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_rescheduled'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'
		  AND payload_json::jsonb->>'is_error'='true'),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND kind='user'),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND kind='assistant'),
		COALESCE((SELECT jsonb_agg(payload_json::jsonb ORDER BY sequence)::text FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),'[]')`, sessionID).
		Scan(&starts, &ends, &reschedules, &errorEnds, &userMessages, &assistantMessages, &finalEndPayloads); err != nil {
		t.Fatalf("read provider timeout production facts: %v", err)
	}
	if starts != 3 || ends != 3 || reschedules != 1 || errorEnds != 2 || userMessages != 2 || assistantMessages != 3 {
		t.Fatalf("provider timeout starts/ends/reschedules/errors/users/assistants = %d/%d/%d/%d/%d/%d; want 3/3/1/2/2/3; ends=%s",
			starts, ends, reschedules, errorEnds, userMessages, assistantMessages, finalEndPayloads)
	}
}

func TestPostgreSQLGatewaySemanticTimeoutWaitsForToolSettlementBeforeRetry(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_provider_timeout_tool"
		threadID  = "sthr_provider_timeout_tool"
		bindingID = "bind_provider_timeout_tool"
		podUID    = "pod_provider_timeout_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := NewPostgreSQLBridgeAPIStore(client)
	store.RuntimeBindingTokenHMACKey = []byte("provider-timeout-tool-signing-key")
	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, store)
	t.Cleanup(stopBridge)
	process := startProviderFailureRuntime(t, admin, bridgeAddress, sessionID, threadID, bindingID, podUID, "semantic_tool_route")

	captureSettled := make(chan error, 1)
	go func() {
		writeID, generation, err := waitForPendingOutputCapture(admin, sessionID, "")
		if err == nil {
			err = settleOutputCaptureGenerationForTest(admin, sessionID, writeID, generation, "staged")
		}
		captureSettled <- err
	}()

	events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	appended, err := events.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_provider_timeout_tool", sessionevent.AppendRequest{
		Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserMessage,
			Content: []sessionevent.ContentBlock{{
				Type: sessionevent.ContentBlockTypeText,
				Text: "read before the semantic timeout",
			}},
		}},
	})
	if err != nil || len(appended.Data) != 1 {
		t.Fatalf("append semantic Tool route input = %#v/%v", appended, err)
	}
	deliverAttachmentRuntimeInput(t, runtimeDB, admin, process.port, sessionID, podUID)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		state, readErr := readProviderFailureRuntimeState(process.statePath)
		var ends, toolUses, toolResults int
		dbErr := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use'),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result')`, sessionID).
			Scan(&ends, &toolUses, &toolResults)
		if readErr == nil && dbErr == nil && state.ProviderInvocations == 1 && state.ToolInvocations == 1 && ends == 1 && toolUses == 1 && toolResults == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	state, err := readProviderFailureRuntimeState(process.statePath)
	if err != nil || state.ProviderInvocations != 1 || state.ToolInvocations != 1 {
		t.Fatalf("pending semantic Tool state = %+v/%v; process=%s", state, err, process.output.String())
	}
	if err := os.WriteFile(process.toolReleasePath, []byte("release"), 0o600); err != nil {
		t.Fatalf("release semantic Tool execution: %v", err)
	}
	waitForProviderFailureFacts(t, admin, sessionID, 3, "idle", process)
	result := process.close(t)
	if captureErr := <-captureSettled; captureErr != nil {
		t.Fatalf("settle semantic Tool output capture: %v", captureErr)
	}
	if result.ProviderInvocations != 3 || result.ToolInvocations != 1 || len(result.ProviderRequestContexts) != 3 || result.ProviderStartedBeforeToolRelease {
		t.Fatalf("semantic Tool provider/tool/context counts = %d/%d/%d; want 3/1/3",
			result.ProviderInvocations, result.ToolInvocations, len(result.ProviderRequestContexts))
	}
	const callID = "call_semantic_tool_route"
	for requestIndex, providerContext := range result.ProviderRequestContexts[1:] {
		callIndex := strings.Index(providerContext, `"modelToolCallId":"`+callID+`","name":"Read"`)
		resultIndex := strings.Index(providerContext, `"modelToolCallId":"`+callID+`","completed"`)
		if callIndex < 0 || resultIndex <= callIndex || strings.Count(providerContext, callID) != 2 || !strings.Contains(providerContext, "semantic tool result") {
			t.Fatalf("provider context %d did not retain one ordered Tool pair: %s", requestIndex+2, providerContext)
		}
	}
}

func TestPostgreSQLProviderFailuresSettleOneTurnAndLaterInputContinues(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		scenario           string
		wantProviderCalls  int
		wantTurns          int
		wantRequestEnds    int
		wantReschedules    int
		wantErrorEnds      int
		wantAssistants     int
		wantOutputCaptures int
		wantFinishIdle     int
		wantKeySelections  []string
		wantPublicStatuses string
		wantPublicTypes    string
		wantPublicMessage  string
	}{
		{name: "platform billing before progress", scenario: "platform_billing_pre_progress", wantProviderCalls: 3, wantTurns: 2, wantRequestEnds: 2, wantAssistants: 2, wantKeySelections: []string{"pfk_provider_failure_bad", "pfk_provider_failure_healthy", "pfk_provider_failure_healthy"}},
		{name: "platform billing after progress", scenario: "platform_billing_post_progress", wantProviderCalls: 2, wantTurns: 2, wantRequestEnds: 2, wantErrorEnds: 1, wantAssistants: 1, wantKeySelections: []string{"pfk_provider_failure_bad", "pfk_provider_failure_healthy"}},
		{name: "platform billing exhausted", scenario: "platform_billing_exhausted", wantProviderCalls: 1, wantTurns: 2, wantRequestEnds: 2, wantErrorEnds: 2, wantKeySelections: []string{"pfk_provider_failure_bad"}, wantPublicStatuses: "exhausted,exhausted", wantPublicTypes: "model_overloaded_error,model_overloaded_error"},
		{name: "statusless transport", scenario: "statusless_transport", wantProviderCalls: 3, wantTurns: 2, wantRequestEnds: 3, wantReschedules: 1, wantErrorEnds: 2, wantAssistants: 1, wantPublicStatuses: "retrying,exhausted", wantPublicTypes: "model_request_failed_error,model_overloaded_error"},
		{name: "provider rate limited", scenario: "provider_rate_limited", wantProviderCalls: 2, wantTurns: 2, wantRequestEnds: 2, wantReschedules: 1, wantErrorEnds: 1, wantAssistants: 2, wantOutputCaptures: 1, wantFinishIdle: 1, wantPublicStatuses: "retrying", wantPublicTypes: "model_rate_limited_error"},
		{name: "invalid Kimi API key", scenario: "invalid_kimi_byok", wantProviderCalls: 2, wantTurns: 2, wantRequestEnds: 2, wantErrorEnds: 1, wantAssistants: 1, wantPublicStatuses: "exhausted", wantPublicTypes: "model_request_failed_error"},
		{name: "OpenAI OAuth refresh rejected", scenario: "invalid_openai_oauth", wantProviderCalls: 1, wantTurns: 2, wantRequestEnds: 2, wantErrorEnds: 1, wantAssistants: 1, wantPublicStatuses: "exhausted", wantPublicTypes: "model_request_failed_error", wantPublicMessage: "OpenAI OAuth credential refresh failed; re-authorization is required."},
		{name: "missing Kimi credential", scenario: "missing_kimi_credential", wantProviderCalls: 1, wantTurns: 2, wantRequestEnds: 2, wantErrorEnds: 1, wantAssistants: 1, wantPublicStatuses: "exhausted", wantPublicTypes: "model_request_failed_error", wantPublicMessage: "This provider requires an explicit session credential."},
		{name: "unavailable OpenAI credential", scenario: "unavailable_openai_credential", wantProviderCalls: 1, wantTurns: 2, wantRequestEnds: 2, wantErrorEnds: 1, wantAssistants: 1, wantPublicStatuses: "exhausted", wantPublicTypes: "model_request_failed_error", wantPublicMessage: "The selected provider credential is not usable."},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.wantOutputCaptures == 0 {
				testCase.wantOutputCaptures = testCase.wantTurns
			}
			if testCase.wantFinishIdle == 0 {
				testCase.wantFinishIdle = testCase.wantTurns
			}
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(testCase.scenario, "_", "")
			sessionID := "sesn_provider_failure_" + suffix
			threadID := "sthr_provider_failure_" + suffix
			bindingID := "bind_provider_failure_" + suffix
			podUID := "pod_provider_failure_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
			client := dbconnect.NewClientForTesting(runtimeDB)
			store := NewPostgreSQLBridgeAPIStore(client)
			store.RuntimeBindingTokenHMACKey = []byte("provider-failure-production-signing-key")
			bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, store)
			t.Cleanup(stopBridge)
			repairCredential := seedProviderFailureCredential(t, runtimeDB, admin, sessionID, testCase.scenario)
			activePodUID := podUID
			process := startProviderFailureRuntime(t, admin, bridgeAddress, sessionID, threadID, bindingID, podUID, testCase.scenario)
			priorProviderInvocations := 0
			priorFinishIdleInvocations := 0
			priorFinishIdleCommitted := true
			priorSensitiveLogLeak := false
			priorOAuthAccessTokenConsumed := false
			priorKeySelections := []string(nil)
			priorKeyQuarantines := []string(nil)

			firstCapturePending := make(chan string, 1)
			releaseFirstCapture := make(chan struct{})
			capturesSettled := make(chan error, 1)
			go func() {
				firstWriteID, generation, err := waitForPendingOutputCapture(admin, sessionID, "")
				if err != nil {
					capturesSettled <- err
					return
				}
				firstCapturePending <- firstWriteID
				<-releaseFirstCapture
				writeID := firstWriteID
				captureGeneration := generation
				for turn := 0; turn < testCase.wantOutputCaptures; turn++ {
					if turn > 0 {
						writeID, captureGeneration, err = waitForPendingOutputCapture(admin, sessionID, writeID)
						if err != nil {
							break
						}
					}
					err = settleOutputCaptureGenerationForTest(admin, sessionID, writeID, captureGeneration, "staged")
					if err != nil {
						break
					}
				}
				capturesSettled <- err
			}()

			events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
			appendMessage := func(idempotencyKey, text string) {
				t.Helper()
				appended, err := events.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, idempotencyKey,
					sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
						Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: text}},
					}}})
				if err != nil || len(appended.Data) != 1 {
					t.Fatalf("append %s provider failure input = %#v/%v", testCase.scenario, appended, err)
				}
			}

			appendMessage("idem_provider_failure_first_"+suffix, "fail only this provider turn")
			deliverAttachmentRuntimeInput(t, runtimeDB, admin, process.port, sessionID, podUID)
			select {
			case <-firstCapturePending:
			case <-time.After(10 * time.Second):
				t.Fatalf("%s closeout did not create output capture custody: %s", testCase.scenario, process.output.String())
			}
			close(releaseFirstCapture)
			firstTurnEnds := 1
			if testCase.scenario == "statusless_transport" {
				firstTurnEnds = 2
			}
			waitForProviderFailureFacts(t, admin, sessionID, firstTurnEnds, "idle", process)
			repairCredential()
			if testCase.scenario == "invalid_openai_oauth" {
				waitForProviderFailureIdleReceipt(t, process, 1)
				prior := process.close(t)
				priorProviderInvocations = prior.ProviderInvocations
				priorFinishIdleInvocations = prior.FinishIdleInvocations
				priorFinishIdleCommitted = prior.FinishIdleResult == "committed"
				priorSensitiveLogLeak = prior.SensitiveLogLeak
				priorOAuthAccessTokenConsumed = prior.OAuthAccessTokenConsumed
				priorKeySelections = prior.PlatformKeySelections
				priorKeyQuarantines = prior.PlatformKeyQuarantines
				if result, err := admin.ExecContext(context.Background(), `DELETE FROM session_runtime_bindings
					WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2 AND binding_generation=1`,
					sessionID, bindingID); err != nil {
					t.Fatalf("fence lost provider credential Runtime binding: %v", err)
				} else if deleted, _ := result.RowsAffected(); deleted != 1 {
					t.Fatalf("fenced lost provider credential Runtime bindings = %d; want 1", deleted)
				}
				const replacementGeneration int64 = 2
				replacementBindingID := bindingID + "_replacement"
				activePodUID = podUID + "_replacement"
				seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, replacementBindingID, replacementGeneration, activePodUID)
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status
					SET binding_id=$2, binding_generation=$3, updated_at=clock_timestamp()
					WHERE workspace_id='default' AND session_id=$1`, sessionID, replacementBindingID, replacementGeneration); err != nil {
					t.Fatalf("install replacement provider credential Runtime status: %v", err)
				}
				process = startProviderFailureRuntimeForBinding(t, admin, bridgeAddress, sessionID, threadID,
					replacementBindingID, replacementGeneration, activePodUID, testCase.scenario)
			}
			appendMessage("idem_provider_failure_second_"+suffix, "continue on the same session")
			deliverAttachmentRuntimeInput(t, runtimeDB, admin, process.port, sessionID, activePodUID)
			waitForProviderFailureFacts(t, admin, sessionID, testCase.wantRequestEnds, "idle", process)
			processFinishIdle := testCase.wantFinishIdle
			if testCase.scenario == "invalid_openai_oauth" {
				processFinishIdle = 1
			}
			waitForProviderFailureIdleReceipt(t, process, processFinishIdle)
			result := process.close(t)
			if captureErr := <-capturesSettled; captureErr != nil {
				t.Fatalf("settle %s output captures: %v", testCase.scenario, captureErr)
			}
			providerInvocations := priorProviderInvocations + result.ProviderInvocations
			finishIdleInvocations := priorFinishIdleInvocations + result.FinishIdleInvocations
			finishIdleCommitted := priorFinishIdleCommitted && result.FinishIdleResult == "committed"
			sensitiveLogLeak := priorSensitiveLogLeak || result.SensitiveLogLeak
			oauthAccessTokenConsumed := priorOAuthAccessTokenConsumed || result.OAuthAccessTokenConsumed
			keySelections := append(priorKeySelections, result.PlatformKeySelections...)
			keyQuarantines := append(priorKeyQuarantines, result.PlatformKeyQuarantines...)
			if providerInvocations != testCase.wantProviderCalls || finishIdleInvocations != testCase.wantFinishIdle || !finishIdleCommitted || sensitiveLogLeak {
				t.Fatalf("%s provider/FinishIdle/log result = %d/%d/%s/%v; want %d/%d/committed/false",
					testCase.scenario, providerInvocations, finishIdleInvocations, result.FinishIdleResult, sensitiveLogLeak,
					testCase.wantProviderCalls, testCase.wantFinishIdle)
			}
			if testCase.scenario == "invalid_openai_oauth" && !oauthAccessTokenConsumed {
				t.Fatal("replacement Runtime did not consume the repaired OAuth access token through the provider fetch boundary")
			}
			if !slices.Equal(keySelections, testCase.wantKeySelections) {
				t.Fatalf("%s platform key selections = %v; want %v", testCase.scenario, keySelections, testCase.wantKeySelections)
			}
			if strings.HasPrefix(testCase.scenario, "platform_billing_") && !slices.Equal(keyQuarantines, []string{"pfk_provider_failure_bad"}) {
				t.Fatalf("%s platform key quarantines = %v; want one bad-key quarantine", testCase.scenario, keyQuarantines)
			}

			var starts, ends, reschedules, errorEnds, users, assistants, publicErrors int
			var durablePayloads, publicRetryStatuses, publicErrorTypes, publicErrorMessages string
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_rescheduled'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true'),
				(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND kind='user'),
				(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND kind='assistant'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),
					COALESCE((SELECT string_agg(payload_json::jsonb #>> '{error,retry_status,type}', ',' ORDER BY sequence) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),''),
					COALESCE((SELECT string_agg(payload_json::jsonb #>> '{error,type}', ',' ORDER BY sequence) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),''),
					COALESCE((SELECT string_agg(payload_json::jsonb #>> '{error,message}', ',' ORDER BY sequence) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),''),
					COALESCE((SELECT string_agg(payload_json || ' ' || projection_json, ' ') FROM session_events WHERE workspace_id='default' AND session_id=$1),'') || ' ' ||
					COALESCE((SELECT string_agg(data_json, ' ') FROM session_messages WHERE workspace_id='default' AND session_id=$1),'')`, sessionID).
				Scan(&starts, &ends, &reschedules, &errorEnds, &users, &assistants, &publicErrors, &publicRetryStatuses, &publicErrorTypes, &publicErrorMessages, &durablePayloads); err != nil {
				t.Fatalf("read %s durable provider failure facts: %v", testCase.scenario, err)
			}
			if starts != testCase.wantRequestEnds || ends != testCase.wantRequestEnds || reschedules != testCase.wantReschedules ||
				errorEnds != testCase.wantErrorEnds || users != testCase.wantTurns || assistants != testCase.wantAssistants {
				t.Fatalf("%s starts/ends/reschedules/errors/users/assistants = %d/%d/%d/%d/%d/%d; want %d/%d/%d/%d/%d/%d",
					testCase.scenario, starts, ends, reschedules, errorEnds, users, assistants,
					testCase.wantRequestEnds, testCase.wantRequestEnds, testCase.wantReschedules, testCase.wantErrorEnds, testCase.wantTurns, testCase.wantAssistants)
			}
			for _, canary := range []string{
				"private-billing-canary",
				"statusless-private-canary",
				"private-byok-canary",
				"provider-failure-canary",
				"credential-unavailable-canary",
				"session-key-",
				"oauth-access-",
				"oauth-refresh-",
				"sk-provider-failure",
			} {
				if strings.Contains(durablePayloads, canary) {
					t.Fatalf("%s durable facts leaked provider-private detail %q", testCase.scenario, canary)
				}
			}
			if testCase.wantPublicStatuses != "" {
				if publicErrors == 0 {
					t.Fatalf("%s emitted no public session.error", testCase.scenario)
				}
				if publicRetryStatuses != testCase.wantPublicStatuses {
					t.Fatalf("%s public retry statuses = %q; want %q", testCase.scenario, publicRetryStatuses, testCase.wantPublicStatuses)
				}
				if publicErrorTypes != testCase.wantPublicTypes {
					t.Fatalf("%s public error types = %q; want %q", testCase.scenario, publicErrorTypes, testCase.wantPublicTypes)
				}
				if testCase.wantPublicMessage != "" && publicErrorMessages != testCase.wantPublicMessage {
					t.Fatalf("%s public error message = %q; want %q", testCase.scenario, publicErrorMessages, testCase.wantPublicMessage)
				}
			}
		})
	}
}

type providerFailureRuntimeProcess struct {
	command         *exec.Cmd
	output          bytes.Buffer
	port            int
	statePath       string
	closePath       string
	toolReleasePath string
}

func startProviderTimeoutRuntime(t *testing.T, admin *sql.DB, bridgeAddress, sessionID, threadID, bindingID, podUID string) *providerFailureRuntimeProcess {
	return startProviderFailureRuntime(t, admin, bridgeAddress, sessionID, threadID, bindingID, podUID, "semantic_timeout")
}

func startProviderFailureRuntime(t *testing.T, admin *sql.DB, bridgeAddress, sessionID, threadID, bindingID, podUID, scenario string) *providerFailureRuntimeProcess {
	return startProviderFailureRuntimeForBinding(t, admin, bridgeAddress, sessionID, threadID, bindingID, 1, podUID, scenario)
}

func startProviderFailureRuntimeForBinding(t *testing.T, admin *sql.DB, bridgeAddress, sessionID, threadID, bindingID string, bindingGeneration int64, podUID, scenario string) *providerFailureRuntimeProcess {
	t.Helper()
	tempDir := t.TempDir()
	readyPath := filepath.Join(tempDir, "ready.json")
	process := &providerFailureRuntimeProcess{
		statePath:       filepath.Join(tempDir, "state.json"),
		closePath:       filepath.Join(tempDir, "close"),
		toolReleasePath: filepath.Join(tempDir, "release-tool"),
	}
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": bridgeAddress, "workspaceId": workspace.DefaultID,
		"sessionId": sessionID, "sessionThreadId": threadID, "bindingId": bindingID,
		"bindingGeneration": bindingGeneration, "targetPodUid": podUID,
		"readyPath": readyPath, "statePath": process.statePath, "closePath": process.closePath,
		"toolReleasePath": process.toolReleasePath, "scenario": scenario,
	})
	if err != nil {
		t.Fatalf("encode provider failure composition: %v", err)
	}
	inputPath := filepath.Join(tempDir, "input.json")
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write provider failure composition: %v", err)
	}
	process.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/provider-failure-production-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	process.command.Dir = "../agent-runtime"
	var databaseSchema string
	if err := admin.QueryRowContext(context.Background(), `SELECT current_schema()`).Scan(&databaseSchema); err != nil {
		t.Fatalf("read provider composition database schema: %v", err)
	}
	process.command.Env = append(os.Environ(), "TETRAL_TEST_DATABASE_SCHEMA="+databaseSchema)
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start provider failure composition: %v", err)
	}
	t.Cleanup(func() {
		if process.command.ProcessState == nil {
			_ = process.command.Process.Kill()
			_ = process.command.Wait()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(readyPath)
		var ready struct {
			Port int `json:"port"`
		}
		if readErr == nil && json.Unmarshal(raw, &ready) == nil && ready.Port > 0 {
			process.port = ready.Port
			return process
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("provider failure composition did not become ready: %s", process.output.String())
	return nil
}

type providerFailureRuntimeState struct {
	ProviderInvocations              int      `json:"providerInvocations"`
	ToolInvocations                  int      `json:"toolInvocations"`
	FinishIdleInvocations            int      `json:"finishIdleInvocations"`
	FinishIdleResult                 string   `json:"finishIdleResult"`
	ProviderRequestContexts          []string `json:"providerRequestContexts"`
	ProviderStartedBeforeToolRelease bool     `json:"providerStartedBeforeToolRelease"`
}

func readProviderFailureRuntimeState(path string) (providerFailureRuntimeState, error) {
	var state providerFailureRuntimeState
	raw, err := os.ReadFile(path)
	if err != nil {
		return state, err
	}
	err = json.Unmarshal(raw, &state)
	return state, err
}

func waitForProviderFailureIdleReceipt(t *testing.T, process *providerFailureRuntimeProcess, minimumInvocations int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		state, err := readProviderFailureRuntimeState(process.statePath)
		if err == nil && state.FinishIdleInvocations >= minimumInvocations && state.FinishIdleResult == "committed" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	state, err := readProviderFailureRuntimeState(process.statePath)
	t.Fatalf("provider failure FinishIdle receipt = %+v/%v; want at least %d committed invocation(s)", state, err, minimumInvocations)
}

func seedProviderFailureCredential(t *testing.T, runtimeDB, admin *sql.DB, sessionID, scenario string) func() {
	t.Helper()
	if scenario != "invalid_kimi_byok" && scenario != "invalid_openai_oauth" &&
		scenario != "missing_kimi_credential" && scenario != "unavailable_openai_credential" {
		return func() {}
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	encryptor, err := vault.NewEncryptor(strings.Repeat("0", 64))
	if err != nil {
		t.Fatalf("create provider failure credential encryptor: %v", err)
	}
	vaultStore := vault.NewPostgreSQLVaultStore(client)
	credentialStore := vault.NewPostgreSQLCredentialStore(client, encryptor)
	createdVault, err := vaultStore.Create(context.Background(), workspace.DefaultID, vault.CreateVaultRequest{
		DisplayName: "provider failure composition",
	})
	if err != nil {
		t.Fatalf("create provider failure Vault: %v", err)
	}
	providerID := "moonshotai"
	accessMode := "user_api_key"
	invalid := vault.CredentialAuth{
		Type: "provider_api_key", ProviderID: providerID, AccessMode: accessMode, Token: "session-key-invalid",
	}
	healthy := invalid
	healthy.Token = "session-key-healthy"
	if scenario == "invalid_openai_oauth" || scenario == "unavailable_openai_credential" {
		providerID = "openai"
		accessMode = "oauth"
		invalid = vault.CredentialAuth{
			Type: "provider_oauth", ProviderID: providerID, AccessMode: accessMode,
			AccessToken: "oauth-access-invalid", RefreshToken: "oauth-refresh-invalid",
			ExpiresAt: "2099-01-01T00:00:00.000Z", AccountID: "account-provider-failure-canary",
		}
		if scenario == "invalid_openai_oauth" {
			invalid.ExpiresAt = "2020-01-01T00:00:00.000Z"
		}
		healthy = invalid
		healthy.AccessToken = "oauth-access-healthy"
		healthy.RefreshToken = "oauth-refresh-healthy"
		healthy.ExpiresAt = "2099-01-01T00:00:00.000Z"
	}
	credential, err := credentialStore.Create(context.Background(), workspace.DefaultID, createdVault.ID, vault.CreateCredentialRequest{
		DisplayName: "provider failure credential", Auth: invalid,
	})
	if err != nil {
		t.Fatalf("create provider failure credential: %v", err)
	}
	bindCredential := func() {
		t.Helper()
		if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_provider_auth (
			workspace_id, session_id, provider_id, vault_id, credential_id, access_mode, created_at, updated_at
		) VALUES ('default',$1,$2,$3,$4,$5,clock_timestamp(),clock_timestamp())`,
			sessionID, providerID, createdVault.ID, credential.ID, accessMode); err != nil {
			t.Fatalf("bind provider failure credential: %v", err)
		}
	}
	if scenario == "missing_kimi_credential" {
		return bindCredential
	}
	var originalEncryptedAuth []byte
	if scenario == "unavailable_openai_credential" {
		if err := admin.QueryRowContext(context.Background(), `SELECT encrypted_auth FROM credentials WHERE workspace_id='default' AND id=$1`, credential.ID).Scan(&originalEncryptedAuth); err != nil {
			t.Fatalf("read provider failure encrypted credential: %v", err)
		}
	}
	bindCredential()
	if scenario == "unavailable_openai_credential" {
		if _, err := admin.ExecContext(context.Background(), `UPDATE credentials SET encrypted_auth=$1 WHERE workspace_id='default' AND id=$2`, []byte("credential-unavailable-canary"), credential.ID); err != nil {
			t.Fatalf("make provider failure credential unavailable: %v", err)
		}
		return func() {
			if _, err := admin.ExecContext(context.Background(), `UPDATE credentials SET encrypted_auth=$1 WHERE workspace_id='default' AND id=$2`, originalEncryptedAuth, credential.ID); err != nil {
				t.Fatalf("repair unavailable provider failure credential: %v", err)
			}
		}
	}
	return func() {
		if _, err := credentialStore.Update(context.Background(), workspace.DefaultID, createdVault.ID, credential.ID, vault.CredentialPatch{Auth: &healthy}); err != nil {
			t.Fatalf("repair provider failure credential through Vault owner: %v", err)
		}
	}
}

func waitForProviderFailureFacts(t *testing.T, admin *sql.DB, sessionID string, wantEnds int, wantThreadStatus string, process *providerFailureRuntimeProcess) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var ends int
		var statusValue string
		err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
			(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='main')`, sessionID).
			Scan(&ends, &statusValue)
		if err == nil && ends >= wantEnds && statusValue == wantThreadStatus {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	var starts, ends, reschedules, providerAttempts, runningEvents, userMessages int
	var statusValue, endPayloads, inboxFacts, messageFacts, eventFacts string
	_ = admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'
		  AND payload_json::jsonb->'reschedule' <> 'null'::jsonb),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='main'),
		COALESCE((SELECT provider_attempts FROM session_turn_retries WHERE workspace_id='default' AND session_id=$1),-1),
		COALESCE((SELECT jsonb_agg(payload_json::jsonb)::text FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),'[]'),
		COALESCE((SELECT jsonb_agg(jsonb_build_object('id',runtime_input_id,'status',status,'sequence_from',sequence_from,'sequence_to',sequence_to) ORDER BY sequence_from)::text
		  FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1),'[]'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_running'),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND kind='user'),
		COALESCE((SELECT jsonb_agg(jsonb_build_object('sequence',sequence,'kind',kind,'data',data_json::jsonb) ORDER BY sequence)::text
		  FROM session_messages WHERE workspace_id='default' AND session_id=$1),'[]'),
		COALESCE((SELECT jsonb_agg(jsonb_build_object('type',type,'payload',payload_json::jsonb) ORDER BY sequence)::text
		  FROM session_events WHERE workspace_id='default' AND session_id=$1),'[]')`, sessionID).
		Scan(&starts, &ends, &reschedules, &statusValue, &providerAttempts, &endPayloads, &inboxFacts, &runningEvents, &userMessages, &messageFacts, &eventFacts)
	state, _ := os.ReadFile(process.statePath)
	t.Fatalf("provider failure facts starts/ends/reschedules/status/attempts/runs/users/state=%d/%d/%d/%s/%d/%d/%d/%s; want ends=%d status=%s; payloads=%s inbox=%s messages=%s events=%s process=%s",
		starts, ends, reschedules, statusValue, providerAttempts, runningEvents, userMessages, state, wantEnds, wantThreadStatus, endPayloads, inboxFacts, messageFacts, eventFacts, process.output.String())
}

func (p *providerFailureRuntimeProcess) close(t *testing.T) struct {
	ProviderInvocations              int      `json:"providerInvocations"`
	ToolInvocations                  int      `json:"toolInvocations"`
	FinishIdleInvocations            int      `json:"finishIdleInvocations"`
	FinishIdleResult                 string   `json:"finishIdleResult"`
	SensitiveLogLeak                 bool     `json:"sensitiveLogLeak"`
	OAuthAccessTokenConsumed         bool     `json:"oauthAccessTokenConsumed"`
	PlatformKeySelections            []string `json:"platformKeySelections"`
	PlatformKeyQuarantines           []string `json:"platformKeyQuarantines"`
	ProviderRequestContexts          []string `json:"providerRequestContexts"`
	ProviderStartedBeforeToolRelease bool     `json:"providerStartedBeforeToolRelease"`
} {
	t.Helper()
	if err := os.WriteFile(p.closePath, []byte("close"), 0o600); err != nil {
		t.Fatalf("close provider failure composition: %v", err)
	}
	if err := p.command.Wait(); err != nil {
		t.Fatalf("wait provider failure composition: %v: %s", err, p.output.String())
	}
	var result struct {
		ProviderInvocations              int      `json:"providerInvocations"`
		ToolInvocations                  int      `json:"toolInvocations"`
		FinishIdleInvocations            int      `json:"finishIdleInvocations"`
		FinishIdleResult                 string   `json:"finishIdleResult"`
		SensitiveLogLeak                 bool     `json:"sensitiveLogLeak"`
		OAuthAccessTokenConsumed         bool     `json:"oauthAccessTokenConsumed"`
		PlatformKeySelections            []string `json:"platformKeySelections"`
		PlatformKeyQuarantines           []string `json:"platformKeyQuarantines"`
		ProviderRequestContexts          []string `json:"providerRequestContexts"`
		ProviderStartedBeforeToolRelease bool     `json:"providerStartedBeforeToolRelease"`
	}
	if err := json.Unmarshal(p.output.Bytes(), &result); err != nil {
		t.Fatalf("decode provider failure composition: %v: %s", err, p.output.String())
	}
	return result
}

func waitForPendingOutputCapture(db *sql.DB, sessionID, excludedWriteID string) (string, int, error) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var writeID string
		var generation int
		err := db.QueryRowContext(context.Background(), `SELECT finish_idle_write_id, capture_generation
			FROM sandbox_output_capture_operations
			WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id<>$2
			  AND state IN ('pending','running')
			ORDER BY created_at LIMIT 1`, sessionID, excludedWriteID).Scan(&writeID, &generation)
		if err == nil {
			return writeID, generation, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", 0, err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", 0, fmt.Errorf("pending output capture was not created for session %s", sessionID)
}

type providerRescheduleRecoveryComposition struct {
	ResultType          string          `json:"resultType"`
	ProviderInvocations int             `json:"providerInvocations"`
	ExecutorInvocations int             `json:"executorInvocations"`
	WaitedMS            []int64         `json:"waitedMs"`
	ProviderContext     json.RawMessage `json:"providerContext"`
	RecoveredTurnEvents []string        `json:"recoveredTurnEventIds"`
	PreloadResult       json.RawMessage `json:"preloadResult"`
	LastSnapshot        json.RawMessage `json:"lastSnapshot"`
	TerminationResults  json.RawMessage `json:"terminationResults"`
	Command             struct {
		WorkspaceID       string `json:"workspaceId"`
		SessionID         string `json:"sessionId"`
		SessionThreadID   string `json:"sessionThreadId"`
		BindingID         string `json:"bindingId"`
		BindingGeneration int64  `json:"bindingGeneration"`
		TargetPodUID      string `json:"targetPodUid"`
		SourceEventID     string `json:"sourceEventId"`
	} `json:"command"`
}

type providerRecoveryProcess struct {
	command    *exec.Cmd
	output     bytes.Buffer
	port       int
	resultPath string
	closePath  string
}

type providerRecoveryTokenSource struct{}

func (providerRecoveryTokenSource) Token(context.Context) (string, error) {
	return "provider-recovery-composition-token", nil
}

type responseLosingRecoveryCommandClient struct {
	*RuntimePodCommandClient
	loseFirst bool
	requests  []*agentruntimev1.RecoverThreadRequest
}

func (c *responseLosingRecoveryCommandClient) RecoverThread(
	ctx context.Context,
	target RuntimePodTarget,
	request *agentruntimev1.RecoverThreadRequest,
) (*agentruntimev1.RecoverThreadResponse, error) {
	c.requests = append(c.requests, request)
	response, err := c.RuntimePodCommandClient.RecoverThread(ctx, target, request)
	if err == nil && c.loseFirst {
		c.loseFirst = false
		return nil, errors.New("injected lost recovery response")
	}
	return response, err
}

func startProviderRecoveryRuntime(t *testing.T, bridgeAddress, sessionID, threadID, podUID string, now time.Time) *providerRecoveryProcess {
	t.Helper()
	tempDir := t.TempDir()
	readyPath := filepath.Join(tempDir, "ready.json")
	process := &providerRecoveryProcess{
		resultPath: filepath.Join(tempDir, "result.json"),
		closePath:  filepath.Join(tempDir, "close"),
	}
	inputPath := filepath.Join(tempDir, "input.json")
	encoded, err := json.Marshal(map[string]any{
		"serveRecovery": true, "bridgeAddress": bridgeAddress, "workspaceId": workspace.DefaultID,
		"sessionId": sessionID, "sessionThreadId": threadID, "targetPodUid": podUID,
		"now": now.Format(time.RFC3339Nano), "readyPath": readyPath,
		"recoveryResultPath": process.resultPath, "closePath": process.closePath,
	})
	if err != nil {
		t.Fatalf("encode serving recovery composition: %v", err)
	}
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write serving recovery composition: %v", err)
	}
	process.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/provider-reschedule-recovery-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	process.command.Dir = "../agent-runtime"
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start serving recovery composition: %v", err)
	}
	t.Cleanup(func() {
		if process.command.ProcessState == nil {
			_ = process.command.Process.Kill()
			_ = process.command.Wait()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(readyPath)
		if readErr == nil {
			var ready struct {
				Port int `json:"port"`
			}
			if json.Unmarshal(raw, &ready) == nil && ready.Port > 0 {
				process.port = ready.Port
				return process
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("serving recovery composition did not become ready: %s", process.output.String())
	return nil
}

func (p *providerRecoveryProcess) recoveryResult(t *testing.T) providerRescheduleRecoveryComposition {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(p.resultPath)
		if err == nil {
			var result providerRescheduleRecoveryComposition
			if json.Unmarshal(raw, &result) == nil {
				return result
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("serving recovery composition did not accept command: %s", p.output.String())
	return providerRescheduleRecoveryComposition{}
}

func (p *providerRecoveryProcess) close(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(p.closePath, nil, 0o600); err != nil {
		t.Fatalf("signal serving recovery composition close: %v", err)
	}
	if err := p.command.Wait(); err != nil {
		t.Fatalf("close serving recovery composition: %v: %s", err, p.output.String())
	}
}

func TestPostgreSQLReplacementRuntimeTerminationReplaysReceiptWithoutResidency(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_recovered_binding_termination"
		threadID       = "sthr_recovered_binding_termination"
		oldBindingID   = "bind_recovered_binding_old"
		newPodUID      = "pod_recovered_binding_new"
		durableTurnID  = "evt_recovered_binding_active_turn"
		modelRequestID = "mreq_recovered_binding_reschedule"
	)
	oldBinding := runtimePodLostBinding(sessionID, oldBindingID, 1)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldBinding.PodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("recovered-binding-termination-signing-key")
	acceptedAt := time.Date(2026, 8, 20, 18, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return acceptedAt }
	oldScope := bridgeAPIScope(sessionID, threadID, oldBindingID, 1, oldBinding.PodUID)
	seedBridgeAPIOpenDurableTurn(t, admin, oldScope, durableTurnID)
	seedBridgeAPIRequestStart(t, store, oldScope, "rwrite_recovered_binding_start", modelRequestID, requestKindAgentProviderRequest, 0)
	if ended, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_recovered_binding_end", ModelRequestId: modelRequestID,
		FinishReason: "error", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "rescheduled"}, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	}); err != nil || ended.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit recoverable provider reschedule: response=%#v err=%v", ended, err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtimeDB), 9090)
	if err := deliveryStore.repairLostRuntimeBinding(context.Background(), "default", sessionID, runtimeBindingForDelivery{
		BindingID: oldBindingID, BindingGeneration: 1,
	}, acceptedAt.Add(100*time.Millisecond)); err != nil {
		t.Fatalf("finalize lost Runtime binding: %v", err)
	}
	var sessionStatus, runtimeStatus string
	var lostBindingID sql.NullString
	var lostBindingGeneration sql.NullInt64
	var remainingBindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		(SELECT binding_id FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		(SELECT binding_generation FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)`, sessionID,
	).Scan(&sessionStatus, &runtimeStatus, &lostBindingID, &lostBindingGeneration, &remainingBindings); err != nil {
		t.Fatalf("read pod-loss provenance cleanup: %v", err)
	}
	if sessionStatus != "rescheduling" || runtimeStatus != "idle" || lostBindingID.Valid || lostBindingGeneration.Valid || remainingBindings != 0 {
		t.Fatalf("pod-loss cleanup session/runtime/provenance/bindings = %s/%s/%+v/%+v/%d", sessionStatus, runtimeStatus, lostBindingID, lostBindingGeneration, remainingBindings)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	replacement := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime", PodName: "runtime-recovered-binding-new",
		PodUID: newPodUID, PodIP: "10.63.0.10",
	}
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{replacement})
	}}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:    JobRunnerConfig{LeaseOwner: "recovered-binding-termination", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active || len(sender.requests) != 1 {
		t.Fatalf("deliver recovered-binding wake = active:%t requests:%d err:%v", active, len(sender.requests), err)
	}
	recoveryRequest, ok := sender.requests[0].(*agentruntimev1.RecoverThreadRequest)
	if !ok || recoveryRequest.GetTargetPodUid() != newPodUID {
		t.Fatalf("recovered-binding wake = %#v; want resolver-owned reschedule recovery", sender.requests[0])
	}
	newBindingID := recoveryRequest.GetBindingId()
	newBindingGeneration := recoveryRequest.GetBindingGeneration()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for recovered-binding Runtime: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	var runningEventsBefore int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='session.status_running'`, sessionID).Scan(&runningEventsBefore); err != nil {
		t.Fatalf("count running Events before replacement preload: %v", err)
	}
	result := runProviderRescheduleRecoveryComposition(t, map[string]any{
		"bridgeAddress": listener.Addr().String(),
		"workspaceId":   "default", "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": newBindingID, "bindingGeneration": newBindingGeneration, "targetPodUid": newPodUID,
		"now":         acceptedAt.Add(200 * time.Millisecond).Format(time.RFC3339Nano),
		"preloadOnly": true, "terminationWriteId": durableTurnID, "terminationReplayCount": 2,
	})
	if result.ResultType != "preloaded" || result.ProviderInvocations != 0 || result.ExecutorInvocations != 0 {
		t.Fatalf("replacement Runtime cold preload = %+v", result)
	}
	terminationResults := string(result.TerminationResults)
	if strings.Count(terminationResults, `"type":"committed"`) != 1 || strings.Count(terminationResults, `"type":"duplicate"`) != 1 {
		t.Fatalf("replacement Runtime termination/replay = %s; want committed then exact duplicate", terminationResults)
	}
	var storedStatus string
	var storedBindingID sql.NullString
	var storedBindingGeneration sql.NullInt64
	var runningEventsAfter, terminationOperations, liveBindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		(SELECT binding_id FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		(SELECT binding_generation FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_running'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='commit_runtime_termination'),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)`, sessionID,
	).Scan(&storedStatus, &storedBindingID, &storedBindingGeneration, &runningEventsAfter, &terminationOperations, &liveBindings); err != nil {
		t.Fatalf("read recovered terminal residency: %v", err)
	}
	if storedStatus != "idle" || storedBindingID.Valid || storedBindingGeneration.Valid || runningEventsAfter != runningEventsBefore || terminationOperations != 1 || liveBindings != 0 {
		t.Fatalf("terminal residency status/binding/generation/running Events/operations/live bindings = %s/%v/%v/%d/%d/%d; want idle/null/null/%d/1/0", storedStatus, storedBindingID, storedBindingGeneration, runningEventsAfter, terminationOperations, liveBindings, runningEventsBefore)
	}
	failureJSON := `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`
	if replay, replayErr := store.CommitRuntimeTermination(context.Background(), &bridgev1.CommitRuntimeTerminationRequest{
		Scope: oldScope, RuntimeWriteId: durableTurnID, FailureJson: failureJSON,
	}); replayErr != nil || replay.GetDuplicate() == nil {
		t.Fatalf("exact termination receipt replay after unbinding = %#v/%v; want duplicate", replay, replayErr)
	}
	newScope := bridgeAPIScope(sessionID, threadID, newBindingID, newBindingGeneration, newPodUID)
	if ordinary, ordinaryErr := (BridgeAPIServer{store: store}).WriteEvent(context.Background(), closeoutWriteEventRequest(newScope, "rwrite_recovered_binding_post_terminal")); ordinaryErr != nil || ordinary.GetStale() == nil {
		t.Fatalf("ordinary replacement declaration after termination = %#v/%v; want stale", ordinary, ordinaryErr)
	}
}

func TestPostgreSQLProviderRescheduleColdRecoversCommittedToolWithoutReexecution(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID         = "sesn_provider_reschedule_recovery"
		threadID          = "sthr_provider_reschedule_recovery"
		oldBindingID      = "bind_provider_reschedule_old"
		oldPodUID         = "pod_provider_reschedule_old"
		newPodUID         = "pod_provider_reschedule_new"
		historicalID      = "mreq_provider_reschedule_historical"
		historicalRetryID = "mreq_provider_reschedule_historical_retry"
		modelRequestID    = "mreq_provider_reschedule_original"
		modelToolCallID   = "call_provider_reschedule_original"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_provider_reschedule_user", "sevt_provider_reschedule_user", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"read the original file"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, threadID); err != nil {
		t.Fatalf("seed provider reschedule user context: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,
		visibility,session_visible,model_request_id,projection_json,created_at,updated_at
	)
	SELECT 'default',$1,$2,'evt_provider_reschedule_audit_start_'||item,item*2-1,
	       'span.model_request_start','{}','internal',false,'mreq_provider_reschedule_audit_'||item,
	       jsonb_build_object('context_through_message_sequence',1,'request_kind','agent_provider_request')::text,
	       clock_timestamp(),clock_timestamp()
	  FROM generate_series(1,500) item
	UNION ALL
	SELECT 'default',$1,$2,'evt_provider_reschedule_audit_end_'||item,item*2,
	       'span.model_request_end',
	       jsonb_build_object(
	         'model_request_start_id','evt_provider_reschedule_audit_start_'||item,
	         'is_error',true,'error_kind','gateway_stream_error',
	         'provider_context_retention',jsonb_build_object(
	           'disposition','failed','tool_use_event_ids',jsonb_build_array(),'repair_event_ids',jsonb_build_array()
	         )
	       )::text,
	       'internal',false,'mreq_provider_reschedule_audit_'||item,'{}',
	       clock_timestamp(),clock_timestamp()
	  FROM generate_series(1,500) item`, sessionID, threadID); err != nil {
		t.Fatalf("seed provider reschedule audit history: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldPodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("provider-reschedule-recovery-signing-key")
	acceptedAt := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return acceptedAt }
	oldScope := bridgeAPIScope(sessionID, threadID, oldBindingID, 1, oldPodUID)
	seedBridgeAPIOpenDurableTurn(t, admin, oldScope, "evt_provider_reschedule_historical_turn")
	seedBridgeAPIRequestStart(t, store, oldScope, "rwrite_provider_reschedule_historical_start", historicalID, requestKindAgentProviderRequest, 1)
	historical, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_provider_reschedule_historical_end", ModelRequestId: historicalID,
		FinishReason: "error", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "rescheduled"},
		IsError:                  true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	})
	if err != nil || historical.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit historical provider reschedule: response=%#v err=%v", historical, err)
	}
	historicalEndEventID := historical.GetCommitted().GetRequestEndEventId()
	seedBridgeAPIRequestStart(t, store, oldScope, "rwrite_provider_reschedule_historical_retry_start", historicalRetryID, requestKindAgentProviderRequest, 1)
	historicalRetry, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_provider_reschedule_historical_retry_end", ModelRequestId: historicalRetryID,
		FinishReason: "stop", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed"},
	})
	if err != nil || historicalRetry.GetCommitted() == nil {
		t.Fatalf("complete historical provider retry: response=%#v err=%v", historicalRetry, err)
	}
	historicalRetryEndEventID := historicalRetry.GetCommitted().GetRequestEndEventId()
	if idle, err := finishIdleWithStagedCaptureForTest(t, admin, store, &bridgev1.FinishIdleRequest{
		Scope: oldScope, DurableTurnId: "evt_provider_reschedule_historical_turn", StopReasonJson: `{"type":"end_turn"}`,
	}); err != nil || idle.GetCommitted() == nil {
		t.Fatalf("close historical provider retry: response=%#v err=%v", idle, err)
	}
	seedBridgeAPIOpenDurableTurn(t, admin, oldScope, "evt_provider_reschedule_durable_turn")
	seedBridgeAPIRequestStart(t, store, oldScope, "rwrite_provider_reschedule_start", modelRequestID, requestKindAgentProviderRequest, 1)

	if message, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_provider_reschedule_partial", ModelRequestId: modelRequestID,
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"discarded partial text"}]}`,
		AssistantContextDelta: bridgeTextContextDeltaForTest("discarded partial text"),
	}); err != nil || message.GetCommitted() == nil {
		t.Fatalf("write failed request partial text: response=%#v err=%v", message, err)
	}
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_provider_reschedule_tool", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeToolDeclarationForTest(modelToolCallID, "Read", `{"path":"original.txt"}`, "allow", "sandbox_execute"),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write original Tool Use: response=%#v err=%v", toolUse, err)
	}
	if accepted, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: oldScope, ToolUseEventId: toolUse.GetCommitted().GetEventId(),
	}); err != nil || accepted.GetCommitted() == nil {
		t.Fatalf("accept original Tool execution: response=%#v err=%v", accepted, err)
	}
	endRequest := &bridgev1.WriteRequestEndRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_provider_reschedule_end", ModelRequestId: modelRequestID,
		FinishReason: "error", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "rescheduled", AssistantMessageSequence: toolUse.GetCommitted().AssignedMessageSequence,
			ToolUseEventIds: []string{toolUse.GetCommitted().GetEventId()},
		}, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	}
	if committed, err := store.WriteRequestEnd(context.Background(), endRequest); err != nil || committed.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit provider reschedule before lost acknowledgement: response=%#v err=%v", committed, err)
	}
	replayed, err := store.WriteRequestEnd(context.Background(), endRequest)
	if err != nil || replayed.GetDuplicate().GetRescheduled() == nil {
		t.Fatalf("replay provider reschedule after lost acknowledgement: response=%#v err=%v", replayed, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json = jsonb_set(data_json::jsonb, '{parts}',
			(data_json::jsonb -> 'parts') || '[{"type":"tool_call","modelToolCallId":"call_uncommitted_fragment","toolName":"Write","canonicalInput":{"path":"never.txt"}}]'::jsonb)::text
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequestID); err != nil {
		t.Fatalf("seed uncommitted sibling Tool fragment: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `ANALYZE session_events`); err != nil {
		t.Fatalf("analyze provider reschedule history: %v", err)
	}
	var planJSON string
	if err := admin.QueryRowContext(
		context.Background(),
		"EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON) "+loadContextTurnEventsSQL,
		"default", sessionID, threadID, int64(0), "evt_provider_reschedule_durable_turn",
		`["`+modelRequestID+`"]`, `["`+toolUse.GetCommitted().GetEventId()+`"]`, "running",
	).Scan(&planJSON); err != nil {
		t.Fatalf("EXPLAIN current provider reschedule selection: %v", err)
	}
	var planDocuments []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(planJSON), &planDocuments); err != nil || len(planDocuments) != 1 {
		t.Fatalf("decode provider reschedule plan: err=%v plan=%s", err, planJSON)
	}
	var sessionEventIndex bool
	walkPostgreSQLPlan(planDocuments[0].Plan, func(node map[string]any) {
		relation, _ := node["Relation Name"].(string)
		index, _ := node["Index Name"].(string)
		if relation == "session_events" && index != "" {
			sessionEventIndex = true
		}
	})
	selectedRows, _ := planDocuments[0].Plan["Actual Rows"].(float64)
	if !sessionEventIndex || selectedRows > 8 {
		t.Fatalf("provider reschedule plan index:%t selected:%.0f; want bounded current facts\n%s",
			sessionEventIndex, selectedRows, planJSON)
	}

	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtimeDB), 9090)
	if err := deliveryStore.repairLostRuntimeBinding(context.Background(), "default", sessionID, runtimeBindingForDelivery{
		BindingID: oldBindingID, BindingGeneration: 1,
	}, acceptedAt.Add(100*time.Millisecond)); err != nil {
		t.Fatalf("repair lost Runtime after committed reschedule: %v", err)
	}
	var routeStatus string
	var routeDecision sql.NullString
	var terminalToolResults, oldBindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_pending_tool_uses
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3),
		(SELECT decision FROM session_pending_tool_uses
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3),
		(SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		    AND type='agent.tool_result' AND payload_json::jsonb ->> 'tool_use_event_id'=$3),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)`,
		sessionID, threadID, toolUse.GetCommitted().GetEventId(),
	).Scan(&routeStatus, &routeDecision, &terminalToolResults, &oldBindings); err != nil {
		t.Fatalf("read production pod-loss reschedule ownership: %v", err)
	}
	if routeStatus != "resolving" || !routeDecision.Valid || routeDecision.String != "allow" || terminalToolResults != 0 || oldBindings != 0 {
		t.Fatalf("pod-loss reschedule route/result/bindings = %s/%v/%d/%d; want resolving allow/0/0",
			routeStatus, routeDecision, terminalToolResults, oldBindings)
	}
	var rescheduleEventID string
	if err := admin.QueryRowContext(context.Background(), `SELECT event_id FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='session.status_rescheduled' ORDER BY sequence DESC LIMIT 1`, sessionID, threadID).Scan(&rescheduleEventID); err != nil {
		t.Fatalf("read durable reschedule root: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for provider reschedule recovery: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	runtimeProcess := startProviderRecoveryRuntime(
		t, listener.Addr().String(), sessionID, threadID, newPodUID, acceptedAt.Add(250*time.Millisecond),
	)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	replacement := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime", PodName: "runtime-provider-reschedule-new",
		PodUID: newPodUID, PodIP: "127.0.0.1",
	}
	deliveryStore.RuntimeGRPCPort = runtimeProcess.port
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{replacement})
	}}
	sender := &responseLosingRecoveryCommandClient{
		RuntimePodCommandClient: NewRuntimePodCommandClient(providerRecoveryTokenSource{}), loseFirst: true,
	}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:    JobRunnerConfig{LeaseOwner: "provider-reschedule-recovery", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("deliver provider reschedule recovery with lost response = active:%t err:%v", active, err)
	}
	preloaded := runtimeProcess.recoveryResult(t)
	if preloaded.Command.SourceEventID != rescheduleEventID || preloaded.Command.TargetPodUID != newPodUID {
		t.Fatalf("provider reschedule recovery command = %#v; want exact durable reschedule root", preloaded.Command)
	}
	if preloaded.ResultType != "preloaded" || preloaded.ProviderInvocations != 0 || preloaded.ExecutorInvocations != 0 ||
		!strings.Contains(string(preloaded.PreloadResult), `"ok":true`) ||
		!strings.Contains(string(preloaded.LastSnapshot), `"observed":true`) ||
		!strings.Contains(string(preloaded.LastSnapshot), `"hasUnsettledToolOwner":true`) ||
		strings.Contains(string(preloaded.LastSnapshot), "discarded partial text") ||
		strings.Contains(string(preloaded.LastSnapshot), "call_uncommitted_fragment") ||
		strings.Contains(string(preloaded.LastSnapshot), historicalID) ||
		strings.Contains(string(preloaded.LastSnapshot), historicalRetryID) ||
		slices.Contains(preloaded.RecoveredTurnEvents, historicalEndEventID) ||
		slices.Contains(preloaded.RecoveredTurnEvents, historicalRetryEndEventID) {
		t.Fatalf("replacement Runtime nonterminal preload = %+v snapshot=%s", preloaded, preloaded.LastSnapshot)
	}
	var recoveryJobID, recoveryQueueStatus, recoveredBindingID string
	var recoveredBindingGeneration int64
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT id FROM queue_jobs WHERE workspace_id='default' AND partition_key=$2 AND kind=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND partition_key=$2 AND kind=$3),
		(SELECT binding_id FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT binding_generation FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)`,
		sessionID, queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID), queue.KindRuntimeRecovery,
	).Scan(&recoveryJobID, &recoveryQueueStatus, &recoveredBindingID, &recoveredBindingGeneration); err != nil {
		t.Fatalf("read response-lost recovery owner: %v", err)
	}
	if recoveryQueueStatus != queue.StatusPending || recoveredBindingID != preloaded.Command.BindingID || recoveredBindingGeneration != preloaded.Command.BindingGeneration {
		t.Fatalf("response-lost recovery = Queue %s binding %s/%d; want pending and %s/%d",
			recoveryQueueStatus, recoveredBindingID, recoveredBindingGeneration,
			preloaded.Command.BindingID, preloaded.Command.BindingGeneration)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1`, recoveryJobID); err != nil {
		t.Fatalf("make response-lost recovery replay available: %v", err)
	}
	active, err = runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("replay response-lost recovery = active:%t err:%v", active, err)
	}
	if len(sender.requests) != 2 ||
		sender.requests[0].GetRecoveryLeaseRef().GetJobId() != sender.requests[1].GetRecoveryLeaseRef().GetJobId() ||
		sender.requests[0].GetRecoveryLeaseRef().GetLeaseToken() == sender.requests[1].GetRecoveryLeaseRef().GetLeaseToken() ||
		sender.requests[0].GetBindingId() != sender.requests[1].GetBindingId() ||
		sender.requests[0].GetBindingGeneration() != sender.requests[1].GetBindingGeneration() {
		t.Fatalf("response-lost recovery requests = %#v; want one durable job, renewed lease, and one binding generation", sender.requests)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
		WHERE workspace_id='default' AND id=$1`, recoveryJobID).Scan(&recoveryQueueStatus); err != nil {
		t.Fatalf("read replayed recovery Queue status: %v", err)
	}
	if recoveryQueueStatus != queue.StatusAcknowledged {
		t.Fatalf("replayed recovery Queue status = %q; want acked", recoveryQueueStatus)
	}
	runtimeProcess.close(t)
	newBindingID := preloaded.Command.BindingID
	newBindingGeneration := preloaded.Command.BindingGeneration
	newScope := bridgeAPIScope(sessionID, threadID, newBindingID, newBindingGeneration, newPodUID)
	settleSandboxExecutionForHotReceiptProof(t, runtimeDB, admin, newScope, toolUse.GetCommitted().GetEventId(),
		`{"status":"success","result":{"content":"original result"}}`)
	settled, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		newScope,
		bridgeCompletedToolSettlementForTest(toolUse.GetCommitted().GetEventId(), "original result"),
	))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle original Tool Result through replacement Runtime identity: response=%#v err=%v", settled, err)
	}

	captureSettled := make(chan error, 1)
	go func() {
		captureSettled <- settleOutputCaptureGenerationForTest(admin, sessionID, "evt_provider_reschedule_durable_turn", 1, "staged")
	}()
	result := runProviderRescheduleRecoveryComposition(t, map[string]any{
		"bridgeAddress": listener.Addr().String(),
		"workspaceId":   "default", "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": newBindingID, "bindingGeneration": newBindingGeneration, "targetPodUid": newPodUID,
		"now": acceptedAt.Add(500 * time.Millisecond).Format(time.RFC3339Nano),
	})
	if result.ResultType != "completed" || result.ProviderInvocations != 1 || result.ExecutorInvocations != 0 {
		t.Fatalf("replacement Runtime recovery = %+v", result)
	}
	if err := <-captureSettled; err != nil {
		t.Fatalf("stage provider reschedule closeout capture: %v", err)
	}
	if len(result.WaitedMS) == 0 || result.WaitedMS[0] != 500 {
		t.Fatalf("replacement Runtime accepted-deadline wait = %v; want first wait 500ms", result.WaitedMS)
	}
	providerContext := string(result.ProviderContext)
	expectedProviderContext := `[{"role":1,"content":[{"text":{"text":"read the original file"}}]},{"role":2,"content":[{"toolCall":{"modelToolCallId":"call_provider_reschedule_original","name":"Read","inputJson":"{\"path\":\"original.txt\"}"}},{"toolResult":{"modelToolCallId":"call_provider_reschedule_original","completed":{"outputJson":"{\"text\":\"original result\"}"}}}]}]`
	if providerContext != expectedProviderContext {
		t.Fatalf("recovered provider context did not preserve the exact narrow Tool pair: %s", providerContext)
	}

	var starts, ends, toolUses, toolResults int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result')`, sessionID).
		Scan(&starts, &ends, &toolUses, &toolResults); err != nil {
		t.Fatalf("read provider reschedule recovery census: %v", err)
	}
	if starts != 504 || ends != 504 || toolUses != 1 || toolResults != 1 {
		t.Fatalf("provider reschedule recovery census starts/ends/tools/results = %d/%d/%d/%d", starts, ends, toolUses, toolResults)
	}
}

func TestPostgreSQLProviderRescheduleColdCarriesCreatedSubagentWithoutRecreation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_subagent_reschedule_recovery"
		threadID       = "sthr_subagent_reschedule_recovery"
		oldBindingID   = "bind_subagent_reschedule_old"
		newPodUID      = "pod_subagent_reschedule_new"
		modelRequestID = "mreq_subagent_reschedule_cold"
		taskName       = "recovery-worker"
		prompt         = "finish the durable task"
	)
	oldBinding := runtimePodLostBinding(sessionID, oldBindingID, 1)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_subagent_reschedule_user", "sevt_subagent_reschedule_user", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"spawn the durable worker"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, threadID); err != nil {
		t.Fatalf("seed subagent reschedule user context: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldBinding.PodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("subagent-reschedule-recovery-signing-key")
	oldScope := bridgeAPIScope(sessionID, threadID, oldBindingID, 1, oldBinding.PodUID)
	toolRunnerResult := runSubagentProductionComposition(
		t, BridgeAPIServer{store: store}, sessionID, threadID, oldBindingID, 1, oldBinding.PodUID, taskName, prompt, "all",
	)
	if toolRunnerResult.ResultType != "observed" || toolRunnerResult.ProviderInvocations != 3 {
		t.Fatalf("ToolRunner child creation before cold reschedule = %+v", toolRunnerResult)
	}
	var childID, durableTurnID string
	var boundarySequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT id FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$2 AND role='subagent'),
		(SELECT COALESCE(MAX(sequence),0) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='session.status_running' ORDER BY sequence ASC LIMIT 1)`,
		sessionID, threadID).Scan(&childID, &boundarySequence, &durableTurnID); err != nil {
		t.Fatalf("read ToolRunner-created subagent facts: %v", err)
	}
	acceptedAt := time.Date(2026, 8, 20, 16, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return acceptedAt }
	var priorProviderAttempts int64
	if err := admin.QueryRowContext(context.Background(), `SELECT provider_attempts FROM session_turn_retries
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).Scan(&priorProviderAttempts); err != nil {
		t.Fatalf("read prior ToolRunner provider attempts: %v", err)
	}
	seedBridgeAPIRequestStart(t, store, oldScope, "rwrite_subagent_reschedule_cold_start", modelRequestID, requestKindAgentProviderRequest, boundarySequence)
	if ended, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_subagent_reschedule_cold_end", ModelRequestId: modelRequestID,
		FinishReason: "error", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "rescheduled"}, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: int32(priorProviderAttempts + 1), Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	}); err != nil || ended.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit subagent provider reschedule: response=%#v err=%v", ended, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtimeDB, sessionID, oldBinding, acceptedAt.Add(250*time.Millisecond)); err != nil {
			t.Fatalf("repair committed subagent delivery attempt %d: %v", attempt+1, err)
		}
	}
	var sessionStatus, threadStatus string
	var providerAttempts int64
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT provider_attempts FROM session_turn_retries WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2)`,
		sessionID, threadID).Scan(&sessionStatus, &threadStatus, &providerAttempts); err != nil {
		t.Fatalf("read repaired reschedule ownership: %v", err)
	}
	if sessionStatus != "rescheduling" || threadStatus != "rescheduling" || providerAttempts != priorProviderAttempts+1 {
		t.Fatalf("repaired reschedule ownership session/thread/attempts = %s/%s/%d", sessionStatus, threadStatus, providerAttempts)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for subagent reschedule recovery: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	var rescheduleEventID string
	if err := admin.QueryRowContext(context.Background(), `SELECT event_id FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='session.status_rescheduled' ORDER BY sequence DESC LIMIT 1`, sessionID, threadID).Scan(&rescheduleEventID); err != nil {
		t.Fatalf("read subagent reschedule recovery root: %v", err)
	}
	runtimeProcess := startProviderRecoveryRuntime(t, listener.Addr().String(), sessionID, threadID, newPodUID, acceptedAt.Add(300*time.Millisecond))
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtimeDB), runtimeProcess.port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-subagent-reschedule-new",
			PodUID: newPodUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(providerRecoveryTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "subagent-reschedule-recovery", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	for delivery := 0; delivery < 2; delivery++ {
		active, runErr := runner.RunOnceWithActivity(context.Background())
		if runErr != nil || !active {
			t.Fatalf("deliver queued child input and subagent reschedule recovery step %d = active:%t err:%v", delivery+1, active, runErr)
		}
	}
	preloaded := runtimeProcess.recoveryResult(t)
	if preloaded.Command.SourceEventID != rescheduleEventID || preloaded.Command.TargetPodUID != newPodUID || preloaded.ResultType != "preloaded" {
		t.Fatalf("subagent recovery preload = %+v; want exact Queue-owned recovery", preloaded)
	}
	runtimeProcess.close(t)
	newBindingID := preloaded.Command.BindingID
	newBindingGeneration := preloaded.Command.BindingGeneration
	captureSettled := make(chan error, 1)
	go func() {
		captureSettled <- settleOutputCaptureGenerationForTest(admin, sessionID, durableTurnID, 1, "staged")
	}()
	result := runProviderRescheduleRecoveryComposition(t, map[string]any{
		"bridgeAddress": listener.Addr().String(),
		"workspaceId":   "default", "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": newBindingID, "bindingGeneration": newBindingGeneration, "targetPodUid": newPodUID,
		"now": acceptedAt.Add(500 * time.Millisecond).Format(time.RFC3339Nano),
	})
	if err := <-captureSettled; err != nil {
		t.Fatalf("stage subagent reschedule closeout capture: %v", err)
	}
	if result.ResultType != "completed" || result.ProviderInvocations != 1 || result.ExecutorInvocations != 0 {
		t.Fatalf("replacement Runtime subagent recovery = %+v", result)
	}
	recoveredSnapshot := string(result.LastSnapshot)
	if strings.Count(recoveredSnapshot, `"toolName":"spawn_agent"`) != 1 ||
		strings.Count(recoveredSnapshot, `task_name: recovery-worker`) != 1 ||
		!strings.Contains(recoveredSnapshot, childID) {
		t.Fatalf("cold-loaded Runtime did not preserve one exact ToolRunner spawn pair: %s", recoveredSnapshot)
	}
	providerContext := string(result.ProviderContext)
	expectedProviderContext := fmt.Sprintf(`[{"role":1,"content":[{"text":{"text":"spawn the durable worker"}}]},{"role":2,"content":[{"toolCall":{"modelToolCallId":"call_subagent_production","name":"spawn_agent","inputJson":"{\"agent_type\":\"worker\",\"fork_turns\":\"all\",\"prompt\":\"finish the durable task\",\"task_name\":\"recovery-worker\"}"}},{"toolResult":{"modelToolCallId":"call_subagent_production","completed":{"outputJson":"{\"text\":\"task_name: recovery-worker\\nsession_thread_id: %s\\nstatus: delivered\"}"}}}]},{"role":2,"content":[{"text":{"text":"child started"}}]}]`, childID)
	if providerContext != expectedProviderContext {
		t.Fatalf("rescheduled Provider context did not preserve the exact completed spawn pair: %s", providerContext)
	}
	var children, createOperations, toolUses, toolResults int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$2 AND role='subagent'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result')`, sessionID, threadID).
		Scan(&children, &createOperations, &toolUses, &toolResults); err != nil {
		t.Fatalf("read recovered subagent census: %v", err)
	}
	if children != 1 || createOperations != 1 || toolUses != 1 || toolResults != 1 {
		t.Fatalf("recovered subagent census children/operations/tool uses/results = %d/%d/%d/%d", children, createOperations, toolUses, toolResults)
	}
}

func TestPostgreSQLPodLossAfterRetryStartSettlesConsumedReschedule(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID   = "sesn_consumed_reschedule_pod_loss"
		threadID    = "sthr_consumed_reschedule_pod_loss"
		bindingID   = "bind_consumed_reschedule_pod_loss"
		originalID  = "mreq_consumed_reschedule_original"
		retryID     = "mreq_consumed_reschedule_retry"
		durableTurn = "evt_consumed_reschedule_turn"
	)
	binding := runtimePodLostBinding(sessionID, bindingID, 1)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, binding.PodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	acceptedAt := time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return acceptedAt }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, binding.PodUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, durableTurn)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_consumed_reschedule_original_start", originalID, requestKindAgentProviderRequest, 0)
	if ended, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_consumed_reschedule_original_end", ModelRequestId: originalID,
		FinishReason: "error", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "rescheduled"}, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	}); err != nil || ended.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit original reschedule: response=%#v err=%v", ended, err)
	}
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_consumed_reschedule_retry_start", retryID, requestKindAgentProviderRequest, 0)
	if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtimeDB, sessionID, binding, acceptedAt.Add(2*time.Second)); err != nil {
		t.Fatalf("repair pod loss after retry start: %v", err)
	}
	var sessionStatus, threadStatus, runtimeStatus string
	var providerAttempts int64
	var retryEnds int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1 AND binding_id=$3 AND binding_generation=1),
		(SELECT provider_attempts FROM session_turn_retries WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='span.model_request_end' AND model_request_id=$4
		  AND payload_json::jsonb->>'error_kind'='runtime_pod_lost')`,
		sessionID, threadID, bindingID, retryID).Scan(&sessionStatus, &threadStatus, &runtimeStatus, &providerAttempts, &retryEnds); err != nil {
		t.Fatalf("read consumed reschedule closeout: %v", err)
	}
	if sessionStatus != "idle" || threadStatus != "idle" || runtimeStatus != "idle" || providerAttempts != 0 || retryEnds != 1 {
		t.Fatalf("consumed reschedule closeout session/thread/runtime/attempts/retry ends = %s/%s/%s/%d/%d",
			sessionStatus, threadStatus, runtimeStatus, providerAttempts, retryEnds)
	}
}

func runProviderRescheduleRecoveryComposition(t *testing.T, input map[string]any) providerRescheduleRecoveryComposition {
	t.Helper()
	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode provider reschedule recovery input: %v", err)
	}
	inputPath := t.TempDir() + "/input.json"
	if err := os.WriteFile(inputPath, inputJSON, 0o600); err != nil {
		t.Fatalf("write provider reschedule recovery input: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/provider-reschedule-recovery-composition.ts", inputPath) //nolint:gosec // Fixed Runtime composition fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run provider reschedule recovery composition: %v: %s", err, output)
	}
	var result providerRescheduleRecoveryComposition
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode provider reschedule recovery composition: %v: %s", err, output)
	}
	return result
}
