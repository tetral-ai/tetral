package static

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/eventwire"
)

func TestPublicEventProjectionTracksFrozenForkSDKUnion(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	sdkRoot := forkSDKRootForStaticTest(t, engineRoot)
	assertFrozenForkSDKTree(t, sdkRoot)

	registry := readSDKCompatibilityRegistry(t, filepath.Join(sdkRoot, "tests", "compatibility", "compat-cases.json"))
	registryEventTypes := sdkRegistrySessionEventTypes(t, registry, false)
	requiredEventTypes := sdkRegistrySessionEventTypes(t, registry, true)
	for eventType := range bridgePublicWriteEventTypes(t, engineRoot) {
		requiredEventTypes[eventType] = struct{}{}
	}
	projectionEventTypes := stringSliceSet(eventwire.PublicEventTypes())
	assertStaticStringSetsEqual(t, "registry/producer-selected public events and shared projection", requiredEventTypes, projectionEventTypes)

	fixtures := publicProjectionSDKFixturePayloads()
	fixtureTypes := make(map[string]struct{}, len(fixtures))
	for eventType := range fixtures {
		fixtureTypes[eventType] = struct{}{}
	}
	assertStaticStringSetsEqual(t, "selected public event types and projection fixtures", requiredEventTypes, fixtureTypes)

	runtimeErrorTypes := runtimeProducedSessionErrorTypes(t, engineRoot)
	processedAt := "2026-07-14T12:34:56Z"
	var projectedEvents []string
	requiredTypes := sortedStaticStringSet(requiredEventTypes)
	for _, eventType := range requiredTypes {
		if eventType == "session.error" {
			for _, errorType := range runtimeErrorTypes {
				payload := sessionErrorProjectionFixturePayload(t, errorType)
				projectedEvents = append(projectedEvents, projectSDKPublicEvent(t, eventType+"_"+errorType, eventType, payload, &processedAt))
			}
			continue
		}
		projectedEvents = append(projectedEvents, projectSDKPublicEvent(t, eventType, eventType, fixtures[eventType], &processedAt))
	}

	eventsPath, err := filepath.Abs(filepath.Join(sdkRoot, "src", "resources", "beta", "sessions", "events.ts"))
	if err != nil {
		t.Fatalf("resolve frozen fork SDK events path: %v", err)
	}
	fixtureSource := publicProjectionTypeScriptFixture(filepath.ToSlash(eventsPath), registryEventTypes, requiredEventTypes, projectedEvents)
	runForkSDKFixtureTypecheck(t, engineRoot, "public-event-projection-fork-sdk-compat.ts", fixtureSource)
}

func assertFrozenForkSDKTree(t *testing.T, sdkRoot string) {
	t.Helper()
	head := exec.Command("git", "-C", sdkRoot, "rev-parse", "HEAD") //nolint:gosec // fixed git inspection of configured local SDK checkout.
	headOutput, err := head.CombinedOutput()
	if err != nil {
		t.Fatalf("read frozen fork SDK HEAD: %v\n%s", err, headOutput)
	}
	if got := strings.TrimSpace(string(headOutput)); got != frozenForkSDKCommit {
		t.Fatalf("fork SDK HEAD = %s; want frozen %s", got, frozenForkSDKCommit)
	}
	status := exec.Command("git", "-C", sdkRoot, "status", "--short") //nolint:gosec // fixed git inspection of configured local SDK checkout.
	statusOutput, err := status.CombinedOutput()
	if err != nil {
		t.Fatalf("read frozen fork SDK status: %v\n%s", err, statusOutput)
	}
	if strings.TrimSpace(string(statusOutput)) != "" {
		t.Fatalf("fork SDK checkout is dirty; frozen evidence requires a clean tree:\n%s", statusOutput)
	}
}

func sdkRegistrySessionEventTypes(t *testing.T, registry []sdkCompatibilityRegistryCase, supportedOnly bool) map[string]struct{} {
	t.Helper()
	eventTypes := map[string]struct{}{}
	for _, compatibilityCase := range registry {
		if !strings.HasPrefix(compatibilityCase.ID, "T-COMPAT-EVOUT-") || (supportedOnly && compatibilityCase.Status != "supported") {
			continue
		}
		fields := strings.Fields(compatibilityCase.Surface)
		if len(fields) == 0 {
			continue
		}
		eventType := fields[0]
		if eventType == "event_start" || eventType == "event_delta" || strings.HasPrefix(eventType, "webhook:") {
			continue
		}
		if !strings.Contains(eventType, ".") {
			t.Fatalf("SDK compatibility output row %s has unrecognized event surface %q", compatibilityCase.ID, compatibilityCase.Surface)
		}
		eventTypes[eventType] = struct{}{}
	}
	if len(eventTypes) == 0 {
		t.Fatal("SDK compatibility registry selected no session event types")
	}
	return eventTypes
}

func bridgePublicWriteEventTypes(t *testing.T, engineRoot string) map[string]struct{} {
	t.Helper()
	source := finalArchitectureReadText(t, filepath.Join(engineRoot, "services", "bridge", "bridge_api_events.go"))
	start := strings.Index(source, "func writeEventTypeAllowed(eventType string) bool")
	if start < 0 {
		t.Fatal("Bridge producer source is missing writeEventTypeAllowed")
	}
	end := strings.Index(source[start:], "\n}\n\nfunc ")
	if end < 0 {
		t.Fatal("Bridge producer source is missing bounded writeEventTypeAllowed")
	}
	body := source[start : start+end]
	literal := regexp.MustCompile(`"([a-z][a-z_]*\.[a-z][a-z_]*)"`)
	eventTypes := map[string]struct{}{}
	for _, match := range literal.FindAllStringSubmatch(body, -1) {
		eventType := match[1]
		if strings.HasPrefix(eventType, "approval_review.") {
			continue
		}
		eventTypes[eventType] = struct{}{}
	}
	if len(eventTypes) == 0 {
		t.Fatal("Bridge write-event producer source selected no public event types")
	}
	for eventType, sourcePath := range map[string]string{
		"agent.tool_result":             "services/bridge/bridge_api_tool_settlement.go",
		"agent.mcp_tool_result":         "services/bridge/bridge_api_tool_settlement.go",
		"agent.thread_message_sent":     "services/bridge/completion_mail.go",
		"agent.thread_message_received": "services/bridge/completion_mail.go",
	} {
		dedicatedSource := finalArchitectureReadText(t, filepath.Join(engineRoot, sourcePath))
		if !strings.Contains(dedicatedSource, strconv.Quote(eventType)) {
			t.Fatalf("Bridge dedicated producer %s is missing public event %q", sourcePath, eventType)
		}
		eventTypes[eventType] = struct{}{}
	}
	return eventTypes
}

func runtimeProducedSessionErrorTypes(t *testing.T, engineRoot string) []string {
	t.Helper()
	source := finalArchitectureReadText(t, filepath.Join(engineRoot, "services", "agent-runtime", "packages", "core", "src", "contracts", "runtime.ts"))
	start := strings.Index(source, "const SessionMcpErrorSchema")
	if start < 0 {
		t.Fatal("Runtime producer source is missing public session error schemas")
	}
	end := strings.Index(source[start:], "export const RuntimeFailureSchema")
	if end < 0 {
		t.Fatal("Runtime producer source is missing bounded public session error schemas")
	}
	body := source[start : start+end]
	literal := regexp.MustCompile(`z\.literal\("([a-z_]+_error|unknown_error)"\)`)
	errorTypes := map[string]struct{}{}
	for _, match := range literal.FindAllStringSubmatch(body, -1) {
		errorTypes[match[1]] = struct{}{}
	}
	if len(errorTypes) == 0 {
		t.Fatal("Runtime producer source selected no public session.error members")
	}
	return sortedStaticStringSet(errorTypes)
}

func publicProjectionSDKFixturePayloads() map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"user.message":                      json.RawMessage(`{"content":[{"type":"text","text":"hello"}]}`),
		"user.interrupt":                    json.RawMessage(`{"session_thread_id":"sthr_interrupt"}`),
		"user.tool_confirmation":            json.RawMessage(`{"tool_use_id":"sevt_tool_confirmation","result":"deny","deny_message":"not now","session_thread_id":"sthr_confirmation"}`),
		"agent.message":                     json.RawMessage(`{"content":[{"type":"text","text":"answer"}]}`),
		"agent.thinking":                    json.RawMessage(`{}`),
		"agent.tool_use":                    json.RawMessage(`{"name":"Bash","input":{"command":"pwd"},"evaluated_permission":"ask","session_thread_id":"sthr_tool"}`),
		"agent.tool_result":                 json.RawMessage(`{"tool_use_id":"sevt_tool_use","content":[{"type":"text","text":"done"}],"is_error":false}`),
		"agent.mcp_tool_use":                json.RawMessage(`{"name":"create_issue","input":{"title":"Bug"},"mcp_server_name":"github","evaluated_permission":"allow","session_thread_id":"sthr_mcp"}`),
		"agent.mcp_tool_result":             json.RawMessage(`{"mcp_tool_use_id":"sevt_mcp_tool_use","content":[{"type":"text","text":"created"}],"is_error":false}`),
		"agent.thread_context_compacted":    json.RawMessage(`{}`),
		"agent.thread_message_sent":         json.RawMessage(`{"delivery_id":"delivery_sent","source_thread_id":"sthr_source_distinct","target_thread_id":"sthr_target_distinct","target_task_name":"callable_target","source_tool_use_event_id":"sevt_source_tool","message":{"content":[{"type":"text","text":"first block"},{"type":"text","text":"second block"}]},"task_name":"wrong_alias","agent_type":"research"}`),
		"agent.thread_message_received":     json.RawMessage(`{"delivery_id":"delivery_received","source_thread_id":"sthr_source_received_distinct","source_task_name":"callable_source","source_tool_use_event_id":"sevt_source_tool_received","message":{"content":[{"type":"text","text":"first block"},{"type":"text","text":"second block"}]},"task_name":"wrong_alias","agent_type":"general"}`),
		"session.status_running":            json.RawMessage(`{}`),
		"session.status_rescheduled":        json.RawMessage(`{}`),
		"session.status_idle":               json.RawMessage(`{"stop_reason":{"type":"end_turn"}}`),
		"session.status_terminated":         json.RawMessage(`{}`),
		"session.error":                     json.RawMessage(`{}`),
		"session.thread_created":            json.RawMessage(`{"session_thread_id":"sthr_created_distinct","parent_thread_id":"sthr_parent_distinct","role":"subagent","visibility":"public","task_name":"callable_created","agent_type":"worker"}`),
		"session.thread_status_running":     json.RawMessage(`{"session_thread_id":"sthr_running_distinct","task_name":"callable_running","agent_type":"general"}`),
		"session.thread_status_idle":        json.RawMessage(`{"session_thread_id":"sthr_idle_distinct","task_name":"callable_idle","agent_type":"research","stop_reason":{"type":"requires_action","event_ids":["sevt_wait_second","sevt_wait_first"]}}`),
		"session.thread_status_rescheduled": json.RawMessage(`{"session_thread_id":"sthr_rescheduled_distinct","task_name":"callable_rescheduled","agent_type":"worker"}`),
		"session.thread_status_terminated":  json.RawMessage(`{"session_thread_id":"sthr_terminated_distinct","task_name":"callable_terminated","agent_type":"general"}`),
		"session.updated":                   json.RawMessage(`{"title":"new title"}`),
		"session.deleted":                   json.RawMessage(`{}`),
		"span.model_request_start":          json.RawMessage(`{}`),
		"span.model_request_end":            json.RawMessage(`{"model_request_start_id":"sevt_model_start","is_error":false,"model_usage":{"cache_creation_input_tokens":1,"cache_read_input_tokens":2,"input_tokens":3,"output_tokens":4,"speed":"standard"}}`),
	}
}

func sessionErrorProjectionFixturePayload(t *testing.T, errorType string) json.RawMessage {
	t.Helper()
	errorPayload := map[string]any{
		"type":         errorType,
		"message":      "projected runtime error",
		"retry_status": map[string]any{"type": "terminal"},
	}
	if strings.HasPrefix(errorType, "mcp_") {
		errorPayload["mcp_server_name"] = "github"
	}
	payload, err := json.Marshal(map[string]any{"error": errorPayload})
	if err != nil {
		t.Fatalf("marshal %s projection fixture: %v", errorType, err)
	}
	return payload
}

func projectSDKPublicEvent(t *testing.T, idSuffix string, eventType string, payload json.RawMessage, processedAt *string) string {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("decode %s projection fixture payload: %v", eventType, err)
	}
	for name, value := range map[string]any{
		"id":               "payload_collision",
		"type":             "payload.collision",
		"processed_at":     "payload_collision",
		"payload":          map[string]any{"secret": true},
		"session_id":       "sesn_internal",
		"thread_id":        "sthr_internal",
		"runtime_input_id": "rin_internal",
		"queue_job_id":     "job_internal",
		"partition_key":    "partition_internal",
		"correlation_id":   "correlation_internal",
	} {
		fields[name] = value
	}
	durablePayload, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal %s durable projection fixture: %v", eventType, err)
	}
	id := "sevt_sdk_" + strings.NewReplacer(".", "_", "-", "_").Replace(idSuffix)
	projected, err := eventwire.MarshalPublicEvent(id, eventType, durablePayload, processedAt)
	if err != nil {
		t.Fatalf("project %s fixture: %v", eventType, err)
	}
	var event map[string]json.RawMessage
	if err := json.Unmarshal(projected, &event); err != nil {
		t.Fatalf("decode projected %s fixture: %v", eventType, err)
	}
	for _, forbidden := range []string{"payload", "session_id", "thread_id", "runtime_input_id", "queue_job_id", "partition_key", "correlation_id"} {
		if _, exists := event[forbidden]; exists {
			t.Fatalf("projected %s fixture retained forbidden field %q: %s", eventType, forbidden, projected)
		}
	}
	if string(event["id"]) != strconv.Quote(id) || string(event["type"]) != strconv.Quote(eventType) || string(event["processed_at"]) != strconv.Quote(*processedAt) {
		t.Fatalf("projected %s fixture did not preserve authoritative row envelope: %s", eventType, projected)
	}
	return string(projected)
}

func publicProjectionTypeScriptFixture(sdkEventsPath string, registryEventTypes map[string]struct{}, requiredEventTypes map[string]struct{}, projectedEvents []string) string {
	var fixture strings.Builder
	fmt.Fprintf(&fixture, "import type { BetaManagedAgentsSessionEvent } from %q;\n\n", sdkEventsPath)
	fixture.WriteString("type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;\n")
	fixture.WriteString("type Assert<T extends true> = T;\n")
	fmt.Fprintf(&fixture, "type RegistryEventType = %s;\n", typeScriptStringUnion(registryEventTypes))
	fmt.Fprintf(&fixture, "type EngineEventType = %s;\n", typeScriptStringUnion(requiredEventTypes))
	fixture.WriteString("type RegistryTracksForkUnion = Assert<Equal<RegistryEventType, BetaManagedAgentsSessionEvent['type']>>;\n")
	fixture.WriteString("type EngineTypesExistInFork = Assert<Equal<EngineEventType, Extract<RegistryEventType, EngineEventType>>>;\n\n")
	for index, event := range projectedEvents {
		fmt.Fprintf(&fixture, "const projectedEvent%d = %s satisfies BetaManagedAgentsSessionEvent;\n", index, event)
	}
	fixture.WriteString("\nconst projectedEvents = [\n")
	for index := range projectedEvents {
		fmt.Fprintf(&fixture, "  projectedEvent%d,\n", index)
	}
	fixture.WriteString("] satisfies readonly BetaManagedAgentsSessionEvent[];\n")
	fixture.WriteString("void projectedEvents;\n")
	return fixture.String()
}

func typeScriptStringUnion(values map[string]struct{}) string {
	sorted := sortedStaticStringSet(values)
	quoted := make([]string, 0, len(sorted))
	for _, value := range sorted {
		quoted = append(quoted, strconv.Quote(value))
	}
	return strings.Join(quoted, " | ")
}

func stringSliceSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func sortedStaticStringSet(values map[string]struct{}) []string {
	sorted := make([]string, 0, len(values))
	for value := range values {
		sorted = append(sorted, value)
	}
	sort.Strings(sorted)
	return sorted
}

func assertStaticStringSetsEqual(t *testing.T, label string, want map[string]struct{}, got map[string]struct{}) {
	t.Helper()
	var missing []string
	var extra []string
	for value := range want {
		if _, ok := got[value]; !ok {
			missing = append(missing, value)
		}
	}
	for value := range got {
		if _, ok := want[value]; !ok {
			extra = append(extra, value)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("%s differ: missing=%v extra=%v", label, missing, extra)
	}
}
