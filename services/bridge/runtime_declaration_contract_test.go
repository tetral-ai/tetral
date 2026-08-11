package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestRuntimeDeclarationContractPreservesCanonicalBusinessValues(t *testing.T) {
	const canary = "CANARY_TOKEN_VALUE"
	create := &bridgev1.RuntimeMessageCreate{
		MessageKind:     bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REVIEWER_INPUT,
		MessageInfoJson: `{"role":"user","origin":"runtime","status":"completed","responseId":"response_1"}`,
		Parts: []*bridgev1.RuntimePartCreate{{
			PartKind: "tool",
			PartJson: `{"type":"tool","toolCallId":"call_1","toolName":"inspect","state":{"status":"running","input":{"value":{"ordinary":"` + canary + `"},"preview":"` + canary + `","truncated":false}}}`,
		}},
	}
	message, err := validateRuntimeMessageCreate(create)
	if err != nil {
		t.Fatalf("validate canonical declaration: %v", err)
	}
	if message["origin"] != "runtime" || message["responseId"] != "response_1" {
		t.Fatalf("canonical message values changed: %#v", message)
	}
	part, err := validateRuntimePartCreate(create.GetParts()[0])
	if err != nil {
		t.Fatalf("validate canonical part: %v", err)
	}
	if !strings.Contains(part["state"].(map[string]any)["input"].(map[string]any)["preview"].(string), canary) {
		t.Fatalf("ordinary preview canary changed: %#v", part)
	}
}

func TestRuntimeDeclarationBoundsMeasureJSONStringifyUTF8(t *testing.T) {
	metadataValue := strings.Repeat("x", runtimeProviderMetadataMaxBytes-15) + "\u2028"
	part := &bridgev1.RuntimePartCreate{
		PartKind: "reasoning",
		PartJson: `{"type":"reasoning","providerMetadata":{"value":"` + metadataValue + `"},"text":"ok","truncated":false,"status":"completed"}`,
	}
	if _, err := validateRuntimePartCreate(part); err != nil {
		t.Fatalf("validate exact JSON.stringify metadata bound: %v", err)
	}
	if got := runtimeJSONBytes(map[string]any{"value": metadataValue}); got != runtimeProviderMetadataMaxBytes {
		t.Fatalf("metadata bytes = %d; want %d", got, runtimeProviderMetadataMaxBytes)
	}
	part.PartJson = `{"type":"reasoning","providerMetadata":{"value":"` + metadataValue + `x"},"text":"ok","truncated":false,"status":"completed"}`
	if _, err := validateRuntimePartCreate(part); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("over-bound metadata err = %v; want InvalidArgument", err)
	}
}

func TestRuntimeDeclarationIntegersMatchJSONSafeDomain(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		value json.Number
		want  bool
	}{
		{name: "maximum safe integer", value: "9007199254740991", want: true},
		{name: "safe exponent form", value: "1e3", want: true},
		{name: "integral decimal exponent form", value: "1.5e1", want: true},
		{name: "maximum safe integer exponent form", value: "90071992547409910e-1", want: true},
		{name: "first unsafe integer", value: "9007199254740992", want: false},
		{name: "rounded unsafe integer", value: "9007199254740993", want: false},
		{name: "near-limit fraction rounded by float64", value: "9007199254740991.1", want: false},
		{name: "fraction", value: "1.5", want: false},
		{name: "negative", value: "-1", want: false},
		{name: "malformed exponent", value: "1e", want: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := runtimeNonNegativeInteger(testCase.value); got != testCase.want {
				t.Fatalf("runtimeNonNegativeInteger(%s) = %t; want %t", testCase.value, got, testCase.want)
			}
		})
	}

	create := &bridgev1.RuntimeMessageCreate{
		MessageKind: bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REVIEWER_INPUT,
		MessageInfoJson: `{"role":"assistant","origin":"runtime","status":"completed","usage":{` +
			`"inputTokens":9007199254740991,"outputTokens":0,"reasoningTokens":0,"cacheReadTokens":0,"cacheWriteTokens":0}}`,
	}
	if _, err := validateRuntimeMessageCreate(create); err != nil {
		t.Fatalf("validate maximum-safe message usage: %v", err)
	}
	create.MessageInfoJson = strings.Replace(create.MessageInfoJson, "9007199254740991", "9007199254740992", 1)
	if _, err := validateRuntimeMessageCreate(create); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsafe message usage err = %v; want InvalidArgument", err)
	}
}

func TestRuntimePartCreateContractIsSharedByCreateAppendAndSeal(t *testing.T) {
	carriers := []struct {
		name string
		run  func(*bridgev1.RuntimePartCreate) error
	}{
		{
			name: "message create",
			run: func(part *bridgev1.RuntimePartCreate) error {
				_, err := canonicalRuntimeMessageCreate(&bridgev1.RuntimeMessageCreate{
					MessageKind:     bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_USER_INPUT,
					MessageInfoJson: `{"role":"user","origin":"user","status":"completed"}`,
					Parts:           []*bridgev1.RuntimePartCreate{part},
				})
				return err
			},
		},
		{
			name: "assistant append",
			run: func(part *bridgev1.RuntimePartCreate) error {
				_, err := canonicalRuntimeAssistantPartAppend(&bridgev1.RuntimeAssistantPartAppend{Parts: []*bridgev1.RuntimePartCreate{part}})
				return err
			},
		},
		{
			name: "request end trailing append",
			run: func(part *bridgev1.RuntimePartCreate) error {
				_, err := writeRequestEndDeclarationDigest(&bridgev1.WriteRequestEndRequest{
					Scope:          bridgeAPIScope("session_1", "thread_1", "binding_1", 1, "pod_1"),
					RuntimeWriteId: "write_1", ModelRequestId: "request_1", ModelRequestStartEventId: "event_1",
					RequestKind: "agent_provider_request", FinishReason: "stop", UsageJson: `{}`,
					TrailingPartAppend: &bridgev1.RuntimeAssistantPartAppend{Parts: []*bridgev1.RuntimePartCreate{part}},
				}, "agent_provider_request", "stop", `{}`, `[]`, `[]`)
				return err
			},
		},
	}
	mutations := []struct {
		name      string
		canonical *bridgev1.RuntimePartCreate
		invalid   *bridgev1.RuntimePartCreate
	}{
		{
			name:      "unknown field",
			canonical: &bridgev1.RuntimePartCreate{PartKind: "text", PartJson: `{"type":"text","text":"unchanged","truncated":false,"status":"completed"}`},
			invalid:   &bridgev1.RuntimePartCreate{PartKind: "text", PartJson: `{"type":"text","text":"unchanged","truncated":false,"status":"completed","unknown":true}`},
		},
		{
			name:      "durable identity",
			canonical: &bridgev1.RuntimePartCreate{PartKind: "step-start", PartJson: `{"type":"step-start","stepIndex":0}`},
			invalid:   &bridgev1.RuntimePartCreate{PartKind: "step-start", PartJson: `{"type":"step-start","stepIndex":0,"id":"part_1"}`},
		},
		{
			name:      "unsafe integer",
			canonical: &bridgev1.RuntimePartCreate{PartKind: "step-start", PartJson: `{"type":"step-start","stepIndex":9007199254740991}`},
			invalid:   &bridgev1.RuntimePartCreate{PartKind: "step-start", PartJson: `{"type":"step-start","stepIndex":9007199254740992}`},
		},
		{
			name:      "malformed member",
			canonical: &bridgev1.RuntimePartCreate{PartKind: "text", PartJson: `{"type":"text","text":"unchanged","truncated":false,"status":"completed"}`},
			invalid:   &bridgev1.RuntimePartCreate{PartKind: "text", PartJson: `{"type":"text","text":"unchanged","truncated":false}`},
		},
		{
			name:      "over-bound preview",
			canonical: &bridgev1.RuntimePartCreate{PartKind: "tool", PartJson: `{"type":"tool","toolCallId":"call_1","toolName":"inspect","state":{"status":"running","input":{"value":{},"preview":"ok","truncated":false}}}`},
			invalid:   &bridgev1.RuntimePartCreate{PartKind: "tool", PartJson: `{"type":"tool","toolCallId":"call_1","toolName":"inspect","state":{"status":"running","input":{"value":{},"preview":"` + strings.Repeat("x", runtimePreviewTextMaxBytes+1) + `","truncated":true}}}`},
		},
		{
			name:      "uncanonical Tool error",
			canonical: &bridgev1.RuntimePartCreate{PartKind: "tool", PartJson: `{"type":"tool","toolCallId":"call_1","toolName":"inspect","state":{"status":"error","error":{"type":"tool_failure","message":"[redacted:safe]"}}}`},
			invalid:   &bridgev1.RuntimePartCreate{PartKind: "tool", PartJson: `{"type":"tool","toolCallId":"call_1","toolName":"inspect","state":{"status":"error","error":{"type":"tool_failure","message":"authorization: bearer secret"}}}`},
		},
	}

	for _, carrier := range carriers {
		t.Run(carrier.name, func(t *testing.T) {
			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					if err := carrier.run(mutation.canonical); err != nil {
						t.Fatalf("canonical part rejected: %v", err)
					}
					if err := carrier.run(mutation.invalid); status.Code(err) != codes.InvalidArgument {
						t.Fatalf("mutated part err = %v; want InvalidArgument", err)
					}
				})
			}
		})
	}
}

func TestRuntimeDeclarationContractRejectsUncanonicalTransformFieldsOnly(t *testing.T) {
	fields := []string{"message", "constraint", "messageId", "partId", "sessionId", "providerId", "modelId"}
	for _, field := range fields {
		t.Run(field, func(t *testing.T) {
			failure := map[string]any{
				"type": "runtime", "code": "gateway_stream_error", "message": "safe failure",
				"retryable": true, "fatal": false,
			}
			failure[field] = "https://secret.example/path"
			failureJSON, err := json.Marshal(failure)
			if err != nil {
				t.Fatal(err)
			}
			create := &bridgev1.RuntimeMessageCreate{
				MessageKind:     bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REJECTION,
				MessageInfoJson: `{"role":"assistant","origin":"agent","status":"failed","error":` + string(failureJSON) + `}`,
				Parts: []*bridgev1.RuntimePartCreate{{
					PartKind: "text",
					PartJson: `{"type":"text","text":"https://secret.example/path","truncated":false,"status":"completed"}`,
				}},
			}
			if _, err := validateRuntimeMessageCreate(create); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("uncanonical failure field err = %v; want InvalidArgument", err)
			}
			failure[field] = "[redacted:safe]"
			failureJSON, err = json.Marshal(failure)
			if err != nil {
				t.Fatal(err)
			}
			create.MessageInfoJson = `{"role":"assistant","origin":"agent","status":"failed","error":` + string(failureJSON) + `}`
			if _, err := validateRuntimeMessageCreate(create); err != nil {
				t.Fatalf("canonical failure field rejected: %v", err)
			}
		})
	}
}

func TestRuntimeDeclarationContractRejectsUnknownDurableAndOverBoundFields(t *testing.T) {
	cases := []struct {
		name    string
		message string
		part    string
	}{
		{name: "unknown message field", message: `{"role":"user","origin":"runtime","status":"completed","unknown":true}`, part: `{"type":"text","text":"ok","truncated":false,"status":"completed"}`},
		{name: "durable message identity", message: `{"id":"message_1","role":"user","origin":"runtime","status":"completed"}`, part: `{"type":"text","text":"ok","truncated":false,"status":"completed"}`},
		{name: "durable part identity", message: `{"role":"user","origin":"runtime","status":"completed"}`, part: `{"id":"part_1","type":"text","text":"ok","truncated":false,"status":"completed"}`},
		{name: "over-bound metadata", message: `{"role":"user","origin":"runtime","status":"completed"}`, part: `{"type":"reasoning","providerMetadata":{"value":"` + strings.Repeat("x", runtimeProviderMetadataMaxBytes) + `"},"text":"ok","truncated":false,"status":"completed"}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := validateRuntimeMessageCreate(&bridgev1.RuntimeMessageCreate{
				MessageKind:     bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REVIEWER_INPUT,
				MessageInfoJson: testCase.message,
				Parts:           []*bridgev1.RuntimePartCreate{{PartKind: declarationPartKind(testCase.part), PartJson: testCase.part}},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("err = %v; want InvalidArgument", err)
			}
		})
	}
}

func declarationPartKind(raw string) string {
	if strings.Contains(raw, `"type":"reasoning"`) {
		return "reasoning"
	}
	return "text"
}

func TestRuntimeDeclarationRejectionLoggingIsBoundedAndFailOpen(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	logRuntimeDeclarationRejected(logger, bridgeAPIScope("session_1", "thread_1", "binding_1", 1, "pod_1"), runtimeDeclarationRejectionEvidence{
		Active: true, Kind: "canonicality", Operation: bridgeOpWriteEvent, OperationID: "write_1", MessageOrPart: "text",
	}, status.Error(codes.InvalidArgument, "raw provider payload marker"))
	logText := logs.String()
	for _, fragment := range []string{
		`"event.kind":"runtime_declaration_rejected"`,
		`"rejection.kind":"canonicality"`,
		`"operation.id":"write_1"`,
		`"declaration.kind":"text"`,
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("rejection log missing %s: %s", fragment, logText)
		}
	}
	if strings.Contains(logText, "raw provider payload marker") {
		t.Fatalf("rejection log exposed raw error: %s", logText)
	}

	logRuntimeDeclarationRejected(slog.New(panicSlogHandler{}), nil, runtimeDeclarationRejectionEvidence{
		Active: true, Kind: "authorization", Operation: bridgeOpCommitInputs,
	}, status.Error(codes.PermissionDenied, "denied"))
}

func TestRuntimeDeclarationWritersLogCanonicalRejectionsOnceAndFailOpen(t *testing.T) {
	const payloadCanary = "raw provider payload marker"
	invalidPart := &bridgev1.RuntimePartCreate{
		PartKind: "text",
		PartJson: `{"type":"text","text":"ordinary","truncated":false,"status":"completed","forbidden":"` + payloadCanary + `"}`,
	}
	scope := bridgeAPIScope("session_1", "thread_1", "binding_1", 1, "pod_1")
	cases := []struct {
		name      string
		operation string
		call      func(*PostgreSQLBridgeAPIStore) error
	}{
		{
			name: "commit inputs", operation: bridgeOpCommitInputs,
			call: func(store *PostgreSQLBridgeAPIStore) error {
				_, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
					Scope: scope, RuntimeInputId: "input_1", InputKind: "messages",
					EventIds: []string{"event_1"}, SequenceFrom: 1, SequenceTo: 1,
					MessageCreates: []*bridgev1.RuntimeMessageCreate{{
						SourceEventId:   bridgeAPIString("event_1"),
						MessageKind:     bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_USER_INPUT,
						MessageInfoJson: `{"role":"user","origin":"user","status":"completed"}`,
						Parts:           []*bridgev1.RuntimePartCreate{invalidPart},
					}},
				})
				return err
			},
		},
		{
			name: "write event", operation: bridgeOpWriteEvent,
			call: func(store *PostgreSQLBridgeAPIStore) error {
				_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
					Scope: scope, RuntimeWriteId: "write_1", ModelRequestId: "request_1",
					EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"ordinary"}]}`,
					Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: &bridgev1.RuntimeAssistantPartAppend{
						Parts: []*bridgev1.RuntimePartCreate{invalidPart},
					}},
				})
				return err
			},
		},
		{
			name: "write request end", operation: bridgeOpWriteRequestEnd,
			call: func(store *PostgreSQLBridgeAPIStore) error {
				_, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
					Scope: scope, RuntimeWriteId: "end_1", ModelRequestId: "request_1", ModelRequestStartEventId: "start_1",
					RequestKind: "agent_provider_request", FinishReason: "stop", UsageJson: `{}`,
					TrailingPartAppend: &bridgev1.RuntimeAssistantPartAppend{Parts: []*bridgev1.RuntimePartCreate{invalidPart}},
				})
				return err
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var logs bytes.Buffer
			store := NewPostgreSQLBridgeAPIStore(nil)
			store.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
			if err := testCase.call(store); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("writer err = %v; want InvalidArgument", err)
			}
			lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
			if len(lines) != 1 || !strings.Contains(lines[0], `"event.kind":"runtime_declaration_rejected"`) ||
				!strings.Contains(lines[0], `"operation":"`+testCase.operation+`"`) ||
				!strings.Contains(lines[0], `"rejection.kind":"canonicality"`) {
				t.Fatalf("writer rejection logs = %q; want one canonicality record", logs.String())
			}
			if strings.Contains(logs.String(), payloadCanary) {
				t.Fatalf("writer rejection log exposed declaration payload: %s", logs.String())
			}

			store.Logger = slog.New(panicSlogHandler{})
			if err := testCase.call(store); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("writer err with throwing logger = %v; want unchanged InvalidArgument", err)
			}
		})
	}
}

func TestPostgreSQLRuntimeDeclarationWritersLogRelationalRejectionsSafely(t *testing.T) {
	const (
		sessionID = "sesn_declaration_relational_log"
		threadID  = "thr_declaration_relational_log"
		bindingID = "bind_declaration_relational_log"
		podUID    = "pod_declaration_relational_log"
		canary    = "RELATIONAL_DECLARATION_PAYLOAD_CANARY"
	)
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	appendValue := bridgeRuntimeOutputAppendForTest(t, scope, "relational_log", "agent.message", "completed",
		bridgeRuntimePartCreateForTest{kind: "text", json: `{"type":"text","text":"` + canary + `","truncated":false,"status":"completed"}`})

	cases := []struct {
		name                string
		operation           string
		threadRoleAvailable bool
		call                func(*PostgreSQLBridgeAPIStore, string) error
	}{
		{
			name: "write event", operation: bridgeOpWriteEvent, threadRoleAvailable: true,
			call: func(store *PostgreSQLBridgeAPIStore, writeID string) error {
				_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
					Scope: scope, RuntimeWriteId: writeID, ModelRequestId: "mreq_missing_relational_log",
					EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"` + canary + `"}]}`,
					Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: appendValue},
				})
				return err
			},
		},
		{
			name: "write request end", operation: bridgeOpWriteRequestEnd,
			call: func(store *PostgreSQLBridgeAPIStore, writeID string) error {
				_, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
					Scope: scope, RuntimeWriteId: writeID, ModelRequestId: "mreq_missing_relational_log",
					ModelRequestStartEventId: "evt_missing_relational_log", RequestKind: "agent_provider_request",
					FinishReason: "stop", UsageJson: `{}`, TrailingPartAppend: appendValue,
				})
				return err
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var logs bytes.Buffer
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
			store.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
			err := testCase.call(store, "rwrite_"+strings.ReplaceAll(testCase.name, " ", "_"))
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("relational rejection err = %v; want FailedPrecondition", err)
			}
			lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
			if len(lines) != 1 || !strings.Contains(lines[0], `"event.kind":"runtime_declaration_rejected"`) ||
				!strings.Contains(lines[0], `"operation":"`+testCase.operation+`"`) ||
				!strings.Contains(lines[0], `"rejection.kind":"lineage"`) {
				t.Fatalf("relational rejection logs = %q; want one classified record", logs.String())
			}
			if strings.Contains(lines[0], `"thread.role":"main"`) != testCase.threadRoleAvailable {
				t.Fatalf("relational rejection thread role availability = %q", logs.String())
			}
			if strings.Contains(logs.String(), canary) {
				t.Fatalf("relational rejection log exposed declaration payload: %s", logs.String())
			}

			store.Logger = slog.New(panicSlogHandler{})
			throwingErr := testCase.call(store, "rwrite_throwing_"+strings.ReplaceAll(testCase.name, " ", "_"))
			if status.Code(throwingErr) != codes.FailedPrecondition {
				t.Fatalf("relational rejection with throwing logger err = %v; want FailedPrecondition", throwingErr)
			}
		})
	}
}

func TestWriteEventWithoutRuntimeDeclarationDoesNotEmitDeclarationRejection(t *testing.T) {
	var logs bytes.Buffer
	store := NewPostgreSQLBridgeAPIStore(nil)
	store.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("session_1", "thread_1", "binding_1", 1, "pod_1"),
		RuntimeWriteId: "write_1", EventType: "not.writable", PayloadJson: `{}`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent err = %v; want InvalidArgument", err)
	}
	if logs.Len() != 0 {
		t.Fatalf("ordinary event rejection emitted declaration log: %s", logs.String())
	}
}

type panicSlogHandler struct{}

func (panicSlogHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (panicSlogHandler) Handle(context.Context, slog.Record) error { panic("logger failed") }
func (panicSlogHandler) WithAttrs([]slog.Attr) slog.Handler        { return panicSlogHandler{} }
func (panicSlogHandler) WithGroup(string) slog.Handler             { return panicSlogHandler{} }
