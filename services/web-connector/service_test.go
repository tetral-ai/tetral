package webconnector

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

func TestRunWebRejectsMissingAuthenticatedIdentityBeforeDependencies(t *testing.T) {
	t.Parallel()
	service := NewService(blob.NewFakeBlobStore(), &fakeBackend{}, NewBindingVerifier([]byte("binding-verifier-key-with-at-least-32-bytes"), time.Now), NewMetrics(), time.Now, nil)
	var logs bytes.Buffer
	service.WithLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	_, err := service.RunWeb(context.Background(), validRequest(t))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s", status.Code(err))
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &record); err != nil {
		t.Fatalf("decode failure log: %v", err)
	}
	delete(record, "time")
	if len(record) != 4 ||
		record["level"] != "ERROR" ||
		record["msg"] != "web.request.failed" ||
		record["operation"] != "search" ||
		record["grpc.code"] != "Unauthenticated" {
		t.Fatalf("failure log fields = %#v", record)
	}
}

func TestRunWebRejectsMismatchedBindingBeforeBackendAndStorage(t *testing.T) {
	t.Parallel()
	backend := &fakeBackend{}
	service := NewService(blob.NewFakeBlobStore(), backend, NewBindingVerifier([]byte("binding-verifier-key-with-at-least-32-bytes"), time.Now), NewMetrics(), time.Now, nil)
	request := validRequest(t)
	request.RuntimeBindingToken = signRequest(request, "different-pod", time.Now().Add(time.Hour), []byte("binding-verifier-key-with-at-least-32-bytes"))
	ctx := grpcauth.ContextWithIdentity(context.Background(), grpcauth.Identity{KubernetesPodUID: "runtime-pod"})
	_, err := service.RunWeb(ctx, request)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %s", status.Code(err))
	}
	if backend.calls != 0 {
		t.Fatalf("backend calls = %d", backend.calls)
	}
}

func TestRunWebSearchAndOpenSumBackendUsage(t *testing.T) {
	t.Parallel()
	backend := &fakeBackend{
		search:        []SearchHit{{URL: "https://example.com/", Title: "Example", Description: "sample"}},
		page:          Page{URL: "https://example.com/", Title: "Example", Content: "alpha\nbeta alpha", TargetHTTPStatus: 200},
		searchOutcome: BackendOutcome{Kind: BackendSuccess, Tokens: 17, Requests: 2},
		fetchOutcome:  BackendOutcome{Kind: BackendSuccess, Tokens: 29, Requests: 1},
	}
	key := []byte("binding-verifier-key-with-at-least-32-bytes")
	now := func() time.Time { return time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC) }
	service := NewService(blob.NewFakeBlobStore(), backend, NewBindingVerifier(key, now), NewMetrics(), now, nil)
	request := validRequest(t)
	request.Input = &providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "example", Domains: []string{"example.com", "example.org"}}}, Open: []*providergatewayv1.WebOpenRequest{{Url: strptr("https://example.com/")}}}
	request.RuntimeBindingToken = signRequest(request, "runtime-pod", now().Add(time.Hour), key)
	ctx := grpcauth.ContextWithIdentity(context.Background(), grpcauth.Identity{KubernetesPodUID: "runtime-pod"})
	response, err := service.RunWeb(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
		t.Fatalf("status = %s", response.GetStatus())
	}
	if backend.calls != 2 {
		t.Fatalf("backend calls = %d; want search+fetch", backend.calls)
	}
	usage := response.GetUsage()
	if usage.GetOperation() != "mixed" || usage.GetBackendTokens() != 46 || usage.GetWebSearchRequests() != 2 || usage.GetWebFetchRequests() != 1 {
		t.Fatalf("usage = %+v", usage)
	}
	if usage.GetStoredBytes() <= 0 || usage.GetDurationMs() < 0 {
		t.Fatalf("usage = %+v", response.GetUsage())
	}
	if len(response.GetRefs()) != 2 {
		t.Fatalf("refs = %d", len(response.GetRefs()))
	}
}

func TestRunWebBoundsEightEscapeDenseOpenWindowsBeforeRuntimeEncoding(t *testing.T) {
	t.Parallel()
	controlDense := strings.Repeat(string([]byte{0x01}), maxSnapshotBytes)
	pages := make([]Page, maxOperations)
	outcomes := make([]BackendOutcome, maxOperations)
	opens := make([]*providergatewayv1.WebOpenRequest, maxOperations)
	for index := range maxOperations {
		url := fmt.Sprintf("https://example.com/page-%d", index)
		pages[index] = Page{URL: url, Content: controlDense, TargetHTTPStatus: 200}
		outcomes[index] = BackendOutcome{Kind: BackendSuccess, Requests: 1, TargetHTTPStatus: int32ptr(200)}
		opens[index] = &providergatewayv1.WebOpenRequest{Url: strptr(url)}
	}
	service, key, now := testService(blob.NewFakeBlobStore(), &sequenceFetchBackend{pages: pages, outcomes: outcomes})
	request := testRequest(&providergatewayv1.WebToolInput{Open: opens}, "event-eight-escape-dense-windows", key, now)

	response, err := service.RunWeb(testContext(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
		t.Fatalf("status = %s, text = %q", response.GetStatus(), response.GetResultText())
	}
	if len([]byte(response.GetResultText())) > maxVisibleResultBytes {
		t.Fatalf("visible result bytes = %d; want <= %d", len([]byte(response.GetResultText())), maxVisibleResultBytes)
	}
	canonical, err := json.Marshal(map[string]string{"text": response.GetResultText()})
	if err != nil {
		t.Fatal(err)
	}
	if len(canonical) > maxModelVisibleToolOutputJSONBytes {
		t.Fatalf("canonical tool output bytes = %d; want <= %d", len(canonical), maxModelVisibleToolOutputJSONBytes)
	}
	if !response.GetWindowTruncated() || response.NextLineno != nil || strings.Count(response.GetResultText(), "[truncated — continue with open(") != maxOperations {
		t.Fatalf("truncation markers/window flags = %d/%t/%v; want %d/true/nil",
			strings.Count(response.GetResultText(), "[truncated — continue with open("), response.GetWindowTruncated(), response.NextLineno, maxOperations)
	}
}

func TestRunWebFailedSearchDoesNotCountSuccessfulBackendRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		query      string
		outcome    BackendOutcome
		wantStatus providergatewayv1.RunWebStatus
	}{
		{
			name:       "unresolvable query",
			query:      "site:unresolvable.invalid",
			outcome:    BackendOutcome{Kind: BackendToolError, Message: "Search query could not be resolved.", Tokens: 11},
			wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR,
		},
		{
			name:       "key pool exhaustion",
			query:      "provider capacity",
			outcome:    BackendOutcome{Kind: BackendRuntimeError, Message: "Web backend request failed.", Tokens: 13},
			wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			backend := &fakeBackend{searchOutcome: test.outcome}
			key := []byte("binding-verifier-key-with-at-least-32-bytes")
			now := func() time.Time { return time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC) }
			service := NewService(blob.NewFakeBlobStore(), backend, NewBindingVerifier(key, now), NewMetrics(), now, nil)
			request := validRequest(t)
			request.Input = &providergatewayv1.WebToolInput{
				SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: test.query}},
			}
			request.RuntimeBindingToken = signRequest(request, "runtime-pod", now().Add(time.Hour), key)
			ctx := grpcauth.ContextWithIdentity(context.Background(), grpcauth.Identity{KubernetesPodUID: "runtime-pod"})

			response, err := service.RunWeb(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if response.GetStatus() != test.wantStatus {
				t.Fatalf("status = %s; want %s", response.GetStatus(), test.wantStatus)
			}
			if backend.calls != 1 {
				t.Fatalf("backend calls = %d; want one failed search", backend.calls)
			}
			usage := response.GetUsage()
			if usage.GetOperation() != "search" ||
				usage.GetBackendTokens() != test.outcome.Tokens ||
				usage.GetWebSearchRequests() != 0 ||
				usage.GetWebFetchRequests() != 0 {
				t.Fatalf("usage = %+v; want failed search with zero successful request counts", usage)
			}
		})
	}
}

func TestRunWebValidationErrorHasUsageAndNoSideEffects(t *testing.T) {
	t.Parallel()
	backend := &fakeBackend{}
	key := []byte("binding-verifier-key-with-at-least-32-bytes")
	now := func() time.Time { return time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC) }
	service := NewService(blob.NewFakeBlobStore(), backend, NewBindingVerifier(key, now), NewMetrics(), now, nil)
	request := validRequest(t)
	request.Input = &providergatewayv1.WebToolInput{}
	request.RuntimeBindingToken = signRequest(request, "runtime-pod", now().Add(time.Hour), key)
	ctx := grpcauth.ContextWithIdentity(context.Background(), grpcauth.Identity{KubernetesPodUID: "runtime-pod"})
	response, err := service.RunWeb(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR || response.GetUsage() == nil {
		t.Fatalf("response = %+v", response)
	}
	if backend.calls != 0 {
		t.Fatalf("backend calls = %d", backend.calls)
	}
}

type fakeBackend struct {
	calls         int
	search        []SearchHit
	page          Page
	searchOutcome BackendOutcome
	fetchOutcome  BackendOutcome
}

func (f *fakeBackend) Search(context.Context, string, []string) ([]SearchHit, BackendOutcome) {
	f.calls++
	return f.search, f.searchOutcome
}
func (f *fakeBackend) Fetch(context.Context, string) (Page, BackendOutcome) {
	f.calls++
	return f.page, f.fetchOutcome
}

func validRequest(t *testing.T) *providergatewayv1.RunWebRequest {
	t.Helper()
	r := &providergatewayv1.RunWebRequest{WorkspaceId: "ws", SessionId: "ses", SessionThreadId: "thr", ToolUseEventId: "evt", BindingId: "bind", BindingGeneration: 1, Input: &providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "example"}}}}
	r.RuntimeBindingToken = signRequest(r, "runtime-pod", time.Now().Add(time.Hour), []byte("binding-verifier-key-with-at-least-32-bytes"))
	return r
}
func signRequest(r *providergatewayv1.RunWebRequest, pod string, exp time.Time, key []byte) string {
	payload, _ := json.Marshal(map[string]any{"v": 1, "workspace_id": r.GetWorkspaceId(), "session_id": r.GetSessionId(), "session_thread_id": r.GetSessionThreadId(), "binding_id": r.GetBindingId(), "binding_generation": r.GetBindingGeneration(), "runtime_pod_uid": pod, "exp": exp.Unix()})
	part := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(part))
	return "rtbt_v1." + part + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func strptr(v string) *string { return &v }
