package webconnector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidationFailuresPerformNoBackendOrBlobOperations(t *testing.T) {
	t.Parallel()
	queries := make([]*providergatewayv1.WebSearchQuery, 9)
	for i := range queries {
		queries[i] = &providergatewayv1.WebSearchQuery{Q: "q"}
	}
	tests := []struct {
		name    string
		input   *providergatewayv1.WebToolInput
		mutate  func(*providergatewayv1.RunWebRequest)
		message string
	}{
		{name: "missing-scope-or-event-identity", input: &providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "q"}}}, mutate: func(request *providergatewayv1.RunWebRequest) { request.SessionThreadId = "" }, message: "invalid web request: missing scope or event identity"},
		{name: "empty-input", input: &providergatewayv1.WebToolInput{}, message: "invalid web request: empty input"},
		{name: "input-item-limit-exceeded", input: &providergatewayv1.WebToolInput{SearchQuery: queries}, message: "invalid web request: input item limit exceeded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objects := blob.NewFakeBlobStore()
			backend := &fakeBackend{}
			service, key, now := testService(objects, backend)
			request := testRequest(test.input, "event-validation-"+test.name, key, now)
			if test.mutate != nil {
				test.mutate(request)
			}
			response, err := service.RunWeb(testContext(), request)
			if err != nil {
				t.Fatal(err)
			}
			if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR || response.GetResultText() != test.message {
				t.Fatalf("response=%+v; want %q", response, test.message)
			}
			if backend.calls != 0 || objects.Len() != 0 {
				t.Fatalf("backend=%d objects=%d", backend.calls, objects.Len())
			}
		})
	}
}

func TestOpenByRefAndFindReadStoredSnapshotWithoutBackendCalls(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{}
	service, key, now := testService(objects, backend)
	stored := NewSnapshotStore(objects, zeroRandom, now)
	ref, _, err := stored.StorePage(context.Background(), Scope{"ws", "ses", "thr"}, Page{URL: "https://example.com/", Title: "Example", Content: "Alpha\nbeta alpha", TargetHTTPStatus: 200})
	if err != nil {
		t.Fatal(err)
	}
	input := &providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{RefId: stringptr(ref.ID)}}, Find: []*providergatewayv1.WebFindRequest{{RefId: ref.ID, Pattern: "(?i)alpha"}}}
	response, err := service.RunWeb(testContext(), testRequest(input, "event-local", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || backend.calls != 0 {
		t.Fatalf("response=%+v backend=%d", response, backend.calls)
	}
	if response.GetUsage().GetBackendTokens() != 0 || response.GetUsage().GetWebFetchRequests() != 0 {
		t.Fatalf("usage=%+v", response.GetUsage())
	}
	if !strings.Contains(response.GetResultText(), "2 matches") {
		t.Fatalf("result=%q", response.GetResultText())
	}
}

func TestFindOnSearchStubMaterializesSnapshotBeforeScanningAndThenStaysLocal(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{
		page: Page{
			URL:              "https://example.com/article",
			Title:            "Article",
			Content:          "Needle one\nplain\nneedle two",
			TargetHTTPStatus: 200,
		},
		fetchOutcome: BackendOutcome{
			Kind:             BackendSuccess,
			Tokens:           37,
			Requests:         1,
			TargetHTTPStatus: int32ptr(200),
		},
	}
	service, key, now := testService(objects, backend)
	stored := NewSnapshotStore(objects, zeroRandom, now)
	scope := Scope{"ws", "ses", "thr"}
	stub, _, err := stored.StoreStub(context.Background(), scope, SearchHit{URL: "https://example.com/article", Title: "Article"})
	if err != nil {
		t.Fatal(err)
	}

	input := &providergatewayv1.WebToolInput{Find: []*providergatewayv1.WebFindRequest{{RefId: stub.ID, Pattern: "(?i)needle"}}}
	first, err := service.RunWeb(testContext(), testRequest(input, "event-find-stub-first", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if first.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || !strings.Contains(first.GetResultText(), "2 matches") {
		t.Fatalf("first response=%+v", first)
	}
	if backend.calls != 1 || !objects.Has(docKey(scope, stub.ID)) {
		t.Fatalf("backend calls=%d snapshot=%t; want one lazy fetch and a materialized document", backend.calls, objects.Has(docKey(scope, stub.ID)))
	}
	if first.GetUsage().GetOperation() != "find" || first.GetUsage().GetWebFetchRequests() != 1 || first.GetUsage().GetBackendTokens() != 37 || first.GetUsage().GetTargetHttpStatus() != 200 {
		t.Fatalf("first usage=%+v", first.GetUsage())
	}

	second, err := service.RunWeb(testContext(), testRequest(input, "event-find-stub-local", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if second.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || !strings.Contains(second.GetResultText(), "2 matches") {
		t.Fatalf("second response=%+v", second)
	}
	if backend.calls != 1 {
		t.Fatalf("backend calls=%d; materialized find must stay local", backend.calls)
	}
	if second.GetUsage().GetWebFetchRequests() != 0 || second.GetUsage().GetBackendTokens() != 0 || second.GetUsage().TargetHttpStatus != nil {
		t.Fatalf("second usage=%+v", second.GetUsage())
	}
}

func TestFindOnSearchStubAppliesStoredURLClassifierBeforeLazyFetch(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{}
	service, key, now := testService(objects, backend)
	stored := NewSnapshotStore(objects, zeroRandom, now)
	scope := Scope{"ws", "ses", "thr"}
	stub, _, err := stored.StoreStub(context.Background(), scope, SearchHit{URL: "http://127。0。0。1/", Title: "denied"})
	if err != nil {
		t.Fatal(err)
	}

	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Find: []*providergatewayv1.WebFindRequest{{RefId: stub.ID, Pattern: "needle"}}}, "event-find-denied-stub", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR || response.GetResultText() != "URL not allowed: non_public_ip" {
		t.Fatalf("response=%+v", response)
	}
	if backend.calls != 0 || objects.Has(docKey(scope, stub.ID)) {
		t.Fatalf("backend calls=%d snapshot=%t; denied stored URL must stop before fetch", backend.calls, objects.Has(docKey(scope, stub.ID)))
	}
}

func TestFindOnSearchStubPreservesLazyFetchFailureTaxonomyAndUsage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		outcome           BackendOutcome
		wantStatus        providergatewayv1.RunWebStatus
		wantText          string
		wantFetchRequests int64
	}{
		{
			name:       "invalid-target",
			outcome:    BackendOutcome{Kind: BackendToolError, Tokens: 11},
			wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR,
			wantText:   "URL could not be fetched",
		},
		{
			name:       "backend-unavailable",
			outcome:    BackendOutcome{Kind: BackendRuntimeError, Tokens: 13},
			wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR,
			wantText:   "web backend temporarily unavailable",
		},
		{
			name:              "target-http-error",
			outcome:           BackendOutcome{Kind: BackendToolError, Tokens: 17, Requests: 1, TargetHTTPStatus: int32ptr(404)},
			wantStatus:        providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR,
			wantText:          "target returned HTTP 404",
			wantFetchRequests: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objects := blob.NewFakeBlobStore()
			backend := &fakeBackend{fetchOutcome: test.outcome}
			service, key, now := testService(objects, backend)
			stored := NewSnapshotStore(objects, zeroRandom, now)
			scope := Scope{"ws", "ses", "thr"}
			stub, _, err := stored.StoreStub(context.Background(), scope, SearchHit{URL: "https://example.com/article", Title: "Article"})
			if err != nil {
				t.Fatal(err)
			}

			response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Find: []*providergatewayv1.WebFindRequest{{RefId: stub.ID, Pattern: "needle"}}}, "event-find-failure-"+test.name, key, now))
			if err != nil {
				t.Fatal(err)
			}
			if response.GetStatus() != test.wantStatus || response.GetResultText() != test.wantText {
				t.Fatalf("response=%+v; want status=%s text=%q", response, test.wantStatus, test.wantText)
			}
			if backend.calls != 1 || objects.Has(docKey(scope, stub.ID)) {
				t.Fatalf("backend calls=%d snapshot=%t; failed fetch must not materialize a document", backend.calls, objects.Has(docKey(scope, stub.ID)))
			}
			if response.GetUsage().GetOperation() != "find" || response.GetUsage().GetWebFetchRequests() != test.wantFetchRequests || response.GetUsage().GetBackendTokens() != test.outcome.Tokens {
				t.Fatalf("usage=%+v", response.GetUsage())
			}
			if test.outcome.TargetHTTPStatus == nil {
				if response.GetUsage().TargetHttpStatus != nil {
					t.Fatalf("target_http_status=%d; want absent", response.GetUsage().GetTargetHttpStatus())
				}
			} else if response.GetUsage().GetTargetHttpStatus() != *test.outcome.TargetHTTPStatus {
				t.Fatalf("target_http_status=%d; want %d", response.GetUsage().GetTargetHttpStatus(), *test.outcome.TargetHTTPStatus)
			}
		})
	}
}

func TestOversizedFindPatternIsRejectedAtTheServiceBoundary(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{}
	service, key, now := testService(objects, backend)
	stored := NewSnapshotStore(objects, zeroRandom, now)
	ref, _, err := stored.StorePage(context.Background(), Scope{"ws", "ses", "thr"}, Page{URL: "https://example.com/", Content: "content"})
	if err != nil {
		t.Fatal(err)
	}
	input := &providergatewayv1.WebToolInput{Find: []*providergatewayv1.WebFindRequest{{RefId: ref.ID, Pattern: strings.Repeat("a", maxPatternBytes+1)}}}
	response, err := service.RunWeb(testContext(), testRequest(input, "event-oversized-find", key, now))
	if status.Code(err) != codes.InvalidArgument || response != nil {
		t.Fatalf("RunWeb response/error = %+v/%v; want InvalidArgument", response, err)
	}
	if backend.calls != 0 {
		t.Fatalf("backend calls=%d", backend.calls)
	}
}

func TestOperationLevelBoundsReturnToolErrorsInsteadOfProtocolErrors(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{}
	service, key, now := testService(objects, backend)
	stored := NewSnapshotStore(objects, zeroRandom, now)
	ref, _, err := stored.StorePage(context.Background(), Scope{"ws", "ses", "thr"}, Page{URL: "https://example.com/", Content: "content"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		input *providergatewayv1.WebToolInput
		text  string
	}{
		{name: "zero-line", input: &providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{RefId: stringptr(ref.ID), Lineno: int32ptr(0)}}}, text: "lineno out of range: document has 1 lines"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, runErr := service.RunWeb(testContext(), testRequest(test.input, "event-operation-bound-"+test.name, key, now))
			if runErr != nil {
				t.Fatalf("RunWeb returned protocol error: %v", runErr)
			}
			if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR || response.GetResultText() != test.text {
				t.Fatalf("response=%+v; want %q", response, test.text)
			}
		})
	}
	if backend.calls != 0 {
		t.Fatalf("backend calls=%d", backend.calls)
	}
}

func TestDirectOpenStoresOnlyTheImmutableDocumentSnapshot(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{page: Page{URL: "https://example.com/", Title: "Example", Content: "body", TargetHTTPStatus: 200}, fetchOutcome: BackendOutcome{Kind: BackendSuccess, Requests: 1}}
	service, key, now := testService(objects, backend)
	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr("https://example.com/")}}}, "event-direct-open", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || len(response.GetRefs()) != 1 {
		t.Fatalf("response=%+v", response)
	}
	scope := Scope{"ws", "ses", "thr"}
	id := response.GetRefs()[0].GetRefId()
	if !objects.Has(docKey(scope, id)) || objects.Has(metaKey(scope, id)) {
		t.Fatalf("doc=%t meta=%t; direct open must store only .doc", objects.Has(docKey(scope, id)), objects.Has(metaKey(scope, id)))
	}
}

func TestAlternateAndMalformedLocalIPSpellingsAreDeniedBeforeBackendAccess(t *testing.T) {
	t.Parallel()
	for index, raw := range []string{"http://2130706433/", "http://0x7f000001/", "http://017700000001/", "http://127.1/", "http://127..1/", "http://1.2.foo.127/", "http://[fe80::1%25eth0]/", "https://example.com:99999/"} {
		objects := blob.NewFakeBlobStore()
		backend := &fakeBackend{}
		service, key, now := testService(objects, backend)
		response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr(raw)}}}, fmt.Sprintf("event-local-ip-%d", index), key, now))
		if err != nil {
			t.Fatal(err)
		}
		if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR || !strings.HasPrefix(response.GetResultText(), "URL not allowed: ") {
			t.Fatalf("url=%q response=%+v", raw, response)
		}
		if backend.calls != 0 || objects.Len() != 1 {
			t.Fatalf("url=%q backend=%d objects=%d; want only the durable job result", raw, backend.calls, objects.Len())
		}
	}
}

func TestDirectOpenReportsConnectorCaptureTruncationImmediately(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{
		page:         Page{URL: "https://example.com/large", Content: strings.Repeat("x", maxSnapshotBytes+1), TargetHTTPStatus: 200},
		fetchOutcome: BackendOutcome{Kind: BackendSuccess, Requests: 1, TargetHTTPStatus: int32ptr(200)},
	}
	service, key, now := testService(objects, backend)
	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr("https://example.com/large")}}}, "event-capture-truncation", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || !response.GetSourceIncomplete() {
		t.Fatalf("status=%s source_incomplete=%t; connector-side capture truncation must be visible on the first open", response.GetStatus(), response.GetSourceIncomplete())
	}
	if !strings.Contains(response.GetResultText(), "source truncated at capture") {
		t.Fatalf("result=%q; missing capture-truncation warning", response.GetResultText())
	}
}

func TestWhatwgCompatiblePublicUnderscoreHostReachesBackend(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{page: Page{URL: "https://foo_bar.example/", Content: "body", TargetHTTPStatus: 200}, fetchOutcome: BackendOutcome{Kind: BackendSuccess, Requests: 1, TargetHTTPStatus: int32ptr(200)}}
	service, key, now := testService(objects, backend)
	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr("https://foo_bar.example/")}}}, "event-underscore-host", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || backend.calls != 1 {
		t.Fatalf("response=%+v backend_calls=%d", response, backend.calls)
	}
}

func TestJobStoreFailuresAreRuntimeFailuresNotDeliveryConflicts(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"get", "put", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			inner := blob.NewFakeBlobStore()
			objects := &jobFailureBlobStore{BlobStore: inner, mode: mode}
			backend := &fakeBackend{search: []SearchHit{{URL: "https://example.com/", Title: "Example"}}, searchOutcome: BackendOutcome{Kind: BackendSuccess, Requests: 1}}
			service, key, now := testService(objects, backend)
			response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "query"}}}, "event-job-failure-"+mode, key, now))
			if err != nil {
				t.Fatal(err)
			}
			if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR || response.GetResultText() != "web backend temporarily unavailable" {
				t.Fatalf("response=%+v", response)
			}
			if response.GetResultText() == "tool delivery conflict" {
				t.Fatal("storage failure was misclassified as an idempotency conflict")
			}
		})
	}
}

func TestRecordedSearchFixturePersistsEveryRenderedStubField(t *testing.T) {
	t.Parallel()
	fixture := loadRecordedFixture(t, "search-no-content.json")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(fixture.Status)
		_, _ = w.Write(fixture.Response)
	}))
	defer server.Close()
	objects := blob.NewFakeBlobStore()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"fixture-key"}, time.Now)
	service, key, now := testService(objects, backend)
	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "kubernetes documentation"}}}, "event-recorded-search", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || len(response.GetRefs()) != 8 {
		t.Fatalf("response=%+v", response)
	}
	stored := NewSnapshotStore(objects, nil, now)
	for _, ref := range response.GetRefs() {
		hit, loadErr := stored.LoadMeta(context.Background(), Scope{"ws", "ses", "thr"}, ref.GetRefId())
		if loadErr != nil {
			t.Fatalf("load stub %s: %v", ref.GetRefId(), loadErr)
		}
		if hit.URL != ref.GetUrl() || hit.Title != ref.GetTitle() {
			t.Fatalf("stub=%+v ref=%+v", hit, ref)
		}
	}
	second, err := stored.LoadMeta(context.Background(), Scope{"ws", "ses", "thr"}, response.GetRefs()[1].GetRefId())
	if err != nil || second.Date != "May 30, 2026" {
		t.Fatalf("dated stub=%+v err=%v", second, err)
	}
}

func TestRefLookupIsIsolatedByAuthenticatedEnvelopeScope(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	service, key, now := testService(objects, &fakeBackend{})
	stored := NewSnapshotStore(objects, zeroRandom, now)
	ref, _, err := stored.StorePage(context.Background(), Scope{"other", "ses", "thr"}, Page{URL: "https://example.com/", Content: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	input := &providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{RefId: stringptr(ref.ID)}}}
	response, err := service.RunWeb(testContext(), testRequest(input, "event-scope", key, now))
	if err != nil {
		t.Fatal(err)
	}
	want := "invalid or expired ref: " + ref.ID + "; re-open by URL if needed"
	if response.GetResultText() != want {
		t.Fatalf("result=%q want=%q", response.GetResultText(), want)
	}
}

func TestOpenByRefMapsMetadataReadFailureToRuntimeErrorWithoutBackendCall(t *testing.T) {
	t.Parallel()
	objects := &metadataReadFailureBlobStore{BlobStore: blob.NewFakeBlobStore()}
	backend := &fakeBackend{}
	service, key, now := testService(objects, backend)
	refID := "r_aaaaaaaaaaaaaaaaaaaaaaaaaa"
	input := &providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{RefId: stringptr(refID)}}}
	response, err := service.RunWeb(testContext(), testRequest(input, "event-meta-read-failure", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR {
		t.Fatalf("status=%s", response.GetStatus())
	}
	if response.GetResultText() != "web backend temporarily unavailable" {
		t.Fatalf("result=%q", response.GetResultText())
	}
	if objects.docReads != 1 || objects.metaReads != 1 {
		t.Fatalf("document reads=%d metadata reads=%d", objects.docReads, objects.metaReads)
	}
	if backend.calls != 0 {
		t.Fatalf("backend calls=%d", backend.calls)
	}
}

func TestLazyUpgradeWritesOneImmutableSiblingAndConcurrentOpenServesWinner(t *testing.T) {
	objects := blob.NewFakeBlobStore()
	backend := &concurrentBackend{page: Page{URL: "https://example.com/", Title: "Example", Content: "winner body", TargetHTTPStatus: 200}}
	service, key, now := testService(objects, backend)
	stored := NewSnapshotStore(objects, zeroRandom, now)
	stub, _, err := stored.StoreStub(context.Background(), Scope{"ws", "ses", "thr"}, SearchHit{URL: "https://example.com/", Title: "Example"})
	if err != nil {
		t.Fatal(err)
	}
	input := &providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{RefId: stringptr(stub.ID)}}}
	responses := make([]*providergatewayv1.RunWebResponse, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range responses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			responses[i], errs[i] = service.RunWeb(testContext(), testRequest(input, "event-race-"+string(rune('a'+i)), key, now))
		}(i)
	}
	wg.Wait()
	for i := range responses {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if !strings.Contains(responses[i].GetResultText(), "winner body") {
			t.Fatalf("response %d=%q", i, responses[i].GetResultText())
		}
	}
	if !objects.Has(docKey(Scope{"ws", "ses", "thr"}, stub.ID)) {
		t.Fatal("snapshot sibling missing")
	}
}

func TestIdempotentReplayIsExactAndConflictingInputDoesNotReexecute(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{search: []SearchHit{{URL: "https://example.com/", Title: "Example"}}}
	service, key, now := testService(objects, backend)
	request := testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "one"}}}, "event-replay", key, now)
	first, err := service.RunWeb(testContext(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.RunWeb(testContext(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.String() != second.String() {
		t.Fatalf("replay differs\nfirst=%s\nsecond=%s", first, second)
	}
	if backend.calls != 1 {
		t.Fatalf("backend calls=%d", backend.calls)
	}
	conflict := testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "two"}}}, "event-replay", key, now)
	response, err := service.RunWeb(testContext(), conflict)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR || response.GetResultText() != "tool delivery conflict" {
		t.Fatalf("response=%+v", response)
	}
	if backend.calls != 1 {
		t.Fatalf("conflict reexecuted backend: %d", backend.calls)
	}
	if err := objects.Delete(context.Background(), jobKey(Scope{"ws", "ses", "thr"}, "event-replay")); err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunWeb(testContext(), request); err != nil {
		t.Fatal(err)
	}
	if backend.calls != 2 {
		t.Fatalf("expired replay backend calls=%d; want reexecution", backend.calls)
	}
}

func TestRuntimeFailureIsNotPersistedAndSameKeyRetryReexecutes(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &sequenceSearchBackend{
		results: []sequenceSearchResult{
			{outcome: BackendOutcome{Kind: BackendRuntimeError, Message: "temporary failure"}},
			{hits: []SearchHit{{URL: "https://example.com/", Title: "Example"}}, outcome: BackendOutcome{Kind: BackendSuccess, Requests: 1}},
		},
	}
	service, key, now := testService(objects, backend)
	request := testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "retryable"}}}, "event-runtime-retry", key, now)

	first, err := service.RunWeb(testContext(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR {
		t.Fatalf("first status = %s; want runtime_error", first.GetStatus())
	}
	second, err := service.RunWeb(testContext(), request)
	if err != nil {
		t.Fatal(err)
	}
	if second.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
		t.Fatalf("second status = %s; want completed", second.GetStatus())
	}
	if backend.calls != 2 {
		t.Fatalf("backend calls = %d; want retry execution", backend.calls)
	}
}

func TestMultiItemCompositionUsesFieldOrderBlankLinesAndDeduplicatedRefs(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{search: []SearchHit{{URL: "https://search.example/", Title: "Search"}}}
	service, key, now := testService(objects, backend)
	stored := NewSnapshotStore(objects, zeroRandom, now)
	known, _, err := stored.StorePage(context.Background(), Scope{"ws", "ses", "thr"}, Page{URL: "https://known.example/", Title: "Known", Content: "needle", TargetHTTPStatus: 200})
	if err != nil {
		t.Fatal(err)
	}
	input := &providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "query"}}, Open: []*providergatewayv1.WebOpenRequest{{RefId: stringptr(known.ID)}}, Find: []*providergatewayv1.WebFindRequest{{RefId: known.ID, Pattern: "needle"}}}
	response, err := service.RunWeb(testContext(), testRequest(input, "event-mixed", key, now))
	if err != nil {
		t.Fatal(err)
	}
	text := response.GetResultText()
	searchAt := strings.Index(text, "Search results")
	openAt := strings.Index(text, "lines 1-1")
	findAt := strings.LastIndex(text, "1 match")
	ordered := searchAt >= 0 && openAt > searchAt && findAt > openAt
	if !ordered || strings.Count(text, "\n\n") < 4 {
		t.Fatalf("composition=%q", text)
	}
	if response.GetUsage().GetOperation() != "mixed" || response.GetUsage().GetWebSearchRequests() != 1 || response.GetUsage().GetWebFetchRequests() != 0 {
		t.Fatalf("usage=%+v", response.GetUsage())
	}
	if len(response.GetRefs()) != 2 || response.GetRefs()[1].GetRefId() != known.ID {
		t.Fatalf("refs=%+v; want search ref followed by one deduplicated known ref", response.GetRefs())
	}
	knownRef := response.GetRefs()[1]
	if knownRef.GetLineStart() != 1 || knownRef.GetLineEnd() != 1 || knownRef.GetTotalLines() != 1 {
		t.Fatalf("known ref coordinates=%+v", knownRef)
	}
}

func TestEmptyFindPatternUsesRE2SemanticsAndCompletes(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	service, key, now := testService(objects, &fakeBackend{})
	stored := NewSnapshotStore(objects, zeroRandom, now)
	ref, _, err := stored.StorePage(context.Background(), Scope{"ws", "ses", "thr"}, Page{URL: "https://example.com/", Content: "one\ntwo", TargetHTTPStatus: 200})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Find: []*providergatewayv1.WebFindRequest{{RefId: ref.ID, Pattern: ""}}}, "event-empty-pattern", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || !strings.Contains(response.GetResultText(), "2 matches") {
		t.Fatalf("response=%+v", response)
	}
}

func TestWindowErrorCleansPrivateSnapshotButPreservesWriteUsage(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{page: Page{URL: "https://example.com/", Content: "one", TargetHTTPStatus: 200}, fetchOutcome: BackendOutcome{Kind: BackendSuccess, Requests: 1, TargetHTTPStatus: int32ptr(200)}}
	service, key, now := testService(objects, backend)
	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr("https://example.com/"), Lineno: int32ptr(2)}}}, "event-window-error", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR || response.GetUsage().GetStoredBytes() == 0 {
		t.Fatalf("response=%+v", response)
	}
	if objects.Len() != 1 {
		t.Fatalf("objects=%d; want only durable error job", objects.Len())
	}
	if len(objects.Deletes()) != 1 || !strings.HasSuffix(objects.Deletes()[0], ".doc") {
		t.Fatalf("deletes=%v; want private snapshot cleanup", objects.Deletes())
	}
}

func TestMultiItemResponseReducesSingularFieldsWithoutLastItemWins(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &sequenceFetchBackend{
		pages: []Page{
			{URL: "https://one.example/", Content: strings.Repeat("one\n", maxWindowLines+1), SourceIncomplete: true},
			{URL: "https://two.example/", Content: "two"},
		},
		outcomes: []BackendOutcome{
			{Kind: BackendSuccess, TargetHTTPStatus: int32ptr(201), Requests: 1},
			{Kind: BackendSuccess, TargetHTTPStatus: int32ptr(202), Requests: 1},
		},
	}
	service, key, now := testService(objects, backend)
	input := &providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{
		{Url: stringptr("https://one.example/")},
		{Url: stringptr("https://two.example/")},
	}}
	response, err := service.RunWeb(testContext(), testRequest(input, "event-multi-reduction", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
		t.Fatalf("response=%+v", response)
	}
	if !response.GetSourceIncomplete() {
		t.Fatal("source_incomplete=false; want logical OR across items")
	}
	if response.NextLineno != nil || !response.GetWindowTruncated() {
		t.Fatalf("next_lineno=%v window_truncated=%v; want unset/true for two windows with any truncated member", response.NextLineno, response.GetWindowTruncated())
	}
	if response.GetUsage().TargetHttpStatus != nil {
		t.Fatalf("target_http_status=%v; want unset for two applicable fetches", response.GetUsage().TargetHttpStatus)
	}
}

func TestTargetHTTPFailureReturnsClosedTemplateAndWritesNoSnapshot(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	target := int32(404)
	backend := &fakeBackend{fetchOutcome: BackendOutcome{Kind: BackendToolError, TargetHTTPStatus: &target, Tokens: 12, Requests: 1}}
	service, key, now := testService(objects, backend)
	input := &providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr("https://example.com/missing")}}}
	response, err := service.RunWeb(testContext(), testRequest(input, "event-target-error", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetResultText() != "target returned HTTP 404" || response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR {
		t.Fatalf("response=%+v", response)
	}
	if response.GetUsage().GetBackendTokens() != 12 || response.GetUsage().GetWebFetchRequests() != 1 {
		t.Fatalf("usage=%+v", response.GetUsage())
	}
	if objects.Len() != 1 {
		t.Fatalf("objects=%d; want idempotency record only", objects.Len())
	}
}

func TestBackendFailureTaxonomyProducesClosedPublicResponsesWithoutSnapshots(t *testing.T) {
	tests := []struct {
		name       string
		fixture    string
		outer      int
		body       string
		wantStatus providergatewayv1.RunWebStatus
		wantText   string
		timeout    bool
	}{
		{name: "syntactic-input", fixture: "reader-syntactic-invalid.json", wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR, wantText: "URL could not be fetched"},
		{name: "unresolvable-target", fixture: "reader-unresolvable.json", wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR, wantText: "URL could not be fetched"},
		{name: "target-http-error", fixture: "reader-target-not-found.json", wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR, wantText: "target returned HTTP 404"},
		{name: "unauthorized-pool-exhaustion", fixture: "reader-unauthorized.json", wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, wantText: "web backend temporarily unavailable"},
		{name: "payment-required-pool-exhaustion", outer: http.StatusPaymentRequired, body: `{"code":402}`, wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, wantText: "web backend temporarily unavailable"},
		{name: "rate-limited-pool-exhaustion", outer: http.StatusTooManyRequests, body: `{"code":429}`, wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, wantText: "web backend temporarily unavailable"},
		{name: "upstream-service-failure", outer: http.StatusServiceUnavailable, wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, wantText: "web backend temporarily unavailable"},
		{name: "residual-client-error", outer: http.StatusTeapot, body: `{"code":418,"name":"FixtureResidualClientError","status":41800}`, wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR, wantText: "URL could not be fetched"},
		{name: "transport-timeout", timeout: true, wantStatus: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, wantText: "web backend temporarily unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var client *http.Client
			var endpoint string
			if test.timeout {
				client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, context.DeadlineExceeded })}
				endpoint = "https://reader.invalid/"
			} else {
				outer, body := test.outer, []byte(test.body)
				if test.fixture != "" {
					fixture := loadRecordedFixture(t, test.fixture)
					outer, body = fixture.Status, fixture.Response
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(outer)
					_, _ = w.Write(body)
				}))
				defer server.Close()
				client, endpoint = server.Client(), server.URL
			}
			objects := blob.NewFakeBlobStore()
			backend := NewJinaBackend(client, endpoint, endpoint, []string{"fixture-key"}, time.Now)
			service, key, now := testService(objects, backend)
			response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr("https://example.com/")}}}, "event-taxonomy-"+test.name, key, now))
			if err != nil {
				t.Fatal(err)
			}
			if response.GetStatus() != test.wantStatus || response.GetResultText() != test.wantText {
				t.Fatalf("response=%+v; want status=%s text=%q", response, test.wantStatus, test.wantText)
			}
			wantObjects := 1
			if test.wantStatus == providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR {
				wantObjects = 0
			}
			if len(response.GetRefs()) != 0 || objects.Len() != wantObjects {
				t.Fatalf("refs=%v objects=%d; want %d persisted job records", response.GetRefs(), objects.Len(), wantObjects)
			}
		})
	}
}

func TestRecordedBackendFixturesDriveAllOperationsEndToEnd(t *testing.T) {
	searchFixture := loadRecordedFixture(t, "search-no-content.json")
	readerFixture := loadRecordedFixture(t, "reader-success.json")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		fixture := readerFixture
		if request.Header.Get("X-Respond-With") == "no-content" {
			fixture = searchFixture
			var body map[string]string
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body["q"] != "kubernetes documentation" {
				t.Errorf("search request body=%v err=%v", body, err)
			}
		} else {
			var body map[string]string
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body["url"] != "https://example.com/" {
				t.Errorf("reader request body=%v err=%v", body, err)
			}
		}
		w.WriteHeader(fixture.Status)
		_, _ = w.Write(fixture.Response)
	}))
	defer server.Close()
	objects := blob.NewFakeBlobStore()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL+"/reader", []string{"fixture-key"}, time.Now)
	service, key, now := testService(objects, backend)

	search, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "kubernetes documentation"}}}, "event-fixture-search", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if search.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || len(search.GetRefs()) == 0 || search.GetUsage().GetWebSearchRequests() != 1 {
		t.Fatalf("search=%+v", search)
	}
	opened, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr("https://example.com/")}}}, "event-fixture-open", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if opened.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || !strings.Contains(opened.GetResultText(), "Example Domain") || opened.GetUsage().GetWebFetchRequests() != 1 {
		t.Fatalf("open=%+v", opened)
	}
	refID := opened.GetRefs()[0].GetRefId()

	found, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Find: []*providergatewayv1.WebFindRequest{{RefId: refID, Pattern: "Example Domain"}}}, "event-fixture-find", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if found.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || !strings.Contains(found.GetResultText(), "1 match") || found.GetUsage().GetWebFetchRequests() != 0 {
		t.Fatalf("find=%+v", found)
	}
	if calls.Load() != 2 {
		t.Fatalf("backend calls=%d; want search plus lazy-open only", calls.Load())
	}
}

func TestConcurrentIdempotentDeliveryReturnsWinnerAndCleansLoserObjects(t *testing.T) {
	inner := blob.NewFakeBlobStore()
	barrier := &jobBarrierBlobStore{inner: inner, ready: make(chan struct{})}
	backend := &concurrentSearchBackend{}
	service, key, now := testService(barrier, backend)
	request := testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "query"}}}, "event-concurrent", key, now)
	responses := make([]*providergatewayv1.RunWebResponse, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range responses {
		wg.Add(1)
		go func(i int) { defer wg.Done(); responses[i], errs[i] = service.RunWeb(testContext(), request) }(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if responses[0].String() != responses[1].String() {
		t.Fatalf("responses differ: %s / %s", responses[0], responses[1])
	}
	if inner.Len() != 2 {
		t.Fatalf("objects=%d; want winner metadata and job", inner.Len())
	}
	if len(inner.Deletes()) != 1 {
		t.Fatalf("deletes=%v; want loser metadata cleanup", inner.Deletes())
	}
}

func TestConcurrentLazyUpgradeLoserNeverDeletesTheSharedWinnerSnapshot(t *testing.T) {
	inner := blob.NewFakeBlobStore()
	store := &lazyUpgradeJobRaceBlobStore{
		inner:           inner,
		firstJobBlocked: make(chan struct{}),
		winnerStored:    make(chan struct{}),
	}
	backend := &concurrentBackend{page: Page{URL: "https://example.com/", Title: "Example", Content: "winner body", TargetHTTPStatus: 200}}
	service, key, now := testService(store, backend)
	snapshotStore := NewSnapshotStore(inner, zeroRandom, now)
	stub, _, err := snapshotStore.StoreStub(context.Background(), Scope{"ws", "ses", "thr"}, SearchHit{URL: "https://example.com/", Title: "Example"})
	if err != nil {
		t.Fatal(err)
	}
	request := func() *providergatewayv1.RunWebRequest {
		return testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{RefId: stringptr(stub.ID)}}}, "event-lazy-job-race", key, now)
	}

	firstResult := make(chan *providergatewayv1.RunWebResponse, 1)
	firstError := make(chan error, 1)
	go func() {
		response, runErr := service.RunWeb(testContext(), request())
		firstResult <- response
		firstError <- runErr
	}()
	<-store.firstJobBlocked
	second, err := service.RunWeb(testContext(), request())
	if err != nil {
		t.Fatal(err)
	}
	first := <-firstResult
	if err = <-firstError; err != nil {
		t.Fatal(err)
	}
	if first.String() != second.String() {
		t.Fatalf("responses differ: %s / %s", first, second)
	}
	if !inner.Has(docKey(Scope{"ws", "ses", "thr"}, stub.ID)) {
		t.Fatal("losing idempotency execution deleted the shared lazy-upgrade snapshot")
	}
	replayed, err := service.RunWeb(testContext(), request())
	if err != nil || !strings.Contains(replayed.GetResultText(), "winner body") {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
}

func TestJobPersistenceFailurePreservesIncurredBackendUsage(t *testing.T) {
	t.Parallel()
	inner := blob.NewFakeBlobStore()
	objects := &jobFailureBlobStore{BlobStore: inner, mode: "put"}
	backend := &fakeBackend{
		page:         Page{URL: "https://example.com/", Content: "body", TargetHTTPStatus: 200},
		fetchOutcome: BackendOutcome{Kind: BackendSuccess, Tokens: 41, Requests: 1, TargetHTTPStatus: int32ptr(200)},
	}
	service, key, now := testService(objects, backend)
	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{Url: stringptr("https://example.com/")}}}, "event-job-write-usage", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR || response.GetResultText() != "web backend temporarily unavailable" {
		t.Fatalf("response=%+v", response)
	}
	usage := response.GetUsage()
	if usage.GetBackendTokens() != 41 || usage.GetWebFetchRequests() != 1 || usage.GetTargetHttpStatus() != 200 || usage.GetStoredBytes() == 0 {
		t.Fatalf("usage=%+v; incurred backend call and cache write must remain accounted", usage)
	}
	if inner.Len() != 0 {
		t.Fatalf("objects=%d; failed execution cleanup did not remove its private snapshot", inner.Len())
	}
}

func TestMalformedJobRecordsFailAsCacheUnavailableBeforeHashComparison(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty-object", raw: `{}`},
		{name: "missing-settled-at", raw: `{"input_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","response":{"status":"RUN_WEB_STATUS_COMPLETED","result_text":"ok","usage":{"operation":"search"}}}`},
		{name: "invalid-settled-at", raw: `{"input_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","response":{"status":"RUN_WEB_STATUS_COMPLETED","result_text":"ok","usage":{"operation":"search"}},"settled_at":"yesterday"}`},
		{name: "invalid-hash", raw: `{"input_hash":"not-a-hash","response":{"status":"RUN_WEB_STATUS_COMPLETED","result_text":"ok","usage":{"operation":"search"}},"settled_at":"2026-07-17T00:00:00Z"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inner := blob.NewFakeBlobStore()
			if err := putBytes(context.Background(), inner, jobKey(Scope{"ws", "ses", "thr"}, "event-malformed"), []byte(test.raw)); err != nil {
				t.Fatal(err)
			}
			service, key, now := testService(inner, &fakeBackend{})
			response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "query"}}}, "event-malformed", key, now))
			if err != nil {
				t.Fatal(err)
			}
			if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR || response.GetResultText() != "web backend temporarily unavailable" {
				t.Fatalf("response=%+v", response)
			}
		})
	}
}

func TestDeniedURLStoredInStubNeverCallsBackendOrCreatesSnapshot(t *testing.T) {
	t.Parallel()
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{}
	service, key, now := testService(objects, backend)
	stored := NewSnapshotStore(objects, zeroRandom, now)
	stub, _, err := stored.StoreStub(context.Background(), Scope{"ws", "ses", "thr"}, SearchHit{URL: "http://127。0。0。1/", Title: "denied"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.RunWeb(testContext(), testRequest(&providergatewayv1.WebToolInput{Open: []*providergatewayv1.WebOpenRequest{{RefId: stringptr(stub.ID)}}}, "event-denied-stub", key, now))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR || !strings.Contains(response.GetResultText(), "URL not allowed") {
		t.Fatalf("response=%+v", response)
	}
	if backend.calls != 0 || objects.Has(docKey(Scope{"ws", "ses", "thr"}, stub.ID)) {
		t.Fatalf("backend calls=%d snapshot=%v", backend.calls, objects.Has(docKey(Scope{"ws", "ses", "thr"}, stub.ID)))
	}
}

type concurrentBackend struct {
	mu    sync.Mutex
	calls int
	page  Page
}
type concurrentSearchBackend struct{ calls atomic.Int64 }

type sequenceSearchResult struct {
	hits    []SearchHit
	outcome BackendOutcome
}

type sequenceSearchBackend struct {
	results []sequenceSearchResult
	calls   int
}

func (backend *sequenceSearchBackend) Search(context.Context, string, []string) ([]SearchHit, BackendOutcome) {
	result := backend.results[backend.calls]
	backend.calls++
	return result.hits, result.outcome
}

func (*sequenceSearchBackend) Fetch(context.Context, string) (Page, BackendOutcome) {
	panic("unexpected fetch")
}

type sequenceFetchBackend struct {
	pages    []Page
	outcomes []BackendOutcome
	next     int
}

func (b *sequenceFetchBackend) Search(context.Context, string, []string) ([]SearchHit, BackendOutcome) {
	panic("unexpected search")
}

func (b *sequenceFetchBackend) Fetch(context.Context, string) (Page, BackendOutcome) {
	index := b.next
	b.next++
	return b.pages[index], b.outcomes[index]
}

type metadataReadFailureBlobStore struct {
	blob.BlobStore
	docReads  int
	metaReads int
}

type jobFailureBlobStore struct {
	blob.BlobStore
	mode string
}

func (s *jobFailureBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if s.mode == "get" && strings.Contains(key, "/jobs/") {
		return nil, errors.New("test job read failure")
	}
	if s.mode == "corrupt" && strings.Contains(key, "/jobs/") {
		return io.NopCloser(strings.NewReader("{")), nil
	}
	return s.BlobStore.Get(ctx, key)
}

func (s *jobFailureBlobStore) Put(ctx context.Context, key string, body io.Reader, size int64) error {
	if s.mode == "put" && strings.Contains(key, "/jobs/") {
		return errors.New("test job write failure")
	}
	return s.BlobStore.Put(ctx, key, body, size)
}

func (s *metadataReadFailureBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	switch {
	case strings.HasSuffix(key, ".doc"):
		s.docReads++
		return nil, &blob.NotFoundError{Key: key}
	case strings.HasSuffix(key, ".meta"):
		s.metaReads++
		return nil, errors.New("test storage read failure")
	default:
		return s.BlobStore.Get(ctx, key)
	}
}

func (b *concurrentSearchBackend) Search(context.Context, string, []string) ([]SearchHit, BackendOutcome) {
	b.calls.Add(1)
	return []SearchHit{{URL: "https://example.com/", Title: "Example"}}, BackendOutcome{Kind: BackendSuccess, Requests: 1}
}
func (b *concurrentSearchBackend) Fetch(context.Context, string) (Page, BackendOutcome) {
	panic("unexpected fetch")
}

type jobBarrierBlobStore struct {
	inner    *blob.FakeBlobStore
	arrivals atomic.Int32
	ready    chan struct{}
	once     sync.Once
}

type lazyUpgradeJobRaceBlobStore struct {
	inner           *blob.FakeBlobStore
	jobPuts         atomic.Int32
	firstJobBlocked chan struct{}
	winnerStored    chan struct{}
}

func (s *lazyUpgradeJobRaceBlobStore) Put(ctx context.Context, key string, body io.Reader, size int64) error {
	if !strings.Contains(key, "/jobs/") {
		return s.inner.Put(ctx, key, body, size)
	}
	if s.jobPuts.Add(1) == 1 {
		close(s.firstJobBlocked)
		select {
		case <-s.winnerStored:
		case <-ctx.Done():
			return ctx.Err()
		}
		return s.inner.Put(ctx, key, body, size)
	}
	err := s.inner.Put(ctx, key, body, size)
	close(s.winnerStored)
	return err
}

func (s *lazyUpgradeJobRaceBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}
func (s *lazyUpgradeJobRaceBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}
func (s *lazyUpgradeJobRaceBlobStore) CopyObject(ctx context.Context, source, destination string) error {
	return s.inner.CopyObject(ctx, source, destination)
}
func (s *lazyUpgradeJobRaceBlobStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}
func (s *lazyUpgradeJobRaceBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

func (s *jobBarrierBlobStore) Put(ctx context.Context, key string, body io.Reader, size int64) error {
	if strings.Contains(key, "/jobs/") {
		if s.arrivals.Add(1) == 2 {
			s.once.Do(func() { close(s.ready) })
		}
		select {
		case <-s.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.inner.Put(ctx, key, body, size)
}
func (s *jobBarrierBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}
func (s *jobBarrierBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}
func (s *jobBarrierBlobStore) CopyObject(ctx context.Context, source, destination string) error {
	return s.inner.CopyObject(ctx, source, destination)
}
func (s *jobBarrierBlobStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}
func (s *jobBarrierBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

func (b *concurrentBackend) Search(context.Context, string, []string) ([]SearchHit, BackendOutcome) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	return nil, BackendOutcome{Kind: BackendSuccess, Requests: 1}
}
func (b *concurrentBackend) Fetch(context.Context, string) (Page, BackendOutcome) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	return b.page, BackendOutcome{Kind: BackendSuccess, Requests: 1}
}
func zeroRandom(dst []byte) (int, error) { clear(dst); return len(dst), nil }
func testService(objects blob.BlobStore, backend Backend) (*Service, []byte, func() time.Time) {
	key := []byte("binding-verifier-key-with-at-least-32-bytes")
	now := func() time.Time { return time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC) }
	return NewService(objects, backend, NewBindingVerifier(key, now), NewMetrics(), now, nil), key, now
}
func testRequest(input *providergatewayv1.WebToolInput, event string, key []byte, now func() time.Time) *providergatewayv1.RunWebRequest {
	request := &providergatewayv1.RunWebRequest{WorkspaceId: "ws", SessionId: "ses", SessionThreadId: "thr", ToolUseEventId: event, BindingId: "bind", BindingGeneration: 1, Input: input}
	request.RuntimeBindingToken = signRequest(request, "runtime-pod", now().Add(time.Hour), key)
	return request
}
func testContext() context.Context {
	return grpcauth.ContextWithIdentity(context.Background(), grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: "runtime-pod"})
}
