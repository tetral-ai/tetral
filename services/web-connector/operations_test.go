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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
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
}

func TestRuntimeFailureIsDurableAndSameIdentityDoesNotReexecute(t *testing.T) {
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
	if second.String() != first.String() {
		t.Fatalf("runtime failure replay differs\nfirst=%s\nsecond=%s", first, second)
	}
	if backend.calls != 1 {
		t.Fatalf("backend calls = %d; want one first execution", backend.calls)
	}
}

func TestExpiredFirstExecutionClaimSettlesUnknownWithoutSecondBackendCall(t *testing.T) {
	objects := blob.NewFakeBlobStore()
	backend := &blockingSearchBackend{started: make(chan struct{}), release: make(chan struct{})}
	nowValue := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	now := func() time.Time { return nowValue }
	key := []byte("binding-verifier-key-with-at-least-32-bytes")
	service := NewService(objects, backend, NewBindingVerifier(key, now), NewMetrics(), now, nil)
	service.jobClaimDuration = time.Second
	service.jobPollInterval = time.Millisecond
	request := testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "query"}}}, "event-expired-claim", key, now)

	firstContext, cancelFirst := context.WithCancel(testContext())
	firstResult := make(chan *providergatewayv1.RunWebResponse, 1)
	firstError := make(chan error, 1)
	go func() {
		response, err := service.RunWeb(firstContext, request)
		firstResult <- response
		firstError <- err
	}()
	<-backend.started
	cancelFirst()
	first := <-firstResult
	if err := <-firstError; err != nil {
		t.Fatalf("first RunWeb: %v", err)
	}
	if first.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR {
		t.Fatalf("first status = %s; want runtime error", first.GetStatus())
	}

	nowValue = nowValue.Add(2 * time.Second)
	replayed, err := service.RunWeb(testContext(), request)
	if err != nil {
		t.Fatalf("expired claim replay: %v", err)
	}
	if replayed.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR ||
		replayed.GetResultText() != "web backend temporarily unavailable" {
		t.Fatalf("expired claim replay = %+v", replayed)
	}
	if backend.calls.Load() != 1 {
		t.Fatalf("backend calls = %d; want original uncertain call only", backend.calls.Load())
	}
	secondReplay, err := service.RunWeb(testContext(), request)
	if err != nil || secondReplay.String() != replayed.String() || backend.calls.Load() != 1 {
		t.Fatalf("settled replay = %+v/%v calls=%d", secondReplay, err, backend.calls.Load())
	}
}

func TestMaximumWebCallWithThreeKeyJinaRotationRemainsOwnedUntilExactReplay(t *testing.T) {
	clock := newProviderAttemptClock(time.Now().UTC())
	transport := &sequentialJinaRotationTransport{clock: clock}
	backend := NewJinaBackend(
		&http.Client{Transport: transport},
		"https://search.test/",
		"https://reader.test/",
		[]string{"one", "two", "three"},
		clock.Now,
	)
	objects := &gatedCASBlobStore{
		BlobStore: blob.NewFakeBlobStore(),
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	key := []byte("binding-verifier-key-with-at-least-32-bytes")
	service := NewService(objects, backend, NewBindingVerifier(key, clock.Now), NewMetrics(), clock.Now, zeroRandom)
	scope := Scope{"ws", "ses", "thr"}
	input := maximumWebCallInput()
	request := &providergatewayv1.RunWebRequest{
		WorkspaceId: "ws", SessionId: "ses", SessionThreadId: "thr",
		ToolUseEventId: "event-maximum-claim", BindingId: "bind", BindingGeneration: 1,
		Input: input,
	}
	started := clock.Now()
	request.RuntimeBindingToken = signRequest(request, "runtime-pod", started.Add(2*time.Hour), key)
	if validation := validateSemanticEnvelope(request); validation != "" || !validStructuralRequest(request) {
		t.Fatalf("maximum request validation = %q/%t", validation, validStructuralRequest(request))
	}
	winnerResult := make(chan *providergatewayv1.RunWebResponse, 1)
	winnerError := make(chan error, 1)
	go func() {
		response, err := service.RunWeb(testContext(), request)
		winnerResult <- response
		winnerError <- err
	}()
	<-objects.started

	if transport.Calls() != maxOperations*maxSearchDomains*3 {
		t.Fatalf("provider attempts = %d; want %d", transport.Calls(), maxOperations*maxSearchDomains*3)
	}
	wantDuration := time.Duration(maxOperations*maxSearchDomains*3)*BackendRequestTimeout + webJobResultCommitMargin
	if elapsed := clock.Now().Sub(started); elapsed != wantDuration-webJobResultCommitMargin {
		t.Fatalf("maximum execution elapsed = %s; want %s", elapsed, wantDuration-webJobResultCommitMargin)
	}
	raw, etag, err := service.store.GetJobVersion(context.Background(), scope, request.GetToolUseEventId())
	record, parseErr := parseJobRecord(raw)
	claimExpiry, expiryErr := time.Parse(time.RFC3339Nano, record.LeaseExpiresAt)
	if err != nil || parseErr != nil || expiryErr != nil || record.State != webJobStateInFlight || !claimExpiry.Equal(started.Add(wantDuration)) {
		t.Fatalf("maximum claim = %q/%q/%v/%v/%v; want expiry %s", etag, record.State, err, parseErr, expiryErr, started.Add(wantDuration))
	}
	if deadline := objects.Deadline(); !deadline.Equal(claimExpiry) {
		t.Fatalf("winner CAS deadline = %s; want stored expiry %s", deadline, claimExpiry)
	}

	clock.Set(claimExpiry.Add(-time.Nanosecond))
	waits := 0
	service.waitForJobPoll = func(context.Context, time.Duration) error {
		waits++
		return context.Canceled
	}
	waiterResponse, waitErr := service.RunWeb(testContext(), request)
	if waitErr != nil || waiterResponse.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR || waits != 1 {
		t.Fatalf("pre-expiry waiter = %+v/%v waits=%d", waiterResponse, waitErr, waits)
	}
	raw, currentETag, err := service.store.GetJobVersion(context.Background(), scope, request.GetToolUseEventId())
	record, parseErr = parseJobRecord(raw)
	if err != nil || parseErr != nil || currentETag != etag || record.State != webJobStateInFlight || objects.Calls() != 1 {
		t.Fatalf("pre-expiry ownership = %q/%q/%d/%v/%v; want original in-flight claim", currentETag, record.State, objects.Calls(), err, parseErr)
	}

	close(objects.release)
	winner := <-winnerResult
	if err := <-winnerError; err != nil || winner.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
		t.Fatalf("maximum-shape winner = %+v/%v", winner, err)
	}
	replayed, err := service.RunWeb(testContext(), request)
	if err != nil || replayed.String() != winner.String() || transport.Calls() != maxOperations*maxSearchDomains*3 {
		t.Fatalf("exact replay = %+v/%v calls=%d; want %s", replayed, err, transport.Calls(), winner)
	}
}

func TestWebClaimExpiryIsImmutableAndEqualityTransfersSettlementOwnership(t *testing.T) {
	objects := blob.NewFakeBlobStore()
	clock := &webJobFakeClock{now: time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)}
	service := NewService(objects, &fakeBackend{}, nil, NewMetrics(), clock.Now, zeroRandom)
	service.jobClaimDuration = time.Second
	service.waitForJobPoll = clock.Wait
	scope := Scope{"ws", "ses", "thr"}
	inputHash := CanonicalInputHash(canonicalInput(maximumWebCallInput()))
	started := clock.Now()
	claim, _, err := service.claimJob(context.Background(), scope, "event-immutable-expiry", inputHash, "search", started)
	if err != nil {
		t.Fatal(err)
	}

	clock.now = started.Add(400 * time.Millisecond)
	waiterContext, cancelWaiter := context.WithCancel(context.Background())
	clock.cancel = cancelWaiter
	_, _, err = service.claimJob(waiterContext, scope, "event-immutable-expiry", inputHash, "search", started)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-expiry observation = %v; want cancellation", err)
	}
	raw, _, err := service.store.GetJobVersion(context.Background(), scope, "event-immutable-expiry")
	record, parseErr := parseJobRecord(raw)
	if err != nil || parseErr != nil || record.LeaseExpiresAt != claim.ExpiresAt.Format(time.RFC3339Nano) {
		t.Fatalf("stored immutable expiry = %q/%v/%v; want %s", record.LeaseExpiresAt, err, parseErr, claim.ExpiresAt.Format(time.RFC3339Nano))
	}

	clock.now = claim.ExpiresAt
	winner := &providergatewayv1.RunWebResponse{
		Status:     providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED,
		ResultText: "late winner",
		Usage:      &providergatewayv1.WebUsage{Operation: "search"},
	}
	if err := service.persistClaimedJob(context.Background(), scope, "event-immutable-expiry", inputHash, claim, winner); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("winner commit at expiry = %v; want deadline exceeded", err)
	}
	waitsAtExpiry := len(clock.waits)
	nowAtExpiry := clock.Now()
	_, settled, err := service.claimJob(context.Background(), scope, "event-immutable-expiry", inputHash, "search", started)
	if err != nil || settled.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR {
		t.Fatalf("equality settlement = %+v/%v", settled, err)
	}
	if len(clock.waits) != waitsAtExpiry || !clock.Now().Equal(nowAtExpiry) {
		t.Fatalf("equality settlement polled/advanced: waits %d->%d, now %s->%s", waitsAtExpiry, len(clock.waits), nowAtExpiry, clock.Now())
	}
}

func TestWinnerCASBlockedAcrossClaimExpiryCannotOverwriteAbandonedSettlement(t *testing.T) {
	objects := &blockingCASBlobStore{
		BlobStore: blob.NewFakeBlobStore(),
		started:   make(chan struct{}),
	}
	service := NewService(objects, &fakeBackend{}, nil, NewMetrics(), time.Now, zeroRandom)
	service.jobClaimDuration = 50 * time.Millisecond
	service.jobCommitMargin = 5 * time.Second
	scope := Scope{"ws", "ses", "thr"}
	inputHash := CanonicalInputHash(canonicalInput(maximumWebCallInput()))
	claim, _, err := service.claimJob(context.Background(), scope, "event-cross-expiry-cas", inputHash, "search", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	winner := &providergatewayv1.RunWebResponse{
		Status:     providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED,
		ResultText: "winner must not cross expiry",
		Usage:      &providergatewayv1.WebUsage{Operation: "search"},
	}
	commitResult := make(chan error, 1)
	commitContext, cancelCommit := context.WithCancel(context.Background())
	defer cancelCommit()
	go func() {
		commitResult <- service.persistClaimedJob(commitContext, scope, "event-cross-expiry-cas", inputHash, claim, winner)
	}()
	<-objects.started
	if deadline := objects.Deadline(); !deadline.Equal(claim.ExpiresAt) {
		cancelCommit()
		<-commitResult
		t.Fatalf("cross-expiry CAS deadline = %s; want stored expiry %s", deadline, claim.ExpiresAt)
	}
	if err := <-commitResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cross-expiry winner commit = %v; want deadline exceeded", err)
	}
	_, settled, err := service.claimJob(context.Background(), scope, "event-cross-expiry-cas", inputHash, "search", time.Now())
	if err != nil || settled.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR || settled.GetResultText() != "web backend temporarily unavailable" {
		t.Fatalf("abandoned settlement = %+v/%v", settled, err)
	}
	_, replay, err := service.claimJob(context.Background(), scope, "event-cross-expiry-cas", inputHash, "search", time.Now())
	if err != nil || replay.String() != settled.String() || strings.Contains(replay.GetResultText(), "winner") {
		t.Fatalf("settled replay = %+v/%v; want exact abandoned result", replay, err)
	}
}

func TestWinnerCASIsBoundedByResultCommitMargin(t *testing.T) {
	objects := &blockingCASBlobStore{
		BlobStore: blob.NewFakeBlobStore(),
		started:   make(chan struct{}),
	}
	service := NewService(objects, &fakeBackend{}, nil, NewMetrics(), time.Now, zeroRandom)
	service.jobClaimDuration = time.Second
	service.jobCommitMargin = 25 * time.Millisecond
	scope := Scope{"ws", "ses", "thr"}
	inputHash := CanonicalInputHash(canonicalInput(maximumWebCallInput()))
	claim, _, err := service.claimJob(context.Background(), scope, "event-commit-margin", inputHash, "search", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	winner := &providergatewayv1.RunWebResponse{
		Status: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED,
		Usage:  &providergatewayv1.WebUsage{Operation: "search"},
	}
	commitResult := make(chan error, 1)
	go func() {
		commitResult <- service.persistClaimedJob(context.Background(), scope, "event-commit-margin", inputHash, claim, winner)
	}()
	<-objects.started
	if err := <-commitResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commit-margin result = %v; want deadline exceeded", err)
	}
	if !time.Now().Before(claim.ExpiresAt) {
		t.Fatalf("claim expired before commit margin proved its independent bound")
	}
	raw, _, err := service.store.GetJobVersion(context.Background(), scope, "event-commit-margin")
	record, parseErr := parseJobRecord(raw)
	if err != nil || parseErr != nil || record.State != webJobStateInFlight {
		t.Fatalf("post-margin claim = %q/%v/%v; want in flight", record.State, err, parseErr)
	}
}

func TestWinnerCASPreservesParentCancellationAndEarlierDeadline(t *testing.T) {
	objects := &observingCASBlobStore{BlobStore: blob.NewFakeBlobStore(), err: errors.New("stop after observing commit context")}
	service := NewService(objects, &fakeBackend{}, nil, NewMetrics(), time.Now, zeroRandom)
	scope := Scope{"ws", "ses", "thr"}
	inputHash := CanonicalInputHash(canonicalInput(maximumWebCallInput()))
	response := &providergatewayv1.RunWebResponse{
		Status: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED,
		Usage:  &providergatewayv1.WebUsage{Operation: "search"},
	}

	cancelledClaim, _, err := service.claimJob(context.Background(), scope, "event-cancelled-parent-cas", inputHash, "search", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.persistClaimedJob(cancelledContext, scope, "event-cancelled-parent-cas", inputHash, cancelledClaim, response); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled-parent commit = %v; want context canceled", err)
	}

	deadlineClaim, _, err := service.claimJob(context.Background(), scope, "event-earlier-parent-cas", inputHash, "search", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	parentDeadline := time.Now().Add(250 * time.Millisecond)
	deadlineContext, cancelDeadline := context.WithDeadline(context.Background(), parentDeadline)
	defer cancelDeadline()
	if err := service.persistClaimedJob(deadlineContext, scope, "event-earlier-parent-cas", inputHash, deadlineClaim, response); !errors.Is(err, objects.err) {
		t.Fatalf("earlier-parent commit = %v; want observer error", err)
	}
	if !objects.deadline.Equal(parentDeadline) {
		t.Fatalf("winner CAS deadline = %s; want parent deadline %s", objects.deadline, parentDeadline)
	}
}

func TestWebClaimWaitUsesBoundedBackoffThenSettlesAbandonedIdentity(t *testing.T) {
	objects := blob.NewFakeBlobStore()
	clock := &webJobFakeClock{now: time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)}
	service := NewService(objects, &fakeBackend{}, nil, NewMetrics(), clock.Now, zeroRandom)
	service.waitForJobPoll = clock.Wait
	scope := Scope{"ws", "ses", "thr"}
	inputHash := CanonicalInputHash(canonicalInput(maximumWebCallInput()))
	started := clock.Now()
	if claim, replay, err := service.claimJob(context.Background(), scope, "event-abandoned-maximum", inputHash, "search", started); err != nil || claim == nil || replay != nil {
		t.Fatalf("initial abandoned claim = %#v/%+v/%v", claim, replay, err)
	}
	_, replay, err := service.claimJob(context.Background(), scope, "event-abandoned-maximum", inputHash, "search", started)
	if err != nil || replay.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR {
		t.Fatalf("abandoned replay = %+v/%v", replay, err)
	}
	if len(clock.waits) > 1000 {
		t.Fatalf("poll count = %d; want <= 1000", len(clock.waits))
	}
	wantPrefix := []time.Duration{25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second}
	if len(clock.waits) < len(wantPrefix) {
		t.Fatalf("polls = %v; want prefix %v", clock.waits, wantPrefix)
	}
	remaining := webJobClaimDuration(1)
	for index, wait := range clock.waits {
		if index < len(wantPrefix) && wait != wantPrefix[index] {
			t.Fatalf("poll %d = %s; want %s", index, wait, wantPrefix[index])
		}
		if wait > time.Second || wait > remaining {
			t.Fatalf("poll %d = %s with %s remaining", index, wait, remaining)
		}
		remaining -= wait
	}
	if remaining != 0 || !clock.Now().Equal(started.Add(webJobClaimDuration(1))) {
		t.Fatalf("polling stopped with remaining=%s now=%s", remaining, clock.Now())
	}

	cancelledObjects := blob.NewFakeBlobStore()
	cancelledClock := &webJobFakeClock{now: started}
	cancelledService := NewService(cancelledObjects, &fakeBackend{}, nil, NewMetrics(), cancelledClock.Now, zeroRandom)
	cancelledService.waitForJobPoll = cancelledClock.Wait
	if claim, _, claimErr := cancelledService.claimJob(context.Background(), scope, "event-cancelled-wait", inputHash, "search", started); claimErr != nil || claim == nil {
		t.Fatalf("cancelled wait claim = %#v/%v", claim, claimErr)
	}
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = cancelledService.claimJob(cancelledContext, scope, "event-cancelled-wait", inputHash, "search", started)
	if !errors.Is(err, context.Canceled) || len(cancelledClock.waits) != 0 || !cancelledClock.Now().Equal(started) {
		t.Fatalf("cancelled wait = %v waits=%v now=%s", err, cancelledClock.waits, cancelledClock.Now())
	}
}

func maximumWebCallInput() *providergatewayv1.WebToolInput {
	queries := make([]*providergatewayv1.WebSearchQuery, maxOperations)
	for index := range queries {
		queries[index] = &providergatewayv1.WebSearchQuery{
			Q:       fmt.Sprintf("maximum query %d", index),
			Domains: []string{"one.example", "two.example", "three.example", "four.example"},
		}
	}
	return &providergatewayv1.WebToolInput{SearchQuery: queries}
}

type webJobFakeClock struct {
	now    time.Time
	waits  []time.Duration
	cancel context.CancelFunc
}

func (c *webJobFakeClock) Now() time.Time { return c.now }

func (c *webJobFakeClock) Wait(ctx context.Context, delay time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.waits = append(c.waits, delay)
	if c.cancel != nil {
		cancel := c.cancel
		c.cancel = nil
		cancel()
		return context.Canceled
	}
	c.now = c.now.Add(delay)
	return nil
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
			if len(response.GetRefs()) != 0 || objects.Len() != 1 {
				t.Fatalf("refs=%v objects=%d; want one durable job and zero snapshots", response.GetRefs(), objects.Len())
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

func TestConcurrentIdempotentDeliveryExecutesBackendOnceAndReturnsWinner(t *testing.T) {
	objects := blob.NewFakeBlobStore()
	backend := &blockingSearchBackend{started: make(chan struct{}), release: make(chan struct{})}
	service, key, now := testService(objects, backend)
	request := testRequest(&providergatewayv1.WebToolInput{SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "query"}}}, "event-concurrent", key, now)
	responses := make([]*providergatewayv1.RunWebResponse, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); responses[0], errs[0] = service.RunWeb(testContext(), request) }()
	<-backend.started
	wg.Add(1)
	go func() { defer wg.Done(); responses[1], errs[1] = service.RunWeb(testContext(), request) }()
	close(backend.release)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if responses[0].String() != responses[1].String() {
		t.Fatalf("responses differ: %s / %s", responses[0], responses[1])
	}
	if backend.calls.Load() != 1 {
		t.Fatalf("backend calls=%d; want one first execution", backend.calls.Load())
	}
}

func TestJobPersistenceFailurePreservesIncurredBackendUsage(t *testing.T) {
	t.Parallel()
	inner := blob.NewFakeBlobStore()
	objects := &jobFailureBlobStore{BlobStore: inner, mode: "compare_and_swap"}
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
	if inner.Len() != 1 {
		t.Fatalf("objects=%d; want only the durable in-flight claim after result persistence failure", inner.Len())
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

type blockingSearchBackend struct {
	calls   atomic.Int64
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type sequenceSearchResult struct {
	hits    []SearchHit
	outcome BackendOutcome
}

type sequenceSearchBackend struct {
	results []sequenceSearchResult
	calls   int
}

func (*sequenceSearchBackend) MaxAttemptsPerCall() int { return 1 }

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

func (*sequenceFetchBackend) MaxAttemptsPerCall() int { return 1 }

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

type blockingCASBlobStore struct {
	blob.BlobStore
	started  chan struct{}
	once     sync.Once
	deadline time.Time
}

func (s *blockingCASBlobStore) CompareAndSwap(ctx context.Context, key, expectedETag string, body io.Reader, size int64) error {
	blocked := false
	s.once.Do(func() {
		blocked = true
		s.deadline, _ = ctx.Deadline()
		close(s.started)
	})
	if blocked {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.BlobStore.(compareAndSwapBlobStore).CompareAndSwap(ctx, key, expectedETag, body, size)
}

func (s *blockingCASBlobStore) Deadline() time.Time { return s.deadline }

type gatedCASBlobStore struct {
	blob.BlobStore
	started  chan struct{}
	release  chan struct{}
	mu       sync.Mutex
	calls    int
	deadline time.Time
}

func (s *gatedCASBlobStore) CompareAndSwap(ctx context.Context, key, expectedETag string, body io.Reader, size int64) error {
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	if first {
		s.deadline, _ = ctx.Deadline()
	}
	s.mu.Unlock()
	if first {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.BlobStore.(compareAndSwapBlobStore).CompareAndSwap(ctx, key, expectedETag, body, size)
}

func (s *gatedCASBlobStore) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *gatedCASBlobStore) Deadline() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadline
}

type providerAttemptClock struct {
	mu  sync.Mutex
	now time.Time
}

func newProviderAttemptClock(now time.Time) *providerAttemptClock {
	return &providerAttemptClock{now: now}
}

func (c *providerAttemptClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *providerAttemptClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

func (c *providerAttemptClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

type sequentialJinaRotationTransport struct {
	clock *providerAttemptClock
	mu    sync.Mutex
	calls int
}

func (t *sequentialJinaRotationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	expectedKey := []string{"one", "two", "three"}[t.calls%3]
	actualKey := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if actualKey != expectedKey {
		return nil, fmt.Errorf("provider key %q at attempt %d; want %q", actualKey, t.calls, expectedKey)
	}
	t.calls++
	t.clock.Advance(BackendRequestTimeout)
	statusCode := http.StatusTooManyRequests
	body := ""
	if actualKey == "three" {
		statusCode = http.StatusOK
		body = `{"code":200,"data":[],"meta":{"usage":{"tokens":0}}}`
	}
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func (t *sequentialJinaRotationTransport) Calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

type observingCASBlobStore struct {
	blob.BlobStore
	deadline time.Time
	err      error
}

func (s *observingCASBlobStore) CompareAndSwap(ctx context.Context, _ string, _ string, _ io.Reader, _ int64) error {
	s.deadline, _ = ctx.Deadline()
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.err
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

func (s *jobFailureBlobStore) CompareAndSwap(ctx context.Context, key, expectedETag string, body io.Reader, size int64) error {
	if s.mode == "compare_and_swap" && strings.Contains(key, "/jobs/") {
		return errors.New("test job compare-and-swap failure")
	}
	store, ok := s.BlobStore.(compareAndSwapBlobStore)
	if !ok {
		return errors.New("test job store does not support compare-and-swap")
	}
	return store.CompareAndSwap(ctx, key, expectedETag, body, size)
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

func (b *blockingSearchBackend) Search(ctx context.Context, _ string, _ []string) ([]SearchHit, BackendOutcome) {
	b.calls.Add(1)
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, BackendOutcome{Kind: BackendRuntimeError}
	}
	return []SearchHit{{URL: "https://example.com/", Title: "Example"}}, BackendOutcome{Kind: BackendSuccess, Requests: 1}
}

func (*blockingSearchBackend) MaxAttemptsPerCall() int { return 1 }
func (b *blockingSearchBackend) Fetch(context.Context, string) (Page, BackendOutcome) {
	panic("unexpected fetch")
}

func (b *concurrentBackend) Search(context.Context, string, []string) ([]SearchHit, BackendOutcome) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	return nil, BackendOutcome{Kind: BackendSuccess, Requests: 1}
}

func (*concurrentBackend) MaxAttemptsPerCall() int { return 1 }
func (b *concurrentBackend) Fetch(context.Context, string) (Page, BackendOutcome) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	return b.page, BackendOutcome{Kind: BackendSuccess, Requests: 1}
}
func zeroRandom(dst []byte) (int, error) { clear(dst); return len(dst), nil }
func testService(objects blob.BlobStore, backend Backend) (*Service, []byte, func() time.Time) {
	key := []byte("binding-verifier-key-with-at-least-32-bytes")
	nowValue := time.Now().UTC()
	now := func() time.Time { return nowValue }
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
