package webconnector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
)

func TestJinaBackendUsesClosedHeadersAndCommittedReaderFixture(t *testing.T) {
	t.Parallel()
	fixture, err := os.ReadFile(filepath.Join("testdata", "reader-success.json"))
	if err != nil {
		t.Fatal(err)
	}
	var header http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixtureResponse(t, fixture))
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"fixture-key"}, time.Now)
	page, outcome := backend.Fetch(context.Background(), "https://example.com/")
	if outcome.Kind != BackendSuccess {
		t.Fatalf("outcome = %+v", outcome)
	}
	if page.PublishedTime != "Wed, 15 Jul 2026 18:48:48 GMT" {
		t.Fatalf("publishedTime = %q", page.PublishedTime)
	}
	if page.URL != "https://example.com/" || page.Title != "Example Domain" || page.TargetHTTPStatus != 200 {
		t.Fatalf("mapped page identity/status = %+v", page)
	}
	if page.Content != "# Example Domain\n\nThis domain is for use in documentation examples without needing permission. Avoid use in operations.\n\n[Learn more](https://iana.org/domains/example)" {
		t.Fatalf("mapped page content = %q", page.Content)
	}
	if page.Tokens != 765 {
		t.Fatalf("tokens = %d", page.Tokens)
	}
	want := map[string]string{"Accept": "application/json", "Content-Type": "application/json", "X-Return-Format": "markdown", "X-Timeout": "30", "X-Token-Budget": "262144", "X-With-Generated-Alt": "true", "Authorization": "Bearer fixture-key"}
	assertExactHeaders(t, header, want, int64(len(`{"url":"https://example.com/"}`)))
}

func TestJinaBackendReaderUsageComesFromTheDataUsageBlock(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":200,"data":{"title":"Example","url":"https://example.com/","content":"body","httpStatus":200,"usage":{"tokens":17}},"meta":{"usage":{"tokens":99}}}`))
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"fixture-key"}, time.Now)
	page, outcome := backend.Fetch(context.Background(), "https://example.com/")
	if outcome.Kind != BackendSuccess || outcome.Tokens != 17 || page.Tokens != 17 {
		t.Fatalf("page=%+v outcome=%+v; want data usage tokens", page, outcome)
	}
}

func TestJinaBackendDefaultClientSendsOnlyClosedReaderHeaders(t *testing.T) {
	t.Parallel()
	fixture := loadRecordedFixture(t, "reader-success.json")
	var header http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		header = request.Header.Clone()
		w.WriteHeader(fixture.Status)
		_, _ = w.Write(fixture.Response)
	}))
	defer server.Close()
	backend := NewJinaBackend(nil, server.URL, server.URL, []string{"fixture-key"}, time.Now)
	if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("outcome=%+v", outcome)
	}
	want := map[string]string{"Accept": "application/json", "Content-Type": "application/json", "X-Return-Format": "markdown", "X-Timeout": "30", "X-Token-Budget": "262144", "X-With-Generated-Alt": "true", "Authorization": "Bearer fixture-key"}
	assertExactHeaders(t, header, want, int64(len(`{"url":"https://example.com/"}`)))
}

func TestJinaBackendTreatsTargetRedirectStatusAsReadableContent(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":200,"data":{"title":"Redirected","url":"https://example.com/final","content":"redirect body","httpStatus":302,"usage":{"tokens":23}}}`))
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"fixture-key"}, time.Now)
	page, outcome := backend.Fetch(context.Background(), "https://example.com/start")
	if outcome.Kind != BackendSuccess || outcome.TargetHTTPStatus == nil || *outcome.TargetHTTPStatus != 302 {
		t.Fatalf("page=%+v outcome=%+v", page, outcome)
	}
	if page.Content != "redirect body" || page.TargetHTTPStatus != 302 || page.Tokens != 23 {
		t.Fatalf("page=%+v", page)
	}
}

func TestJinaBackendUnauthorizedKeyRemainsDisabledForBackendLifetime(t *testing.T) {
	fixture := loadRecordedFixture(t, "reader-success.json")
	unauthorized := loadRecordedFixture(t, "reader-unauthorized.json")
	clock := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	keys := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		keys = append(keys, key)
		if key == "one" {
			w.WriteHeader(unauthorized.Status)
			_, _ = w.Write(unauthorized.Response)
			return
		}
		w.WriteHeader(fixture.Status)
		_, _ = w.Write(fixture.Response)
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"one", "two"}, func() time.Time { return clock })
	if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("first outcome=%+v", outcome)
	}
	clock = clock.Add(365 * 24 * time.Hour)
	if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("second outcome=%+v", outcome)
	}
	want := []string{"one", "two", "two"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("keys=%v want=%v; unauthorized key was retried", keys, want)
	}
	restarted := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"one", "two"}, func() time.Time { return clock })
	if _, outcome := restarted.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("restart outcome=%+v", outcome)
	}
	want = []string{"one", "two", "two", "one", "two"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("keys after restart=%v want=%v", keys, want)
	}
}

func TestJinaBackendAttemptBoundRemainsConstructionFixedAfterKeysBecomeDeadOrCooling(t *testing.T) {
	fixture := loadRecordedFixture(t, "reader-success.json")
	clock := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		calls[key]++
		switch key {
		case "dead":
			w.WriteHeader(http.StatusUnauthorized)
		case "cooling":
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(fixture.Status)
			_, _ = w.Write(fixture.Response)
		}
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"dead", "cooling", "healthy"}, func() time.Time { return clock })
	if backend.MaxAttemptsPerCall() != 3 {
		t.Fatalf("initial attempt bound = %d; want 3", backend.MaxAttemptsPerCall())
	}
	if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("rotating fetch outcome = %+v", outcome)
	}
	if calls["dead"] != 1 || calls["cooling"] != 1 || calls["healthy"] != 1 {
		t.Fatalf("rotation calls = %#v; want one attempt per configured key", calls)
	}
	if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("post-health-change fetch outcome = %+v", outcome)
	}
	if backend.MaxAttemptsPerCall() != 3 || calls["dead"] != 1 || calls["cooling"] != 1 || calls["healthy"] != 2 {
		t.Fatalf("stable attempt bound/calls = %d/%#v", backend.MaxAttemptsPerCall(), calls)
	}

	service := NewService(blob.NewFakeBlobStore(), backend, nil, NewMetrics(), func() time.Time { return clock }, zeroRandom)
	wantDuration := time.Duration(maxOperations*maxSearchDomains*3)*BackendRequestTimeout + webJobResultCommitMargin
	if service.jobClaimDuration != wantDuration {
		t.Fatalf("three-key claim duration = %s; want %s", service.jobClaimDuration, wantDuration)
	}
	inputHash := CanonicalInputHash(canonicalInput(maximumWebCallInput()))
	claim, replay, err := service.claimJob(context.Background(), Scope{"ws", "ses", "thr"}, "event-post-health-change", inputHash, "search", clock)
	if err != nil || replay != nil || claim == nil || !claim.ExpiresAt.Equal(clock.Add(wantDuration)) {
		t.Fatalf("post-health-change claim = %#v/%+v/%v; want expiry %s", claim, replay, err, clock.Add(wantDuration))
	}
}

func TestJinaBackendCooldownBoundariesAvoidCallsUntilExactExpiry(t *testing.T) {
	fixture := loadRecordedFixture(t, "reader-success.json")
	tests := []struct {
		name     string
		status   int
		cooldown time.Duration
	}{
		{name: "payment-required-is-3600-seconds", status: http.StatusPaymentRequired, cooldown: 3600 * time.Second},
		{name: "rate-limited-is-60-seconds", status: http.StatusTooManyRequests, cooldown: 60 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				if calls == 1 {
					w.WriteHeader(test.status)
					return
				}
				w.WriteHeader(fixture.Status)
				_, _ = w.Write(fixture.Response)
			}))
			defer server.Close()
			backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"one"}, func() time.Time { return clock })
			if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendUnavailable || calls != 1 {
				t.Fatalf("initial outcome=%+v calls=%d", outcome, calls)
			}
			clock = clock.Add(test.cooldown - time.Nanosecond)
			if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendUnavailable || calls != 1 {
				t.Fatalf("before expiry outcome=%+v calls=%d; want no backend call", outcome, calls)
			}
			clock = clock.Add(time.Nanosecond)
			if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess || calls != 2 {
				t.Fatalf("at expiry outcome=%+v calls=%d", outcome, calls)
			}
		})
	}
}

func TestJinaBackendRotatesUnavailableKeysWithoutCallingWhenAllCoolingDown(t *testing.T) {
	clock := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	unauthorized := loadRecordedFixture(t, "reader-unauthorized.json")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(unauthorized.Status)
			_, _ = w.Write(unauthorized.Response)
		} else {
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"one", "two"}, func() time.Time { return clock })
	_, outcome := backend.Fetch(context.Background(), "https://example.com/")
	if outcome.Kind != BackendUnavailable || calls != 2 {
		t.Fatalf("first outcome=%+v calls=%d", outcome, calls)
	}
	_, outcome = backend.Fetch(context.Background(), "https://example.com/")
	if outcome.Kind != BackendUnavailable || calls != 2 {
		t.Fatalf("second outcome=%+v calls=%d; want no additional call", outcome, calls)
	}
}

func TestJinaBackendRoundRobinSkipsCooldownAndReusesKeyAtExactExpiry(t *testing.T) {
	fixture := loadRecordedFixture(t, "reader-success.json")
	clock := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	keys := []string{}
	first := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		keys = append(keys, key)
		if key == "one" && first {
			first = false
			w.WriteHeader(http.StatusPaymentRequired)
			return
		}
		w.WriteHeader(fixture.Status)
		_, _ = w.Write(fixture.Response)
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"one", "two"}, func() time.Time { return clock })
	if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("first outcome=%+v", outcome)
	}
	if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("second outcome=%+v", outcome)
	}
	clock = clock.Add(time.Hour)
	if _, outcome := backend.Fetch(context.Background(), "https://example.com/"); outcome.Kind != BackendSuccess {
		t.Fatalf("third outcome=%+v", outcome)
	}
	want := []string{"one", "two", "two", "one"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("keys=%v want=%v", keys, want)
	}
}

func TestJinaBackendConcurrentFailureUpdatesNeverShortenCooldownOrReviveDeadKeys(t *testing.T) {
	t.Parallel()
	clock := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	backend := NewJinaBackend(nil, "https://search.invalid/", "https://reader.invalid/", []string{"one"}, func() time.Time { return clock })

	longApplied := make(chan struct{})
	done := make(chan struct{})
	go func() {
		backend.disable(0, time.Hour, false)
		close(longApplied)
	}()
	go func() {
		<-longApplied
		backend.disable(0, time.Minute, false)
		close(done)
	}()
	<-done
	clock = clock.Add(time.Minute + time.Second)
	if _, _, ok := backend.acquireKey(map[int]bool{}); ok {
		t.Fatal("shorter concurrent cooldown revived a key before the one-hour deadline")
	}
	clock = clock.Add(time.Hour)
	if _, _, ok := backend.acquireKey(map[int]bool{}); !ok {
		t.Fatal("key did not recover after the longest cooldown elapsed")
	}

	backend.disable(0, 0, true)
	backend.disable(0, time.Minute, false)
	if _, _, ok := backend.acquireKey(map[int]bool{}); ok {
		t.Fatal("later cooldown update revived a dead key")
	}
}

func TestJinaBackendMapsEveryCommittedSyntheticFailureArm(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "backend-error-synthetic.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Outcome         string          `json:"outcome"`
			OuterHTTPStatus *int            `json:"outer_http_status"`
			Response        json.RawMessage `json:"response"`
		}
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	for index, item := range fixture.Cases {
		t.Run(fmt.Sprintf("case-%d", index), func(t *testing.T) {
			if item.Outcome == "transport-timeout" {
				client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, context.DeadlineExceeded })}
				backend := NewJinaBackend(client, "https://search.invalid/", "https://reader.invalid/", []string{"key"}, time.Now)
				_, outcome := backend.Fetch(context.Background(), "https://example.com/")
				if outcome.Kind != BackendRuntimeError {
					t.Fatalf("outcome=%+v", outcome)
				}
				return
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(*item.OuterHTTPStatus)
				if item.Response != nil {
					_, _ = w.Write(item.Response)
				}
			}))
			defer server.Close()
			backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"key"}, time.Now)
			_, outcome := backend.Fetch(context.Background(), "https://example.com/")
			want := BackendRuntimeError
			if *item.OuterHTTPStatus == 418 {
				want = BackendToolError
			}
			if *item.OuterHTTPStatus == 402 || *item.OuterHTTPStatus == 429 {
				want = BackendUnavailable
			}
			if outcome.Kind != want {
				t.Fatalf("status=%d outcome=%+v want=%v", *item.OuterHTTPStatus, outcome, want)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestJinaBackendMapsCommittedSearchFixtureAndUsesOnlyAllowedHeaders(t *testing.T) {
	t.Parallel()
	fixture := loadRecordedFixture(t, "search-no-content.json")
	var header http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Clone()
		w.WriteHeader(fixture.Status)
		_, _ = w.Write(fixture.Response)
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"fixture-key"}, time.Now)
	hits, outcome := backend.Search(context.Background(), "kubernetes documentation", nil)
	if outcome.Kind != BackendSuccess || outcome.Tokens != 10000 || len(hits) != 8 {
		t.Fatalf("hits=%d outcome=%+v", len(hits), outcome)
	}
	if hits[0].Title != "Kubernetes Documentation" || hits[0].URL != "https://kubernetes.io/docs/home/" || hits[0].Description == "" {
		t.Fatalf("first hit = %+v", hits[0])
	}
	if hits[1].Date != "May 30, 2026" {
		t.Fatalf("dated hit = %+v", hits[1])
	}
	want := map[string]string{"Accept": "application/json", "Content-Type": "application/json", "X-Respond-With": "no-content", "Authorization": "Bearer fixture-key"}
	assertExactHeaders(t, header, want, int64(len(`{"q":"kubernetes documentation"}`)))
}

func TestJinaBackendFanoutUsesSiteHeaderAndDeduplicatesCommittedHits(t *testing.T) {
	t.Parallel()
	fixture := loadRecordedFixture(t, "search-site-filter.json")
	sites := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sites = append(sites, r.Header.Get("X-Site"))
		w.WriteHeader(fixture.Status)
		_, _ = w.Write(fixture.Response)
	}))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"key"}, time.Now)
	hits, outcome := backend.Search(context.Background(), "kubernetes deployment", []string{"kubernetes.io", "example.com"})
	if outcome.Kind != BackendSuccess || outcome.Requests != 2 || len(hits) != 10 {
		t.Fatalf("hits=%d outcome=%+v", len(hits), outcome)
	}
	if len(sites) != 2 || sites[0] != "kubernetes.io" || sites[1] != "example.com" {
		t.Fatalf("sites = %#v", sites)
	}
}

func TestJinaBackendRejectsTooManyDomainsBeforeNetwork(t *testing.T) {
	t.Parallel()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer server.Close()
	backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"key"}, time.Now)
	_, outcome := backend.Search(context.Background(), "query", []string{"a", "b", "c", "d", "e"})
	if outcome.Kind != BackendToolError || calls != 0 {
		t.Fatalf("outcome=%+v calls=%d", outcome, calls)
	}
}

func TestJinaBackendMapsCommittedTargetAndInputFailuresWithoutPage(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"reader-target-not-found.json", "reader-syntactic-invalid.json", "reader-unresolvable.json"} {
		t.Run(name, func(t *testing.T) {
			fixture := loadRecordedFixture(t, name)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(fixture.Status)
				_, _ = w.Write(fixture.Response)
			}))
			defer server.Close()
			backend := NewJinaBackend(server.Client(), server.URL, server.URL, []string{"key"}, time.Now)
			page, outcome := backend.Fetch(context.Background(), "https://example.com/")
			if outcome.Kind != BackendToolError || page.Content != "" {
				t.Fatalf("page=%+v outcome=%+v", page, outcome)
			}
			if name == "reader-target-not-found.json" && (outcome.TargetHTTPStatus == nil || *outcome.TargetHTTPStatus != 404) {
				t.Fatalf("target status = %v", outcome.TargetHTTPStatus)
			}
		})
	}
}

type recordedFixture struct {
	Status   int
	Response []byte
}

func loadRecordedFixture(t *testing.T, name string) recordedFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		OuterHTTPStatus int             `json:"outer_http_status"`
		Response        json.RawMessage `json:"response"`
	}
	if err = json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return recordedFixture{value.OuterHTTPStatus, value.Response}
}
func assertExactHeaders(t *testing.T, got http.Header, want map[string]string, wantContentLength int64) {
	t.Helper()
	got = got.Clone()
	if contentLength := got.Get("Content-Length"); contentLength != strconv.FormatInt(wantContentLength, 10) {
		t.Errorf("automatic Content-Length=%q want=%d", contentLength, wantContentLength)
	}
	got.Del("Content-Length")
	if len(got) != len(want) {
		t.Errorf("header count=%d want=%d; got=%v", len(got), len(want), got)
	}
	for name, values := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected header %s=%q", name, values)
		}
	}
	for name, value := range want {
		if got.Get(name) != value {
			t.Errorf("header %s=%q; want %q", name, got.Get(name), value)
		}
	}
}

func fixtureResponse(t *testing.T, fixture []byte) []byte {
	t.Helper()
	var envelope struct {
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(fixture, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Response
}
