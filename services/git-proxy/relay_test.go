package gitproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestProxyRejectsInvalidRoutesBeforeUpstream(t *testing.T) {
	upstreamCalls := 0
	var logs bytes.Buffer
	_, hash := deterministicTicket(t, 8)
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{}, HandlerOptions{AccessLogger: NewJSONAccessLogger(&logs)})

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ticket/github.com/tetral-ai/tetral/info/refs", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404", recorder.Code)
	}
	if upstreamCalls != 0 {
		t.Fatalf("upstreamCalls = %d; want 0", upstreamCalls)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("access log lines = %d; want 1: %q", len(lines), logs.String())
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("parse access log: %v\n%s", err, lines[0])
	}
	assertExactAccessLogFields(t, record)
	assertLogString(t, record, "operation", "")
	assertLogString(t, record, "decision", "rejected:invalid_route")
	assertLogNumber(t, record, "upstream_status", http.StatusNotFound)
}

func TestProxyRejectsMalformedQueryBeforeUpstream(t *testing.T) {
	upstreamCalls := 0
	_, hash := deterministicTicket(t, 9)
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{}, HandlerOptions{})

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ticket/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack&bad=x;y", nil))

	if recorder.Code != http.StatusNotFound || upstreamCalls != 0 {
		t.Fatalf("status/upstreamCalls = %d/%d; want 404/0", recorder.Code, upstreamCalls)
	}
}

func TestProxyRejectsBadTicketsBeforeUpstream(t *testing.T) {
	hashMismatchToken, hashMismatchLookupHash := deterministicTicket(t, 24)
	_, hashMismatchStoredHash := deterministicTicket(t, 25)
	rotatedToken, rotatedHash := deterministicTicket(t, 26)
	rotatedAt := time.Date(2026, 7, 3, 17, 58, 0, 0, time.UTC)
	cases := []struct {
		name    string
		token   string
		tickets map[string]*gitticket.Ticket
	}{
		{
			name:    "missing",
			token:   deterministicMissingTicket(t),
			tickets: map[string]*gitticket.Ticket{},
		},
		{
			name:    "malformed",
			token:   "short",
			tickets: map[string]*gitticket.Ticket{},
		},
		{
			name:  "hash-mismatch",
			token: hashMismatchToken,
			tickets: map[string]*gitticket.Ticket{
				string(hashMismatchLookupHash): {
					WorkspaceID: workspace.DefaultID,
					SessionID:   "sesn_hash_mismatch",
					TicketID:    "gittkt_hash_mismatch",
					TokenHash:   hashMismatchStoredHash,
					Status:      gitticket.StatusLive,
				},
			},
		},
		{
			name:  "rotated-past-grace",
			token: rotatedToken,
			tickets: map[string]*gitticket.Ticket{
				string(rotatedHash): {
					WorkspaceID: workspace.DefaultID,
					SessionID:   "sesn_rotated_expired",
					TicketID:    "gittkt_rotated_expired",
					TokenHash:   rotatedHash,
					Status:      gitticket.StatusRotated,
					RotatedAt:   &rotatedAt,
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstreamCalls := 0
			proxy := testProxyWithTickets(t, tc.tickets, func(w http.ResponseWriter, _ *http.Request) {
				upstreamCalls++
				w.WriteHeader(http.StatusOK)
			}, fakeRepositoryAuthorizer{})

			recorder := httptest.NewRecorder()
			proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+tc.token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d; want 401", recorder.Code)
			}
			if upstreamCalls != 0 {
				t.Fatalf("upstreamCalls = %d; want 0", upstreamCalls)
			}
		})
	}
}

func TestProxyDedicatedHeaderAndLegacyCutoverTable(t *testing.T) {
	token, hash := deterministicTicket(t, 41)
	for _, testCase := range []struct {
		name          string
		legacyEnabled bool
		path          string
		header        string
		wantStatus    int
		wantUpstream  int32
	}{
		{name: "header during cutover", legacyEnabled: true, path: "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", header: token, wantStatus: http.StatusOK, wantUpstream: 1},
		{name: "header after close", path: "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", header: token, wantStatus: http.StatusOK, wantUpstream: 1},
		{name: "legacy during cutover", legacyEnabled: true, path: "/" + token + "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", wantStatus: http.StatusOK, wantUpstream: 1},
		{name: "legacy after close", path: "/" + token + "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", wantStatus: http.StatusNotFound},
		{name: "missing header", path: "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", wantStatus: http.StatusUnauthorized},
		{name: "malformed header", path: "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", header: "malformed", wantStatus: http.StatusUnauthorized},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var upstreamCalls int32
			proxy := testProxyWithTicketsForCutover(t, liveTickets(hash), func(w http.ResponseWriter, request *http.Request) {
				atomic.AddInt32(&upstreamCalls, 1)
				if got := request.Header.Get("X-Tetral-Git-Ticket"); got != "" {
					t.Errorf("upstream X-Tetral-Git-Ticket = %q; want stripped", got)
				}
				w.WriteHeader(http.StatusOK)
			}, fakeRepositoryAuthorizer{}, HandlerOptions{}, testCase.legacyEnabled)
			request := httptest.NewRequest(http.MethodGet, testCase.path, nil)
			if testCase.header != "" {
				request.Header.Set("X-Tetral-Git-Ticket", testCase.header)
			}
			recorder := httptest.NewRecorder()
			proxy.ServeHTTP(recorder, request)
			if recorder.Code != testCase.wantStatus || atomic.LoadInt32(&upstreamCalls) != testCase.wantUpstream {
				t.Fatalf("status/upstream = %d/%d; want %d/%d", recorder.Code, upstreamCalls, testCase.wantStatus, testCase.wantUpstream)
			}
		})
	}
}

func TestProxyInjectsAuthorizationOnlyForAllowlistedRepositories(t *testing.T) {
	token, hash := deterministicTicket(t, 9)
	var authorizations []string
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, r *http.Request) {
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionInjected, Authorization: GitHubBasicAuthorization("gh-token")},
			"tetral-ai/public": {Decision: DecisionAnonymous},
		},
	})

	for _, repo := range []string{"tetral", "public"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/"+repo+"/info/refs?service=git-upload-pack", nil)
		request.Header.Set("Authorization", "Bearer sandbox-supplied-value")
		proxy.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s status = %d; want 200", repo, recorder.Code)
		}
	}
	if len(authorizations) != 2 {
		t.Fatalf("authorizations = %v", authorizations)
	}
	if authorizations[0] != GitHubBasicAuthorization("gh-token") {
		t.Fatalf("allowlisted authorization = %q", authorizations[0])
	}
	if authorizations[1] != "" {
		t.Fatalf("anonymous authorization = %q; want empty", authorizations[1])
	}
}

func TestProxyForwardsOnlyContractedGitHeaders(t *testing.T) {
	token, hash := deterministicTicket(t, 32)
	var upstreamHeader http.Header
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, r *http.Request) {
		upstreamHeader = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionInjected, Authorization: GitHubBasicAuthorization("gh-token")},
		},
	})

	request := httptest.NewRequest(http.MethodPost, "/"+token+"/github.com/tetral-ai/tetral/git-upload-pack", strings.NewReader("0000"))
	request.Header.Set("Authorization", "Bearer sandbox-supplied-value")
	request.Header.Set("X-Tetral-Git-Ticket", token)
	request.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	request.Header.Set("Accept", "application/x-git-upload-pack-result")
	request.Header.Set("Git-Protocol", "version=2")
	request.Header.Set("Content-Encoding", "gzip")
	request.Header.Set("Accept-Encoding", "gzip")
	request.Header.Set("Cookie", "session=leak")
	request.Header.Set("X-Tetral-Internal-Principal", "principal-leak")
	request.Header.Set("X-Tetral-Workspace-Id", "workspace-leak")
	request.Header.Set("X-Api-Key", "api-key-leak")
	request.Header.Set("User-Agent", "sandbox-user-agent")

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", recorder.Code)
	}
	for name, want := range map[string]string{
		"Authorization":    GitHubBasicAuthorization("gh-token"),
		"Content-Type":     "application/x-git-upload-pack-request",
		"Accept":           "application/x-git-upload-pack-result",
		"Git-Protocol":     "version=2",
		"Content-Encoding": "gzip",
		"Accept-Encoding":  "gzip",
	} {
		if got := upstreamHeader.Get(name); got != want {
			t.Fatalf("upstream %s = %q; want %q in %#v", name, got, want, upstreamHeader)
		}
	}
	for _, forbidden := range []string{
		"Cookie",
		"X-Tetral-Internal-Principal",
		"X-Tetral-Workspace-Id",
		"X-Api-Key",
		"User-Agent",
		"X-Forwarded-For",
		"X-Tetral-Git-Ticket",
	} {
		if values, ok := upstreamHeader[forbidden]; ok && len(values) > 0 {
			t.Fatalf("upstream forwarded forbidden header %s=%q in %#v", forbidden, values, upstreamHeader)
		}
	}
}

func TestProxyRereadsRepositoryTokenOnceForBodylessInfoRefsUnauthorized(t *testing.T) {
	token, hash := deterministicTicket(t, 33)
	var authorizations []string
	var authorizationCalls int
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, r *http.Request) {
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		if len(authorizations) == 1 {
			http.Error(w, "stale credential", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}, repositoryAuthorizerFunc(func(_ context.Context, _ RepositoryAuthRequest) (RepositoryAuthDecision, error) {
		authorizationCalls++
		token := "old-token"
		if authorizationCalls == 2 {
			token = "new-token"
		}
		return RepositoryAuthDecision{Decision: DecisionInjected, Authorization: GitHubBasicAuthorization(token)}, nil
	}))

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q; want 200 after repository-token reread", recorder.Code, recorder.Body.String())
	}
	if !reflect.DeepEqual(authorizations, []string{GitHubBasicAuthorization("old-token"), GitHubBasicAuthorization("new-token")}) {
		t.Fatalf("upstream authorizations = %v; want old then reread token", authorizations)
	}
	if authorizationCalls != 2 {
		t.Fatalf("authorization calls = %d; want initial read plus one reactive reread", authorizationCalls)
	}
}

func TestProxyRelaysOriginalUnauthorizedWhenReactiveRereadFindsRepositoryUnmounted(t *testing.T) {
	token, hash := deterministicTicket(t, 42)
	var upstreamCalls int
	var authorizationCalls int
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		w.Header().Set("X-GitHub-Request-Id", "request-original")
		http.Error(w, "original unauthorized response", http.StatusUnauthorized)
	}, repositoryAuthorizerFunc(func(_ context.Context, _ RepositoryAuthRequest) (RepositoryAuthDecision, error) {
		authorizationCalls++
		if authorizationCalls == 1 {
			return RepositoryAuthDecision{
				Decision:      DecisionInjected,
				Authorization: GitHubBasicAuthorization("old-token"),
			}, nil
		}
		return RepositoryAuthDecision{Decision: DecisionAnonymous}, nil
	}))

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))

	if recorder.Code != http.StatusUnauthorized || recorder.Body.String() != "original unauthorized response\n" {
		t.Fatalf("response = %d %q; want original upstream 401", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("X-GitHub-Request-Id") != "request-original" {
		t.Fatalf("request id header = %q; want original upstream header", recorder.Header().Get("X-GitHub-Request-Id"))
	}
	if upstreamCalls != 1 || authorizationCalls != 2 {
		t.Fatalf("upstream/authorization calls = %d/%d; want one upstream request and one bounded reread", upstreamCalls, authorizationCalls)
	}
}

func TestProxyClassifiesReactiveCredentialRereadFailures(t *testing.T) {
	for _, test := range []struct {
		name         string
		rereadErr    error
		wantStatus   int
		wantDecision string
	}{
		{
			name:         "credential required",
			rereadErr:    ErrGitHubCredentialRequired,
			wantStatus:   http.StatusFailedDependency,
			wantDecision: "rejected:credential_required",
		},
		{
			name:         "credential store error",
			rereadErr:    errors.New("credential store unavailable"),
			wantStatus:   statusBadUpstream,
			wantDecision: "rejected:credential_error",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			token, hash := deterministicTicket(t, 41)
			var logs bytes.Buffer
			var authorizationCalls int
			proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "stale credential", http.StatusUnauthorized)
			}, repositoryAuthorizerFunc(func(_ context.Context, _ RepositoryAuthRequest) (RepositoryAuthDecision, error) {
				authorizationCalls++
				if authorizationCalls == 1 {
					return RepositoryAuthDecision{
						Decision:      DecisionInjected,
						Authorization: GitHubBasicAuthorization("old-token"),
					}, nil
				}
				return RepositoryAuthDecision{}, test.rereadErr
			}), HandlerOptions{AccessLogger: NewJSONAccessLogger(&logs)})

			recorder := httptest.NewRecorder()
			proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d body=%q; want %d", recorder.Code, recorder.Body.String(), test.wantStatus)
			}
			if authorizationCalls != 2 {
				t.Fatalf("authorization calls = %d; want initial read plus one reread", authorizationCalls)
			}
			var record map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &record); err != nil {
				t.Fatalf("parse access log: %v\n%s", err, logs.String())
			}
			assertLogString(t, record, "decision", test.wantDecision)
		})
	}
}

func TestProxyStopsAfterOneBodylessInfoRefsReread(t *testing.T) {
	token, hash := deterministicTicket(t, 35)
	var upstreamCalls int
	var authorizationCalls int
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		http.Error(w, "still unauthorized", http.StatusUnauthorized)
	}, repositoryAuthorizerFunc(func(_ context.Context, _ RepositoryAuthRequest) (RepositoryAuthDecision, error) {
		authorizationCalls++
		return RepositoryAuthDecision{
			Decision:      DecisionInjected,
			Authorization: GitHubBasicAuthorization("token-" + strconv.Itoa(authorizationCalls)),
		}, nil
	}))

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body=%q; want second upstream 401", recorder.Code, recorder.Body.String())
	}
	if upstreamCalls != 2 || authorizationCalls != 2 {
		t.Fatalf("upstream/authorization calls = %d/%d; want one request and one bounded reread retry", upstreamCalls, authorizationCalls)
	}
}

func TestProxyDoesNotRereadOrReplayRequestBodiesAfterUnauthorized(t *testing.T) {
	token, hash := deterministicTicket(t, 34)
	var authorizations []string
	var authorizationCalls int
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, r *http.Request) {
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		http.Error(w, "stale credential", http.StatusUnauthorized)
	}, repositoryAuthorizerFunc(func(_ context.Context, _ RepositoryAuthRequest) (RepositoryAuthDecision, error) {
		authorizationCalls++
		return RepositoryAuthDecision{Decision: DecisionInjected, Authorization: GitHubBasicAuthorization("old-token")}, nil
	}))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/"+token+"/github.com/tetral-ai/tetral/git-upload-pack", stringsReader("pack-data"))
	proxy.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body=%q; want original 401 without body replay", recorder.Code, recorder.Body.String())
	}
	if !reflect.DeepEqual(authorizations, []string{GitHubBasicAuthorization("old-token")}) {
		t.Fatalf("upstream authorizations = %v; want exactly one non-replayed request", authorizations)
	}
	if authorizationCalls != 1 {
		t.Fatalf("authorization calls = %d; want no post-401 reread for a request body", authorizationCalls)
	}
}

func TestProxyLeavesAnonymousUpstreamAuthFailuresUnchanged(t *testing.T) {
	for index, upstreamStatus := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(strconv.Itoa(upstreamStatus), func(t *testing.T) {
			token, hash := deterministicTicket(t, byte(30+index))
			var authorizationCalls int
			proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "private repo", upstreamStatus)
			}, repositoryAuthorizerFunc(func(_ context.Context, _ RepositoryAuthRequest) (RepositoryAuthDecision, error) {
				authorizationCalls++
				return RepositoryAuthDecision{Decision: DecisionAnonymous}, nil
			}))

			recorder := httptest.NewRecorder()
			proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/private/info/refs?service=git-upload-pack", nil))

			if recorder.Code != upstreamStatus {
				t.Fatalf("status = %d body=%q; want upstream %d passthrough", recorder.Code, recorder.Body.String(), upstreamStatus)
			}
			if authorizationCalls != 1 {
				t.Fatalf("authorization calls = %d; want no anonymous reread", authorizationCalls)
			}
		})
	}
}

func TestProxyDoesNotRereadInjectedCredentialAfterForbidden(t *testing.T) {
	token, hash := deterministicTicket(t, 21)
	var authorizationCalls int
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream forbidden", http.StatusForbidden)
	}, repositoryAuthorizerFunc(func(_ context.Context, _ RepositoryAuthRequest) (RepositoryAuthDecision, error) {
		authorizationCalls++
		return RepositoryAuthDecision{Decision: DecisionInjected, Authorization: GitHubBasicAuthorization("token")}, nil
	}))

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/private/info/refs?service=git-upload-pack", nil))

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d body=%q; want upstream 403 passthrough", recorder.Code, recorder.Body.String())
	}
	if authorizationCalls != 1 {
		t.Fatalf("authorization calls = %d; want no 403 reread", authorizationCalls)
	}
}

func TestProxyRewritesGitHubRedirectsBackThroughProxy(t *testing.T) {
	token, hash := deterministicTicket(t, 10)
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://github.com/tetral-ai/renamed.git/info/refs?service=git-upload-pack")
		w.WriteHeader(http.StatusMovedPermanently)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	})

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))

	if recorder.Code != http.StatusMovedPermanently {
		t.Fatalf("status = %d; want 301", recorder.Code)
	}
	got := recorder.Header().Get("Location")
	want := "https://git.tetral.test/github.com/tetral-ai/renamed.git/info/refs?service=git-upload-pack"
	if got != want {
		t.Fatalf("Location = %q; want %q", got, want)
	}
}

func TestProxyLeavesNonRedirectLocationHeadersUnchanged(t *testing.T) {
	token, hash := deterministicTicket(t, 31)
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://github.com/tetral-ai/renamed.git/info/refs?service=git-upload-pack")
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	})

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", recorder.Code)
	}
	got := recorder.Header().Get("Location")
	want := "https://github.com/tetral-ai/renamed.git/info/refs?service=git-upload-pack"
	if got != want {
		t.Fatalf("Location = %q; want upstream non-redirect header %q", got, want)
	}
}

func TestProxyStreamsRequestAndResponseBodies(t *testing.T) {
	token, hash := deterministicTicket(t, 11)
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream body: %v", err)
		}
		if string(body) != "pack-data" {
			t.Fatalf("upstream body = %q", string(body))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pack-response"))
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	})

	request := httptest.NewRequest(http.MethodPost, "/"+token+"/github.com/tetral-ai/tetral/git-upload-pack", io.NopCloser(stringsReader("pack-data")))
	request.ContentLength = int64(len("pack-data"))
	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", recorder.Code)
	}
	if recorder.Body.String() != "pack-response" {
		t.Fatalf("body = %q", recorder.Body.String())
	}
}

func TestProxyAllowsTwoRequestOperationAcrossTicketRotationGrace(t *testing.T) {
	token, hash := deterministicTicket(t, 27)
	now := time.Date(2026, 7, 3, 18, 0, 0, 0, time.UTC)
	tickets := liveTickets(hash)
	ticket := tickets[string(hash)]
	upstreamCalls := 0
	proxy := testProxyWithTickets(t, tickets, func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	})

	infoRefs := httptest.NewRecorder()
	proxy.ServeHTTP(infoRefs, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))
	if infoRefs.Code != http.StatusOK {
		t.Fatalf("info/refs status = %d; want 200", infoRefs.Code)
	}

	rotatedWithinGrace := now.Add(-30 * time.Second)
	ticket.Status = gitticket.StatusRotated
	ticket.RotatedAt = &rotatedWithinGrace
	uploadPack := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/"+token+"/github.com/tetral-ai/tetral/git-upload-pack", stringsReader("pack-data"))
	request.ContentLength = int64(len("pack-data"))
	proxy.ServeHTTP(uploadPack, request)
	if uploadPack.Code != http.StatusOK {
		t.Fatalf("upload-pack status = %d; want 200 within rotation grace", uploadPack.Code)
	}
	if upstreamCalls != 2 {
		t.Fatalf("upstreamCalls after grace request = %d; want 2", upstreamCalls)
	}

	rotatedPastGrace := now.Add(-2 * time.Minute)
	ticket.RotatedAt = &rotatedPastGrace
	expired := httptest.NewRecorder()
	proxy.ServeHTTP(expired, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))
	if expired.Code != http.StatusUnauthorized {
		t.Fatalf("expired rotated status = %d; want 401", expired.Code)
	}
	if upstreamCalls != 2 {
		t.Fatalf("upstreamCalls after expired rotated ticket = %d; want still 2", upstreamCalls)
	}
}

func TestProxyIdleProgressTimeoutCancelsStalledUpstream(t *testing.T) {
	token, hash := deterministicTicket(t, 17)
	entered := make(chan struct{})
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	}, HandlerOptions{IdleProgressTimeout: 25 * time.Millisecond})

	done := make(chan int, 1)
	go func() {
		recorder := httptest.NewRecorder()
		proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))
		done <- recorder.Code
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("upstream was not reached")
	}
	select {
	case status := <-done:
		if status != statusBadUpstream {
			t.Fatalf("status = %d; want idle timeout surfaced as bad upstream", status)
		}
	case <-time.After(time.Second):
		t.Fatal("idle progress timeout did not cancel stalled upstream")
	}
}

func TestProxyProgressingTransferHasNoTotalTimeout(t *testing.T) {
	token, hash := deterministicTicket(t, 18)
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 5; i++ {
			_, _ = w.Write([]byte("x"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	}, HandlerOptions{IdleProgressTimeout: 50 * time.Millisecond})

	startedAt := time.Now()
	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q; want 200 for progressing transfer", recorder.Code, recorder.Body.String())
	}
	if recorder.Body.String() != "xxxxx" {
		t.Fatalf("body = %q; want byte-identical progressing transfer", recorder.Body.String())
	}
	if elapsed := time.Since(startedAt); elapsed <= 50*time.Millisecond {
		t.Fatalf("elapsed = %s; test did not exceed idle timeout long enough to prove absence of total timeout", elapsed)
	}
}

func TestProxyUploadProgressPreventsIdleTimeout(t *testing.T) {
	token, hash := deterministicTicket(t, 19)
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Fatalf("read upstream body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	}, HandlerOptions{IdleProgressTimeout: 50 * time.Millisecond})

	startedAt := time.Now()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/"+token+"/github.com/tetral-ai/tetral/git-receive-pack", &slowChunkReader{
		chunks: []string{"p", "a", "c", "k", "-data"},
		delay:  20 * time.Millisecond,
	})
	request.ContentLength = int64(len("pack-data"))
	proxy.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q; want 200 for progressing upload", recorder.Code, recorder.Body.String())
	}
	if recorder.Body.String() != "ok" {
		t.Fatalf("body = %q; want upstream response", recorder.Body.String())
	}
	if elapsed := time.Since(startedAt); elapsed <= 50*time.Millisecond {
		t.Fatalf("elapsed = %s; test did not exceed idle timeout long enough to prove upload progress reset", elapsed)
	}
}

func TestProxyDefaultIdleProgressTimeoutIsContractConstant(t *testing.T) {
	if got := (HandlerOptions{}).withDefaults().IdleProgressTimeout; got != IdleProgressTimeout {
		t.Fatalf("default idle progress timeout = %s; want %s", got, IdleProgressTimeout)
	}
}

func TestProxyConfigHasNoTotalTransferTimeout(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(Config{}), reflect.TypeOf(HandlerOptions{})} {
		for index := 0; index < typ.NumField(); index++ {
			name := strings.ToLower(typ.Field(index).Name)
			if strings.Contains(name, "total") && strings.Contains(name, "timeout") {
				t.Fatalf("%s exposes total-transfer timeout field %q", typ.Name(), typ.Field(index).Name)
			}
		}
	}
}

func TestProxyStreams100MiBWithBoundedRSS(t *testing.T) {
	token, hash := deterministicTicket(t, 28)
	const payloadSize = 100 * 1024 * 1024
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := io.CopyN(w, zeroReader{}, payloadSize); err != nil {
			t.Fatalf("write upstream payload: %v", err)
		}
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	})

	runtime.GC()
	before := processMemoryBytes(t)
	recorder := newDiscardResponseWriter()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))
	runtime.GC()
	after := processMemoryBytes(t)

	if recorder.status != http.StatusOK {
		t.Fatalf("status = %d; want 200", recorder.status)
	}
	if recorder.bytes != payloadSize {
		t.Fatalf("bytes = %d; want streamed %d", recorder.bytes, payloadSize)
	}
	if after > before {
		const maxDelta = 8 * 1024 * 1024
		if delta := after - before; delta > maxDelta {
			t.Fatalf("process memory delta = %d bytes; want <= %d for 100 MiB stream", delta, maxDelta)
		}
	}
}

func TestProxyGracefullyDrainsInFlightTransferOnShutdown(t *testing.T) {
	token, hash := deterministicTicket(t, 29)
	upstreamEntered := make(chan struct{})
	allowComplete := make(chan struct{})
	proxy := testProxyWithTickets(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		close(upstreamEntered)
		_, _ = w.Write([]byte("pack-a"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-allowComplete
		_, _ = w.Write([]byte("pack-b"))
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	})
	readiness := workload.NewReadiness()
	readiness.MarkReady()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- workload.Run(ctx, workload.Config{
			ServiceName:           ServiceName,
			DeploymentEnvironment: "test",
			ServiceVersion:        "unit",
			Listener:              listener,
			Handler:               BuildHTTPHandler(readiness, proxy),
			Readiness:             readiness,
			ShutdownTimeout:       time.Second,
			Logger:                workload.NewLogger(io.Discard, ServiceName, "test", "unit"),
		})
	}()
	baseURL := "http://" + listener.Addr().String()
	waitForReady(t, baseURL)

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	bodyDone := make(chan struct {
		body string
		err  error
	}, 1)
	go func() {
		response, err := client.Get(baseURL + "/" + token + "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack")
		if err != nil {
			bodyDone <- struct {
				body string
				err  error
			}{err: err}
			return
		}
		defer func() { _ = response.Body.Close() }()
		body, readErr := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusOK && readErr == nil {
			readErr = &unexpectedStatusError{status: response.StatusCode}
		}
		bodyDone <- struct {
			body string
			err  error
		}{body: string(body), err: readErr}
	}()

	select {
	case <-upstreamEntered:
	case <-time.After(time.Second):
		t.Fatal("in-flight transfer did not reach upstream")
	}
	cancel()
	waitForNewConnectionsRefused(t, baseURL)
	close(allowComplete)

	select {
	case result := <-bodyDone:
		if result.err != nil {
			t.Fatalf("in-flight transfer failed: %v", result.err)
		}
		if result.body != "pack-apack-b" {
			t.Fatalf("body = %q; want byte-identical drained pack", result.body)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight transfer did not complete within drain grace")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("workload shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("workload did not exit within drain grace")
	}
}

func TestProxyAccessLogHasExactContractFields(t *testing.T) {
	token, hash := deterministicTicket(t, 12)
	var logs bytes.Buffer
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	}, HandlerOptions{AccessLogger: NewJSONAccessLogger(&logs)})

	request := httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil)
	request.Header.Set("X-Request-ID", "req-access-log")
	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", recorder.Code)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("access log lines = %d; want 1: %q", len(lines), logs.String())
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("parse access log: %v\n%s", err, lines[0])
	}
	assertExactAccessLogFields(t, record)
	assertLogString(t, record, "service.name", ServiceName)
	assertLogString(t, record, "service.version", "unknown")
	assertLogString(t, record, "deployment.environment", "local")
	assertLogString(t, record, "request.id", "req-access-log")
	assertLogString(t, record, "workspace.id", string(workspace.DefaultID))
	assertLogString(t, record, "session.id", "sesn_git_proxy")
	assertLogString(t, record, "operation", string(endpointRefsUpload))
	assertLogString(t, record, "event.kind", AccessLogEventKind)
	assertLogString(t, record, "component", ServiceName)
	assertLogString(t, record, "ticket_id", "gittkt_live")
	assertLogString(t, record, "owner_repo", "tetral-ai/tetral")
	assertLogString(t, record, "decision", DecisionAnonymous)
	assertLogNumber(t, record, "upstream_status", http.StatusOK)
	assertLogNumber(t, record, "bytes_out", 2)
	if record["time"] == "" {
		t.Fatal("time is empty")
	}
	assertLogString(t, record, "level", "INFO")
	assertLogString(t, record, "msg", AccessLogEventKind)
	if _, ok := record["endpoint"]; ok {
		t.Fatalf("access log kept stale endpoint field: %#v", record)
	}
	if _, ok := record["duration_ms"]; ok {
		t.Fatalf("access log kept stale duration_ms field: %#v", record)
	}
}

func TestProxyAccessLogLeakScan(t *testing.T) {
	token, hash := deterministicTicket(t, 13)
	secretToken := "gh-secret-token"
	authorization := GitHubBasicAuthorization(secretToken)
	var logs bytes.Buffer
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionInjected, Authorization: authorization},
		},
	}, HandlerOptions{AccessLogger: NewJSONAccessLogger(&logs)})

	request := httptest.NewRequest(http.MethodGet, "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil)
	request.Header.Set("X-Request-ID", token)
	request.Header.Set("X-Tetral-Git-Ticket", token)
	request.Header.Set("Authorization", "Bearer sandbox-supplied-secret")
	proxy.ServeHTTP(httptest.NewRecorder(), request)

	output := logs.String()
	for _, forbidden := range []string{token, secretToken, authorization, "Bearer sandbox-supplied-secret"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("access log leaked %q in %s", forbidden, output)
		}
	}
}

func TestProxyEnforcesConnectionAndRequestBodyLimits(t *testing.T) {
	token, hash := deterministicTicket(t, 14)
	var upstreamCalls int32
	entered := make(chan struct{})
	release := make(chan struct{})
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&upstreamCalls, 1) == 1 {
			close(entered)
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	}, HandlerOptions{Limiter: NewTicketConnectionLimiter(1)})

	firstDone := make(chan int, 1)
	go func() {
		recorder := httptest.NewRecorder()
		proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))
		firstDone <- recorder.Code
	}()
	<-entered

	second := httptest.NewRecorder()
	proxy.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d; want 429", second.Code)
	}
	close(release)
	if firstStatus := <-firstDone; firstStatus != http.StatusOK {
		t.Fatalf("first status = %d; want 200", firstStatus)
	}

	bodyCapProxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	}, HandlerOptions{})
	oversized := httptest.NewRequest(http.MethodPost, "/"+token+"/github.com/tetral-ai/tetral/git-receive-pack", stringsReader("pack"))
	oversized.ContentLength = MaxRequestBodyBytes + 1
	recorder := httptest.NewRecorder()
	bodyCapProxy.ServeHTTP(recorder, oversized)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status = %d; want 413", recorder.Code)
	}
	if got := atomic.LoadInt32(&upstreamCalls); got != 1 {
		t.Fatalf("upstreamCalls = %d; want only first in-flight request to reach upstream", got)
	}
}

func TestProxyStreamsUnknownLengthRequestBodyWithCap(t *testing.T) {
	token, hash := deterministicTicket(t, 16)
	var logs bytes.Buffer
	var upstreamCalls int32
	var upstreamBodies []string
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		upstreamBodies = append(upstreamBodies, string(body))
		w.WriteHeader(http.StatusOK)
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionAnonymous},
		},
	}, HandlerOptions{
		AccessLogger:        NewJSONAccessLogger(&logs),
		MaxRequestBodyBytes: 4,
	})

	request := httptest.NewRequest(http.MethodPost, "/"+token+"/github.com/tetral-ai/tetral/git-receive-pack", stringsReader("1234"))
	request.ContentLength = -1
	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 for small unknown-length body", recorder.Code)
	}
	if got := atomic.LoadInt32(&upstreamCalls); got != 1 || len(upstreamBodies) != 1 || upstreamBodies[0] != "1234" {
		t.Fatalf("upstreamCalls=%d bodies=%v; want one streamed unknown-length body", got, upstreamBodies)
	}

	oversized := httptest.NewRequest(http.MethodPost, "/"+token+"/github.com/tetral-ai/tetral/git-receive-pack", stringsReader("12345"))
	oversized.ContentLength = -1
	oversizedRecorder := httptest.NewRecorder()
	proxy.ServeHTTP(oversizedRecorder, oversized)

	if oversizedRecorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status = %d; want 413 for streamed over-cap unknown-length body", oversizedRecorder.Code)
	}
	// Unknown-length bodies are capped by the streaming reader. Depending on
	// ReverseProxy scheduling, the fake upstream handler may or may not be
	// entered before the cap error aborts the request body read; the contract
	// guarantee here is bounded streaming and 413. The zero-upstream
	// case is covered by the known ContentLength cap assertion above.
	if got := atomic.LoadInt32(&upstreamCalls); got < 1 || got > 2 {
		t.Fatalf("upstreamCalls = %d; want only the small request plus optional failed over-cap attempt", got)
	}
	if len(upstreamBodies) != 1 || upstreamBodies[0] != "1234" {
		t.Fatalf("upstream bodies = %v; want only successful small unknown-length body", upstreamBodies)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("access log lines = %d; want 2: %q", len(lines), logs.String())
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &record); err != nil {
		t.Fatalf("parse access log: %v\n%s", err, lines[1])
	}
	assertLogString(t, record, "decision", "rejected:request_body_too_large")
	assertLogNumber(t, record, "upstream_status", http.StatusRequestEntityTooLarge)
	assertLogNumber(t, record, "bytes_in", 4)
}

func TestProxyMetricsExposeContractSeries(t *testing.T) {
	token, hash := deterministicTicket(t, 15)
	metrics := NewGitProxyMetrics()
	proxy := testProxyWithTicketsAndOptions(t, liveTickets(hash), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}, fakeRepositoryAuthorizer{
		decisions: map[string]RepositoryAuthDecision{
			"tetral-ai/tetral": {Decision: DecisionInjected, Authorization: GitHubBasicAuthorization("gh-token")},
		},
	}, HandlerOptions{Metrics: metrics})

	proxy.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/"+token+"/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil))
	recorder := httptest.NewRecorder()
	metrics.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body := recorder.Body.String()
	for _, series := range []string{
		"gitproxy_active_connections",
		"gitproxy_bytes_relayed_total",
		"gitproxy_requests_total",
		"gitproxy_upstream_latency_seconds",
		"gitproxy_ticket_rejections_total",
	} {
		if !strings.Contains(body, series) {
			t.Fatalf("metrics body missing %s:\n%s", series, body)
		}
	}
	for _, alert := range []string{
		AlertTicketRejectionSpike,
		AlertUpstream5xxRatio,
	} {
		if !strings.Contains(body, "# CONTRACT gitproxy_alert_condition "+alert) {
			t.Fatalf("metrics body missing contract alert condition %s:\n%s", alert, body)
		}
	}
	if !strings.Contains(body, `gitproxy_requests_total{endpoint="refs-upload",decision="injected",upstream_status="200"} 1`) {
		t.Fatalf("metrics body missing injected request row:\n%s", body)
	}
	if !strings.Contains(body, `gitproxy_upstream_latency_seconds_bucket{le="+Inf"} 1`) {
		t.Fatalf("metrics body missing latency +Inf bucket:\n%s", body)
	}
	if !strings.Contains(body, `gitproxy_upstream_latency_seconds_count 1`) {
		t.Fatalf("metrics body missing latency count:\n%s", body)
	}
}

func testProxyWithTickets(t *testing.T, tickets map[string]*gitticket.Ticket, upstream http.HandlerFunc, authorizer RepositoryAuthorizer) http.Handler {
	t.Helper()
	return testProxyWithTicketsAndOptions(t, tickets, upstream, authorizer, HandlerOptions{})
}

func testProxyWithTicketsAndOptions(t *testing.T, tickets map[string]*gitticket.Ticket, upstream http.HandlerFunc, authorizer RepositoryAuthorizer, options HandlerOptions) http.Handler {
	return testProxyWithTicketsForCutover(t, tickets, upstream, authorizer, options, true)
}

func testProxyWithTicketsForCutover(t *testing.T, tickets map[string]*gitticket.Ticket, upstream http.HandlerFunc, authorizer RepositoryAuthorizer, options HandlerOptions, legacyPathCutover bool) http.Handler {
	t.Helper()
	upstreamServer := httptest.NewServer(upstream)
	t.Cleanup(upstreamServer.Close)
	upstreamURL, err := url.Parse(upstreamServer.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	publicBase, err := url.Parse("https://git.tetral.test")
	if err != nil {
		t.Fatalf("parse public base: %v", err)
	}
	if options.Transport == nil {
		options.Transport = upstreamRewriteTransport{base: upstreamURL}
	}
	if options.PublicBaseURL == nil {
		options.PublicBaseURL = publicBase
	}
	options.LegacyPathCutover = legacyPathCutover
	return NewHTTPHandler(TicketValidator{
		Store:         fakeTicketStore{tickets: tickets},
		Now:           func() time.Time { return time.Date(2026, 7, 3, 18, 0, 0, 0, time.UTC) },
		RotationGrace: time.Minute,
	}, authorizer, options)
}

type upstreamRewriteTransport struct {
	base *url.URL
	next http.RoundTripper
}

func (t upstreamRewriteTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.URL.Scheme = t.base.Scheme
	clone.URL.Host = t.base.Host
	transport := t.next
	if transport == nil {
		transport = http.DefaultTransport
	}
	return transport.RoundTrip(clone)
}

func liveTickets(hash []byte) map[string]*gitticket.Ticket {
	return map[string]*gitticket.Ticket{
		string(hash): {
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_git_proxy",
			TicketID:    "gittkt_live",
			TokenHash:   hash,
			Status:      gitticket.StatusLive,
		},
	}
}

type fakeRepositoryAuthorizer struct {
	decisions map[string]RepositoryAuthDecision
	requests  chan<- RepositoryAuthRequest
}

func (a fakeRepositoryAuthorizer) AuthorizeRepository(_ context.Context, request RepositoryAuthRequest) (RepositoryAuthDecision, error) {
	if a.requests != nil {
		a.requests <- request
	}
	if decision, ok := a.decisions[request.Owner+"/"+request.Repo]; ok {
		return decision, nil
	}
	return RepositoryAuthDecision{Decision: DecisionAnonymous}, nil
}

type repositoryAuthorizerFunc func(context.Context, RepositoryAuthRequest) (RepositoryAuthDecision, error)

func (f repositoryAuthorizerFunc) AuthorizeRepository(ctx context.Context, request RepositoryAuthRequest) (RepositoryAuthDecision, error) {
	return f(ctx, request)
}

func stringsReader(value string) io.Reader {
	return &stringReader{value: value}
}

func processMemoryBytes(t *testing.T) uint64 {
	t.Helper()
	if rss, err := linuxRSSBytes(); err == nil {
		return rss
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Alloc
}

func linuxRSSBytes() (uint64, error) {
	body, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(body))
	if len(fields) < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return pages * uint64(os.Getpagesize()), nil
}

func newDiscardResponseWriter() *discardResponseWriter {
	return &discardResponseWriter{header: http.Header{}}
}

type discardResponseWriter struct {
	header http.Header
	status int
	bytes  int64
}

func (w *discardResponseWriter) Header() http.Header {
	return w.header
}

func (w *discardResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *discardResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.bytes += int64(len(p))
	return len(p), nil
}

func (w *discardResponseWriter) Flush() {}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for index := range p {
		p[index] = 0
	}
	return len(p), nil
}

func waitForReady(t *testing.T, baseURL string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(baseURL + "/ready")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("git-proxy workload did not become ready")
}

func waitForNewConnectionsRefused(t *testing.T, baseURL string) {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(baseURL + "/ready")
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("new git-proxy connections were still accepted after shutdown began")
}

type unexpectedStatusError struct {
	status int
}

func (e *unexpectedStatusError) Error() string {
	return "unexpected status " + strconv.Itoa(e.status)
}

func assertExactAccessLogFields(t *testing.T, record map[string]any) {
	t.Helper()
	want := map[string]struct{}{
		"time":                   {},
		"level":                  {},
		"msg":                    {},
		"service.name":           {},
		"service.version":        {},
		"deployment.environment": {},
		"request.id":             {},
		"workspace.id":           {},
		"session.id":             {},
		"operation":              {},
		"event.kind":             {},
		"component":              {},
		"ticket_id":              {},
		"owner_repo":             {},
		"decision":               {},
		"upstream_status":        {},
		"bytes_in":               {},
		"bytes_out":              {},
		"duration.ms":            {},
	}
	if len(record) != len(want) {
		t.Fatalf("access log field count = %d; want %d: %#v", len(record), len(want), record)
	}
	for key := range want {
		if _, ok := record[key]; !ok {
			t.Fatalf("access log missing field %q: %#v", key, record)
		}
	}
	for key := range record {
		if _, ok := want[key]; !ok {
			t.Fatalf("access log has extra field %q: %#v", key, record)
		}
	}
}

func assertLogString(t *testing.T, record map[string]any, key string, want string) {
	t.Helper()
	if got, ok := record[key].(string); !ok || got != want {
		t.Fatalf("%s = %v; want %q", key, record[key], want)
	}
}

func assertLogNumber(t *testing.T, record map[string]any, key string, want int) {
	t.Helper()
	got, ok := record[key].(float64)
	if !ok || int(got) != want {
		t.Fatalf("%s = %v; want %d", key, record[key], want)
	}
}

type stringReader struct {
	value string
	index int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.index >= len(r.value) {
		return 0, io.EOF
	}
	n := copy(p, r.value[r.index:])
	r.index += n
	return n, nil
}

type slowChunkReader struct {
	chunks []string
	delay  time.Duration
	index  int
}

func (r *slowChunkReader) Read(p []byte) (int, error) {
	if r.index >= len(r.chunks) {
		return 0, io.EOF
	}
	if r.index > 0 {
		time.Sleep(r.delay)
	}
	n := copy(p, r.chunks[r.index])
	r.index++
	return n, nil
}
