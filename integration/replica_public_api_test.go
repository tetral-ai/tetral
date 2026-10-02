package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	authservice "github.com/tetral-ai/tetral/services/auth"
)

func TestPostgreSQLReplicaPublicControlPlane(t *testing.T) {
	if os.Getenv(replicaPublicAPIChildDirEnv) != "" {
		runReplicaPublicAPIChild(t)
		return
	}
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	key, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(key)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := strings.Repeat("b", auth.MinBootstrapKeyBytes)
	// Exercise the installed Auth authority, including its SELECT-only access to
	// workspaces, rather than the broader shared test role used by the API fixture.
	authDB := storagetest.OpenWorkloadDB(t, admin, "auth").DB
	var canReadWorkspace, canUpdateWorkspace bool
	if err := authDB.QueryRowContext(ctx, `SELECT has_table_privilege(current_user,'public.workspaces','SELECT'),has_table_privilege(current_user,'public.workspaces','UPDATE')`).Scan(&canReadWorkspace, &canUpdateWorkspace); err != nil || !canReadWorkspace || canUpdateWorkspace {
		t.Fatalf("installed Auth workspace privileges: read=%t update=%t err=%v", canReadWorkspace, canUpdateWorkspace, err)
	}
	// Concurrent identical bootstrap starts must converge on one authority.
	authRouters := make([]http.Handler, 2)
	var starts sync.WaitGroup
	errs := make(chan error, 2)
	bootstrapBarrier := &replicaBootstrapBarrier{t: t, ready: make(chan struct{})}
	for i := range authRouters {
		pool := storagetest.OpenRuntimeRoleDBWithTracer(t, authDB, bootstrapBarrier)
		starts.Add(1)
		go func(i int) {
			defer starts.Done()
			router, err := authservice.BuildRouter(ctx, authservice.RouterBuildConfig{RawDatabase: pool, Config: authservice.Config{BootstrapAPIKey: bootstrap, BootstrapWorkspaceID: workspace.DefaultID, InternalPrincipalPrivateKeyB64: key, InternalPrincipalTTL: 2 * time.Second}})
			authRouters[i] = router
			errs <- err
		}(i)
	}
	starts.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var bootstrapCount int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM api_keys WHERE workspace_id='default' AND key_kind='bootstrap' AND revoked_at IS NULL`).Scan(&bootstrapCount); err != nil || bootstrapCount != 1 {
		t.Fatalf("bootstrap authorities=%d/%v", bootstrapCount, err)
	}
	authServers := make([]*httptest.Server, 2)
	apiServers := make([]*httptest.Server, 2)
	var authCounts, apiCounts [2]atomic.Int64
	var apiChildren [2]*replicaPublicAPIChild
	for i := range authServers {
		index := i
		authServers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authCounts[index].Add(1)
			authRouters[index].ServeHTTP(w, r)
		}))
		t.Cleanup(authServers[i].Close)
		apiChildren[i] = startReplicaPublicAPIChild(ctx, t, runtimeDB, signer.PublicKeyBase64())
		target, err := url.Parse(apiChildren[i].URL)
		if err != nil {
			t.Fatal(err)
		}
		proxy := httputil.NewSingleHostReverseProxy(target)
		apiServers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { apiCounts[index].Add(1); proxy.ServeHTTP(w, r) }))
		t.Cleanup(apiServers[i].Close)
	}
	var edges []*httptest.Server
	for _, pair := range [][2]int{{0, 0}, {1, 1}, {0, 1}, {1, 0}} {
		handler, err := newSDKIntegrationEdge(authServers[pair[0]].URL, apiServers[pair[1]].URL, apiServers[pair[1]].URL)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		edges = append(edges, server)
	}
	standard := mintSDKIntegrationAPIKey(t, edges[0].URL, bootstrap)
	if _, err := admin.ExecContext(ctx, `INSERT INTO workspaces(id,type,name,created_at) VALUES('replica_other','workspace','Other',now())`); err != nil {
		t.Fatal(err)
	}
	otherKey, err := auth.NewAPIKeyStore(admin).CreateForWorkspace(ctx, "replica_other", "replica-other")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "bun", "run", "testdata/replica-public-client.ts", edges[0].URL, edges[1].URL)
	command.Env = append(os.Environ(), "REPLICA_API_KEY="+standard)
	output, err := command.CombinedOutput()
	if err != nil {
		safe := strings.ReplaceAll(string(output), standard, "<redacted>")
		t.Fatalf("pinned SDK child: %v %s", err, safe)
	}
	var report struct {
		OK            bool   `json:"ok"`
		SessionID     string `json:"sessionId"`
		AgentID       string `json:"agentId"`
		EnvironmentID string `json:"environmentId"`
	}
	if err := json.Unmarshal(output, &report); err != nil || !report.OK || report.SessionID == "" {
		t.Fatalf("missing SDK report: %s/%v", output, err)
	}
	sessionPath := "/v1/sessions/" + report.SessionID + "?beta=true"
	request := func(base, method, path, key, body string, headers map[string]string) (int, []byte) {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("X-Api-Key", key)
		r.Header.Set("Content-Type", "application/json")
		for name, value := range headers {
			r.Header.Set(name, value)
		}
		started := time.Now()
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			replicaRecordCompletion(t, "public_authentication_api", method, base, "transport_error", started)
			t.Fatal(err)
		}
		defer func() { _ = response.Body.Close() }()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		replicaRecordCompletion(t, "public_authentication_api", method, base, http.StatusText(response.StatusCode), started)
		return response.StatusCode, raw
	}
	for i, edge := range edges {
		code, raw := request(edge.URL, "GET", sessionPath, standard, "", map[string]string{"X-Tetral-Internal-Principal": "forged", "X-Tetral-Workspace-Id": "replica_other"})
		if code != 200 || !bytes.Contains(raw, []byte(report.SessionID)) {
			t.Fatalf("Auth/API pair%d rejected correct scope: %d %s", i, code, raw)
		}
	}
	for _, edge := range edges[:2] {
		for _, credential := range []string{"", "invalid-fixture", otherKey.APIKey} {
			code, raw := request(edge.URL, "GET", sessionPath, credential, "", map[string]string{"X-Tetral-Internal-Principal": "forged"})
			want := 401
			if credential == otherKey.APIKey {
				want = 404
			}
			if code != want || bytes.Contains(raw, []byte(report.SessionID)) {
				t.Fatalf("cross-scope/forged credentials status=%d want%d body=%s", code, want, raw)
			}
		}
	}
	// Auth tokens are issued by each actual replica and consumed by both APIs.
	for authIndex, server := range authServers {
		r, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/internal/auth/authorize", nil)
		if err != nil {
			t.Fatal(err)
		}
		for name, value := range map[string]string{"X-Api-Key": standard, "X-Original-Method": "GET", "X-Original-Path": strings.Split(sessionPath, "?")[0], "X-Request-Id": "replica-principal", "X-Forwarded-For": "127.0.0.1", "X-Tetral-Internal-Principal": "forged"} {
			r.Header.Set(name, value)
		}
		started := time.Now()
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
		replicaRecordCompletion(t, "public_authentication_api", "authorize", server.URL, http.StatusText(response.StatusCode), started)
		token := response.Header.Get("X-Tetral-Internal-Principal")
		if response.StatusCode != 200 || token == "" {
			t.Fatalf("Auth%d mint status%d", authIndex, response.StatusCode)
		}
		for apiIndex, apiServer := range apiServers {
			code, raw := request(apiServer.URL, "GET", sessionPath, "", "", map[string]string{"X-Tetral-Internal-Principal": token, "X-Tetral-Workspace-Id": "replica_other"})
			if code != 200 || !bytes.Contains(raw, []byte(report.SessionID)) {
				t.Fatalf("principal Auth%d→API%d: %d %s", authIndex, apiIndex, code, raw)
			}
			for _, invalid := range [][2]string{{"GET", "/v1/sessions/other?beta=true"}, {"POST", sessionPath}} {
				code, _ = request(apiServer.URL, invalid[0], invalid[1], "", "", map[string]string{"X-Tetral-Internal-Principal": token})
				if code != 401 {
					t.Fatalf("principal path/method mismatch=%d", code)
				}
			}
		}
		_, claims, err := signer.Verify(token, "GET", strings.Split(sessionPath, "?")[0])
		if err != nil {
			t.Fatal(err)
		}
		expiry, err := time.Parse(time.RFC3339, claims.ExpiresAt)
		if err != nil {
			t.Fatal(err)
		}
		timer := time.NewTimer(time.Until(expiry) + 20*time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		}
		for _, apiServer := range apiServers {
			code, _ := request(apiServer.URL, "GET", sessionPath, "", "", map[string]string{"X-Tetral-Internal-Principal": token})
			if code != 401 {
				t.Fatalf("expired principal accepted: %d", code)
			}
		}
	}
	for _, edge := range edges[:2] {
		code, raw := request(edge.URL, "POST", sessionPath, otherKey.APIKey, `{"agent":{"approval_mode":"ask_for_approval"}}`, nil)
		if code != 404 || bytes.Contains(raw, []byte(report.SessionID)) {
			t.Fatalf("cross-workspace write=%d %s", code, raw)
		}
	}
	code, raw := request(edges[0].URL, "GET", sessionPath, standard, "", nil)
	if code != 200 || !bytes.Contains(raw, []byte(`"approval_mode":"full_access"`)) {
		t.Fatalf("wrong workspace changed owner state: %d %s", code, raw)
	}
	eventPath := "/v1/sessions/" + report.SessionID + "/events?beta=true"
	body := `{"events":[{"type":"user.message","content":[{"type":"text","text":"replica-public-input"}]}]}`
	headers := map[string]string{"Idempotency-Key": "replica-public-input-once"}
	code, first := replicaPublicLoseResponse(ctx, t, edges[0].Config.Handler, eventPath, standard, body, headers)
	if code != 200 {
		t.Fatalf("append=%d %s", code, first)
	}
	code, replay := request(edges[1].URL, "POST", eventPath, standard, body, headers)
	if code != 200 || !bytes.Equal(first, replay) {
		t.Fatalf("append replay=%d %s first=%s", code, replay, first)
	}
	code, conflict := request(edges[1].URL, "POST", eventPath, standard, strings.ReplaceAll(body, "replica-public-input", "changed"), headers)
	if code != 409 {
		t.Fatalf("append conflict=%d %s", code, conflict)
	}
	var eventCount, inboxCount, queueCount int
	if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='user.message'),(SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1),(SELECT count(*) FROM queue_jobs WHERE causal_session_id=$1 AND kind='runtime_input')`, report.SessionID).Scan(&eventCount, &inboxCount, &queueCount); err != nil || eventCount != 1 || inboxCount != 1 || queueCount != 1 {
		t.Fatalf("event/inbox/queue effects=%d/%d/%d %v", eventCount, inboxCount, queueCount, err)
	}
	// An ordinary create has no replay contract. Preserve its observed committed
	// identity and reconcile by read, while asserting only one row was created.
	var sessionsBefore int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM sessions WHERE workspace_id='default'`).Scan(&sessionsBefore); err != nil {
		t.Fatal(err)
	}
	ordinaryBody, _ := json.Marshal(map[string]any{"agent": report.AgentID, "environment_id": report.EnvironmentID, "vault_ids": []string{}})
	ordinaryCode, ordinaryRaw := replicaPublicLoseResponse(ctx, t, edges[0].Config.Handler, "/v1/sessions?beta=true", standard, string(ordinaryBody), nil)
	var ordinary struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(ordinaryRaw, &ordinary); err != nil || ordinaryCode != 200 || ordinary.ID == "" {
		t.Fatalf("ordinary write observed result=%d %s/%v", ordinaryCode, ordinaryRaw, err)
	}
	code, raw = request(edges[1].URL, "GET", "/v1/sessions/"+ordinary.ID+"?beta=true", standard, "", nil)
	if code != 200 || !bytes.Contains(raw, []byte(ordinary.ID)) {
		t.Fatalf("ordinary outcome reconciliation=%d %s", code, raw)
	}
	var sessionsAfter int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM sessions WHERE workspace_id='default'`).Scan(&sessionsAfter); err != nil || sessionsAfter != sessionsBefore+1 {
		t.Fatalf("ordinary replay effects %d→%d/%v", sessionsBefore, sessionsAfter, err)
	}

	// Hold a read already inside B's persistence boundary while A is killed
	// below. B must complete that admitted request and accept new requests.
	reached, releaseRead := apiChildren[1].arm(t, "from sessions")
	type asyncResult struct {
		status int
		body   []byte
		err    error
	}
	admitted := make(chan asyncResult, 1)
	go func() {
		r, _ := http.NewRequestWithContext(ctx, "GET", edges[1].URL+sessionPath, nil)
		r.Header.Set("X-Api-Key", standard)
		started := time.Now()
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			replicaRecordCompletion(t, "public_authentication_api", "GET", edges[1].URL, "transport_error", started)
			admitted <- asyncResult{err: err}
			return
		}
		defer func() { _ = response.Body.Close() }()
		raw, err := io.ReadAll(response.Body)
		replicaRecordCompletion(t, "public_authentication_api", "GET", edges[1].URL, http.StatusText(response.StatusCode), started)
		admitted <- asyncResult{status: response.StatusCode, body: raw, err: err}
	}()
	replicaPublicBarrierReached(t, reached)

	// Terminate A with its event/inbox/queue inserts still uncommitted. An
	// independent administrator sees neither provisional nor partial effects.
	reached, _ = apiChildren[0].arm(t, "insert into queue_jobs")
	uncommitted := make(chan asyncResult, 1)
	go func() {
		r, _ := http.NewRequestWithContext(ctx, "POST", edges[0].URL+eventPath, strings.NewReader(strings.ReplaceAll(body, "replica-public-input", "replica-terminated-input")))
		r.Header.Set("X-Api-Key", standard)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "replica-terminated-input")
		started := time.Now()
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			replicaRecordCompletion(t, "public_authentication_api", "POST", edges[0].URL, "transport_error", started)
			uncommitted <- asyncResult{err: err}
			return
		}
		defer func() { _ = response.Body.Close() }()
		raw, err := io.ReadAll(response.Body)
		replicaRecordCompletion(t, "public_authentication_api", "POST", edges[0].URL, http.StatusText(response.StatusCode), started)
		uncommitted <- asyncResult{status: response.StatusCode, body: raw, err: err}
	}()
	replicaPublicBarrierReached(t, reached)
	assertEffects := func() {
		t.Helper()
		var events, inbox, jobs int
		if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='user.message'),(SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1),(SELECT count(*) FROM queue_jobs WHERE causal_session_id=$1 AND kind='runtime_input')`, report.SessionID).Scan(&events, &inbox, &jobs); err != nil || events != 1 || inbox != 1 || jobs != 1 {
			t.Fatalf("partial append transaction=%d/%d/%d %v", events, inbox, jobs, err)
		}
	}
	assertEffects()
	databasePID := apiChildren[0].assertUncommittedTransaction(ctx, t, admin)
	apiChildren[0].killAndJoin(t)
	apiChildren[0].awaitDatabaseDisconnect(ctx, t, admin, databasePID)
	apiServers[0].CloseClientConnections()
	apiServers[0].Close()
	releaseRead()
	select {
	case result := <-admitted:
		if result.err != nil || result.status != 200 || !bytes.Contains(result.body, []byte(report.SessionID)) {
			t.Fatalf("survivor admitted request failed: %d/%v", result.status, result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case result := <-uncommitted:
		if result.err == nil && result.status < 400 {
			t.Fatalf("terminated write acknowledged=%d %s", result.status, result.body)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertEffects()
	// The surviving child owns its original listener, pool, and router. Its
	// new route must list/read state, replay the committed receipt, and accept
	// the identity whose uncommitted attempt died with A.
	survivorHandler, err := newSDKIntegrationEdge(authServers[1].URL, apiChildren[1].URL, apiChildren[1].URL)
	if err != nil {
		t.Fatal(err)
	}
	survivor := httptest.NewServer(survivorHandler)
	t.Cleanup(survivor.Close)
	code, raw = request(survivor.URL, "GET", sessionPath, standard, "", nil)
	if code != 200 || !bytes.Contains(raw, []byte(report.SessionID)) {
		t.Fatalf("survivor new read=%d %s", code, raw)
	}
	code, raw = request(survivor.URL, "GET", "/v1/sessions?beta=true", standard, "", nil)
	if code != 200 || !bytes.Contains(raw, []byte(report.SessionID)) {
		t.Fatalf("survivor list=%d %s", code, raw)
	}
	code, raw = request(survivor.URL, "GET", eventPath, standard, "", nil)
	if code != 200 || !bytes.Contains(raw, []byte("replica-public-input")) || bytes.Contains(raw, []byte("replica-terminated-input")) {
		t.Fatalf("survivor rollback/event read=%d %s", code, raw)
	}
	code, replay = request(survivor.URL, "POST", eventPath, standard, body, headers)
	if code != 200 || !bytes.Equal(first, replay) {
		t.Fatalf("survivor original receipt replay=%d %s", code, replay)
	}
	terminatedBody := strings.ReplaceAll(body, "replica-public-input", "replica-terminated-input")
	terminatedHeaders := map[string]string{"Idempotency-Key": "replica-terminated-input"}
	code, recovered := request(survivor.URL, "POST", eventPath, standard, terminatedBody, terminatedHeaders)
	if code != 200 {
		t.Fatalf("survivor new append=%d %s", code, recovered)
	}
	code, replay = request(survivor.URL, "POST", eventPath, standard, terminatedBody, terminatedHeaders)
	if code != 200 || !bytes.Equal(recovered, replay) {
		t.Fatalf("survivor recovered receipt replay=%d %s", code, replay)
	}
	if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='user.message'),(SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1),(SELECT count(*) FROM queue_jobs WHERE causal_session_id=$1 AND kind='runtime_input')`, report.SessionID).Scan(&eventCount, &inboxCount, &queueCount); err != nil || eventCount != 2 || inboxCount != 2 || queueCount != 2 {
		t.Fatalf("survivor append/replay effects=%d/%d/%d %v", eventCount, inboxCount, queueCount, err)
	}

	var keyID string
	if err := admin.QueryRowContext(ctx, `SELECT id FROM api_keys WHERE key_digest=$1`, auth.DigestAPIKey(standard)).Scan(&keyID); err != nil {
		t.Fatal(err)
	}
	code, raw = request(edges[0].URL, "DELETE", "/v1/api_keys/"+keyID, bootstrap, "", nil)
	if code != 204 {
		t.Fatalf("revoke=%d %s", code, raw)
	}
	for i, edge := range edges[:2] {
		code, _ := request(edge.URL, "GET", sessionPath, standard, "", nil)
		if code != 401 {
			t.Fatalf("Auth%d accepted freshly revoked credential: %d", i, code)
		}
	}
	for i := range authCounts {
		if authCounts[i].Load() == 0 || apiCounts[i].Load() == 0 {
			t.Fatalf("replica%d unexercised", i)
		}
	}
	t.Logf("pinned SDK create/read/list/approval patch,4 Auth/API routes; effects=%d/%d/%d; concurrent bootstrap1; principals both Auth→both API with expiry/method/path/query/scope fences; lost committed append replay exact; ordinary lost write reconciled without replay; survivor admitted+new read; actual API child SIGKILL/join at proven uncommitted transaction then existing survivor lists/reads/appends/replays; revocation both Auth; private replica requests Auth=%d/%d API=%d/%d", eventCount, inboxCount, queueCount, authCounts[0].Load(), authCounts[1].Load(), apiCounts[0].Load(), apiCounts[1].Load())
}

type replicaBootstrapBarrier struct {
	t       *testing.T
	mu      sync.Mutex
	arrived int
	ready   chan struct{}
}

func (b *replicaBootstrapBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "pg_advisory_xact_lock(hashtextextended('tetral.auth.bootstrap:'") {
		b.mu.Lock()
		b.arrived++
		if b.arrived == 2 {
			close(b.ready)
		}
		b.mu.Unlock()
		select {
		case <-b.ready:
		case <-ctx.Done():
		}
	}
	return ctx
}
func (b *replicaBootstrapBarrier) TraceQueryEnd(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	var pgErr *pgconn.PgError
	if errors.As(data.Err, &pgErr) {
		b.t.Logf("bootstrap database error code=%s constraint=%s", pgErr.Code, pgErr.ConstraintName)
	}
}
