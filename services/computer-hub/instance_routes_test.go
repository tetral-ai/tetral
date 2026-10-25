package computerhub

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// hubFixture serves the instance routes over a test database as the
// computer_hub role, with an API key principal of one Workspace.
type hubFixture struct {
	t         *testing.T
	ctx       context.Context
	admin     *sql.DB
	ws        workspace.ID
	url       string
	signer    *auth.InternalPrincipalSigner
	principal auth.Principal
	logs      *lockedBuffer
	events    *InstanceEvents
	// recordStatements counts the statements that name the idempotency
	// records table.
	recordStatements atomic.Int64
	// countPause, when set, holds every credential limit count that long
	// after it returns, so concurrent issuances overlap between their count
	// and their insert.
	countPause atomic.Int64
	// errorBodies holds every non-2xx body the test received.
	errorBodies []string
	mu          sync.Mutex
}

func newHubFixture(t *testing.T, maxLifetime time.Duration) *hubFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	f := &hubFixture{t: t, ctx: ctx, admin: admin, ws: workspace.DefaultID, logs: &lockedBuffer{}}
	if _, err := workspace.NewSeeder(admin).Seed(ctx, f.ws, "hub"); err != nil {
		t.Fatal(err)
	}
	hubDB := storagetest.OpenWorkloadDB(t, admin, "computer_hub").OpenWorkload(t, "computer_hub", hubTracer{f})
	hubDB.SetMaxOpenConns(16)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if f.signer, err = auth.NewInternalPrincipalSigner(private); err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifier(public)
	if err != nil {
		t.Fatal(err)
	}
	logger := workload.NewProcessLogger(f.logs, ServiceName, "test", "test", workload.DefaultDiagnosticConfig()).Logger
	client := dbconnect.NewClientForTesting(hubDB)
	f.events = NewInstanceEvents(client, "0.0.0", logger)
	go func() { _ = f.events.Run(ctx) }()
	router := NewRouter(verifier, WithLogger(logger), WithInstanceRoutes(InstanceRoutes{
		Database:              client,
		MaxCredentialLifetime: maxLifetime,
		PageTokenKey:          bytes.Repeat([]byte{7}, 32),
		Events:                f.events,
	}))
	server := httptest.NewServer(BuildHTTPHandler(workload.NewReadiness(), router))
	t.Cleanup(server.Close)
	f.url = server.URL
	f.principal = auth.IndependentKeyPrincipal(workspace.Workspace{ID: f.ws}, "key_hub_test")
	return f
}

type hubTracer struct{ f *hubFixture }

type limitCountKey struct{}

func (h hubTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "instance_idempotency_records") {
		h.f.recordStatements.Add(1)
	}
	if strings.Contains(data.SQL, "SELECT count(*) FROM instance_credentials") {
		return context.WithValue(ctx, limitCountKey{}, true)
	}
	return ctx
}

func (h hubTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(limitCountKey{}) != nil {
		time.Sleep(time.Duration(h.f.countPause.Load()))
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// do sends one request with a freshly minted principal. key, when not nil,
// is the Idempotency-Key header's values.
func (f *hubFixture) do(method, target string, key []string, body string) (int, []byte) {
	f.t.Helper()
	path := target
	if parsed, err := url.Parse(target); err == nil {
		path = parsed.Path
	}
	token, err := f.signer.Mint(f.principal, method, path, "req_hub", time.Minute)
	if err != nil {
		f.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(f.ctx, method, f.url+target, strings.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header.Set("X-Tetral-Internal-Principal", token)
	for _, value := range key {
		request.Header.Add("Idempotency-Key", value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	if response.StatusCode >= 300 {
		f.mu.Lock()
		f.errorBodies = append(f.errorBodies, string(data))
		f.mu.Unlock()
	}
	return response.StatusCode, data
}

type createdInstance struct {
	Instance   map[string]any `json:"instance"`
	Credential map[string]any `json:"credential"`
}

func (f *hubFixture) createInstance(key, body string) (int, createdInstance) {
	f.t.Helper()
	status, data := f.do(http.MethodPost, "/v1/instances", []string{key}, body)
	var created createdInstance
	if status == http.StatusCreated {
		decodeJSON(f.t, data, &created)
	}
	return status, created
}

func (f *hubFixture) issue(instanceID, key, body string) (int, map[string]any) {
	f.t.Helper()
	status, data := f.do(http.MethodPost, "/v1/instances/"+instanceID+"/credentials", []string{key}, body)
	var credential map[string]any
	if status == http.StatusCreated {
		decodeJSON(f.t, data, &credential)
	}
	return status, credential
}

func (f *hubFixture) count(query string, args ...any) int {
	f.t.Helper()
	var n int
	if err := f.admin.QueryRowContext(f.ctx, query, args...).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

// rowCounts counts the rows of the three instance tables.
func (f *hubFixture) rowCounts() [3]int {
	return [3]int{
		f.count(`SELECT count(*) FROM instances`),
		f.count(`SELECT count(*) FROM instance_credentials`),
		f.count(`SELECT count(*) FROM instance_idempotency_records`),
	}
}

func decodeJSON(t *testing.T, data []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
}

func requireAPIError(t *testing.T, status int, body []byte, wantStatus int, wantType, wantInMessage string) {
	t.Helper()
	var envelope struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	decodeJSON(t, body, &envelope)
	if status != wantStatus || envelope.Type != "error" || envelope.Error.Type != wantType || !strings.Contains(envelope.Error.Message, wantInMessage) {
		t.Fatalf("status=%d body=%s; want %d %s naming %q", status, body, wantStatus, wantType, wantInMessage)
	}
}

// TestInstanceCreationReplaysWithoutASecondInstance proves that a retried
// creation and a retried response-delivery issuance return the committed
// identities without a key, and that a changed request with a used key is a
// conflict.
func TestInstanceCreationReplaysWithoutASecondInstance(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	status, first := f.createInstance("create-1", `{"name":"laptop","credential":{}}`)
	if status != http.StatusCreated || first.Credential["connection_key"] == nil {
		t.Fatalf("create = %d %+v", status, first)
	}
	instanceID, credentialID := first.Instance["id"].(string), first.Credential["id"].(string)
	// The first response is lost; the retry has the same key and body.
	status, replay := f.createInstance("create-1", `{ "credential": {}, "name": "laptop" }`)
	if status != http.StatusCreated || replay.Instance["id"] != instanceID || replay.Credential["id"] != credentialID || replay.Credential["connection_key"] != nil {
		t.Fatalf("replay = %d %+v; want the same IDs and connection_key null", status, replay)
	}
	if n := f.count(`SELECT count(*) FROM instances`); n != 1 {
		t.Fatalf("instances = %d after the replay; want 1", n)
	}
	if status, _ := f.do(http.MethodDelete, "/v1/instances/"+instanceID+"/credentials/"+credentialID, nil, ""); status != http.StatusNoContent {
		t.Fatalf("revoke = %d", status)
	}
	status, replacement := f.issue(instanceID, "replace-1", `{"delivery":"response"}`)
	if status != http.StatusCreated || replacement["connection_key"] == nil || replacement["id"] == credentialID {
		t.Fatalf("replacement = %d %+v", status, replacement)
	}
	if n := f.count(`SELECT count(*) FROM instances`); n != 1 {
		t.Fatalf("instances = %d after the replacement; want 1", n)
	}
	status, body := f.do(http.MethodPost, "/v1/instances", []string{"create-1"}, `{"name":"desktop","credential":{}}`)
	requireAPIError(t, status, body, http.StatusConflict, "idempotency_conflict_error", "Idempotency-Key")
	status, replayed := f.issue(instanceID, "replace-1", `{"delivery":"response"}`)
	if status != http.StatusCreated || replayed["id"] != replacement["id"] || replayed["connection_key"] != nil {
		t.Fatalf("issuance replay = %d %+v; want credential %v with connection_key null", status, replayed, replacement["id"])
	}
}

// TestInstanceCreationBoundsNamesAndIdempotencyKeys proves the name and
// Idempotency-Key bounds, and that a refused key is refused before any
// idempotency record is read.
func TestInstanceCreationBoundsNamesAndIdempotencyKeys(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	for _, tc := range []struct {
		name   string
		key    []string
		body   string
		status int
		names  string
	}{
		{"name of 256 code points", []string{"name-256"}, `{"name":"` + strings.Repeat("界", 256) + `","credential":{}}`, http.StatusCreated, ""},
		{"name of 257 code points", []string{"name-257"}, `{"name":"` + strings.Repeat("界", 257) + `","credential":{}}`, http.StatusBadRequest, "name"},
		{"empty name", []string{"name-empty"}, `{"name":"","credential":{}}`, http.StatusBadRequest, "name"},
		{"name a number", []string{"name-number"}, `{"name":5,"credential":{}}`, http.StatusBadRequest, "name"},
		{"key sent twice", []string{"key-a", "key-b"}, `{"name":"laptop","credential":{}}`, http.StatusBadRequest, "Idempotency-Key"},
		{"key of only spaces", []string{"   "}, `{"name":"laptop","credential":{}}`, http.StatusBadRequest, "Idempotency-Key"},
		{"key of 256 bytes", []string{strings.Repeat("k", 256)}, `{"name":"laptop","credential":{}}`, http.StatusBadRequest, "Idempotency-Key"},
		{"key of 255 bytes", []string{strings.Repeat("k", 255)}, `{"name":"laptop","credential":{}}`, http.StatusCreated, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, statements := f.rowCounts(), f.recordStatements.Load()
			status, body := f.do(http.MethodPost, "/v1/instances", tc.key, tc.body)
			if tc.status == http.StatusCreated {
				if status != http.StatusCreated {
					t.Fatalf("status=%d body=%s; want 201", status, body)
				}
				return
			}
			requireAPIError(t, status, body, tc.status, "invalid_request_error", tc.names)
			if after := f.rowCounts(); after != before {
				t.Fatalf("rows %v -> %v; a refused request wrote", before, after)
			}
			if tc.names == "Idempotency-Key" && f.recordStatements.Load() != statements {
				t.Fatal("a refused Idempotency-Key read an idempotency record")
			}
		})
	}
}

// TestCredentialLimitHoldsUnderConcurrentIssuance races ten response-delivery
// issuances on an instance that holds one active credential. Each limit count
// pauses after it returns, so without the instance lock every issuance would
// count before any of them inserts.
func TestCredentialLimitHoldsUnderConcurrentIssuance(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, created := f.createInstance("limit-create", `{"name":"laptop","credential":{}}`)
	instanceID := created.Instance["id"].(string)
	f.countPause.Store(int64(100 * time.Millisecond))
	statuses := make([]int, 10)
	bodies := make([][]byte, 10)
	var wg sync.WaitGroup
	for i := range statuses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses[i], bodies[i] = f.do(http.MethodPost, "/v1/instances/"+instanceID+"/credentials", []string{fmt.Sprintf("limit-%d", i)}, `{"delivery":"response"}`)
		}()
	}
	wg.Wait()
	created201 := 0
	for i, status := range statuses {
		if status == http.StatusCreated {
			created201++
			continue
		}
		requireAPIError(t, status, bodies[i], http.StatusConflict, "credential_limit_error", "")
	}
	if created201 != 1 {
		t.Fatalf("statuses %v; want exactly one 201", statuses)
	}
	if n := f.count(`SELECT count(*) FROM instance_credentials WHERE instance_id=$1 AND revoked_at IS NULL AND expires_at>clock_timestamp()`, instanceID); n != instance.CredentialLimit {
		t.Fatalf("unrevoked unexpired credentials = %d; want %d", n, instance.CredentialLimit)
	}
}

// TestNoConnectionKeyOutsideItsIssuanceResponse collects every key the hub
// issued and searches every other response, the idempotency rows, and the
// hub's log and audit output for it.
func TestNoConnectionKeyOutsideItsIssuanceResponse(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	var keys []string
	_, created := f.createInstance("secret-create", `{"name":"laptop","credential":{}}`)
	instanceID := created.Instance["id"].(string)
	keys = append(keys, created.Credential["connection_key"].(string))
	_, issued := f.issue(instanceID, "secret-issue", `{"delivery":"response"}`)
	keys = append(keys, issued["connection_key"].(string))
	var searched []string
	read := func(method, target string, key []string, body string) {
		_, data := f.do(method, target, key, body)
		searched = append(searched, string(data))
	}
	read(http.MethodPost, "/v1/instances", []string{"secret-create"}, `{"name":"laptop","credential":{}}`)
	read(http.MethodPost, "/v1/instances/"+instanceID+"/credentials", []string{"secret-issue"}, `{"delivery":"response"}`)
	read(http.MethodPost, "/v1/instances", []string{"secret-create"}, `{"name":"other","credential":{}}`)
	read(http.MethodPost, "/v1/instances/"+instanceID+"/credentials", []string{"secret-third"}, `{"delivery":"response"}`)
	read(http.MethodPost, "/v1/instances/"+instanceID+"/credentials", []string{"secret-bad"}, `{"delivery":"mail"}`)
	read(http.MethodGet, "/v1/instances/"+instanceID, nil, "")
	read(http.MethodGet, "/v1/instances", nil, "")
	read(http.MethodGet, "/v1/instances/"+instanceID+"/credentials", nil, "")
	read(http.MethodDelete, "/v1/instances/"+instanceID+"/credentials/"+issued["id"].(string), nil, "")
	read(http.MethodDelete, "/v1/instances/"+instanceID, nil, "")
	read(http.MethodPost, "/v1/instances/"+instanceID+"/credentials", []string{"secret-gone"}, `{"delivery":"response"}`)
	rows, err := f.admin.QueryContext(f.ctx, `SELECT row_to_json(r)::text FROM instance_idempotency_records r`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		searched = append(searched, row)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	logs := f.logs.String()
	audits := strings.Count(logs, `"event":"`+instance.AuditEvent+`"`)
	if audits != 4 {
		t.Fatalf("audit records = %d; want 2 issues and 2 revocations", audits)
	}
	searched = append(searched, logs)
	searched = append(searched, f.errorBodies...)
	if len(f.errorBodies) < 3 {
		t.Fatalf("error bodies = %d; want the conflict, limit, invalid and unavailable answers", len(f.errorBodies))
	}
	for _, key := range keys {
		secret := strings.TrimPrefix(key, instance.KeyPrefix)
		for _, text := range searched {
			// row_to_json renders a bytea column as hexadecimal.
			if strings.Contains(text, secret) || strings.Contains(text, hex.EncodeToString([]byte(secret))) {
				t.Fatalf("a key appears outside its issuance response: %.200s", text)
			}
		}
	}
}

// TestInstanceAndCredentialReadsShowStatesWithoutSecrets reads an active
// instance, a deregistered tombstone, and active, revoked and expired
// credentials.
func TestInstanceAndCredentialReadsShowStatesWithoutSecrets(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, live := f.createInstance("read-live", `{"name":"laptop","credential":{}}`)
	liveID := live.Instance["id"].(string)
	activeID := live.Credential["id"].(string)
	_, revoked := f.issue(liveID, "read-revoked", `{"delivery":"response"}`)
	revokedID := revoked["id"].(string)
	f.do(http.MethodDelete, "/v1/instances/"+liveID+"/credentials/"+revokedID, nil, "")
	_, expired := f.issue(liveID, "read-expired", `{"delivery":"response"}`)
	expiredID := expired["id"].(string)
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE instance_credentials SET issued_at=clock_timestamp()-interval '2 hours', expires_at=clock_timestamp()-interval '1 hour' WHERE id=$1`, expiredID); err != nil {
		t.Fatal(err)
	}
	_, gone := f.createInstance("read-gone", `{"name":"old laptop","credential":{}}`)
	goneID := gone.Instance["id"].(string)
	f.do(http.MethodDelete, "/v1/instances/"+goneID, nil, "")

	instanceFields := []string{"binding_count", "cli_update_available", "cli_version", "computer_count", "computer_id", "connection", "id", "kind", "last_credential_reuse", "name", "observed_at", "registration_state", "unused"}
	credentialFields := []string{"expires_at", "id", "in_use", "instance_id", "issued_at", "revoked_at", "state"}
	for _, tc := range []struct{ id, state string }{{liveID, "active"}, {goneID, "deregistered"}} {
		status, data := f.do(http.MethodGet, "/v1/instances/"+tc.id, nil, "")
		var object map[string]any
		decodeJSON(t, data, &object)
		if status != http.StatusOK || object["registration_state"] != tc.state || object["kind"] != "user" || !slices.Equal(sortedKeys(object), instanceFields) {
			t.Fatalf("instance %s = %d %s; want %s with exactly %v", tc.id, status, data, tc.state, instanceFields)
		}
	}
	status, data := f.do(http.MethodGet, "/v1/instances/"+liveID+"/credentials", nil, "")
	var page struct {
		Data     []map[string]any `json:"data"`
		NextPage *string          `json:"next_page"`
	}
	decodeJSON(t, data, &page)
	states := map[string]string{}
	for _, object := range page.Data {
		if !slices.Equal(sortedKeys(object), credentialFields) {
			t.Fatalf("credential fields %v; want exactly %v", sortedKeys(object), credentialFields)
		}
		states[object["id"].(string)] = object["state"].(string)
	}
	want := map[string]string{activeID: "active", revokedID: "revoked", expiredID: "expired"}
	if status != http.StatusOK || len(states) != 3 || states[activeID] != want[activeID] || states[revokedID] != want[revokedID] || states[expiredID] != want[expiredID] || page.NextPage != nil {
		t.Fatalf("credentials = %d %s; want states %v", status, data, want)
	}
	if status, body := f.do(http.MethodGet, "/v1/instances/inst_missing", nil, ""); status != http.StatusNotFound {
		t.Fatalf("missing instance = %d %s", status, body)
	}
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// TestRevocationAndDeregistrationRepeatWithoutChange proves that repeated
// revocations and deregistrations answer 204 and move no timestamp.
func TestRevocationAndDeregistrationRepeatWithoutChange(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, created := f.createInstance("revoke-create", `{"name":"laptop","credential":{}}`)
	instanceID, credentialID := created.Instance["id"].(string), created.Credential["id"].(string)
	revokedAt := func(id string) sql.NullTime {
		var at sql.NullTime
		if err := f.admin.QueryRowContext(f.ctx, `SELECT revoked_at FROM instance_credentials WHERE id=$1`, id).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	revoke := func(id string) {
		t.Helper()
		if status, body := f.do(http.MethodDelete, "/v1/instances/"+instanceID+"/credentials/"+id, nil, ""); status != http.StatusNoContent {
			t.Fatalf("revoke %s = %d %s", id, status, body)
		}
	}
	revoke(credentialID)
	first := revokedAt(credentialID)
	revoke(credentialID)
	if again := revokedAt(credentialID); !first.Valid || again != first {
		t.Fatalf("revoked_at %v then %v; want one unchanged time", first, again)
	}
	_, expired := f.issue(instanceID, "revoke-expired", `{"delivery":"response"}`)
	expiredID := expired["id"].(string)
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE instance_credentials SET issued_at=clock_timestamp()-interval '2 hours', expires_at=clock_timestamp()-interval '1 hour' WHERE id=$1`, expiredID); err != nil {
		t.Fatal(err)
	}
	revoke(expiredID)
	if at := revokedAt(expiredID); at.Valid {
		t.Fatalf("revoking an expired credential set revoked_at %v", at)
	}

	_, other := f.createInstance("deregister-create", `{"name":"desktop","credential":{}}`)
	otherID := other.Instance["id"].(string)
	_, second := f.issue(otherID, "deregister-second", `{"delivery":"response"}`)
	deregisteredAt := func() sql.NullTime {
		var at sql.NullTime
		if err := f.admin.QueryRowContext(f.ctx, `SELECT deregistered_at FROM instances WHERE id=$1`, otherID).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	var deregistrations []sql.NullTime
	for range 2 {
		if status, body := f.do(http.MethodDelete, "/v1/instances/"+otherID, nil, ""); status != http.StatusNoContent {
			t.Fatalf("deregister = %d %s", status, body)
		}
		deregistrations = append(deregistrations, deregisteredAt())
	}
	if !deregistrations[0].Valid || deregistrations[1] != deregistrations[0] {
		t.Fatalf("deregistered_at %v then %v; want one unchanged time", deregistrations[0], deregistrations[1])
	}
	for _, id := range []string{other.Credential["id"].(string), second["id"].(string)} {
		var cause sql.NullString
		if err := f.admin.QueryRowContext(f.ctx, `SELECT revocation_cause FROM instance_credentials WHERE id=$1 AND state='revoked'`, id).Scan(&cause); err != nil || cause.String != "deregistration" {
			t.Fatalf("credential %s cause %v, %v; want deregistration", id, cause, err)
		}
	}
}

// TestCredentialExpiryIsBoundedAndResolvedOnce runs with a one-hour maximum
// lifetime.
func TestCredentialExpiryIsBoundedAndResolvedOnce(t *testing.T) {
	f := newHubFixture(t, time.Hour)
	within := time.Now().UTC().Add(59 * time.Minute).Truncate(time.Second)
	status, created := f.createInstance("expiry-59", `{"name":"laptop","credential":{"expires_at":"`+within.Format(time.RFC3339)+`"}}`)
	if status != http.StatusCreated || created.Credential["expires_at"] != within.Format(time.RFC3339Nano) {
		t.Fatalf("59 minutes = %d %+v; want expires_at %s", status, created.Credential, within.Format(time.RFC3339Nano))
	}
	before := f.rowCounts()
	beyond := time.Now().UTC().Add(61 * time.Minute).Format(time.RFC3339)
	status, body := f.do(http.MethodPost, "/v1/instances", []string{"expiry-61"}, `{"name":"laptop","credential":{"expires_at":"`+beyond+`"}}`)
	requireAPIError(t, status, body, http.StatusBadRequest, "invalid_request_error", "expires_at")
	if after := f.rowCounts(); after != before {
		t.Fatalf("rows %v -> %v; the refused expiry wrote", before, after)
	}
	status, omitted := f.createInstance("expiry-omitted", `{"name":"laptop","credential":{}}`)
	issuedAt, _ := time.Parse(time.RFC3339Nano, omitted.Credential["issued_at"].(string))
	expiresAt, _ := time.Parse(time.RFC3339Nano, omitted.Credential["expires_at"].(string))
	if status != http.StatusCreated || expiresAt.Sub(issuedAt) != time.Hour {
		t.Fatalf("omitted = %d issued %v expires %v; want one hour later", status, issuedAt, expiresAt)
	}
	time.Sleep(1100 * time.Millisecond)
	_, replay := f.createInstance("expiry-omitted", `{"name":"laptop","credential":{}}`)
	if replay.Credential["expires_at"] != omitted.Credential["expires_at"] {
		t.Fatalf("replay expires_at %v; want %v", replay.Credential["expires_at"], omitted.Credential["expires_at"])
	}
}

// TestInstanceListPagesInCreationOrder lists 101 instances.
func TestInstanceListPagesInCreationOrder(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	var created []string
	if err := dbconnect.NewClientForTesting(f.admin).WithWorkspaceTx(f.ctx, string(f.ws), "computer_hub.test_instances", func(tx *dbconnect.Tx) error {
		for i := range 101 {
			row, err := instance.CreateUserInstance(f.ctx, tx, f.ws, fmt.Sprintf("machine %d", i))
			if err != nil {
				return err
			}
			created = append(created, row.ID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	type page struct {
		Data     []map[string]any `json:"data"`
		NextPage *string          `json:"next_page"`
	}
	list := func(query string) (int, page, []byte) {
		status, data := f.do(http.MethodGet, "/v1/instances"+query, nil, "")
		var p page
		if status == http.StatusOK {
			decodeJSON(t, data, &p)
		}
		return status, p, data
	}
	for _, tc := range []struct {
		query string
		size  int
	}{{"", 20}, {"?limit=100", 100}, {"?limit=101", 100}} {
		if status, p, _ := list(tc.query); status != http.StatusOK || len(p.Data) != tc.size {
			t.Fatalf("list%s = %d with %d items; want %d", tc.query, status, len(p.Data), tc.size)
		}
	}
	for _, query := range []string{"?limit=0", "?limit=1.5"} {
		status, _, body := list(query)
		requireAPIError(t, status, body, http.StatusBadRequest, "invalid_request_error", "limit")
	}
	var walked []string
	query := ""
	var token string
	for pages := 0; ; pages++ {
		status, p, body := list(query)
		if status != http.StatusOK || pages > 6 {
			t.Fatalf("page %d = %d %s", pages, status, body)
		}
		for _, object := range p.Data {
			walked = append(walked, object["id"].(string))
		}
		if p.NextPage == nil {
			break
		}
		token = *p.NextPage
		query = "?page=" + url.QueryEscape(token)
	}
	if !slices.Equal(walked, created) {
		t.Fatalf("walk returned %d instances; want all %d once in creation order", len(walked), len(created))
	}
	status, body := f.do(http.MethodGet, "/v1/instances/"+created[0]+"/credentials?page="+url.QueryEscape(token), nil, "")
	requireAPIError(t, status, body, http.StatusBadRequest, "invalid_request_error", "page")
}
