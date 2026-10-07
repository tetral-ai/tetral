package integration

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/testinfra"
)

func TestPostgreSQLProtectedStoreConnections(t *testing.T) {
	p := transporttest.NewPostgreSQL(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	ca := filepath.Join(p.Directory, "ca.pem")
	db := transporttest.Must(dbconnect.OpenProtectedDSN(ctx, p.URL, ca, "postgres.transport.test"))
	defer func() { _ = db.Client.Close() }()
	sql := db.RawDatabaseForExcludedStores
	var secure bool
	if err := sql.QueryRowContext(ctx, "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()").Scan(&secure); err != nil || !secure {
		t.Fatalf("actual PG TLS: secure=%v err=%v", secure, err)
	}
	for name, input := range map[string]struct{ dsn, ca, dns string }{"WrongDNS": {p.URL, ca, "wrong.transport.test"}, "MissingTrust": {p.URL, "", "postgres.transport.test"}, "UnixSocket": {"postgres://postgres:fixture-password@/tetral?host=/tmp", ca, "postgres.transport.test"}} {
		t.Run(name, func(t *testing.T) {
			result, err := dbconnect.OpenProtectedDSN(ctx, input.dsn, input.ca, input.dns)
			if err == nil {
				_ = result.Client.Close()
				t.Fatal("protected constructor accepted invalid transport")
			}
		})
	}
	root2 := transporttest.Must(transporttest.NewAuthority("postgres-replacement"))
	wrong := filepath.Join(p.Directory, "wrong.pem")
	if err := os.WriteFile(wrong, root2.PEM, 0600); err != nil {
		t.Fatal(err)
	}
	if result, err := dbconnect.OpenProtectedDSN(ctx, p.URL, wrong, "postgres.transport.test"); err == nil {
		_ = result.Client.Close()
		t.Fatal("wrong issuer accepted")
	}

	// The administrative composition has the same verified TCP/no-fallback
	// contract even when its input URL asks for sslmode=disable.
	adminConfig, adminOwner, err := dbconnect.OpenProtectedConfig(ctx, p.URL, ca, "postgres.transport.test")
	if err != nil {
		t.Fatal(err)
	}
	if adminConfig.TLSConfig == nil || len(adminConfig.Fallbacks) != 0 {
		t.Fatal("protected administrative fallback survived")
	}
	adminConn, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := adminConn.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := adminOwner.Close(); err != nil {
		t.Fatal(err)
	}
	goTrust := filepath.Join(t.TempDir(), "go-trust")
	p.Trust(t, goTrust, "initial", p.Authority.PEM)
	owned := transporttest.Must(dbconnect.OpenProtectedDSN(ctx, p.URL, filepath.Join(goTrust, "ca.pem"), "postgres.transport.test"))
	defer func() { _ = owned.Client.Close() }()
	tx := transporttest.Must(owned.RawDatabaseForExcludedStores.BeginTx(ctx, nil))
	var transactionPID, idlePID, afterPID int
	if err := tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&transactionPID); err != nil {
		t.Fatal(err)
	}
	if err := owned.RawDatabaseForExcludedStores.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&idlePID); err != nil {
		t.Fatal(err)
	}
	p.Trust(t, goTrust, "overlap", append(append([]byte{}, p.Authority.PEM...), root2.PEM...))
	exists := func(pid int) bool {
		var count int
		return sql.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid=$1", pid).Scan(&count) == nil && count != 0
	}
	awaitStore(t, 5*time.Second, func() bool { return !exists(idlePID) })
	if !exists(transactionPID) {
		t.Fatal("trust replacement terminated an admitted transaction")
	}
	if err := owned.RawDatabaseForExcludedStores.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&afterPID); err != nil || afterPID == idlePID {
		t.Fatalf("fresh trust borrowed retired idle connection: %d %v", afterPID, err)
	}
	var pinnedPID int
	if err := tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pinnedPID); err != nil || pinnedPID != transactionPID {
		t.Fatalf("Go transaction changed generation: %d %v", pinnedPID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	awaitStore(t, 3*time.Second, func() bool { return !exists(transactionPID) })
	if max := owned.RawDatabaseForExcludedStores.Stats().MaxOpenConnections; max != 20 {
		t.Fatalf("native trust retirement changed operator pool bound: %d", max)
	}
	t.Run("NativeNotificationLifecycle", func(t *testing.T) {
		testProtectedNotificationLifecycle(ctx, t, p, sql, root2)
	})
	seed := `CREATE TABLE session_provider_auth (workspace_id text, session_id text, provider_id text,vault_id text,credential_id text,access_mode text,deleted_at text,updated_at text);
 CREATE TABLE sessions (workspace_id text,id text,vault_ids_json text);
 CREATE TABLE credentials (workspace_id text,vault_id text,id text,auth_type text,provider_id text,access_mode text,encrypted_auth bytea,archived_at text,revoked_at text,auth_public_json text,updated_at text,expires_at text,mcp_server_url text);
 CREATE TABLE transport_effects(value text);
 INSERT INTO session_provider_auth VALUES ('wksp_fixture','sesn_fixture','anthropic','vlt_fixture','cred_provider','user_api_key',NULL,'2026-10-02T00:00:00Z');
 INSERT INTO credentials VALUES ('wksp_fixture','vlt_fixture','cred_provider','provider_api_key','anthropic','user_api_key','\x010203',NULL,NULL,'{}',NULL,NULL,NULL);
 INSERT INTO sessions VALUES ('wksp_fixture','sesn_fixture','["vlt_fixture"]');`
	if _, err := sql.ExecContext(ctx, seed); err != nil {
		t.Fatal(err)
	}
	encrypted := encryptStoreFixture(t, []byte(`{"type":"static_bearer","mcp_server_url":"https://api.githubcopilot.com/mcp/","token":"fixture-token"}`))
	if _, err := sql.ExecContext(ctx, `INSERT INTO credentials VALUES ('wksp_fixture','vlt_fixture','cred_mcp','static_bearer',NULL,NULL,$1,NULL,NULL,'{"type":"static_bearer","mcp_server_url":"https://api.githubcopilot.com/mcp/"}',NULL,NULL,NULL)`, encrypted); err != nil {
		t.Fatal(err)
	}
	providerOAuth := encryptStoreFixture(t, []byte(`{"type":"provider_oauth","provider_id":"openai","access_mode":"oauth","access_token":"old-access","refresh_token":"old-refresh","expires_at":"2000-01-01T00:00:00.000Z","account_id":"acct_fixture"}`))
	mcpOAuth := encryptStoreFixture(t, []byte(`{"type":"mcp_oauth","mcp_server_url":"https://api.githubcopilot.com/mcp/","access_token":"old-access","expires_at":"2000-01-01T00:00:00.000Z","refresh":{"refresh_token":"old-refresh","client_id":"fixture-client","token_endpoint":"https://issuer.transport.test/token"}}`))
	if _, err := sql.ExecContext(ctx, `INSERT INTO credentials VALUES ('wksp_fixture','vlt_fixture','cred_oauth','provider_oauth','openai','oauth',$1,NULL,NULL,'{}',NULL,NULL,NULL),('wksp_fixture','vlt_fixture','cred_mcp_oauth','mcp_oauth',NULL,NULL,$2,'pending',NULL,'{"type":"mcp_oauth","mcp_server_url":"https://api.githubcopilot.com/mcp/"}',NULL,NULL,NULL)`, providerOAuth, mcpOAuth); err != nil {
		t.Fatal(err)
	}
	trust := filepath.Join(t.TempDir(), "trust")
	p.Trust(t, trust, "r1", p.Authority.PEM)
	// Both stores run under the locked production Bun runtime, not host Bun.
	image := transporttest.Must(testinfra.PinnedBunImage())
	config := filepath.Join(t.TempDir(), "bun.json")
	transporttest.Must(0, transporttest.WriteJSON(config, map[string]string{"url": p.InternalURL, "caPath": "/trust/ca.pem", "serverName": "postgres.transport.test"}))
	child := transporttest.Must(p.Resources.Run(ctx, testinfra.ContainerSpec{Image: image, Network: p.Network, Mounts: []testinfra.DockerMount{{Source: transporttest.RepositoryRoot(t), Target: "/workspace", ReadOnly: true}, {Source: trust, Target: "/trust", ReadOnly: true}, {Source: wrong, Target: "/wrong.pem", ReadOnly: true}, {Source: config, Target: "/fixture/config.json", ReadOnly: true}}, Ports: []int{8888}, User: "0", Entrypoint: "bun", Command: []string{"/workspace/integration/transporttest/bun/stores.ts", "/fixture/config.json"}}))
	control := "http://" + transporttest.Must(child.Address(ctx, 8888))
	t.Logf("locked Bun reference %s actual %s", image, child.ImageID)
	state := func() map[string]any {
		var out map[string]any
		if fixtureHTTP(ctx, control, "state", nil, &out) != nil {
			return nil
		}
		return out
	}
	awaitStore(t, 15*time.Second, func() bool { return state() != nil })
	var read map[string]any
	if err := fixtureHTTP(ctx, control, "read", nil, &read); err != nil {
		logs, _ := child.Logs(ctx)
		t.Fatalf("real stores: %v logs=%s", err, logs)
	}
	for name, input := range map[string]map[string]string{"WrongDNS": {"serverName": "wrong.transport.test"}, "WrongIssuer": {"caPath": "/wrong.pem"}, "MissingTrust": {"caPath": "/absent.pem"}} {
		t.Run("LockedBun"+name, func(t *testing.T) {
			var out map[string]any
			if err := fixtureHTTP(ctx, control, "negative", input, &out); err != nil || out["accepted"] != false {
				t.Fatalf("actual Bun negative accepted: %v %v", out, err)
			}
		})
	}
	if err := fixtureHTTP(ctx, control, "hold", nil, nil); err != nil {
		t.Fatal(err)
	}
	awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["heldPID"].(float64) > 0 })
	p.Trust(t, trust, "overlap", append(append([]byte{}, p.Authority.PEM...), root2.PEM...))
	awaitStore(t, 5*time.Second, func() bool {
		s := state()
		return s != nil && s["activated"].(float64) >= 1 && s["pools"] == float64(2)
	})
	// A second observation while the old transaction is held must not allocate
	// a third pool. Its current generation is re-read after the old pool joins.
	p.Trust(t, trust, "latest", append(append([]byte{}, root2.PEM...), p.Authority.PEM...))
	time.Sleep(350 * time.Millisecond)
	if s := state(); s["maxPools"] != float64(2) {
		t.Fatalf("pool generation ceiling: %v", s)
	}
	if err := fixtureHTTP(ctx, control, "release", nil, nil); err != nil {
		t.Fatal(err)
	}
	awaitStore(t, 5*time.Second, func() bool {
		s := state()
		return s != nil && s["activated"].(float64) >= 2 && s["pools"] == float64(1)
	})
	if s := state(); s["heldPID"] != s["releasedPID"] {
		t.Fatalf("transaction changed generation: %v", s)
	}
	var committed int
	if err := sql.QueryRowContext(ctx, "SELECT count(*) FROM transport_effects").Scan(&committed); err != nil || committed != 1 {
		t.Fatalf("actual held transaction commit=%d %v", committed, err)
	}
	p.Trust(t, trust, "malformed", []byte("malformed trust"))
	awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["failed"].(float64) > 0 })
	time.Sleep(600 * time.Millisecond)
	if s := state(); s["failed"] != float64(1) || s["reason"] != "invalid_bundle" {
		t.Fatalf("reload degradation diagnostics were not bounded: %v", s)
	}
	if err := fixtureHTTP(ctx, control, "read", nil, &read); err != nil {
		t.Fatal("valid previous Bun generation was lost", err)
	}
	p.Trust(t, trust, "restored", append(append([]byte{}, root2.PEM...), p.Authority.PEM...))
	awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["recovered"] == float64(1) })
	for _, mode := range []string{"silent", "error"} {
		if err := fixtureHTTP(ctx, control, "diagnostics", map[string]string{"mode": mode}, nil); err != nil {
			t.Fatal(err)
		}
		previousFailed, previousRecovered := state()["failed"].(float64), state()["recovered"].(float64)
		p.Trust(t, trust, "malformed-"+mode, []byte("fixture-secret-shaped-invalid-trust"))
		awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["failed"].(float64) > previousFailed })
		if err := fixtureHTTP(ctx, control, "read", nil, &read); err != nil {
			t.Fatalf("%s diagnostics changed SQL admission: %v", mode, err)
		}
		p.Trust(t, trust, "restored-"+mode, append(append([]byte{}, root2.PEM...), p.Authority.PEM...))
		awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["recovered"].(float64) > previousRecovered })
	}
	if err := fixtureHTTP(ctx, control, "diagnostics", map[string]string{"mode": "normal"}, nil); err != nil {
		t.Fatal(err)
	}
	if s := state(); len(s["diagnostics"].([]any)) != 2 {
		t.Fatalf("silent/failed diagnostics emitted payloads: %v", s)
	}
	// The production refresh writers hold actual row-lock transactions through
	// a controlled issuer response while their SQL generation is retired.
	if err := fixtureHTTP(ctx, control, "refresh-start", nil, nil); err != nil {
		t.Fatal(err)
	}
	awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["issuerCalls"] == float64(2) })
	previous := state()["activated"].(float64)
	p.Trust(t, trust, "writers-rotate", append(append([]byte{}, p.Authority.PEM...), root2.PEM...))
	awaitStore(t, 3*time.Second, func() bool {
		s := state()
		return s != nil && s["activated"].(float64) > previous && s["pools"] == float64(2)
	})
	if err := fixtureHTTP(ctx, control, "refresh-release", nil, nil); err != nil {
		t.Fatal("generation did not retain actual refresh work through commit", err)
	}
	awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["pools"] == float64(1) && s["refreshDone"] == true })
	var updated int
	if err := sql.QueryRowContext(ctx, `SELECT count(*) FROM credentials WHERE id IN ('cred_oauth','cred_mcp_oauth') AND updated_at IS NOT NULL AND expires_at IS NOT NULL AND auth_public_json NOT LIKE '%rotated-access%' AND auth_public_json NOT LIKE '%rotated-refresh%'`).Scan(&updated); err != nil || updated != 2 {
		t.Fatalf("actual encrypted refresh commits=%d err=%v", updated, err)
	}
	observeLeaf := func(leaf transporttest.Leaf) {
		t.Helper()
		awaitStore(t, 4*time.Second, func() bool {
			probe, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			config, owner, err := dbconnect.OpenProtectedConfig(probe, p.URL, ca, "postgres.transport.test")
			if err != nil {
				return false
			}
			defer func() { _ = owner.Close() }()
			conn, err := pgx.ConnectConfig(probe, config)
			if err != nil {
				return false
			}
			defer func() { _ = conn.Close(probe) }()
			secure, ok := conn.PgConn().Conn().(*tls.Conn)
			if !ok {
				return false
			}
			state := secure.ConnectionState()
			return !state.DidResume && len(state.PeerCertificates) > 0 && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(leaf.Parsed.Raw)
		})
	}
	renewed := transporttest.Must(p.Authority.ValidLeaf("postgres.transport.test", ""))
	p.Reload(t, "renewed", renewed)
	observeLeaf(renewed)
	short := transporttest.Must(p.Authority.Issue("postgres.transport.test", "", time.Now().Add(-time.Second), time.Now().Add(5*time.Second)))
	p.Reload(t, "issuer-short", short)
	observeLeaf(short)
	awaitStore(t, 7*time.Second, func() bool { return time.Now().After(short.Parsed.NotAfter) })
	if candidate, err := dbconnect.OpenProtectedDSN(ctx, p.URL, ca, "postgres.transport.test"); err == nil {
		_ = candidate.Client.Close()
		t.Fatal("fresh Go PG connection accepted expired server material")
	}
	var expired map[string]any
	if err := fixtureHTTP(ctx, control, "negative", nil, &expired); err != nil || expired["accepted"] != false {
		t.Fatalf("fresh locked-Bun PG connection accepted expired material: %v %v", expired, err)
	}
	p.Reload(t, "issuer-restored", renewed)
	observeLeaf(renewed)
	if err := fixtureHTTP(ctx, control, "read", nil, &read); err != nil {
		t.Fatal("locked-Bun store did not recover after issuer restoration", err)
	}
	// A well-formed update that removes a still-valid CA is authoritative trust
	// removal. The server still presents an R1 leaf, so the R2-only candidate
	// cannot verify: new store work is refused instead of continuing under R1,
	// while a transaction admitted before the update commits on its pool.
	if err := fixtureHTTP(ctx, control, "hold", nil, nil); err != nil {
		t.Fatal(err)
	}
	awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["heldPID"].(float64) > 0 })
	failedBeforeNarrowing := state()["failed"].(float64)
	p.Trust(t, trust, "narrowed-before-migration", root2.PEM)
	awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["failed"].(float64) > failedBeforeNarrowing })
	if err := fixtureHTTP(ctx, control, "release", nil, nil); err != nil {
		t.Fatal("transaction admitted before trust narrowing did not finish on its generation", err)
	}
	if s := state(); s["reason"] != "candidate_verification_failed" || s["heldPID"] != s["releasedPID"] || s["maxPools"] != float64(2) {
		t.Fatalf("narrowed trust failure, held generation or pool ceiling: %v", s)
	}
	if err := sql.QueryRowContext(ctx, "SELECT count(*) FROM transport_effects").Scan(&committed); err != nil || committed != 2 {
		t.Fatalf("transaction held across trust narrowing commit=%d %v", committed, err)
	}
	if err := fixtureHTTP(ctx, control, "read", nil, &read); err == nil {
		t.Fatal("removed CA kept admitting store work after its narrowed candidate failed")
	}
	// Retries of the same unverifiable bundle are spaced (250 ms doubling to a
	// 5 s cap) instead of building a full candidate pool on every 250 ms poll.
	openedBeforeWindow := state()["opened"].(float64)
	time.Sleep(2500 * time.Millisecond)
	if s := state(); s["opened"].(float64)-openedBeforeWindow > 5 {
		t.Fatalf("failing candidate retries were not spaced: %v", s)
	}
	if err := fixtureHTTP(ctx, control, "read", nil, &read); err == nil {
		t.Fatal("store admission reopened without a verified candidate")
	}
	// Once the server presents a chain to the remaining trust, a spaced retry
	// of the same bundle verifies and re-admits work.
	transporttest.Must(0, os.WriteFile(ca, append(append([]byte{}, p.Authority.PEM...), root2.PEM...), 0600))
	leafR2 := transporttest.Must(root2.ValidLeaf("postgres.transport.test", ""))
	activatedBeforeRecovery, recoveredBeforeRecovery := state()["activated"].(float64), state()["recovered"].(float64)
	p.Reload(t, "R2-leaf", leafR2)
	observeLeaf(leafR2)
	awaitStore(t, 8*time.Second, func() bool {
		s := state()
		return s != nil && s["activated"].(float64) > activatedBeforeRecovery && s["recovered"].(float64) > recoveredBeforeRecovery
	})
	if err := fixtureHTTP(ctx, control, "read", nil, &read); err != nil {
		t.Fatal("verified narrowed candidate did not re-admit store work", err)
	}
	// An anchor past its validity is left out instead of invalidating the
	// bundle, so an old CA expiring during the overlap does not disable trust.
	expiredRoot := transporttest.Must(transporttest.NewAuthorityWithValidity("postgres-expired", time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour)))
	failedBeforeOverlap, activatedBeforeOverlap := state()["failed"].(float64), state()["activated"].(float64)
	p.Trust(t, trust, "overlap-with-expired", append(append(append([]byte{}, expiredRoot.PEM...), p.Authority.PEM...), root2.PEM...))
	awaitStore(t, 3*time.Second, func() bool { s := state(); return s != nil && s["activated"].(float64) > activatedBeforeOverlap })
	if s := state(); s["failed"] != failedBeforeOverlap {
		t.Fatalf("expired anchor invalidated the trust bundle: %v", s)
	}
	if err := fixtureHTTP(ctx, control, "read", nil, &read); err != nil {
		t.Fatal("store read failed with an expired anchor in the bundle", err)
	}
	// Planned retirement: every peer already presents an R2 leaf before R1 is
	// removed, so the narrowed candidate verifies and replaces the generation.
	beforeActivation := state()["activated"].(float64)
	p.Trust(t, trust, "R2-only", root2.PEM)
	transporttest.Must(0, os.WriteFile(ca, root2.PEM, 0600))
	awaitStore(t, 4*time.Second, func() bool {
		s := state()
		return s != nil && s["activated"].(float64) > beforeActivation && s["pools"] == float64(1)
	})
	observeLeaf(leafR2)
	if err := fixtureHTTP(ctx, control, "read", nil, &read); err != nil {
		t.Fatal("locked-Bun R2-only store read failed", err)
	}
	if err := fixtureHTTP(ctx, control, "close", nil, nil); err != nil {
		t.Fatal(err)
	}
	if s := state(); s["pools"] != float64(0) || s["closed"] != true {
		t.Fatalf("SQL owner did not join pools: %v", s)
	}
	if err := fixtureHTTP(ctx, control, "stop", nil, nil); err != nil {
		t.Fatal(err)
	}
	if exit, err := child.Wait(ctx); err != nil || exit != 0 {
		t.Fatalf("locked Bun fixture exit %d %v", exit, err)
	}
}

func testProtectedNotificationLifecycle(ctx context.Context, t *testing.T, p *transporttest.PostgreSQL, admin *sql.DB, replacement *transporttest.Authority) {
	t.Helper()
	trust := filepath.Join(t.TempDir(), "notification-trust")
	p.Trust(t, trust, "initial", p.Authority.PEM)
	const application = "protected_notification_fixture"
	const channel = "protected_notification_fixture"
	opened := transporttest.Must(dbconnect.OpenProtectedDSN(ctx, p.URL+"&application_name="+application, filepath.Join(trust, "ca.pem"), "postgres.transport.test"))
	t.Cleanup(func() {
		if err := opened.Client.Close(); err != nil {
			t.Errorf("notification owner cleanup: %v", err)
		}
	})
	type listener struct {
		cancel   context.CancelFunc
		ready    chan struct{}
		payloads chan string
		done     chan struct{}
		err      error
	}
	start := func() *listener {
		listenCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		l := &listener{cancel: cancel, ready: make(chan struct{}), payloads: make(chan string, 2), done: make(chan struct{})}
		go func() {
			defer close(l.done)
			l.err = opened.Client.Listen(listenCtx, "transport.listen", channel, func() { close(l.ready) }, func(payload string) { l.payloads <- payload })
		}()
		t.Cleanup(func() {
			l.cancel()
			select {
			case <-l.done:
			case <-time.After(2 * time.Second):
				t.Error("notification listener cleanup did not join")
			}
		})
		select {
		case <-l.ready:
		case <-l.done:
			t.Fatalf("protected listener failed before readiness: %v", l.err)
		case <-time.After(2 * time.Second):
			t.Fatal("protected listener did not become ready")
		}
		return l
	}
	notify := func(l *listener, payload string) {
		if _, err := admin.ExecContext(ctx, "SELECT pg_notify($1,$2)", channel, payload); err != nil {
			t.Fatal(err)
		}
		select {
		case actual := <-l.payloads:
			if actual != payload {
				t.Fatalf("notification payload=%q want%q", actual, payload)
			}
		case <-l.done:
			t.Fatalf("protected listener ended without delivering payload: %v", l.err)
		case <-time.After(2 * time.Second):
			t.Fatal("protected listener did not deliver committed notification")
		}
	}
	stop := func(l *listener) {
		l.cancel()
		select {
		case <-l.done:
			if !errors.Is(l.err, context.Canceled) {
				t.Fatalf("listener cancellation changed classification: %v", l.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("notification listener cancellation did not join")
		}
	}
	exists := func(pid int) bool {
		var count int
		if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid=$1", pid).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count != 0
	}
	listenerPID := func() int {
		var pid int
		var secure bool
		if err := admin.QueryRowContext(ctx, "SELECT activity.pid, transport.ssl FROM pg_stat_activity activity JOIN pg_stat_ssl transport USING(pid) WHERE application_name=$1", application).Scan(&pid, &secure); err != nil || !secure {
			t.Fatalf("listener connection is not independently observed TLS: ssl=%t err=%v", secure, err)
		}
		if opened.Client.Stats().InUse != 1 {
			t.Fatal("listener did not hold exactly one counted pool lease")
		}
		return pid
	}
	first := start()
	oldPID := listenerPID()
	notify(first, "before-trust-update")
	var idlePID int
	if err := opened.RawDatabaseForExcludedStores.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&idlePID); err != nil || idlePID == oldPID {
		t.Fatalf("fixture did not create a separate idle pool connection: pid=%d err=%v", idlePID, err)
	}
	p.Trust(t, trust, "overlap", append(append([]byte{}, p.Authority.PEM...), replacement.PEM...))
	// Idle socket retirement independently establishes activation. No sleep or
	// readiness callback substitutes for the active listener's actual payload.
	awaitStore(t, 5*time.Second, func() bool { return !exists(idlePID) })
	if !exists(oldPID) || opened.Client.Stats().InUse != 1 {
		t.Fatal("valid trust activation terminated the admitted listener")
	}
	notify(first, "after-trust-update")
	stop(first)
	awaitStore(t, 3*time.Second, func() bool { return !exists(oldPID) })
	if opened.Client.Stats().InUse != 0 {
		t.Fatal("cancelled listener retained its pool lease")
	}
	second := start()
	newPID := listenerPID()
	if newPID == oldPID || newPID == idlePID {
		t.Fatal("fresh listener reborrowed a retired trust-generation connection")
	}
	notify(second, "fresh-trust-listener")
	stop(second)
	if stats := opened.Client.Stats(); stats.InUse != 0 || stats.MaxOpenConnections != 20 {
		t.Fatalf("listener cleanup changed pool ownership or bound: %+v", stats)
	}
	t.Logf("protected notification TLS PIDs old=%d idle=%d fresh=%d; activation retained listener, cancellation joined and retired it", oldPID, idlePID, newPID)
}
func fixtureHTTP(ctx context.Context, base, path string, input any, output any) error {
	body := []byte("{}")
	if input != nil {
		var err error
		body, err = json.Marshal(input)
		if err != nil {
			return err
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != 200 {
		_, _ = io.Copy(io.Discard, response.Body)
		return fmt.Errorf("fixture response %d", response.StatusCode)
	}
	if output != nil {
		return json.NewDecoder(response.Body).Decode(output)
	}
	_, err = io.Copy(io.Discard, response.Body)
	return err
}
func encryptStoreFixture(t *testing.T, plain []byte) []byte {
	t.Helper()
	key, err := hex.DecodeString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	block := transporttest.Must(aes.NewCipher(key))
	gcm := transporttest.Must(cipher.NewGCM(block))
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	return append(nonce, gcm.Seal(nil, nonce, plain, nil)...)
}

func awaitStore(t *testing.T, bound time.Duration, f func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), bound)
	defer cancel()
	if err := transporttest.Await(ctx, f); err != nil {
		t.Fatal(err)
	}
}
