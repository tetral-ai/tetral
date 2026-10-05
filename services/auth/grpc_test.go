package tetralauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/auth/authtest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func TestAuthGRPCConfig(t *testing.T) {
	base := exchangeTestEnv{EnvBootstrapWorkspaceID: "default", EnvBootstrapAPIKey: strings.Repeat("x", 32), EnvInternalPrincipalPrivateKeyB64: mustGenerateTestPrivateKey(t)}
	cfg, err := ConfigFromEnv(base)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCAddress != ":9095" || cfg.GRPCTransport != "plaintext" || cfg.HTTPTransport.Mode != "plaintext" {
		t.Fatal("Auth default listener/transport contract changed")
	}
	for name, setting := range map[string]exchangeTestEnv{
		"HTTP collision": {EnvGRPCAddress: ":8080"}, "metrics collision": {EnvGRPCAddress: ":8081"},
		"unknown mode": {EnvGRPCTransport: "auto"}, "missing native material": {EnvGRPCTransport: "native-mtls"},
		"plaintext with TLS":   {EnvGRPCTLSCAPath: "/trust"},
		"wrong edge namespace": {EnvGRPCTransport: "native-mtls", EnvGRPCTLSCAPath: "/trust", EnvGRPCTLSCertPath: "/cert", EnvGRPCTLSKeyPath: "/key", EnvGRPCTLSEdgeClientURI: "spiffe://tetral.local/ns/other/sa/tetral-public-edge"},
		"HTTP downgrade":       {"TETRAL_HTTP_TRANSPORT": "plaintext", "TETRAL_HTTP_TLS_CA_PATH": "/trust"},
	} {
		t.Run(name, func(t *testing.T) {
			values := exchangeTestEnv{}
			for k, v := range base {
				values[k] = v
			}
			for k, v := range setting {
				values[k] = v
			}
			if _, err := ConfigFromEnv(values); err == nil {
				t.Fatal("unsafe listener configuration accepted")
			}
		})
	}
	base[EnvGRPCTransport] = "native-mtls"
	base[EnvGRPCTLSCAPath] = "/trust"
	base[EnvGRPCTLSCertPath] = "/cert"
	base[EnvGRPCTLSKeyPath] = "/key"
	base[EnvGRPCTLSEdgeClientURI] = "spiffe://tetral.local/ns/envoy-gateway-system/sa/tetral-public-edge"
	if _, err := ConfigFromEnv(base); err != nil {
		t.Fatal(err)
	}
}

// Called by the owning PostgreSQL root: held Check and its usage transaction
// are real, while the existing certificate fixture changes mounted generations.
func externalTestNativeTLS(t *testing.T, ctx context.Context, admin *sql.DB, workloadDB *storagetest.WorkloadDB, runtime *sql.DB, adapter *ExternalAuthorization, signer *auth.InternalPrincipalSigner) {
	// This listener owns an actual independent Auth-role pool, so shutdown can
	// prove its admitted users join before the pool/credential owners close.
	nativeDB := workloadDB.OpenWorkload(t, "auth", nil)
	nativeAdapter, err := NewExternalAuthorization(ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: auth.NewAuthorityResolver(nativeDB, "ws_auth_test")}, Signer: signer, PrincipalTTL: adapter.cfg.PrincipalTTL, Logger: adapter.cfg.Logger})
	if err != nil {
		t.Fatal(err)
	}
	rootA := transporttest.Must(transporttest.NewAuthority("auth-check-a"))
	rootB := transporttest.Must(transporttest.NewAuthority("auth-check-b"))
	dns := "auth.tetral-system.svc.cluster.local"
	edgeURI := "spiffe://tetral.local/ns/envoy-gateway-system/sa/tetral-public-edge"
	leafA := transporttest.Must(rootA.ValidLeaf(dns, "spiffe://tetral.local/ns/tetral-system/sa/auth"))
	leafB := transporttest.Must(rootB.ValidLeaf(dns, "spiffe://tetral.local/ns/tetral-system/sa/auth"))
	edgeA := transporttest.Must(rootA.ValidLeaf("public-edge", edgeURI))
	edgeB := transporttest.Must(rootB.ValidLeaf("public-edge", edgeURI))
	dir := t.TempDir()
	project := func(name string, roots []byte, leaf transporttest.Leaf) {
		t.Helper()
		if err := transporttest.Project(dir, name, map[string][]byte{"ca.crt": roots, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
			t.Fatal(err)
		}
	}
	project("initial", rootA.PEM, leafA)
	cfg := Config{GRPCAddress: "127.0.0.1:0", GRPCTransport: "native-mtls", GRPCTLSCAPath: filepath.Join(dir, "ca.crt"), GRPCTLSCertPath: filepath.Join(dir, "tls.crt"), GRPCTLSKeyPath: filepath.Join(dir, "tls.key"), GRPCTLSEdgeClientURI: edgeURI}
	server, err := OpenExternalAuthorizationServer(ctx, cfg, nativeAdapter, nil)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	runJoined := false
	readiness := workload.NewReadiness()
	readiness.MarkReady()
	go func() { done <- server.Run(runCtx, readiness) }()
	t.Cleanup(func() {
		stop()
		if !runJoined {
			if err := <-done; err != nil {
				t.Errorf("native listener did not join: %v", err)
			}
		}
		if readiness.Ready() {
			t.Error("native listener drain retained readiness")
		}
		_ = server.Close()
	})
	clientConfig := func(roots []byte, leaf transporttest.Leaf, name string) *tls.Config {
		t.Helper()
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(roots) {
			t.Fatal("invalid client fixture trust")
		}
		config := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: name, RootCAs: pool}
		if len(leaf.Certificate) != 0 {
			pair, err := tls.X509KeyPair(leaf.Certificate, leaf.Key)
			if err != nil {
				t.Fatal(err)
			}
			config.Certificates = []tls.Certificate{pair}
		}
		return config
	}
	dial := func(creds credentials.TransportCredentials) (*grpc.ClientConn, authv3.AuthorizationClient) {
		t.Helper()
		conn, err := grpc.NewClient(server.listener.Addr().String(), grpc.WithTransportCredentials(creds))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn, authv3.NewAuthorizationClient(conn)
	}
	_, clientA := dial(credentials.NewTLS(clientConfig(rootA.PEM, edgeA, dns)))
	key, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", "native-held")
	if err != nil {
		t.Fatal(err)
	}
	request := externalTestRequest("GET", "/v1/sessions", key.APIKey, nil)
	assertAllow := func(client authv3.AuthorizationClient) {
		t.Helper()
		response, err := client.Check(ctx, request)
		if err != nil || externalTestStatus(response) != 200 {
			t.Fatalf("native Check failed: %v status=%d", err, externalTestStatus(response))
		}
		if _, _, err := signer.Verify(externalTestPrincipal(response), "GET", "/v1/sessions"); err != nil {
			t.Fatal(err)
		}
	}
	assertAllow(clientA)
	wrongRole := transporttest.Must(rootA.ValidLeaf("controller", "spiffe://tetral.local/ns/envoy-gateway-system/sa/envoy-gateway"))
	expired := transporttest.Must(rootA.Issue("public-edge", edgeURI, time.Now().Add(-time.Hour), time.Now().Add(-time.Second)))
	for name, creds := range map[string]credentials.TransportCredentials{
		"wrong role": credentials.NewTLS(clientConfig(rootA.PEM, wrongRole, dns)), "wrong DNS": credentials.NewTLS(clientConfig(rootA.PEM, edgeA, "other.tetral-system.svc.cluster.local")),
		"wrong CA": credentials.NewTLS(clientConfig(rootB.PEM, edgeB, dns)), "missing leaf": credentials.NewTLS(clientConfig(rootA.PEM, transporttest.Leaf{}, dns)), "expired leaf": credentials.NewTLS(clientConfig(rootA.PEM, expired, dns)), "plaintext": insecure.NewCredentials(),
	} {
		t.Run(name, func(t *testing.T) {
			negativeKey, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", "native negative "+name)
			if err != nil {
				t.Fatal(err)
			}
			negativeRequest := externalTestRequest("GET", "/v1/sessions", negativeKey.APIKey, nil)
			_, client := dial(creds)
			callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			response, err := client.Check(callCtx, negativeRequest)
			if err == nil || externalTestPrincipal(response) != "" {
				t.Fatal("invalid native transport admitted Check")
			}
			var touched sql.NullTime
			if err := admin.QueryRowContext(ctx, `SELECT last_used_at FROM api_keys WHERE id=$1`, negativeKey.ID).Scan(&touched); err != nil || touched.Valid {
				t.Fatal("invalid native transport reached credential admission/usage")
			}
		})
	}
	assertAllow(clientA)
	held, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback()
	var holder int
	if err := held.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
		t.Fatal(err)
	}
	if _, err := held.ExecContext(ctx, `UPDATE api_keys SET name=name WHERE id=$1`, key.ID); err != nil {
		t.Fatal(err)
	}
	result := make(chan *authv3.CheckResponse, 1)
	failed := make(chan error, 1)
	go func() {
		response, err := clientA.Check(ctx, request)
		if err != nil {
			failed <- err
		} else {
			result <- response
		}
	}()
	externalAwaitBlock(t, ctx, admin, holder)
	overlap := append(append([]byte{}, rootA.PEM...), rootB.PEM...)
	renewed := transporttest.Must(rootA.ValidLeaf(dns, "spiffe://tetral.local/ns/tetral-system/sa/auth"))
	project("renewed-overlap", overlap, renewed)
	if err := server.credentials.Reload(); err != nil {
		t.Fatal(err)
	}
	if err := held.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failed:
		t.Fatalf("leaf/overlap replacement interrupted admitted Check: %v", err)
	case response := <-result:
		if externalTestStatus(response) != 200 {
			t.Fatal("held Check lost admission")
		}
	case <-ctx.Done():
		t.Fatal("held Check did not join")
	}
	// A new-root client observes the new Auth leaf on a fresh real handshake.
	project("new-leaves", overlap, leafB)
	if err := server.credentials.Reload(); err != nil {
		t.Fatal(err)
	}
	observed := make(chan string, 1)
	newConfig := clientConfig(overlap, edgeB, dns)
	newConfig.VerifyConnection = func(state tls.ConnectionState) error {
		select {
		case observed <- state.PeerCertificates[0].SerialNumber.String():
		default:
		}
		return nil
	}
	_, clientB := dial(credentials.NewTLS(newConfig))
	assertAllow(clientB)
	select {
	case serial := <-observed:
		if serial != leafB.Parsed.SerialNumber.String() {
			t.Fatal("fresh handshake did not observe replacement Auth leaf")
		}
	case <-ctx.Done():
		t.Fatal("new root handshake not observed")
	}
	assertAllow(clientA)
	// The explicit rollout drain normally precedes old-root removal. Retain one
	// old channel to prove a missed operator drain still rejects new Check work.
	project("retired", rootB.PEM, leafB)
	if err := server.credentials.Reload(); err != nil {
		t.Fatal(err)
	}
	retiredKey, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", "retired channel negative")
	if err != nil {
		t.Fatal(err)
	}
	retiredRequest := externalTestRequest("GET", "/v1/sessions", retiredKey.APIKey, nil)
	response, err := clientA.Check(ctx, retiredRequest)
	if err == nil || externalTestPrincipal(response) != "" {
		t.Fatal("reused retired-root channel admitted a new principal")
	}
	var touched sql.NullTime
	if err := admin.QueryRowContext(ctx, `SELECT last_used_at FROM api_keys WHERE id=$1`, retiredKey.ID).Scan(&touched); err != nil || touched.Valid {
		t.Fatal("retired channel reached credential admission/usage")
	}
	response, err = clientB.Check(ctx, retiredRequest)
	if err != nil || externalTestStatus(response) != 200 {
		t.Fatal("new-root channel rejected the same valid negative-control credential")
	}
	if p, _, err := signer.Verify(externalTestPrincipal(response), "GET", "/v1/sessions"); err != nil || p.APIKeyID != retiredKey.ID {
		t.Fatal("new-root positive control lost credential binding")
	}
	assertAllow(clientB)
	project("malformed", []byte("not a trust bundle"), leafB)
	if err := server.credentials.Reload(); err == nil {
		t.Fatal("malformed trust activated")
	}
	assertAllow(clientB)
	// Hold a real admitted Check in its owning transaction while shutdown
	// withdraws readiness. Grace must preserve the admitted RPC and join it
	// before the credential watcher and independent pool close.
	drainKey, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", "native drain")
	if err != nil {
		t.Fatal(err)
	}
	drainTx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer drainTx.Rollback()
	var drainHolder int
	if err := drainTx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&drainHolder); err != nil {
		t.Fatal(err)
	}
	if _, err := drainTx.ExecContext(ctx, `UPDATE api_keys SET name=name WHERE id=$1`, drainKey.ID); err != nil {
		t.Fatal(err)
	}
	drainResult := make(chan *authv3.CheckResponse, 1)
	drainFailure := make(chan error, 1)
	go func() {
		response, err := clientB.Check(ctx, externalTestRequest("GET", "/v1/sessions", drainKey.APIKey, nil))
		if err != nil {
			drainFailure <- err
		} else {
			drainResult <- response
		}
	}()
	externalAwaitBlock(t, ctx, admin, drainHolder)
	stop()
	withdrawal, withdrawCancel := context.WithTimeout(ctx, 2*time.Second)
	defer withdrawCancel()
	for readiness.Ready() {
		select {
		case <-withdrawal.Done():
			t.Fatal("Check shutdown did not withdraw readiness")
		case <-time.After(5 * time.Millisecond):
		}
	}
	select {
	case err := <-done:
		runJoined = true
		t.Fatalf("listener joined before held Check completed: %v", err)
	default:
	}
	if err := drainTx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-drainFailure:
		t.Fatalf("shutdown cancelled an admitted Check inside grace: %v", err)
	case response := <-drainResult:
		if externalTestStatus(response) != 200 {
			t.Fatal("admitted Check did not complete during grace")
		}
		if p, _, err := signer.Verify(externalTestPrincipal(response), "GET", "/v1/sessions"); err != nil || p.APIKeyID != drainKey.ID {
			t.Fatal("drained Check principal lost credential binding")
		}
	case <-ctx.Done():
		t.Fatal("admitted Check did not join during shutdown")
	}
	select {
	case err := <-done:
		runJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Check listener did not join after admitted work")
	}
	var drainedTouch sql.NullTime
	if err := admin.QueryRowContext(ctx, `SELECT last_used_at FROM api_keys WHERE id=$1`, drainKey.ID).Scan(&drainedTouch); err != nil || !drainedTouch.Valid {
		t.Fatal("drained Check lost committed usage")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := server.credentials.ServerTLSConfig(edgeURI); err == nil {
		t.Fatal("credential owner remains active after joined listener close")
	}
	if err := nativeDB.Close(); err != nil {
		t.Fatal(err)
	}
	if nativeDB.Stats().InUse != 0 {
		t.Fatal("Check shutdown left an admitted pool user")
	}
}
