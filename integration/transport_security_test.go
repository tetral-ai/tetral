package integration

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/integration/transporttest"
)

func transportClient(t *testing.T, root []byte, leaf transporttest.Leaf) *tls.Config {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(root) {
		t.Fatal("fixture root is invalid")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: "receiver.transport.test", NextProtos: []string{"h2"}, SessionTicketsDisabled: true}
	if len(leaf.Certificate) > 0 {
		pair, err := tls.X509KeyPair(leaf.Certificate, leaf.Key)
		if err != nil {
			t.Fatal(err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	return config
}
func transportHealth(ctx context.Context, address string, config *tls.Config) (tls.ConnectionState, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	creds := insecure.NewCredentials()
	if config != nil {
		creds = credentials.NewTLS(config)
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds))
	if err != nil {
		return tls.ConnectionState{}, err
	}
	defer func() { _ = connection.Close() }()
	var remote peer.Peer
	_, err = grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.Peer(&remote))
	if err != nil {
		return tls.ConnectionState{}, err
	}
	if auth, ok := remote.AuthInfo.(credentials.TLSInfo); ok {
		return auth.State, nil
	}
	return tls.ConnectionState{}, nil
}
func requireTransportAllowed(t *testing.T, p *transporttest.ProxyPair, config *tls.Config) tls.ConnectionState {
	t.Helper()
	state, err := transportHealth(t.Context(), p.ReceiverAddress, config)
	if err != nil {
		t.Fatalf("allowed peer did not reach actual backend: %v", err)
	}
	if state.DidResume || state.Version != tls.VersionTLS13 || state.NegotiatedProtocol != "h2" || len(state.PeerCertificates) == 0 {
		t.Fatalf("fresh TLS1.3/h2 handshake was not observed: version=%x resumed=%v", state.Version, state.DidResume)
	}
	if len(state.PeerCertificates[0].URIs) != 1 || state.PeerCertificates[0].URIs[0].String() != transporttest.ReceiverURI {
		t.Fatal("unexpected authenticated receiver role")
	}
	return state
}
func requireTransportDenied(t *testing.T, p *transporttest.ProxyPair, config *tls.Config) {
	t.Helper()
	before, err := p.Operations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transportHealth(t.Context(), p.ReceiverAddress, config); err == nil {
		t.Fatal("unauthorized transport reached backend")
	}
	after, err := p.Operations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("unauthorized transport admitted operations: before=%d after=%d", before, after)
	}
}

// projectReceiverLeaf replaces the receiver Envoy's SDS leaf and waits until a
// fresh handshake presents it; probe must trust the leaf's issuer.
func projectReceiverLeaf(t *testing.T, p *transporttest.ProxyPair, generation string, leaf transporttest.Leaf, probe *tls.Config) {
	t.Helper()
	if err := transporttest.Project(filepath.Join(p.Directory, "receiver", "leaf"), generation, map[string][]byte{"tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal(err)
	}
	reload, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := transporttest.Await(reload, func() bool {
		state, err := transportHealth(reload, p.ReceiverAddress, probe)
		return err == nil && !state.DidResume && len(state.PeerCertificates) > 0 && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(leaf.Parsed.Raw)
	}); err != nil {
		t.Fatalf("receiver did not present leaf %s: %v", generation, err)
	}
}

// replaceSourceProxy starts a fresh caller Envoy, so its next call performs a
// full upstream handshake instead of reusing a pooled connection.
func replaceSourceProxy(t *testing.T, p *transporttest.ProxyPair) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p.ReplaceSource(ctx, t)
	if err := transporttest.Await(ctx, func() bool {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.SourceAdmin+"/ready", nil)
		if err != nil {
			return false
		}
		response, err := (&http.Client{Timeout: time.Second}).Do(request)
		if err != nil {
			return false
		}
		defer func() { _ = response.Body.Close() }()
		return response.StatusCode == http.StatusOK
	}); err != nil {
		t.Fatal("replacement caller proxy did not become ready", err)
	}
}

// requireCallerDenied calls through the caller Envoy, which must refuse the
// receiver certificate itself: the call fails, the caller proxy records a
// certificate verification failure and the backend admits no operation.
func requireCallerDenied(t *testing.T, p *transporttest.ProxyPair) {
	t.Helper()
	operations := transporttest.Must(p.Operations(t.Context()))
	failures := transporttest.Must(transporttest.VerifyFailures(t.Context(), p.SourceAdmin))
	if _, err := transportHealth(t.Context(), p.SourceAddress, nil); err == nil {
		t.Fatal("caller proxy accepted an invalid receiver certificate")
	}
	if after := transporttest.Must(p.Operations(t.Context())); after != operations {
		t.Fatalf("caller-denied transport admitted operations: before=%d after=%d", operations, after)
	}
	if after := transporttest.Must(transporttest.VerifyFailures(t.Context(), p.SourceAdmin)); after <= failures {
		t.Fatalf("caller proxy recorded no certificate verification failure: before=%d after=%d", failures, after)
	}
}

// requireCallerAllowed waits for the caller Envoy path to succeed again. The
// bound covers one outlier ejection of the receiver endpoint.
func requireCallerAllowed(t *testing.T, p *transporttest.ProxyPair) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := transporttest.Await(ctx, func() bool { _, err := transportHealth(ctx, p.SourceAddress, nil); return err == nil }); err != nil {
		logs, _ := p.Source.Logs(t.Context())
		t.Fatalf("caller Envoy path did not recover: %v\n%s", err, logs)
	}
}

func TestTransportSecurityPeerValidation(t *testing.T) {
	p := transporttest.NewProxyPair(t)
	allowed := transportClient(t, p.Root.PEM, p.Caller)
	other := transporttest.Must(transporttest.NewAuthority("R2"))
	wrongRoot := transporttest.Must(other.ValidLeaf("caller.transport.test", transporttest.CallerURI))
	wrongRole := transporttest.Must(p.Root.ValidLeaf("unauthorized.transport.test", "spiffe://transport.test/ns/test/sa/unauthorized"))
	expired := transporttest.Must(p.Root.Issue("caller.transport.test", transporttest.CallerURI, time.Now().Add(-120*time.Second), time.Now().Add(-60*time.Second)))
	future := transporttest.Must(p.Root.Issue("caller.transport.test", transporttest.CallerURI, time.Now().Add(60*time.Second), time.Now().Add(10*time.Minute)))
	wrongKey := allowed.Clone()
	differentKey := transportClient(t, p.Root.PEM, wrongRole).Certificates[0].PrivateKey
	wrongKey.Certificates = []tls.Certificate{allowed.Certificates[0]}
	wrongKey.Certificates[0].PrivateKey = differentKey
	// Receiver enforcement: the receiver Envoy refuses each invalid caller.
	cases := []struct {
		name   string
		config *tls.Config
	}{{"Plaintext", nil}, {"AbsentClientCertificate", transportClient(t, p.Root.PEM, transporttest.Leaf{})}, {"WrongClientRoot", transportClient(t, p.Root.PEM, wrongRoot)}, {"ValidUnauthorizedRole", transportClient(t, p.Root.PEM, wrongRole)}, {"ExpiredClient", transportClient(t, p.Root.PEM, expired)}, {"FutureClient", transportClient(t, p.Root.PEM, future)}, {"MismatchedSigningKeyAtHandshake", wrongKey}}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			requireTransportAllowed(t, p, allowed)
			requireTransportDenied(t, p, test.config)
			requireTransportAllowed(t, p, allowed)
		})
	}
	// Caller enforcement: server identity is checked by the calling Envoy,
	// which a test TLS client cannot stand in for. Each invalid receiver leaf
	// is refused on a fresh caller proxy before any backend operation.
	t.Run("CallerRejectsInvalidReceiver", func(t *testing.T) {
		for _, invalid := range []struct {
			name  string
			leaf  transporttest.Leaf
			probe *tls.Config
		}{
			{"unknown-root", transporttest.Must(other.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI)), transportClient(t, other.PEM, p.Caller)},
			{"wrong-identity", transporttest.Must(p.Root.ValidLeaf("receiver.transport.test", "spiffe://transport.test/ns/test/sa/unauthorized")), allowed},
		} {
			projectReceiverLeaf(t, p, "invalid-"+invalid.name, invalid.leaf, invalid.probe)
			replaceSourceProxy(t, p)
			requireCallerDenied(t, p)
			projectReceiverLeaf(t, p, "restored-"+invalid.name, p.ReceiverLeaf, allowed)
			requireCallerAllowed(t, p)
		}
	})
	if _, err := transportHealth(t.Context(), p.SourceAddress, nil); err != nil {
		logs, _ := p.Source.Logs(t.Context())
		t.Fatalf("actual caller Envoy mTLS path failed: %v\n%s", err, logs)
	}
}

func TestTransportSecurityLeafReplacement(t *testing.T) {
	p := transporttest.NewProxyPair(t)
	config := transportClient(t, p.Root.PEM, p.Caller)
	initial := requireTransportAllowed(t, p, config)
	connection, err := grpc.NewClient(p.SourceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	watch, err := grpc_testing.NewTestServiceClient(connection).StreamingOutputCall(ctx, &grpc_testing.StreamingOutputCallRequest{Payload: &grpc_testing.Payload{Body: []byte("leaf-replacement")}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := watch.Recv()
	if err != nil || string(first.GetPayload().GetBody()) != "1" {
		t.Fatal("initial numbered response missing", err)
	}
	leaf := transporttest.Must(p.Root.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI))
	caller := transporttest.Must(p.Root.ValidLeaf("caller.transport.test", transporttest.CallerURI))
	if err := transporttest.Project(filepath.Join(p.Directory, "receiver", "leaf"), "g2", map[string][]byte{"tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal(err)
	}
	if err := transporttest.Project(filepath.Join(p.Directory, "caller", "leaf"), "g2", map[string][]byte{"tls.crt": caller.Certificate, "tls.key": caller.Key}); err != nil {
		t.Fatal(err)
	}
	reload, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	if err := transporttest.Await(reload, func() bool {
		state, err := transportHealth(reload, p.ReceiverAddress, config)
		return err == nil && !state.DidResume && len(state.PeerCertificates) > 0 && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(leaf.Parsed.Raw)
	}); err != nil {
		t.Fatalf("fresh handshake did not observe replacement leaf: %v", err)
	}
	if sha256.Sum256(initial.PeerCertificates[0].Raw) == sha256.Sum256(leaf.Parsed.Raw) {
		t.Fatal("replacement fixture reused original leaf")
	}
	if err := p.ReleaseResponse(ctx, "leaf-replacement"); err != nil {
		t.Fatal(err)
	}
	second, err := watch.Recv()
	if err != nil || string(second.GetPayload().GetBody()) != "2" {
		t.Fatalf("same held stream lost sequence across leaf reload: %v", err)
	}
	if _, err := transportHealth(t.Context(), p.SourceAddress, nil); err != nil {
		t.Fatalf("caller Envoy did not reload its leaf: %v", err)
	}
	// A malformed projected generation cannot activate a mismatched certificate.
	rejected := transporttest.Must(transporttest.RejectedUpdates(ctx, p.ReceiverAdmin))
	if err := transporttest.Project(filepath.Join(p.Directory, "receiver", "leaf"), "bad", map[string][]byte{"tls.crt": leaf.Certificate, "tls.key": []byte("malformed")}); err != nil {
		t.Fatal(err)
	}
	malformed, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()
	if err := transporttest.Await(malformed, func() bool {
		count, err := transporttest.RejectedUpdates(malformed, p.ReceiverAdmin)
		if err == nil && count > rejected {
			return true
		}
		logs, err := p.Receiver.Logs(malformed)
		return err == nil && (strings.Contains(logs, "Failed to load private key") || strings.Contains(logs, "Failed to load certificate"))
	}); err != nil {
		logs, _ := p.Receiver.Logs(t.Context())
		t.Fatalf("malformed leaf was not observed by actual consumer: %v %s", err, logs)
	}
	requireTransportAllowed(t, p, config)
	if err := transporttest.Project(filepath.Join(p.Directory, "receiver", "leaf"), "restored", map[string][]byte{"tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal(err)
	}
	requireTransportAllowed(t, p, config)
}

func TestTransportSecurityTrustTransition(t *testing.T) {
	p := transporttest.NewProxyPair(t)
	connection, err := grpc.NewClient(p.SourceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	heldContext, cancelHeld := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancelHeld()
	held, err := grpc_testing.NewTestServiceClient(connection).StreamingOutputCall(heldContext, &grpc_testing.StreamingOutputCallRequest{Payload: &grpc_testing.Payload{Body: []byte("trust-transition")}})
	if err != nil {
		t.Fatal(err)
	}
	if first, err := held.Recv(); err != nil || string(first.GetPayload().GetBody()) != "1" {
		t.Fatal(err)
	}
	r2 := transporttest.Must(transporttest.NewAuthority("R2"))
	old := transportClient(t, p.Root.PEM, p.Caller)
	requireTransportAllowed(t, p, old)
	caller := transporttest.Must(r2.ValidLeaf("caller.transport.test", transporttest.CallerURI))
	receiver := transporttest.Must(r2.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI))
	overlap := append(append([]byte{}, p.Root.PEM...), r2.PEM...)
	for _, role := range []string{"receiver", "caller"} {
		if err := transporttest.Project(filepath.Join(p.Directory, role, "trust"), "overlap", map[string][]byte{"ca.crt": overlap}); err != nil {
			t.Fatal(err)
		}
	}
	newClient := transportClient(t, overlap, caller)
	reload, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	if err := transporttest.Await(reload, func() bool { _, err := transportHealth(reload, p.ReceiverAddress, newClient); return err == nil }); err != nil {
		t.Fatalf("new-root client was not admitted under overlap: %v", err)
	}
	requireTransportAllowed(t, p, old)
	if err := transporttest.Project(filepath.Join(p.Directory, "receiver", "leaf"), "newroot", map[string][]byte{"tls.crt": receiver.Certificate, "tls.key": receiver.Key}); err != nil {
		t.Fatal(err)
	}
	if err := transporttest.Project(filepath.Join(p.Directory, "caller", "leaf"), "newroot", map[string][]byte{"tls.crt": caller.Certificate, "tls.key": caller.Key}); err != nil {
		t.Fatal(err)
	}
	if err := transporttest.Await(reload, func() bool {
		state, err := transportHealth(reload, p.ReceiverAddress, newClient)
		return err == nil && !state.DidResume && len(state.PeerCertificates) > 0 && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(receiver.Parsed.Raw)
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.ReleaseResponse(heldContext, "trust-transition"); err != nil {
		t.Fatal(err)
	}
	if second, err := held.Recv(); err != nil || string(second.GetPayload().GetBody()) != "2" {
		t.Fatal("admitted old-root proxy stream did not survive overlap", err)
	}
	// Join admitted old-root work before withdrawing its connection owner.
	// This is an explicit local procedure; it does not imply SDS itself closes
	// already authenticated connections or prove a Kubernetes rollout.
	cancelHeld()
	if _, err := held.Recv(); err == nil {
		t.Fatal("old proxy stream did not join cancellation")
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"receiver", "caller"} {
		if err := transporttest.Project(filepath.Join(p.Directory, role, "trust"), "retired", map[string][]byte{"ca.crt": r2.PEM}); err != nil {
			t.Fatal(err)
		}
	}
	// Trust retirement is observed using the still-time-valid R1 client, with
	// overlap receiver trust on that client so only server client-auth can deny it.
	oldCallerWithNewServerTrust := transportClient(t, overlap, p.Caller)
	if err := transporttest.Await(reload, func() bool {
		_, err := transportHealth(reload, p.ReceiverAddress, oldCallerWithNewServerTrust)
		return err != nil
	}); err != nil {
		t.Fatal("retired root remained authorized")
	}
	requireTransportDenied(t, p, oldCallerWithNewServerTrust)
	requireTransportAllowed(t, p, transportClient(t, r2.PEM, caller))
	replacement, cancelReplacement := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancelReplacement()
	p.ReplaceSource(replacement, t)
	if err := transporttest.Await(replacement, func() bool { _, err := transportHealth(replacement, p.SourceAddress, nil); return err == nil }); err != nil {
		t.Fatal("replacement source did not use final R2-only trust", err)
	}
}

func TestTransportSecurityIssuerFailure(t *testing.T) {
	p := transporttest.NewProxyPair(t)
	config := transportClient(t, p.Root.PEM, p.Caller)
	short := transporttest.Must(p.Root.Issue("receiver.transport.test", transporttest.ReceiverURI, time.Now().Add(-time.Second), time.Now().Add(5*time.Second)))
	transporttest.Must(0, transporttest.Project(filepath.Join(p.Directory, "receiver", "leaf"), "short", map[string][]byte{"tls.crt": short.Certificate, "tls.key": short.Key}))
	reload, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := transporttest.Await(reload, func() bool {
		state, err := transportHealth(reload, p.ReceiverAddress, config)
		return err == nil && len(state.PeerCertificates) > 0 && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(short.Parsed.Raw)
	}); err != nil {
		t.Fatal("short certificate never became active", err)
	}
	// Simulate issuing-service loss: no fresh leaf arrives. Established state
	// cannot extend X.509 validity for a new full handshake.
	expiry, cancelExpiry := context.WithDeadline(t.Context(), short.Parsed.NotAfter.Add(2*time.Second))
	defer cancelExpiry()
	if err := transporttest.Await(expiry, func() bool { return time.Now().After(short.Parsed.NotAfter) }); err != nil {
		t.Fatal(err)
	}
	requireTransportDenied(t, p, config)
	// The calling Envoy is the party that enforces receiver validity: on a
	// fresh full handshake it refuses the expired leaf.
	replaceSourceProxy(t, p)
	requireCallerDenied(t, p)
	renewed := transporttest.Must(p.Root.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI))
	transporttest.Must(0, transporttest.Project(filepath.Join(p.Directory, "receiver", "leaf"), "issuer-restored", map[string][]byte{"tls.crt": renewed.Certificate, "tls.key": renewed.Key}))
	restored, cancelRestored := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelRestored()
	if err := transporttest.Await(restored, func() bool {
		state, err := transportHealth(restored, p.ReceiverAddress, config)
		return err == nil && !state.DidResume && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(renewed.Parsed.Raw)
	}); err != nil {
		t.Fatal(err)
	}
	requireTransportAllowed(t, p, config)
	requireCallerAllowed(t, p)
}

func TestTransportSecurityRPCBounds(t *testing.T) {
	p := transporttest.NewProxyPair(t)
	connection, err := grpc.NewClient(p.SourceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	client := grpc_health_v1.NewHealthClient(connection)
	before := transporttest.Must(p.Operations(t.Context()))
	// A caller override longer than the short-RPC default survives the actual
	// HTTP/2 source proxy. The proxy route supplies no independent shorter timer.
	longer, cancelLonger := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancelLonger()
	start := time.Now()
	if _, err := client.Check(longer, &grpc_health_v1.HealthCheckRequest{Service: "delay:3.5s"}); err != nil {
		t.Fatal("proxy truncated nondefault caller budget", err)
	}
	if elapsed := time.Since(start); elapsed < 3*time.Second || elapsed > 7*time.Second {
		t.Fatalf("actual protected call elapsed=%s", elapsed)
	}
	parent, cancelParent := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancelParent()
	start = time.Now()
	_, parentErr := client.Check(parent, &grpc_health_v1.HealthCheckRequest{Service: "delay:3.5s"})
	transporttest.RecordBoundaryCompletion(t, p.Receiver.Name, grpc_health_v1.Health_Check_FullMethodName, "shorter-parent", start, parentErr)
	if err := parentErr; status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("shorter parent deadline lost: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("proxy cancellation unbounded: %s", elapsed)
	}
	joined, cancelJoined := context.WithTimeout(t.Context(), time.Second)
	defer cancelJoined()
	if err := transporttest.Await(joined, func() bool {
		state, err := p.BackendState(joined)
		return err == nil && state["active"] == 0 && state["cancelled"] == 1
	}); err != nil {
		t.Fatal("actual backend did not observe cancellation/join", err)
	}
	after := transporttest.Must(p.Operations(t.Context()))
	if after-before != 2 {
		t.Fatalf("proxy added replay: backend admissions=%d want2", after-before)
	}
	transporttest.MeasureCompletions(t, "routed-protected-short-rpc", p.Receiver.Name, grpc_health_v1.Health_Check_FullMethodName, func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		return err
	})
	after = transporttest.Must(p.Operations(t.Context()))
	// The only upstream is removed; no panic fallback or generic replay can
	// make an unavailable endpoint successful. Connect bound and caller bound
	// still limit the terminal failure.
	if err := p.Receiver.Signal(t.Context(), "STOP"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Receiver.Signal(ctx, "CONT")
	})
	unavailable, cancelUnavailable := context.WithTimeout(t.Context(), 1200*time.Millisecond)
	defer cancelUnavailable()
	start = time.Now()
	_, unavailableErr := client.Check(unavailable, &grpc_health_v1.HealthCheckRequest{})
	transporttest.RecordBoundaryCompletion(t, p.Receiver.Name, grpc_health_v1.Health_Check_FullMethodName, "all-unhealthy", start, unavailableErr)
	if unavailableErr == nil {
		t.Fatal("all-unhealthy upstream succeeded")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("all-unhealthy failure unbounded: %s", elapsed)
	}
	if operations := transporttest.Must(p.Operations(t.Context())); operations != after {
		t.Fatalf("stopped receiver produced extra effect: %d vs%d", operations, after)
	}
}

func TestTransportSecurityStreamFailures(t *testing.T) {
	for _, mode := range []transporttest.FaultMode{transporttest.FaultReset, transporttest.FaultBlackhole} {
		t.Run(string(mode), func(t *testing.T) {
			p := transporttest.NewProxyPair(t)
			forwarder := transporttest.Must(transporttest.NewFaultForwarder(t.Context(), p.SourceAddress))
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := forwarder.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			connection := transporttest.Must(grpc.NewClient(forwarder.Address, grpc.WithTransportCredentials(insecure.NewCredentials())))
			defer func() { _ = connection.Close() }()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			operation := "stream-" + string(mode)
			stream, err := grpc_testing.NewTestServiceClient(connection).StreamingOutputCall(ctx, &grpc_testing.StreamingOutputCallRequest{Payload: &grpc_testing.Payload{Body: []byte(operation)}})
			if err != nil {
				t.Fatal(err)
			}
			first, err := stream.Recv()
			if err != nil || string(first.GetPayload().GetBody()) != "1" {
				t.Fatalf("actual stream never admitted: %v", err)
			}
			selected := transporttest.Must(forwarder.AwaitConnection(ctx))
			if err := forwarder.Arm(operation, selected); err != nil {
				t.Fatal(err)
			}
			state := transporttest.Must(p.BackendState(ctx))
			if state["active"] != 1 {
				t.Fatal("stream admission has no active actual server operation")
			}
			if err := forwarder.Reached(operation); err != nil {
				t.Fatal(err)
			}
			if err := forwarder.Fault(operation, mode); err != nil {
				t.Fatal(err)
			}
			if mode == transporttest.FaultBlackhole {
				if err := p.ReleaseResponse(ctx, operation); err != nil {
					t.Fatal(err)
				}
			}
			start := time.Now()
			_, receiveErr := stream.Recv()
			transporttest.RecordBoundaryCompletion(t, p.Receiver.Name, grpc_testing.TestService_StreamingOutputCall_FullMethodName, string(mode), start, receiveErr)
			if receiveErr == nil {
				t.Fatal("faulted stream resumed or succeeded")
			}
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Fatalf("stream failure exceeded deadline plus1s: %s", elapsed)
			}
			joined, cancelJoined := context.WithTimeout(t.Context(), time.Second)
			defer cancelJoined()
			if err := transporttest.Await(joined, func() bool {
				state, err := p.BackendState(joined)
				return err == nil && state["active"] == 0 && state["cancelled"] == 1
			}); err != nil {
				t.Fatal("actual stream server cancellation did not join", err)
			}
			snapshot := transporttest.Must(forwarder.Snapshot(operation))
			if snapshot.ConnectionID != selected.ID {
				t.Fatal("fault did not apply to selected admitted connection")
			}
			if mode == transporttest.FaultReset {
				if !snapshot.ClientClosed || !snapshot.ServerClosed {
					t.Fatal("reset retained sockets")
				}
			} else {
				if snapshot.ClientClosed || snapshot.ServerClosed || snapshot.DiscardedServerBytes == 0 || snapshot.DiscardedClientBytes == 0 {
					t.Fatalf("blackhole did not retain/discard both directions: %+v", snapshot)
				}
			}
			if err := forwarder.Release(operation); err != nil {
				t.Fatal(err)
			}
		})
	}
}
