package integration

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
)

const directRunnerURI = "spiffe://cluster.local/ns/tetral-system/sa/job-runner"
const directRuntimeURI = "spiffe://cluster.local/ns/tetral-agent-runtime/sa/agent-runtime"
const directRuntimeDNS = "agent-runtime.tetral-agent-runtime.svc.cluster.local"

func directInput(p *transporttest.RuntimePair, id string) *agentruntimev1.AcceptInputRequest {
	return &agentruntimev1.AcceptInputRequest{WorkspaceId: "wksp_transport", SessionId: "sesn_transport", SessionThreadId: "thrd_transport", BindingId: "bind_transport", BindingGeneration: 42, TargetPodUid: p.PodUID, RuntimeProcessId: p.ProcessID, RuntimeInputId: id, InputOrder: 1, Content: &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: `{"messages":[]}`}}
}
func directCall(ctx context.Context, p *transporttest.RuntimePair, request *agentruntimev1.AcceptInputRequest, config *tls.Config, authority string) (*agentruntimev1.AcceptInputResponse, tls.ConnectionState, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "bearer fixture-runner")
	transport := insecure.NewCredentials()
	if config != nil {
		transport = credentials.NewTLS(config)
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(transport)}
	if authority != "" {
		options = append(options, grpc.WithAuthority(authority))
	}
	connection, err := grpc.NewClient(p.Address, options...)
	if err != nil {
		return nil, tls.ConnectionState{}, err
	}
	defer func() { _ = connection.Close() }()
	var remote peer.Peer
	response, err := agentruntimev1.NewAgentRuntimePodServiceClient(connection).AcceptInput(ctx, request, grpc.Peer(&remote))
	var state tls.ConnectionState
	if auth, ok := remote.AuthInfo.(credentials.TLSInfo); ok {
		state = auth.State
	}
	return response, state, err
}
func directTLS(t *testing.T, root []byte, leaf transporttest.Leaf, name string) *tls.Config {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(root) {
		t.Fatal("invalid direct fixture root")
	}
	cfg := &tls.Config{RootCAs: roots, ServerName: name, MinVersion: tls.VersionTLS13, NextProtos: []string{"h2"}, SessionTicketsDisabled: true}
	if len(leaf.Certificate) > 0 {
		pair, err := tls.X509KeyPair(leaf.Certificate, leaf.Key)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg
}
func requireDirectAccepted(t *testing.T, p *transporttest.RuntimePair, id string, cfg *tls.Config) {
	t.Helper()
	response, state, err := directCall(t.Context(), p, directInput(p, id), cfg, "")
	if err != nil || response.GetAccepted() == nil {
		t.Fatalf("actual Runtime handler rejected allowed direct caller: response=%v error=%v", response, err)
	}
	if cfg != nil {
		if state.DidResume || state.Version != tls.VersionTLS13 || len(state.PeerCertificates) == 0 {
			t.Fatal("direct caller did not observe a fresh TLS1.3 handshake")
		}
		found := false
		for _, uri := range state.PeerCertificates[0].URIs {
			found = found || uri.String() == directRuntimeURI
		}
		if !found {
			t.Fatal("direct caller authenticated an unexpected Runtime role")
		}
	}
}

func TestRuntimeDirectTLS(t *testing.T) {
	root := transporttest.Must(transporttest.NewAuthority("runtime-native"))
	caller := transporttest.Must(root.ValidLeaf("runner.transport.test", directRunnerURI))
	cfg := directTLS(t, root.PEM, caller, directRuntimeDNS)
	t.Run("ProtectedProfile", func(t *testing.T) {
		p := transporttest.NewRuntimePair(t, "hardened", "runtime-a", root, true)
		other := transporttest.NewRuntimePair(t, "hardened", "runtime-b", root, true)
		requireDirectAccepted(t, p, "input-initial", cfg)
		otherState := transporttest.Must(other.State(t.Context()))
		if otherState.Operations != 0 {
			t.Fatal("direct delivery reached the other Runtime")
		}
		for _, address := range []string{p.PlainAddress, p.LoopbackAddress} {
			// Docker's host port forwarder may accept TCP before discovering
			// that the container has no listener. Require protocol admission.
			forbidden := *p
			forbidden.Address = address
			if _, _, err := directCall(t.Context(), &forbidden, directInput(p, "forbidden-port"), nil, ""); err == nil {
				t.Fatal("hardened Runtime exposed a forbidden application listener")
			}
		}
		wrongRoot := transporttest.Must(transporttest.NewAuthority("wrong"))
		wrongRole := transporttest.Must(root.ValidLeaf("other.transport.test", "spiffe://cluster.local/ns/tetral-system/sa/other"))
		expired := transporttest.Must(root.Issue("runner.transport.test", directRunnerURI, time.Now().Add(-120*time.Second), time.Now().Add(-60*time.Second)))
		cases := []struct {
			name   string
			config *tls.Config
		}{{"Plaintext", nil}, {"MissingClient", directTLS(t, root.PEM, transporttest.Leaf{}, directRuntimeDNS)}, {"WrongTrust", directTLS(t, wrongRoot.PEM, caller, directRuntimeDNS)}, {"WrongName", directTLS(t, root.PEM, caller, "wrong.transport.test")}, {"WrongRole", directTLS(t, root.PEM, wrongRole, directRuntimeDNS)}, {"ExpiredClient", directTLS(t, root.PEM, expired, directRuntimeDNS)}}
		for _, variant := range cases {
			t.Run(variant.name, func(t *testing.T) {
				before := transporttest.Must(p.State(t.Context()))
				if _, _, err := directCall(t.Context(), p, directInput(p, "denied-"+variant.name), variant.config, ""); err == nil {
					t.Fatal("protected direct listener accepted denied transport")
				}
				after := transporttest.Must(p.State(t.Context()))
				if after.Operations != before.Operations {
					t.Fatal("denied transport admitted Runtime work")
				}
				requireDirectAccepted(t, p, "allowed-"+variant.name, cfg)
			})
		}
		for _, variant := range []string{"PodUID", "ProcessID", "Binding"} {
			request := directInput(p, "fence-"+variant)
			switch variant {
			case "PodUID":
				request.TargetPodUid = other.PodUID
			case "ProcessID":
				request.RuntimeProcessId = other.ProcessID
			case "Binding":
				request.BindingId = "different-binding"
			}
			before := transporttest.Must(p.State(t.Context()))
			response, _, err := directCall(t.Context(), p, request, cfg, "")
			if (variant != "ProcessID" || status.Code(err) != codes.FailedPrecondition) && (err != nil || response.GetRejected() == nil) {
				t.Fatalf("actual Runtime did not reject %s fence: %v %v", variant, response, err)
			}
			after := transporttest.Must(p.State(t.Context()))
			if after.Operations != before.Operations {
				t.Fatal("rejected scope admitted work")
			}
		}
		// Hold an actual unary Runtime handler across SDS renewal, then observe
		// the new certificate on a separate full handshake before releasing it.
		if err := p.Command(t.Context(), "hold"); err != nil {
			t.Fatal(err)
		}
		connection, err := grpc.NewClient(p.Address, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = connection.Close() }()
		ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(t.Context(), "authorization", "bearer fixture-runner"), 20*time.Second)
		defer cancel()
		result := make(chan error, 1)
		before := transporttest.Must(p.State(t.Context()))
		go func() {
			response, err := agentruntimev1.NewAgentRuntimePodServiceClient(connection).AcceptInput(ctx, directInput(p, "held-renewal"))
			if err == nil && response.GetAccepted() == nil {
				err = context.Canceled
			}
			result <- err
		}()
		if err := transporttest.Await(ctx, func() bool { state, err := p.State(ctx); return err == nil && state.Operations == before.Operations+1 }); err != nil {
			t.Fatal(err)
		}
		renewed := transporttest.Must(root.ValidLeaf(directRuntimeDNS, directRuntimeURI))
		if err := transporttest.Project(filepath.Join(p.Directory, "leaf"), "renewed", map[string][]byte{"tls.crt": renewed.Certificate, "tls.key": renewed.Key}); err != nil {
			t.Fatal(err)
		}
		reload, stop := context.WithTimeout(t.Context(), 10*time.Second)
		defer stop()
		if err := transporttest.Await(reload, func() bool {
			dialer := &tls.Dialer{Config: cfg}
			raw, err := dialer.DialContext(reload, "tcp", p.Address)
			if err != nil {
				return false
			}
			defer func() { _ = raw.Close() }()
			state := raw.(*tls.Conn).ConnectionState()
			return !state.DidResume && len(state.PeerCertificates) > 0 && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(renewed.Parsed.Raw)
		}); err != nil {
			t.Fatal("direct listener did not reload its leaf")
		}
		if err := p.Command(t.Context(), "release"); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatalf("held actual Runtime RPC failed across renewal: %v", err)
		}
		// Exercise the shared production Go credential owner against the exact
		// rendered endpoint and required service DNS + Runtime role.
		callerDir := t.TempDir()
		if err := transporttest.Project(callerDir, "g1", map[string][]byte{"ca.crt": root.PEM, "tls.crt": caller.Certificate, "tls.key": caller.Key}); err != nil {
			t.Fatal(err)
		}
		owner, err := transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: filepath.Join(callerDir, "ca.crt"), CertPath: filepath.Join(callerDir, "tls.crt"), KeyPath: filepath.Join(callerDir, "tls.key")})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Close() }()
		productionCfg, err := owner.ClientTLSConfig(directRuntimeDNS, directRuntimeURI)
		if err != nil {
			t.Fatal(err)
		}
		requireDirectAccepted(t, p, "production-loader", productionCfg)
		otherState = transporttest.Must(other.State(t.Context()))
		if otherState.Operations != 0 {
			t.Fatal("selected direct endpoint routed to another Runtime")
		}
	})
	t.Run("StandardProfileControl", func(t *testing.T) {
		p := transporttest.NewRuntimePair(t, "standard-routed", "runtime-standard", root, true)
		response, _, err := directCall(t.Context(), p, directInput(p, "standard"), nil, "unrelated-authority.invalid")
		if err != nil || response.GetAccepted() == nil {
			t.Fatalf("standard19090 ignored-authority contract failed: %v %v", response, err)
		}
		if _, _, err := directCall(t.Context(), p, directInput(p, "tls-not-standard"), cfg, ""); err == nil {
			t.Fatal("standard listener unexpectedly accepted TLS")
		}
	})
	t.Run("MandatoryProxyAbsent", func(t *testing.T) {
		p := transporttest.NewRuntimePair(t, "standard-routed", "runtime-no-proxy", root, false)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		code, err := p.Runtime.Wait(ctx)
		if err != nil || code == 0 {
			t.Fatalf("missing mandatory proxy did not fail startup: code=%d err=%v", code, err)
		}
		if _, _, err := directCall(t.Context(), p, directInput(p, "no-proxy"), nil, ""); err == nil {
			t.Fatal("Runtime admitted work without mandatory proxy")
		}
	})
	t.Run("HardenedMandatoryProxyAbsent", func(t *testing.T) {
		p := transporttest.NewRuntimePair(t, "hardened", "hard-no-proxy", root, false)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		code, err := p.Runtime.Wait(ctx)
		if err != nil || code == 0 {
			t.Fatalf("hardened missing proxy admitted startup: %d %v", code, err)
		}
	})
	for _, missing := range []string{"leaf", "validation"} {
		t.Run("InitialCredentialUnavailable/"+missing, func(t *testing.T) {
			p := transporttest.NewRuntimeWithoutInitialCredentials(t, "hard-missing-"+missing, root, missing)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			code, err := p.Runtime.Wait(ctx)
			if err != nil || code == 0 {
				probe, cancelProbe := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancelProbe()
				request, _ := http.NewRequestWithContext(probe, "GET", p.Admin+"/stats?filter=(sds|warming|ssl_context)&format=json", nil)
				res, probeErr := http.DefaultClient.Do(request)
				var stats []byte
				if probeErr == nil {
					stats, _ = io.ReadAll(res.Body)
					_ = res.Body.Close()
				}
				t.Fatalf("cold missing credential admitted startup: %d %v stats=%s", code, err, stats)
			}
			if _, _, err := directCall(t.Context(), p, directInput(p, "cold-missing"), cfg, ""); err == nil {
				t.Fatal("cold missing credential admitted business operation")
			}
		})
	}
	t.Run("CompletionSamples", func(t *testing.T) {
		for _, profile := range []string{"standard-routed", "hardened"} {
			t.Run(profile, func(t *testing.T) {
				p := transporttest.NewRuntimePair(t, profile, "measured-"+profile, root, true)
				var selected *tls.Config
				if profile == "hardened" {
					selected = cfg
				}
				sequence := 0
				transporttest.MeasureCompletions(t, "runtime-direct-cold-"+profile, p.PodUID, agentruntimev1.AgentRuntimePodService_AcceptInput_FullMethodName, func() error {
					sequence++
					response, _, err := directCall(t.Context(), p, directInput(p, fmt.Sprintf("measured-cold-%d", sequence)), selected, "")
					if err == nil && response.GetAccepted() == nil {
						return errors.New("actual Runtime did not admit measured call")
					}
					return err
				})
				transport := insecure.NewCredentials()
				if selected != nil {
					transport = credentials.NewTLS(selected)
				}
				connection, err := grpc.NewClient(p.Address, grpc.WithTransportCredentials(transport))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = connection.Close() }()
				client := agentruntimev1.NewAgentRuntimePodServiceClient(connection)
				transporttest.MeasureCompletions(t, "runtime-direct-pooled-"+profile, p.PodUID, agentruntimev1.AgentRuntimePodService_AcceptInput_FullMethodName, func() error {
					sequence++
					ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(t.Context(), "authorization", "bearer fixture-runner"), 2*time.Second)
					defer cancel()
					response, err := client.AcceptInput(ctx, directInput(p, fmt.Sprintf("measured-pooled-%d", sequence)))
					if err == nil && response.GetAccepted() == nil {
						return errors.New("actual Runtime did not admit measured call")
					}
					return err
				})
			})
		}
	})

}
