package tetralauth

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/auth/authtest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func externalTestHeader(key, value string) *corev3.HeaderValue {
	return &corev3.HeaderValue{Key: key, RawValue: []byte(value)}
}
func externalTestRequest(method, path, key string, authorization []string) *authv3.CheckRequest {
	headers := []*corev3.HeaderValue{externalTestHeader("x-request-id", "req_ext_authz"), externalTestHeader("x-forwarded-for", "203.0.113.10")}
	if key != "" {
		headers = append(headers, externalTestHeader("x-api-key", key))
	}
	for _, value := range authorization {
		headers = append(headers, externalTestHeader("authorization", value))
	}
	return &authv3.CheckRequest{Attributes: &authv3.AttributeContext{Request: &authv3.AttributeContext_Request{Http: &authv3.AttributeContext_HttpRequest{Id: "18446744073709551615", Method: method, Path: path, HeaderMap: &corev3.HeaderMap{Headers: headers}}}}}
}

func TestExternalRequestMetadataUsesGeneratedHeaderIdentity(t *testing.T) {
	// Envoy v1.39.2 CheckRequestUtils::setHttpRequest serializes stream_id
	// into HttpRequest.Id; the generated UUID remains a separate raw header.
	const requestID = "b321ef5e-fb32-4f42-8811-efb5b7e997aa"
	req := externalTestRequest("POST", "/v1/sessions?limit=1", "", nil)
	req.Attributes.Request.Http.HeaderMap.Headers[0].RawValue = []byte(requestID)
	metadata, err := externalRequestMetadata(req)
	if err != nil || metadata.requestID != requestID || metadata.method != "POST" || metadata.path != "/v1/sessions" {
		t.Fatal("independent Envoy stream ID changed trusted request metadata")
	}
	// A valid stream ID cannot replace a missing or duplicated trusted header.
	for _, values := range [][]string{nil, {""}, {requestID, requestID}} {
		req := externalTestRequest("POST", "/v1/sessions", "", nil)
		req.Attributes.Request.Http.HeaderMap.Headers = req.Attributes.Request.Http.HeaderMap.Headers[1:]
		for _, value := range values {
			req.Attributes.Request.Http.HeaderMap.Headers = append(req.Attributes.Request.Http.HeaderMap.Headers, externalTestHeader("x-request-id", value))
		}
		if _, err := externalRequestMetadata(req); err == nil {
			t.Fatal("stream identity substituted for required unique request header")
		}
	}
}
func externalTestStatus(response *authv3.CheckResponse) int {
	if response.GetOkResponse() != nil {
		return 200
	}
	return int(response.GetDeniedResponse().GetStatus().GetCode())
}
func externalTestPrincipal(response *authv3.CheckResponse) string {
	for _, header := range response.GetOkResponse().GetHeaders() {
		if strings.EqualFold(header.GetHeader().GetKey(), internalPrincipalHeader) {
			return header.GetHeader().GetValue()
		}
	}
	return ""
}

// TestPostgreSQLAuthExternalAuthorization exercises the real gRPC listener,
// Auth-role transactions, and signer against a private PostgreSQL database.
func TestPostgreSQLAuthExternalAuthorization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, admin, privateKey := newTestAuthRouter(t)
	workloadDB := storagetest.OpenWorkloadDB(t, admin, "auth")
	runtime := workloadDB.DB
	usage := auth.NewAPIKeyUsageRecorder(runtime, nil)
	usage.Start(ctx)
	t.Cleanup(usage.Close)
	resolver := auth.NewAuthorityResolver(runtime, "ws_auth_test", usage)
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	var diagnosticOutput bytes.Buffer
	diagnosticConfig := workload.DefaultDiagnosticConfig()
	diagnosticConfig.Level = slog.LevelDebug
	diagnosticOwner := workload.NewProcessLogger(&diagnosticOutput, "auth", "test", "owning", diagnosticConfig)
	defer diagnosticOwner.CloseWithBudget()
	adapter, err := NewExternalAuthorization(ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: resolver}, Signer: signer, PrincipalTTL: DefaultInternalPrincipalTTL, Logger: diagnosticOwner.Logger})
	if err != nil {
		t.Fatal(err)
	}
	// The validated Auth configuration owns the principal TTL and the process
	// owns its exported metrics; neither has a constructor default.
	if _, err := NewExternalAuthorization(ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: resolver}, Signer: signer, Logger: diagnosticOwner.Logger}); err == nil {
		t.Fatal("external authorization accepted an unset principal TTL")
	} else if _, ok := workload.AsConfigError(err); !ok {
		t.Fatalf("unset principal TTL error = %v; want config error", err)
	}
	if _, err := OpenExternalAuthorizationServer(ctx, Config{GRPCAddress: "127.0.0.1:0", GRPCTransport: "plaintext"}, adapter, diagnosticOwner.Logger, nil); err == nil {
		t.Fatal("Check listener opened without its operation metrics")
	} else if _, ok := workload.AsConfigError(err); !ok {
		t.Fatalf("missing Check metrics error = %v; want config error", err)
	}
	metrics := workload.NewOperationMetrics("auth")
	server, err := OpenExternalAuthorizationServer(ctx, Config{GRPCAddress: "127.0.0.1:0", GRPCTransport: "plaintext"}, adapter, diagnosticOwner.Logger, metrics)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	joined := make(chan error, 1)
	runJoined := false
	go func() { joined <- server.Run(runCtx, workload.NewReadiness()) }()
	t.Cleanup(func() {
		stop()
		if !runJoined {
			if err := <-joined; err != nil {
				t.Errorf("gRPC shutdown: %v", err)
			}
		}
		_ = server.Close()
	})
	connection, err := grpc.NewClient(server.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			t.Error("Check client close failed")
		}
	}()
	client := authv3.NewAuthorizationClient(connection)
	check := func(t *testing.T, req *authv3.CheckRequest, want int) *authv3.CheckResponse {
		t.Helper()
		outcome := "success"
		if want >= 400 {
			outcome = "rejected"
		}
		if want >= 500 {
			outcome = "error"
		}
		before := authOperationSample(t, metrics, "tetral_operation_duration_seconds_count", externalAuthorizationCheckMethod, outcome)
		response, err := client.Check(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if externalTestStatus(response) != want {
			t.Fatalf("Check status=%d want=%d", externalTestStatus(response), want)
		}
		if count := authOperationSample(t, metrics, "tetral_operation_duration_seconds_count", externalAuthorizationCheckMethod, outcome); count != before+1 {
			t.Fatalf("actual Check %d did not populate %s: %v -> %v", want, outcome, before, count)
		}
		if want != 200 {
			if response.GetOkResponse() != nil || externalTestPrincipal(response) != "" {
				t.Fatal("denial minted a principal")
			}
			var envelope errorResponse
			if err := json.Unmarshal([]byte(response.GetDeniedResponse().Body), &envelope); err != nil || envelope.Type != "error" || envelope.Error.Type == "" {
				t.Fatal("denial lost SDK JSON envelope")
			}
		}
		return response
	}
	verify := func(t *testing.T, response *authv3.CheckResponse, method, path string) auth.Principal {
		t.Helper()
		p, claims, err := signer.Verify(externalTestPrincipal(response), method, path)
		if err != nil {
			t.Fatal(err)
		}
		if p.Validate() != nil || claims.RequestID != "req_ext_authz" || claims.ForwardedFor != "203.0.113.10" {
			t.Fatal("principal lost typed authority or trusted audit metadata")
		}
		return p
	}
	t.Run("CurrentKeyTouchPathBindingAndHeaderRemoval", func(t *testing.T) {
		request := externalTestRequest("GET", "/v1/api_keys%2Fdetail?limit=1", testBootstrapAPIKey(), nil)
		for _, key := range []string{"X-Tetral-Arbitrary", "x-TeTrAl-Workspace-Id", "X-Original-Method", "x-original-path", "X-Tetral-Internal-Principal"} {
			request.Attributes.Request.Http.HeaderMap.Headers = append(request.Attributes.Request.Http.HeaderMap.Headers, externalTestHeader(key, "forged"))
		}
		response := check(t, request, 200)
		p := verify(t, response, "GET", "/v1/api_keys/detail")
		if p.Workspace.ID != "ws_auth_test" || p.APIKeyID == "" || p.Identity != nil || p.Credential.Kind != auth.CredentialAPIKey {
			t.Fatal("key acquired forged identity or workspace")
		}
		if len(response.GetOkResponse().Headers) != 1 || response.GetOkResponse().Headers[0].AppendAction != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
			t.Fatal("allow must overwrite exactly one internal principal")
		}
		removed := map[string]bool{}
		for _, key := range response.GetOkResponse().HeadersToRemove {
			removed[key] = true
		}
		for _, key := range []string{"x-api-key", "x-tetral-arbitrary", "x-tetral-workspace-id", "x-original-method", "x-original-path"} {
			if !removed[key] {
				t.Fatalf("untrusted header %s survived", key)
			}
		}
		if removed[internalPrincipalHeader] {
			t.Fatal("allow removes newly minted principal")
		}
		for _, binding := range []struct{ method, path string }{{"POST", "/v1/api_keys/detail"}, {"GET", "/v1/api_keys%2Fdetail"}, {"GET", "/v1/api_keys/detail?limit=1"}} {
			if _, _, err := signer.Verify(externalTestPrincipal(response), binding.method, binding.path); err == nil {
				t.Fatal("principal verified wrong method/path binding")
			}
		}
		awaitAPIKeyUsage(ctx, t, admin, p.APIKeyID)
	})
	t.Run("ForeignKeyKeepsActualWorkspace", func(t *testing.T) {
		if _, err := workspace.NewSeeder(admin).Seed(ctx, "ws_foreign_ext", "foreign"); err != nil {
			t.Fatal(err)
		}
		key, err := authtest.SeedIndependentKey(ctx, runtime, "ws_foreign_ext", "foreign")
		if err != nil {
			t.Fatal(err)
		}
		p := verify(t, check(t, externalTestRequest("GET", "/v1/api_keys", key.APIKey, nil), 200), "GET", "/v1/api_keys")
		if p.Workspace.ID != "ws_foreign_ext" || p.APIKeyID != key.ID {
			t.Fatal("foreign key was coerced into bootstrap workspace")
		}
	})
	t.Run("MetadataDenials", func(t *testing.T) {
		cases := map[string]func(*authv3.AttributeContext_HttpRequest){
			"missing request id": func(h *authv3.AttributeContext_HttpRequest) { h.HeaderMap.Headers = h.HeaderMap.Headers[1:] },
			"duplicate request id": func(h *authv3.AttributeContext_HttpRequest) {
				h.HeaderMap.Headers = append(h.HeaderMap.Headers, externalTestHeader("X-Request-ID", "duplicate"))
			},
			"missing forwarded for": func(h *authv3.AttributeContext_HttpRequest) {
				h.HeaderMap.Headers = append(h.HeaderMap.Headers[:1], h.HeaderMap.Headers[2:]...)
			},
			"duplicate forwarded for": func(h *authv3.AttributeContext_HttpRequest) {
				h.HeaderMap.Headers = append(h.HeaderMap.Headers, externalTestHeader("X-Forwarded-For", "127.0.0.1"))
			},
			"empty request id": func(h *authv3.AttributeContext_HttpRequest) { h.HeaderMap.Headers[0].RawValue = nil },
			"method mismatch": func(h *authv3.AttributeContext_HttpRequest) {
				h.HeaderMap.Headers = append(h.HeaderMap.Headers, externalTestHeader(":method", "POST"))
			},
			"path mismatch": func(h *authv3.AttributeContext_HttpRequest) {
				h.HeaderMap.Headers = append(h.HeaderMap.Headers, externalTestHeader(":path", "/other"))
			},
			"invalid path":   func(h *authv3.AttributeContext_HttpRequest) { h.Path = "/%zz" },
			"invalid method": func(h *authv3.AttributeContext_HttpRequest) { h.Method = "GET\r\n" },
			"combined header map": func(h *authv3.AttributeContext_HttpRequest) {
				h.Headers = map[string]string{"x-api-key": testBootstrapAPIKey()}
			},
			"missing raw headers": func(h *authv3.AttributeContext_HttpRequest) { h.HeaderMap = nil },
		}
		for name, mutate := range cases {
			t.Run(name, func(t *testing.T) {
				req := externalTestRequest("GET", "/v1/sessions", testBootstrapAPIKey(), nil)
				mutate(req.Attributes.Request.Http)
				check(t, req, 400)
			})
		}
	})
	t.Run("FrozenBearerAndCredentialSelection", func(t *testing.T) {
		bearer, issuerCalls := externalTestBearer(ctx, t, admin, runtime)
		before := issuerCalls()
		key, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", "selected")
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name          string
			keys          []string
			authorization []string
			want          int
			keyActor      bool
		}{
			{"key ignores duplicated bearer", []string{key.APIKey}, []string{"Bearer invalid", "Bearer " + bearer}, 200, true},
			{"invalid key never falls back", []string{"invalid"}, []string{"Bearer " + bearer}, 401, false},
			{"whitespace key stays exact", []string{" " + key.APIKey}, []string{"Bearer " + bearer}, 401, false},
			{"first repeated key wins", []string{key.APIKey, "invalid"}, []string{"Bearer " + bearer}, 200, true},
			{"invalid first key wins", []string{"invalid", key.APIKey}, []string{"Bearer " + bearer}, 401, false},
			{"empty first key admits bearer", []string{"", key.APIKey}, []string{"bEaReR " + bearer}, 200, false},
			{"repeated bearer denied", nil, []string{"Bearer " + bearer, "Bearer " + bearer}, 401, false},
			{"combined bearer denied", nil, []string{"Bearer " + bearer + ", Bearer invalid"}, 401, false},
			{"bare bearer denied", nil, []string{"Bearer"}, 401, false},
			{"unselected token in query denied", nil, nil, 401, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				request := externalTestRequest("GET", "/v1/sessions?api_key="+key.APIKey, "", tc.authorization)
				for _, value := range tc.keys {
					request.Attributes.Request.Http.HeaderMap.Headers = append(request.Attributes.Request.Http.HeaderMap.Headers, externalTestHeader("x-aPi-kEy", value))
				}
				response := check(t, request, tc.want)
				if tc.want == 200 {
					p := verify(t, response, "GET", "/v1/sessions")
					if tc.keyActor {
						if p.APIKeyID != key.ID || p.Identity != nil {
							t.Fatal("key borrowed bearer identity")
						}
					} else if p.APIKeyID != "" || p.Identity == nil || p.Identity.Kind != auth.IdentityService || p.Authority.Kind != auth.AuthorityIdentityGrant {
						t.Fatal("bearer lost stable typed identity")
					}
				}
			})
		}
		if issuerCalls() != before {
			t.Fatal("frozen bearer admission contacted issuer")
		}
		if _, err := auth.NewPolicyStore(admin).Apply(ctx, auth.PolicyDocument{RevokeWorkspaceGrants: []string{"grant_ext_service"}}); err != nil {
			t.Fatal(err)
		}
		check(t, externalTestRequest("GET", "/v1/sessions", "", []string{"Bearer " + bearer}), 401)
	})
	for _, admissionFirst := range []bool{false, true} {
		name := "RevocationBeforeCheck"
		if admissionFirst {
			name = "CheckBeforeRevocation"
		}
		t.Run(name, func(t *testing.T) {
			key, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", name)
			if err != nil {
				t.Fatal(err)
			}
			held, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := held.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
					t.Error("Check lock transaction rollback failed")
				}
			}()
			var holder int
			if err := held.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
				t.Fatal(err)
			}
			if admissionFirst {
				_, err = held.ExecContext(ctx, `LOCK TABLE workspaces IN ACCESS EXCLUSIVE MODE`)
			} else {
				_, err = held.ExecContext(ctx, `UPDATE api_keys SET revoked_at=clock_timestamp() WHERE id=$1`, key.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan *authv3.CheckResponse, 1)
			failure := make(chan error, 1)
			go func() {
				response, err := client.Check(ctx, externalTestRequest("GET", "/v1/sessions", key.APIKey, nil))
				if err != nil {
					failure <- err
				} else {
					result <- response
				}
			}()
			waiter := externalAwaitBlock(ctx, t, admin, holder)
			revoked := make(chan error, 1)
			if admissionFirst {
				go func() {
					_, err := admin.ExecContext(ctx, `UPDATE api_keys SET revoked_at=clock_timestamp() WHERE id=$1`, key.ID)
					revoked <- err
				}()
				externalAwaitBlock(ctx, t, admin, waiter)
			}
			if err := held.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-failure:
				t.Fatal(err)
			case response := <-result:
				want := 401
				if admissionFirst {
					want = 200
					verify(t, response, "GET", "/v1/sessions")
				}
				if externalTestStatus(response) != want {
					t.Fatal("concurrent revocation order lost")
				}
			case <-ctx.Done():
				t.Fatal("Check did not join")
			}
			if admissionFirst {
				if err := <-revoked; err != nil {
					t.Fatal(err)
				}
			}
			check(t, externalTestRequest("GET", "/v1/sessions", key.APIKey, nil), 401)
		})
	}
	t.Run("CancelledDatabaseWait", func(t *testing.T) {
		key, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", "cancelled")
		if err != nil {
			t.Fatal(err)
		}
		held, err := admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := held.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				t.Error("Check lock transaction rollback failed")
			}
		}()
		var holder int
		if err := held.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
			t.Fatal(err)
		}
		if _, err := held.ExecContext(ctx, `UPDATE api_keys SET name=name WHERE id=$1`, key.ID); err != nil {
			t.Fatal(err)
		}
		cancelCtx, stopCheck := context.WithCancel(ctx)
		defer stopCheck()
		result := make(chan *authv3.CheckResponse, 1)
		go func() {
			response, _ := adapter.Check(cancelCtx, externalTestRequest("GET", "/v1/sessions", key.APIKey, nil))
			result <- response
		}()
		externalAwaitBlock(ctx, t, admin, holder)
		stopCheck()
		select {
		case response := <-result:
			if externalTestStatus(response) != 500 || externalTestPrincipal(response) != "" {
				t.Fatal("cancelled DB Check minted a principal")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("caller cancellation did not interrupt DB wait")
		}
		if err := held.Rollback(); err != nil {
			t.Fatal(err)
		}
		flushAPIKeyUsage(ctx, t, admin, runtime, resolver)
		var used sql.NullTime
		if err := admin.QueryRowContext(ctx, `SELECT last_used_at FROM api_keys WHERE id=$1`, key.ID).Scan(&used); err != nil || used.Valid {
			t.Fatal("cancelled Check submitted usage")
		}
	})
	t.Run("DefaultInfoAdmissionAndSinkSafety", func(t *testing.T) {
		var quiet bytes.Buffer
		owner := workload.NewProcessLogger(&quiet, "auth", "test", "quiet", workload.DefaultDiagnosticConfig())
		good, err := NewExternalAuthorization(ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: resolver}, Signer: signer, PrincipalTTL: DefaultInternalPrincipalTTL, Logger: owner.Logger})
		if err != nil {
			t.Fatal(err)
		}
		response, err := good.Check(ctx, externalTestRequest("GET", "/v1/sessions", testBootstrapAPIKey(), nil))
		if err != nil || externalTestStatus(response) != 200 {
			t.Fatal("healthy admission failed")
		}
		owner.CloseWithBudget()
		if quiet.Len() != 0 {
			t.Fatal("healthy admission emitted default INFO diagnostics")
		}
		throwing, err := NewExternalAuthorization(ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: resolver}, Signer: signer, PrincipalTTL: DefaultInternalPrincipalTTL, Logger: slog.New(externalThrowingDiagnostics{})})
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			key  string
			want int
		}{{testBootstrapAPIKey(), 200}, {"invalid", 401}} {
			response, err := throwing.Check(ctx, externalTestRequest("GET", "/v1/sessions", tc.key, nil))
			if err != nil || externalTestStatus(response) != tc.want {
				t.Fatal("diagnostic sink changed Check outcome")
			}
		}
	})

	t.Run("CheckExposesClosedDatabaseFailureWithoutPrincipalOrUsage", func(t *testing.T) {
		key, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", "unavailable Check")
		if err != nil {
			t.Fatal(err)
		}
		closedDB := workloadDB.OpenWorkload(t, "auth", nil)
		if err := closedDB.Close(); err != nil {
			t.Fatal(err)
		}
		failedAdapter, err := NewExternalAuthorization(ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: auth.NewAuthorityResolver(closedDB, "ws_auth_test", usage)}, Signer: signer, PrincipalTTL: DefaultInternalPrincipalTTL, Logger: diagnosticOwner.Logger})
		if err != nil {
			t.Fatal(err)
		}
		failedMetrics := workload.NewOperationMetrics("auth")
		failedServer, err := OpenExternalAuthorizationServer(ctx, Config{GRPCAddress: "127.0.0.1:0", GRPCTransport: "plaintext"}, failedAdapter, diagnosticOwner.Logger, failedMetrics)
		if err != nil {
			t.Fatal(err)
		}
		failedCtx, stopFailed := context.WithCancel(ctx)
		failedJoined := make(chan error, 1)
		go func() { failedJoined <- failedServer.Run(failedCtx, workload.NewReadiness()) }()
		defer func() {
			stopFailed()
			if err := <-failedJoined; err != nil {
				t.Error(err)
			}
			_ = failedServer.Close()
		}()
		conn, err := grpc.NewClient(failedServer.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := conn.Close(); err != nil {
				t.Error("failed Check client close failed")
			}
		}()
		response, err := authv3.NewAuthorizationClient(conn).Check(ctx, externalTestRequest("GET", "/v1/sessions", key.APIKey, nil))
		if err != nil || externalTestStatus(response) != 500 || externalTestPrincipal(response) != "" {
			t.Fatalf("closed owning DB Check=%v/%v", response, err)
		}
		if authOperationSample(t, failedMetrics, "tetral_operation_duration_seconds_count", externalAuthorizationCheckMethod, "error") != 1 || authOperationSample(t, failedMetrics, "tetral_operation_duration_seconds_count", externalAuthorizationCheckMethod, "success") != 0 {
			t.Fatal("typed 500 was counted as successful gRPC admission")
		}
		flushAPIKeyUsage(ctx, t, admin, runtime, resolver)
		var used sql.NullTime
		if err := admin.QueryRowContext(ctx, `SELECT last_used_at FROM api_keys WHERE id=$1`, key.ID).Scan(&used); err != nil || used.Valid {
			t.Fatal("unavailable Check submitted credential usage")
		}
	})
	t.Run("NativeTLSGenerationAndPeerAdmission", func(t *testing.T) {
		externalTestNativeTLS(ctx, t, admin, workloadDB, runtime, usage, adapter, signer)
	})
	stop()
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	runJoined = true
	if authOperationSample(t, metrics, "tetral_operation_duration_seconds_count", "shutdown_grpc_drain", "success") != 1 {
		t.Fatal("Auth graceful owner did not populate its actual drain")
	}
	diagnosticOwner.CloseWithBudget()
	classes := map[string]bool{}
	provenance := false
	correlated := false
	for _, line := range bytes.Split(bytes.TrimSpace(diagnosticOutput.Bytes()), []byte("\n")) {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if class, ok := event["error.class"].(string); ok {
			classes[class] = true
		}
		if event["auth.identity.kind"] == auth.IdentityService && event["auth.rule.revision"] == float64(1) && event["auth.identity.revision"] == float64(1) && event["auth.grant.revision"] == float64(1) {
			provenance = true
		}
		if event["auth.stage"] == "admission" && event["request.id"] == "req_ext_authz" {
			correlated = true
		}
	}
	if !classes["authentication_error"] || !classes["dependency_unavailable"] || !provenance || !correlated {
		t.Fatalf("Check diagnostics lost established class/provenance/correlation: classes=%v provenance=%t correlation=%t", classes, provenance, correlated)
	}
	for _, forbidden := range []string{testBootstrapAPIKey(), "tetral_sk_", "tetral_at_", "ws_auth_test", "identity_ext_service", "rule_ext_service", "grant_ext_service", "database is closed", "/v1/sessions"} {
		if strings.Contains(diagnosticOutput.String(), forbidden) {
			t.Fatal("Check diagnostic disclosed credentials/selectors or raw dependency material")
		}
	}

}

// awaitAPIKeyUsage waits for the production usage worker, which samples a
// committed admission on a later one-second tick, to write keyID.
func awaitAPIKeyUsage(ctx context.Context, t *testing.T, admin *sql.DB, keyID string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var used bool
		if err := admin.QueryRowContext(ctx, `SELECT last_used_at IS NOT NULL FROM api_keys WHERE id=$1`, keyID).Scan(&used); err != nil {
			t.Fatal(err)
		}
		if used {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("production usage worker did not write the admitted key")
		case <-ticker.C:
		}
	}
}

// flushAPIKeyUsage admits a fresh control key through resolver and waits for
// its sample. A sample submitted earlier for another fresh key through the same
// recorder is due no later than the control's, so it has been attempted by
// then; a negative usage assertion after this call is not vacuous.
func flushAPIKeyUsage(ctx context.Context, t *testing.T, admin, runtime *sql.DB, resolver *auth.AuthorityResolver) {
	t.Helper()
	control, err := authtest.SeedIndependentKey(ctx, runtime, "ws_auth_test", "usage flush control")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.AuthenticateKey(ctx, control.APIKey); err != nil {
		t.Fatal(err)
	}
	awaitAPIKeyUsage(ctx, t, admin, control.ID)
}

func externalAwaitBlock(ctx context.Context, t *testing.T, admin *sql.DB, holder int) int {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiter int
		err := admin.QueryRowContext(ctx, `SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) ORDER BY pid LIMIT 1`, holder).Scan(&waiter)
		if err == nil {
			return waiter
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("Check did not reach real row-lock boundary")
		case <-ticker.C:
		}
	}
}

func externalTestBearer(ctx context.Context, t *testing.T, admin, runtime *sql.DB) (string, func() int64) {
	t.Helper()
	issuer, key, unavailable, calls := newTestFederationIssuer(t)
	t.Cleanup(issuer.Close)
	rule := auth.FederationRule{ID: "rule_ext_service", OrganizationID: "org_ext_service", Issuer: issuer.URL, Audience: "tetral-engine", JWKSURL: issuer.URL + "/keys", Algorithm: "RS256", AllowedCIDRs: []string{"127.0.0.1/32"}, TrustedCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw})), Enabled: true}
	document := auth.PolicyDocument{FederationRules: []auth.FederationRule{rule}, Identities: []auth.IdentityBinding{{ID: "identity_ext_service", OrganizationID: rule.OrganizationID, Issuer: rule.Issuer, Subject: "service", Kind: auth.IdentityService, ServiceAccountID: "service_ext", Enabled: true}}, WorkspaceGrants: []auth.WorkspaceGrant{{ID: "grant_ext_service", IdentityID: "identity_ext_service", WorkspaceID: "ws_auth_test", Role: auth.WorkspaceFullAccess, Enabled: true}}}
	if _, err := auth.NewPolicyStore(admin).Apply(ctx, document); err != nil {
		t.Fatal(err)
	}
	resolver := auth.NewAuthorityResolver(runtime, "ws_auth_test", nil)
	loaded, err := resolver.LoadFederationRule(ctx, rule.ID, rule.OrganizationID)
	if err != nil {
		t.Fatal(err)
	}
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "exchange-key"})
	claims, _ := json.Marshal(map[string]any{"iss": issuer.URL, "sub": "service", "aud": rule.Audience, "exp": time.Now().Add(20 * time.Minute).Unix()})
	content := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(content))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewAssertionVerifier(ctx)
	defer verifier.Close()
	proof, err := verifier.Verify(ctx, loaded, content+"."+base64.RawURLEncoding.EncodeToString(signature))
	if err != nil {
		t.Fatal(err)
	}
	token, err := resolver.Issue(ctx, proof, auth.ExchangeSelectors{ServiceAccountID: "service_ext"})
	if err != nil {
		t.Fatal(err)
	}
	unavailable.Store(true)
	return token.AccessToken, calls.Load
}

type externalThrowingDiagnostics struct{}

func (externalThrowingDiagnostics) Enabled(context.Context, slog.Level) bool { return true }
func (externalThrowingDiagnostics) Handle(context.Context, slog.Record) error {
	panic("diagnostic sink failed")
}
func (h externalThrowingDiagnostics) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h externalThrowingDiagnostics) WithGroup(string) slog.Handler      { return h }
