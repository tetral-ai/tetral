package tetralauth

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workload"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const ExternalAuthorizationTimeout = 5 * time.Second
const internalPrincipalHeader = "x-tetral-internal-principal"

// ExternalAuthorizationConfig borrows the Auth application's domain owners.
// It performs no bootstrap mutation, transport construction, or database opening.
type ExternalAuthorizationConfig struct {
	Authenticator *auth.RequestAuthenticator
	Signer        *auth.InternalPrincipalSigner
	PrincipalTTL  time.Duration
	Logger        *slog.Logger
}

type ExternalAuthorization struct {
	authv3.UnimplementedAuthorizationServer
	cfg ExternalAuthorizationConfig
}

func NewExternalAuthorization(cfg ExternalAuthorizationConfig) (*ExternalAuthorization, error) {
	if cfg.Authenticator == nil || cfg.Authenticator.Resolver == nil || cfg.Signer == nil {
		return nil, errors.New("external authorization requires authenticator and signer")
	}
	if cfg.PrincipalTTL == 0 {
		cfg.PrincipalTTL = DefaultInternalPrincipalTTL
	}
	if cfg.PrincipalTTL <= 0 || cfg.PrincipalTTL > auth.MaxInternalPrincipalTTL {
		return nil, workload.NewConfigError("external authorization principal TTL is invalid")
	}
	if cfg.Logger == nil {
		cfg.Logger = workload.ComponentLogger("auth")
	}
	return &ExternalAuthorization{cfg: cfg}, nil
}

func (a *ExternalAuthorization) Register(registrar grpc.ServiceRegistrar) {
	authv3.RegisterAuthorizationServer(registrar, a)
}

// Check reads only request metadata. The edge must use encode_raw_headers so
// credential selection retains the existing first-header API-key semantics.
func (a *ExternalAuthorization) Check(ctx context.Context, req *authv3.CheckRequest) (response *authv3.CheckResponse, transportErr error) {
	requestID := ""
	defer func() {
		if recover() != nil {
			logExternalAuthorizationPanic(a.cfg.Logger)
			response = deniedExternalAuthorization(errors.New("internal error"), requestID)
			transportErr = nil
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, ExternalAuthorizationTimeout)
	defer cancel()
	metadata, err := externalRequestMetadata(req)
	if err != nil {
		auth.RecordDecision(auth.WithAuditRecorder(ctx, externalAuditRecorder{logger: a.cfg.Logger}), "input", err, auth.AuditEvent{})
		return deniedExternalAuthorization(err, ""), nil
	}
	requestID = metadata.requestID
	ctx = auth.WithAuditRecorder(ctx, externalAuditRecorder{logger: a.cfg.Logger, requestID: requestID})
	principal, err := a.cfg.Authenticator.AuthenticateRequest(ctx, auth.CredentialRequest{
		Method: metadata.method, Path: metadata.path, APIKey: metadata.headers.Get("X-Api-Key"),
		Authorization: metadata.headers.Get("Authorization"), AuthorizationValues: metadata.headers.Values("Authorization"),
	})
	if err != nil {
		return deniedExternalAuthorization(err, requestID), nil
	}
	if err := ctx.Err(); err != nil {
		return deniedExternalAuthorization(err, requestID), nil
	}
	token, err := a.cfg.Signer.MintWithRequestMetadata(principal, metadata.method, metadata.path, requestID, metadata.forwardedFor, a.cfg.PrincipalTTL)
	if err != nil {
		return deniedExternalAuthorization(errors.New("principal issuance failed"), requestID), nil
	}
	return &authv3.CheckResponse{Status: &statuspb.Status{Code: int32(codes.OK)}, HttpResponse: &authv3.CheckResponse_OkResponse{OkResponse: &authv3.OkHttpResponse{
		Headers: []*corev3.HeaderValueOption{externalHeader(internalPrincipalHeader, token)}, HeadersToRemove: metadata.removals,
	}}}, nil
}

type externalMetadata struct {
	method, path, requestID, forwardedFor string
	headers                               http.Header
	removals                              []string
}

func externalRequestMetadata(req *authv3.CheckRequest) (externalMetadata, error) {
	bad := &auth.ValidationError{Message: "invalid request metadata"}
	h := req.GetAttributes().GetRequest().GetHttp()
	if h == nil || h.HeaderMap == nil || len(h.Headers) != 0 || !httpToken(h.Method) || len(h.Method) > 16 || !strings.HasPrefix(h.Path, "/") {
		return externalMetadata{}, bad
	}
	u, err := url.ParseRequestURI(h.Path)
	if err != nil || u.IsAbs() || u.Host != "" || u.Path == "" || len(u.Path) > 4096 {
		return externalMetadata{}, bad
	}
	headers := make(http.Header)
	remove := map[string]bool{}
	for _, entry := range h.HeaderMap.Headers {
		if entry == nil || entry.Key == "" || entry.Value != "" {
			return externalMetadata{}, bad
		}
		key := strings.ToLower(entry.Key)
		if !httpToken(key) && key != ":method" && key != ":path" && key != ":authority" && key != ":scheme" {
			return externalMetadata{}, bad
		}
		value := string(entry.RawValue)
		if strings.ContainsAny(value, "\r\n\x00") {
			return externalMetadata{}, bad
		}
		headers.Add(key, value)
		if key != internalPrincipalHeader && (strings.HasPrefix(key, "x-tetral-") || strings.HasPrefix(key, "x-original-") || key == "x-api-key" || key == "authorization") {
			remove[key] = true
		}
	}
	for key, expected := range map[string]string{":method": h.Method, ":path": h.Path} {
		values := headers.Values(key)
		if len(values) != 0 && (len(values) != 1 || values[0] != expected) {
			return externalMetadata{}, bad
		}
	}
	ids, forwarding := headers.Values("X-Request-Id"), headers.Values("X-Forwarded-For")
	// Envoy's HttpRequest.Id is its decimal stream ID, independent of the
	// generated x-request-id header. Only the trusted header supplies the
	// request identity carried by the signed principal and audit records.
	if len(ids) != 1 || len(ids[0]) > 256 || !visibleMetadata(ids[0], false) || strings.Contains(ids[0], ",") || len(forwarding) != 1 || !visibleMetadata(forwarding[0], true) || strings.TrimSpace(forwarding[0]) == "" || len(forwarding[0]) > 1024 {
		return externalMetadata{}, bad
	}
	removals := make([]string, 0, len(remove))
	for key := range remove {
		removals = append(removals, key)
	}
	sort.Strings(removals)
	return externalMetadata{h.Method, u.Path, ids[0], forwarding[0], headers, removals}, nil
}

func visibleMetadata(value string, spaces bool) bool {
	if value == "" {
		return false
	}
	for _, c := range value {
		if c > 126 || c < 33 && !(spaces && c == 32) {
			return false
		}
	}
	return true
}

func httpToken(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
			return false
		}
	}
	return true
}

func externalHeader(key, value string) *corev3.HeaderValueOption {
	return &corev3.HeaderValueOption{Header: &corev3.HeaderValue{Key: key, Value: value}, AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD}
}

func deniedExternalAuthorization(err error, requestID string) *authv3.CheckResponse {
	status, kind, message := http.StatusInternalServerError, "api_error", "internal error"
	var invalid *auth.AuthenticationError
	var validation *auth.ValidationError
	var unavailable *auth.UnavailableError
	switch {
	case errors.As(err, &invalid):
		status, kind, message = 401, "authentication_error", "missing or invalid credential"
	case errors.As(err, &validation):
		status, kind, message = 400, "invalid_request_error", "invalid request metadata"
	case errors.As(err, &unavailable):
		message = "authentication unavailable"
	}
	body, _ := json.Marshal(errorResponse{Type: "error", Error: errorDetail{Type: kind, Message: message}, RequestID: requestID})
	return &authv3.CheckResponse{Status: &statuspb.Status{Code: int32(codes.PermissionDenied)}, HttpResponse: &authv3.CheckResponse_DeniedResponse{DeniedResponse: &authv3.DeniedHttpResponse{
		Status: &typev3.HttpStatus{Code: typev3.StatusCode(status)}, Headers: []*corev3.HeaderValueOption{externalHeader("content-type", "application/json")}, Body: string(body) + "\n",
	}}}
}

type externalAuditRecorder struct {
	logger    *slog.Logger
	requestID string
}

func (r externalAuditRecorder) RecordAuthEvent(ctx context.Context, event auth.AuditEvent) {
	// A failed diagnostic sink cannot change the domain decision or roll back
	// committed usage. Business panics remain the Check owner's separate 500.
	defer func() { _ = recover() }()
	attrs := []any{"component", "auth", "operation", "auth." + event.Stage, "auth.stage", event.Stage, "auth.result", event.Result, "request.id", r.requestID}
	if event.IdentityKind != "" {
		attrs = append(attrs, "auth.identity.kind", event.IdentityKind)
	}
	for key, value := range map[string]int64{"auth.rule.revision": event.RuleRevision, "auth.identity.revision": event.IdentityRevision, "auth.grant.revision": event.GrantRevision} {
		if value > 0 {
			attrs = append(attrs, key, value)
		}
	}
	if event.Result == "success" {
		r.logger.DebugContext(ctx, "auth."+event.Stage+".success", attrs...)
		return
	}
	class := "authentication_error"
	switch event.Result {
	case "unavailable":
		class = "dependency_unavailable"
	case "denied":
		class = "permission_error"
	case "limited":
		class = "rate_limit_error"
	}
	attrs = append(attrs, "error.class", class, "error.code", event.Code, "error.message_safe", "authentication decision failed")
	r.logger.WarnContext(ctx, "auth."+event.Stage+"."+event.Result, attrs...)
}

func logExternalAuthorizationPanic(logger *slog.Logger) {
	defer func() { _ = recover() }()
	logger.Error("auth.external_authorization.panic", "component", "auth", "operation", "auth.external_authorization", "error.class", "panic", "error.code", "internal_error", "error.message_safe", "internal error")
}
