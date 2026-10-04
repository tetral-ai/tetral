package tetralauth

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const apiKeyBodyByteCap = 1 << 20

type RouterConfig struct {
	ExchangeLimits      ExchangeLimits
	Resolver            *auth.AuthorityResolver
	AssertionVerifier   *auth.AssertionVerifier
	exchangeLimiter     *exchangeLimiter
	Store               *auth.APIKeyStore
	Signer              *auth.InternalPrincipalSigner
	PrincipalTTLSeconds int
	Logger              *slog.Logger
	RequestMetrics      httpapi.RequestMetricsRecorder
}

type apiKeyCreateRequest struct {
	Name string `json:"name"`
}

type apiKeyListResponse struct {
	Data     []auth.APIKeyMetadata `json:"data"`
	NextPage *string               `json:"next_page"`
}

type apiKeyCursor struct {
	WorkspaceID string `json:"workspace_id"`
	AfterID     string `json:"after_id"`
	Limit       int    `json:"limit"`
}

type authorizeResponse struct {
	Allow bool `json:"allow"`
}

func NewRouter(cfg RouterConfig) http.Handler {
	if cfg.ExchangeLimits == (ExchangeLimits{}) {
		cfg.ExchangeLimits = DefaultExchangeLimits()
	}
	if err := cfg.ExchangeLimits.Validate(); err != nil {
		panic(err)
	}
	cfg.exchangeLimiter = newExchangeLimiter(cfg.ExchangeLimits)
	if cfg.Resolver == nil && cfg.Store != nil {
		cfg.Resolver = cfg.Store.AuthorityResolver()
	}
	r := chi.NewRouter()
	logger := cfg.Logger
	if logger == nil {
		logger = workload.ComponentLogger("auth")
	}
	r.Use(httpapi.RequestIDMiddleware)
	r.Use(httpapi.RequestLogMiddleware(logger, httpapi.DefaultSlowRequestThreshold, httpapi.WithRequestLogMetrics(cfg.RequestMetrics)))
	r.Use(httpapi.PublicRecoveryMiddleware(logger))
	r.Post("/internal/auth/authorize", cfg.authorize)
	r.Method(http.MethodPost, "/v1/oauth/token", &exchangeHandler{cfg: cfg})
	r.Route("/v1", func(r chi.Router) {
		r.Use(cfg.signedPrincipalMiddleware)
		r.Method(http.MethodGet, "/api_keys", httpapi.DeclarePublicOperation(http.MethodGet, "/v1/api_keys", cfg.listAPIKeys))
		r.Method(http.MethodPost, "/api_keys", httpapi.DeclarePublicOperation(http.MethodPost, "/v1/api_keys", cfg.createAPIKey))
		r.Method(http.MethodDelete, "/api_keys/{api_key_id}", httpapi.DeclarePublicOperation(http.MethodDelete, "/v1/api_keys/{api_key_id}", cfg.deleteAPIKey))
	})
	return r
}

func (cfg RouterConfig) authorize(w http.ResponseWriter, r *http.Request) {
	if cfg.Store == nil || cfg.Signer == nil {
		writeAuthError(w, r, &auth.AuthenticationError{Message: "authentication unavailable"})
		return
	}
	rawKey := r.Header.Get("X-Api-Key")
	originalMethod := r.Header.Get("X-Original-Method")
	originalPath := r.Header.Get("X-Original-Path")
	requestID := r.Header.Get("X-Request-Id")
	forwardedFor := r.Header.Get("X-Forwarded-For")
	if rawKey == "" && r.Header.Get("Authorization") == "" {
		writeAuthError(w, r, &auth.AuthenticationError{Message: "missing api key"})
		return
	}
	if originalMethod == "" || originalPath == "" {
		writeAuthError(w, r, &auth.ValidationError{Message: "original method and path are required"})
		return
	}
	if requestID == "" || forwardedFor == "" {
		writeAuthError(w, r, &auth.ValidationError{Message: "request id and forwarded-for are required"})
		return
	}
	result, err := (&auth.RequestAuthenticator{Resolver: cfg.Resolver}).AuthenticateRequest(r.Context(), auth.CredentialRequest{Method: originalMethod, Path: originalPath, APIKey: rawKey, Authorization: r.Header.Get("Authorization"), AuthorizationValues: r.Header.Values("Authorization")})
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	token, err := cfg.Signer.MintWithRequestMetadata(result, originalMethod, originalPath, requestID, forwardedFor, principalTTL(cfg))
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	w.Header().Set("X-Tetral-Internal-Principal", token)
	writeAuthJSON(w, http.StatusOK, authorizeResponse{Allow: true})
}

func (cfg RouterConfig) createAPIKey(w http.ResponseWriter, r *http.Request) {
	principal, ok := cfg.principalFromRequest(w, r)
	if !ok {
		return
	}
	var req apiKeyCreateRequest
	if err := decodeStrictBody(w, r, &req); err != nil {
		writeAuthError(w, r, err)
		return
	}
	if err := httpapi.AuthorizePublicRequest(r.Context(), auth.ResourceReference{WorkspaceID: principal.Workspace.ID, Type: "workspace"}); err != nil {
		writeAuthError(w, r, err)
		return
	}
	result, err := cfg.Store.CreateForPrincipal(r.Context(), principal, req.Name)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	writeAuthJSON(w, http.StatusOK, result)
}

func (cfg RouterConfig) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	principal, ok := cfg.principalFromRequest(w, r)
	if !ok {
		return
	}
	limit := parseLimit(r.URL.Query().Get("limit"))
	afterID := ""
	if rawPage := r.URL.Query().Get("page"); rawPage != "" {
		var cursor apiKeyCursor
		if err := cfg.Signer.VerifyCursor(rawPage, &cursor); err != nil {
			writeAuthError(w, r, &auth.ValidationError{Message: "invalid page cursor"})
			return
		}
		if cursor.WorkspaceID != string(principal.Workspace.ID) || cursor.Limit != limit {
			writeAuthError(w, r, &auth.ValidationError{Message: "invalid page cursor"})
			return
		}
		afterID = cursor.AfterID
	}
	if err := httpapi.AuthorizePublicRequest(r.Context(), auth.ResourceReference{WorkspaceID: principal.Workspace.ID, Type: "workspace"}); err != nil {
		writeAuthError(w, r, err)
		return
	}
	keys, hasMore, err := cfg.Store.ListActiveForWorkspace(r.Context(), principal.Workspace.ID, limit, afterID, "")
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	var nextPage *string
	if hasMore && len(keys) > 0 {
		cursor := apiKeyCursor{
			WorkspaceID: string(principal.Workspace.ID),
			AfterID:     keys[len(keys)-1].ID,
			Limit:       limit,
		}
		token, err := cfg.Signer.SignCursor(cursor)
		if err != nil {
			writeAuthError(w, r, err)
			return
		}
		nextPage = &token
	}
	writeAuthJSON(w, http.StatusOK, apiKeyListResponse{Data: keys, NextPage: nextPage})
}

func (cfg RouterConfig) deleteAPIKey(w http.ResponseWriter, r *http.Request) {
	principal, ok := cfg.principalFromRequest(w, r)
	if !ok {
		return
	}
	apiKeyID := chi.URLParam(r, "api_key_id")
	if !strings.HasPrefix(apiKeyID, "ak_") {
		writeAuthError(w, r, &auth.ValidationError{Message: "invalid api key id"})
		return
	}
	metadata, err := cfg.Store.GetForWorkspace(r.Context(), principal.Workspace.ID, apiKeyID)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	if err := httpapi.AuthorizePublicRequest(r.Context(), auth.ResourceReference{WorkspaceID: principal.Workspace.ID, Type: "api_key", ID: metadata.ID}); err != nil {
		writeAuthError(w, r, err)
		return
	}
	if err := cfg.Store.RevokeForWorkspace(r.Context(), principal.Workspace.ID, apiKeyID); err != nil {
		writeAuthError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (cfg RouterConfig) principalFromRequest(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	if principal, ok := auth.PrincipalFromContext(r.Context()); ok {
		return principal, true
	}
	if cfg.Store == nil || cfg.Signer == nil {
		writeAuthError(w, r, &auth.AuthenticationError{Message: "authentication unavailable"})
		return auth.Principal{}, false
	}
	token := r.Header.Get("X-Tetral-Internal-Principal")
	if token == "" {
		writeAuthError(w, r, &auth.AuthenticationError{Message: "missing internal principal"})
		return auth.Principal{}, false
	}
	principal, _, err := cfg.Signer.Verify(token, r.Method, r.URL.Path)
	if err != nil {
		writeAuthError(w, r, err)
		return auth.Principal{}, false
	}
	if principal.Workspace.ID == "" {
		writeAuthError(w, r, workspace.ErrNoWorkspaceInContext)
		return auth.Principal{}, false
	}
	return principal, true
}

func decodeStrictBody(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, apiKeyBodyByteCap)
	defer func() { _ = r.Body.Close() }()
	buffer := new(bytes.Buffer)
	if _, err := buffer.ReadFrom(r.Body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return requestTooLargeError{message: "request body too large"}
		}
		return err
	}
	if err := auth.DecodeStrictJSON(buffer.Bytes(), target); err != nil {
		return &auth.ValidationError{Message: "invalid request body"}
	}

	return nil
}

func parseLimit(raw string) int {
	if raw == "" {
		return 20
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func principalTTL(cfg RouterConfig) time.Duration {
	if cfg.PrincipalTTLSeconds <= 0 {
		return 60 * time.Second
	}
	return time.Duration(cfg.PrincipalTTLSeconds) * time.Second
}

func (cfg RouterConfig) signedPrincipalMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := cfg.principalFromRequest(w, r)
		if !ok {
			return
		}
		ctx := auth.WithPrincipal(r.Context(), principal)
		ctx = workspace.WithContext(ctx, principal.Workspace)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
