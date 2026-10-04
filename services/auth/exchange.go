package tetralauth

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
)

type ExchangeLimits struct {
	BodyBytes          int
	ConcurrentRequests int
	RequestsPerMinute  int
	BodyReadTimeout    time.Duration
}

func DefaultExchangeLimits() ExchangeLimits {
	return ExchangeLimits{BodyBytes: 32 * 1024, ConcurrentRequests: 32, RequestsPerMinute: 60, BodyReadTimeout: 5 * time.Second}
}
func (l ExchangeLimits) Validate() error {
	if l.BodyBytes < 1024 || l.BodyBytes > 64*1024 || l.ConcurrentRequests < 1 || l.ConcurrentRequests > 32 || l.RequestsPerMinute < 1 || l.RequestsPerMinute > 6000 || l.BodyReadTimeout < 100*time.Millisecond || l.BodyReadTimeout > 5*time.Second {
		return &auth.ValidationError{Message: "invalid token exchange limits"}
	}
	return nil
}

//nolint:gosec // G101: OAuth grant identifier, not credential material.
const jwtBearerGrant = "urn:ietf:params:oauth:grant-type:jwt-bearer"

type exchangeRequest struct {
	GrantType        string `json:"grant_type"`
	Assertion        string `json:"assertion"`
	FederationRuleID string `json:"federation_rule_id"`
	OrganizationID   string `json:"organization_id"`
	WorkspaceID      string `json:"workspace_id,omitempty"`
	ServiceAccountID string `json:"service_account_id,omitempty"`
}

// Each actual Auth process limits exchange work independently. No caller ID or
// assertion is retained by this bounded admission fuse.
type exchangeLimiter struct {
	active            chan struct{}
	mu                sync.Mutex
	start             time.Time
	count             int
	requestsPerMinute int
}

func newExchangeLimiter(limits ExchangeLimits) *exchangeLimiter {
	return &exchangeLimiter{active: make(chan struct{}, limits.ConcurrentRequests), requestsPerMinute: limits.RequestsPerMinute}
}
func (l *exchangeLimiter) acquire() bool {
	select {
	case l.active <- struct{}{}:
	default:
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if l.start.IsZero() || !now.Before(l.start.Add(time.Minute)) {
		l.start = now
		l.count = 0
	}
	if l.count >= l.requestsPerMinute {
		<-l.active
		return false
	}
	l.count++
	return true
}
func (l *exchangeLimiter) release() { <-l.active }

type exchangeHandler struct{ cfg RouterConfig }

func (h *exchangeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.cfg.exchange(w, r) }
func (cfg RouterConfig) exchange(w http.ResponseWriter, r *http.Request) {
	controller := http.NewResponseController(w)
	if err := controller.SetReadDeadline(time.Now().Add(cfg.ExchangeLimits.BodyReadTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		writeAuthError(w, r, &auth.UnavailableError{})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(cfg.ExchangeLimits.BodyBytes))
	acquired := false
	defer func() {
		// An admitted slot covers parsing and deadline-bounded body draining.
		// Reset keepalive state only after Close and after releasing that slot.
		_ = r.Body.Close()
		if acquired {
			cfg.exchangeLimiter.release()
			_ = controller.SetReadDeadline(time.Time{})
		}
	}()

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	if !cfg.exchangeLimiter.acquire() {
		// No slot belongs to a rejected request. Terminate its body read now,
		// rather than keeping an unauthenticated drain alive for the full budget.
		w.Header().Set("Connection", "close")
		_ = controller.SetReadDeadline(time.Now())
		w.Header().Set("Retry-After", "60")
		writeAuthError(w, r, exchangeRateError{})
		return
	}
	acquired = true
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeAuthError(w, r, &auth.ValidationError{Message: "token exchange requires application/json"})
		return
	}
	input, err := io.ReadAll(r.Body)
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			writeAuthError(w, r, requestTooLargeError{message: "request body too large"})
		} else {
			writeAuthError(w, r, &auth.ValidationError{Message: "invalid token exchange body"})
		}
		return
	}
	var request exchangeRequest
	trimmed := strings.TrimSpace(string(input))
	if len(trimmed) == 0 || trimmed[0] != '{' || auth.DecodeStrictJSON(input, &request) != nil || request.GrantType != jwtBearerGrant || request.Assertion == "" || len(request.Assertion) > auth.MaxAssertionBytes || !exchangeSelector(request.FederationRuleID) || !exchangeSelector(request.OrganizationID) || request.WorkspaceID != "" && !exchangeSelector(request.WorkspaceID) || request.ServiceAccountID != "" && !exchangeSelector(request.ServiceAccountID) {
		writeAuthError(w, r, &auth.ValidationError{Message: "invalid token exchange request"})
		return
	}
	if cfg.Resolver == nil || cfg.AssertionVerifier == nil {
		writeAuthError(w, r, &auth.UnavailableError{})
		return
	}
	rule, err := cfg.Resolver.LoadFederationRule(r.Context(), request.FederationRuleID, request.OrganizationID)
	if err != nil {
		writeAuthError(w, r, auth.ExchangeError(err))
		return
	}
	proof, err := cfg.AssertionVerifier.Verify(r.Context(), rule, request.Assertion)
	if err != nil {
		writeAuthError(w, r, auth.ExchangeError(err))
		return
	}
	token, err := cfg.Resolver.Issue(r.Context(), proof, auth.ExchangeSelectors{WorkspaceID: request.WorkspaceID, ServiceAccountID: request.ServiceAccountID})
	if err != nil {
		writeAuthError(w, r, auth.ExchangeError(err))
		return
	}
	writeAuthJSON(w, http.StatusOK, token)
}
func exchangeSelector(value string) bool {
	return value != "" && len(value) <= 128 && !strings.ContainsAny(value, "\x00\r\n")
}

type exchangeRateError struct{}

func (exchangeRateError) Error() string { return "token exchange rate exceeded" }
