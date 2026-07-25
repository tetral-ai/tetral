package gitproxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	statusBadUpstream = 502
	badUpstreamBody   = "bad upstream"
	githubScheme      = "https"
	githubHost        = "github.com"
	gitTicketHeader   = gitticket.HeaderName
)

var errRequestBodyTooLarge = errors.New("git-proxy request body too large")
var errReactiveCredentialRetry = errors.New("git-proxy reactive credential retry")

type HandlerOptions struct {
	PublicBaseURL       *url.URL
	Transport           http.RoundTripper
	AccessLogger        AccessLogger
	Metrics             *GitProxyMetrics
	Limiter             *TicketConnectionLimiter
	MaxRequestBodyBytes int64
	IdleProgressTimeout time.Duration
	LegacyPathCutover   bool
}

func (o HandlerOptions) withDefaults() HandlerOptions {
	out := o
	if out.Transport == nil {
		out.Transport = defaultGitHubTransport()
	}
	if out.AccessLogger == nil {
		out.AccessLogger = NoopAccessLogger{}
	}
	if out.Metrics == nil {
		out.Metrics = NewGitProxyMetrics()
	}
	if out.Limiter == nil {
		out.Limiter = NewTicketConnectionLimiter(MaxConnsPerTicket)
	}
	if out.MaxRequestBodyBytes <= 0 {
		out.MaxRequestBodyBytes = MaxRequestBodyBytes
	}
	if out.IdleProgressTimeout <= 0 {
		out.IdleProgressTimeout = IdleProgressTimeout
	}
	return out
}

type Proxy struct {
	TicketValidator TicketValidator
	Authorizer      RepositoryAuthorizer
	Options         HandlerOptions
}

func (p *Proxy) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	startedAt := time.Now()
	options := p.Options.withDefaults()
	tracker := &trackingResponseWriter{ResponseWriter: response}
	var bodyCounter *countingReadCloser
	record := AccessLogRecord{
		RequestID: requestIDForAccessLog(request.Header.Get("X-Request-ID")),
		Decision:  "rejected:invalid_route",
	}
	defer func() {
		if tracker.status == 0 {
			tracker.status = http.StatusOK
		}
		record.UpstreamStatus = tracker.status
		if bodyCounter != nil {
			record.BytesIn = bodyCounter.bytes
		}
		record.BytesOut = tracker.bytes
		duration := time.Since(startedAt)
		record.DurationMS = duration.Milliseconds()
		options.Metrics.ObserveRequest(record.Operation, record.Decision, record.UpstreamStatus, record.BytesIn, record.BytesOut, duration)
		options.AccessLogger.LogAccess(request.Context(), record)
	}()

	parsed, ok := parseGitRequest(request, options.LegacyPathCutover)
	if !ok {
		http.NotFound(tracker, request)
		return
	}
	record.OwnerRepo = parsed.Owner + "/" + parsed.Repo
	record.Operation = string(parsed.Endpoint)

	token := request.Header.Get(gitTicketHeader)
	if token == "" {
		token = parsed.LegacyTicket
	}
	ticket, err := p.TicketValidator.Validate(request.Context(), token)
	if err != nil {
		reason := ticketRejectionReason(token, err)
		record.Decision = "rejected:" + reason
		options.Metrics.ObserveTicketRejection(reason)
		http.Error(tracker, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	record.WorkspaceID = string(ticket.WorkspaceID)
	record.SessionID = ticket.SessionID
	record.TicketID = ticket.TicketID

	if request.ContentLength > options.MaxRequestBodyBytes {
		record.Decision = "rejected:request_body_too_large"
		http.Error(tracker, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
		return
	}
	release, ok := options.Limiter.Acquire(ticket.TicketID)
	if !ok {
		record.Decision = "rejected:max_conns"
		http.Error(tracker, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
		return
	}
	defer release()

	if p.Authorizer == nil {
		record.Decision = "rejected:authorizer_unavailable"
		http.Error(tracker, http.StatusText(http.StatusFailedDependency), http.StatusFailedDependency)
		return
	}
	auth, err := p.Authorizer.AuthorizeRepository(request.Context(), RepositoryAuthRequest{
		WorkspaceID: ticket.WorkspaceID,
		SessionID:   ticket.SessionID,
		Owner:       parsed.Owner,
		Repo:        parsed.Repo,
	})
	if err != nil {
		if errors.Is(err, ErrGitHubCredentialRequired) {
			record.Decision = "rejected:credential_required"
			http.Error(tracker, "credential_required", http.StatusFailedDependency)
			return
		}
		record.Decision = "rejected:credential_error"
		http.Error(tracker, badUpstreamBody, statusBadUpstream)
		return
	}
	record.Decision = auth.Decision
	reactiveRereadAllowed := isBodylessInfoRefsRequest(request, parsed)
	if request.Body != nil {
		bodyCounter = &countingReadCloser{ReadCloser: request.Body, maxBytes: options.MaxRequestBodyBytes}
		request.Body = bodyCounter
	}
	options.Metrics.IncActive()
	defer options.Metrics.DecActive()

	progress := newIdleProgressTracker(options.IdleProgressTimeout)
	upstreamCtx, cancelUpstream := context.WithCancel(request.Context())
	defer cancelUpstream()
	stopProgress := progress.start(upstreamCtx, cancelUpstream)
	defer stopProgress()
	request = request.WithContext(upstreamCtx)
	tracker.onProgress = progress.observe
	if bodyCounter != nil {
		bodyCounter.onProgress = progress.observe
	}

	for attempt := 0; attempt < 2; attempt++ {
		retryAuth, retry := p.proxyGitHubRequest(tracker, request, parsed, auth, options, &record, attempt == 0 && reactiveRereadAllowed)
		if !retry || tracker.status != 0 {
			return
		}
		auth = retryAuth
		record.Decision = auth.Decision
	}
}

// proxyGitHubRequest runs one upstream attempt and, on the initial attempt of a
// bodyless GET /info/refs whose injected token drew an upstream 401, performs
// the git regime's 401-reactive re-read exactly once (allowReactiveReread is
// (attempt == 0 && bodyless GET /info/refs); a body-carrying request has
// already streamed under the no-buffer relay and is never replayed). The
// re-read re-resolves the resource row once. Its outcome table is closed, and
// the initial injected/anonymous/424 arm table is NOT re-applied here — a
// vanished row never becomes an anonymous relay:
//
//	RE-READ RESULT                       ACTION                CLIENT SEES
//	decryptable token                    re-inject, retry once retried upstream response
//	NULL / absent / undecryptable token  rewrite, no retry     424 credential_required
//	no mounted row (detached/deleted)    no retry, no rewrite  original upstream 401 (unchanged)
//	resolver / database error            rewrite, no retry     502 bad upstream
//
// The retry runs as attempt 1 with allowReactiveReread == false, so the re-read
// fires at most once per operation.
func (p *Proxy) proxyGitHubRequest(
	tracker *trackingResponseWriter,
	request *http.Request,
	parsed gitRequest,
	auth RepositoryAuthDecision,
	options HandlerOptions,
	record *AccessLogRecord,
	allowReactiveReread bool,
) (RepositoryAuthDecision, bool) {
	var retryAuth RepositoryAuthDecision
	retryRequested := false
	proxy := &httputil.ReverseProxy{
		Rewrite: func(proxyRequest *httputil.ProxyRequest) {
			upstream := proxyRequest.Out
			inboundHeaders := upstream.Header
			upstream.URL.Scheme = githubScheme
			upstream.URL.Host = githubHost
			upstream.URL.Path = parsed.UpstreamPath
			upstream.URL.RawPath = ""
			upstream.Host = githubHost
			upstream.Header = gitProxyUpstreamHeaders(inboundHeaders)
			if auth.Authorization != "" {
				upstream.Header.Set("Authorization", auth.Authorization)
			}
		},
		FlushInterval: -1,
		Transport:     options.Transport,
		ModifyResponse: func(upstream *http.Response) error {
			rewriteGitHubRedirect(upstream, request, options.PublicBaseURL)
			if allowReactiveReread && shouldRereadGitCredential(upstream, auth) {
				reread, err := p.Authorizer.AuthorizeRepository(request.Context(), RepositoryAuthRequest{
					WorkspaceID: workspace.ID(record.WorkspaceID),
					SessionID:   record.SessionID,
					Owner:       parsed.Owner,
					Repo:        parsed.Repo,
				})
				if errors.Is(err, ErrGitHubCredentialRequired) {
					record.Decision = "rejected:credential_required"
					rewriteUpstreamError(upstream, http.StatusFailedDependency, "credential_required")
					return nil
				}
				if err != nil {
					record.Decision = "rejected:credential_error"
					rewriteUpstreamError(upstream, statusBadUpstream, badUpstreamBody)
					return nil
				}
				if reread.Decision != DecisionInjected || reread.Authorization == "" {
					// Vanished/unmounted row: nothing to re-inject. Returning nil
					// (no retry, no rewrite) relays the original upstream 401
					// unchanged — never an anonymous downgrade.
					return nil
				}
				retryAuth = reread
				retryRequested = true
				return errReactiveCredentialRetry
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, errReactiveCredentialRetry) {
				return
			}
			if errors.Is(err, errRequestBodyTooLarge) {
				record.Decision = "rejected:request_body_too_large"
				http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, badUpstreamBody, statusBadUpstream)
		},
	}
	proxy.ServeHTTP(tracker, request)
	if retryRequested && tracker.status == 0 {
		return retryAuth, true
	}
	return auth, false
}

func gitProxyUpstreamHeaders(inbound http.Header) http.Header {
	upstream := make(http.Header)
	for _, name := range []string{
		"Content-Type",
		"Accept",
		"Git-Protocol",
		"Content-Encoding",
		"Accept-Encoding",
	} {
		if values, ok := inbound[name]; ok {
			upstream[name] = append([]string(nil), values...)
		}
	}
	// Suppress net/http's default User-Agent; the contract's upstream header
	// set is the Git smart-HTTP allowlist above plus injected Authorization.
	upstream["User-Agent"] = nil
	return upstream
}

func shouldRereadGitCredential(upstream *http.Response, auth RepositoryAuthDecision) bool {
	if upstream == nil {
		return false
	}
	if auth.Decision != DecisionInjected || auth.Authorization == "" {
		return false
	}
	return upstream.StatusCode == http.StatusUnauthorized
}

func isBodylessInfoRefsRequest(request *http.Request, parsed gitRequest) bool {
	if request == nil {
		return false
	}
	if request.Method != http.MethodGet ||
		(parsed.Endpoint != endpointRefsUpload && parsed.Endpoint != endpointRefsReceive) {
		return false
	}
	return request.Body == nil || request.Body == http.NoBody || request.ContentLength == 0
}

func rewriteUpstreamError(upstream *http.Response, status int, body string) {
	if upstream == nil {
		return
	}
	if upstream.Body != nil {
		_ = upstream.Body.Close()
	}
	body += "\n"
	upstream.StatusCode = status
	upstream.Status = strconv.Itoa(status) + " " + http.StatusText(status)
	upstream.Body = io.NopCloser(strings.NewReader(body))
	upstream.ContentLength = int64(len(body))
	upstream.Header.Set("Content-Type", "text/plain; charset=utf-8")
	upstream.Header.Set("Content-Length", strconv.Itoa(len(body)))
	upstream.Header.Del("Transfer-Encoding")
}

type trackingResponseWriter struct {
	http.ResponseWriter
	status     int
	bytes      int64
	onProgress func(int)
}

func (w *trackingResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *trackingResponseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(body)
	w.bytes += int64(n)
	if n > 0 && w.onProgress != nil {
		w.onProgress(n)
	}
	return n, err
}

func (w *trackingResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

type countingReadCloser struct {
	io.ReadCloser
	bytes      int64
	maxBytes   int64
	onProgress func(int)
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if r.maxBytes > 0 && int64(n) > r.maxBytes-r.bytes {
		allowed := int(r.maxBytes - r.bytes)
		if allowed < 0 {
			allowed = 0
		}
		r.bytes += int64(allowed)
		if allowed > 0 && r.onProgress != nil {
			r.onProgress(allowed)
		}
		return allowed, errRequestBodyTooLarge
	}
	r.bytes += int64(n)
	if n > 0 && r.onProgress != nil {
		r.onProgress(n)
	}
	return n, err
}

type idleProgressTracker struct {
	timeout      time.Duration
	progress     chan struct{}
	stopOnce     sync.Once
	stop         chan struct{}
	mu           sync.Mutex
	lastProgress time.Time
}

func newIdleProgressTracker(timeout time.Duration) *idleProgressTracker {
	return &idleProgressTracker{
		timeout:  timeout,
		progress: make(chan struct{}, 1),
		stop:     make(chan struct{}),
	}
}

func (t *idleProgressTracker) observe(n int) {
	if t == nil || n <= 0 {
		return
	}
	t.mu.Lock()
	t.lastProgress = time.Now()
	t.mu.Unlock()
	select {
	case t.progress <- struct{}{}:
	default:
	}
}

func (t *idleProgressTracker) start(ctx context.Context, cancel context.CancelFunc) func() {
	if t == nil || t.timeout <= 0 {
		return func() {}
	}
	t.mu.Lock()
	t.lastProgress = time.Now()
	t.mu.Unlock()
	timer := time.NewTimer(t.timeout)
	go func() {
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if remaining, expired := t.checkIdle(cancel); expired {
					return
				} else {
					timer.Reset(remaining)
				}
			case <-t.progress:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(t.timeout)
			case <-t.stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() {
		t.stopOnce.Do(func() {
			close(t.stop)
		})
	}
}

func (t *idleProgressTracker) checkIdle(cancel context.CancelFunc) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	elapsed := time.Since(t.lastProgress)
	if elapsed >= t.timeout {
		cancel()
		return 0, true
	}
	return t.timeout - elapsed, false
}

func ticketRejectionReason(token string, err error) string {
	if !isUnauthorizedTicket(err) {
		return "unauthorized"
	}
	if validateErr := gitticket.ValidateToken(token); validateErr != nil {
		return "malformed"
	}
	return "unauthorized"
}

func rewriteGitHubRedirect(response *http.Response, request *http.Request, publicBase *url.URL) {
	if response.StatusCode < http.StatusMultipleChoices || response.StatusCode >= http.StatusBadRequest {
		return
	}
	location := response.Header.Get("Location")
	if location == "" {
		return
	}
	parsed, err := url.Parse(location)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" {
		return
	}
	base := publicBaseForRequest(request, publicBase)
	rewritten := *base
	rewritten.Path = "/github.com" + parsed.EscapedPath()
	rewritten.RawQuery = parsed.RawQuery
	response.Header.Set("Location", rewritten.String())
}

func publicBaseForRequest(request *http.Request, configured *url.URL) *url.URL {
	if configured != nil {
		out := *configured
		out.Path = strings.TrimRight(out.Path, "/")
		out.RawQuery = ""
		out.Fragment = ""
		return &out
	}
	host := ""
	if request != nil {
		host = request.Host
	}
	if host == "" {
		host = "git-proxy"
	}
	return &url.URL{Scheme: "https", Host: host}
}

func isUnauthorizedTicket(err error) bool {
	return errors.Is(err, ErrTicketUnauthorized)
}
