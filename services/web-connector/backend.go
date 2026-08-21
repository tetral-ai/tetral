package webconnector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type keyState struct {
	value            string
	dead             bool
	unavailableUntil time.Time
}

// BackendRequestTimeout is the maximum duration of one legal backend call.
// The idempotency claim window derives from this same owning limit.
const BackendRequestTimeout = 30 * time.Second

type JinaBackend struct {
	client         *http.Client
	searchEndpoint string
	readerEndpoint string
	now            func() time.Time
	mu             sync.Mutex
	keys           []keyState
	next           int
	metrics        *Metrics
	logger         *slog.Logger
}

func (b *JinaBackend) WithMetrics(metrics *Metrics) *JinaBackend {
	b.metrics = metrics
	return b
}
func (b *JinaBackend) ManagesMetrics() bool                        { return b.metrics != nil }
func (b *JinaBackend) WithLogger(logger *slog.Logger) *JinaBackend { b.logger = logger; return b }

func NewJinaBackend(client *http.Client, searchEndpoint, readerEndpoint string, keys []string, now func() time.Time) *JinaBackend {
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.DisableCompression = true
		client = &http.Client{Transport: transport, Timeout: BackendRequestTimeout}
	} else {
		copyClient := *client
		if transport, ok := client.Transport.(*http.Transport); ok {
			cloned := transport.Clone()
			cloned.DisableCompression = true
			copyClient.Transport = cloned
		} else if client.Transport == nil {
			cloned := http.DefaultTransport.(*http.Transport).Clone()
			cloned.DisableCompression = true
			copyClient.Transport = cloned
		}
		client = &copyClient
	}
	if now == nil {
		now = time.Now
	}
	states := make([]keyState, 0, len(keys))
	for _, key := range keys {
		states = append(states, keyState{value: key})
	}
	return &JinaBackend{client: client, searchEndpoint: searchEndpoint, readerEndpoint: readerEndpoint, keys: states, now: now}
}

type searchResponse struct {
	Code int `json:"code"`
	Data []struct {
		Title       string `json:"title"`
		URL         string `json:"url"`
		Description string `json:"description"`
		Date        string `json:"date"`
	} `json:"data"`
	Meta struct {
		Usage struct {
			Tokens int64 `json:"tokens"`
		} `json:"usage"`
	} `json:"meta"`
}
type readerResponse struct {
	Code int    `json:"code"`
	Name string `json:"name"`
	Data *struct {
		Title         string `json:"title"`
		URL           string `json:"url"`
		Content       string `json:"content"`
		PublishedTime string `json:"publishedTime"`
		HTTPStatus    int32  `json:"httpStatus"`
		Usage         struct {
			Tokens int64 `json:"tokens"`
		} `json:"usage"`
	} `json:"data"`
	Meta struct {
		Usage struct {
			Tokens int64 `json:"tokens"`
		} `json:"usage"`
	} `json:"meta"`
}

func (b *JinaBackend) Search(ctx context.Context, q string, domains []string) ([]SearchHit, BackendOutcome) {
	if len(domains) > maxSearchDomains {
		return nil, BackendOutcome{Kind: BackendToolError, Message: "Too many search domains."}
	}
	targets := domains
	if len(targets) == 0 {
		targets = []string{""}
	}
	seen := map[string]bool{}
	all := make([]SearchHit, 0, maxSearchHits)
	overall := BackendOutcome{Kind: BackendSuccess}
	for _, domain := range targets {
		var response searchResponse
		outcome := b.call(ctx, b.searchEndpoint, map[string]string{"q": q}, searchHeaders(domain), &response)
		overall.Requests += outcome.Requests
		overall.Tokens += outcome.Tokens
		if outcome.Kind != BackendSuccess {
			return nil, outcomeWithTotals(outcome, overall)
		}
		overall.Tokens += response.Meta.Usage.Tokens
		if b.metrics != nil {
			b.metrics.ObserveBackendTokens("search", response.Meta.Usage.Tokens)
		}
		for _, hit := range response.Data {
			if !seen[hit.URL] && len(all) < maxSearchHits {
				seen[hit.URL] = true
				all = append(all, SearchHit{URL: hit.URL, Title: hit.Title, Description: hit.Description, Date: hit.Date})
			}
		}
	}
	return all, overall
}

func (b *JinaBackend) Fetch(ctx context.Context, target string) (Page, BackendOutcome) {
	var response readerResponse
	outcome := b.call(ctx, b.readerEndpoint, map[string]string{"url": target}, readerHeaders(), &response)
	if outcome.Kind != BackendSuccess {
		return Page{}, outcome
	}
	if response.Data == nil {
		return Page{}, BackendOutcome{Kind: BackendToolError, Message: "URL could not be fetched.", Requests: outcome.Requests}
	}
	tokens := response.Data.Usage.Tokens
	if b.metrics != nil {
		b.metrics.ObserveBackendTokens("reader", tokens)
	}
	statusCode := response.Data.HTTPStatus
	if statusCode < 200 || statusCode > 599 {
		return Page{}, BackendOutcome{Kind: BackendRuntimeError, Message: "Web backend returned an invalid response.", Tokens: tokens, Requests: outcome.Requests}
	}
	if statusCode >= 400 && statusCode <= 599 {
		return Page{}, BackendOutcome{Kind: BackendToolError, TargetHTTPStatus: &statusCode, Message: "Target URL returned an error.", Tokens: tokens, Requests: outcome.Requests}
	}
	return Page{URL: response.Data.URL, Title: response.Data.Title, PublishedTime: response.Data.PublishedTime, Content: response.Data.Content, TargetHTTPStatus: statusCode, Tokens: tokens}, BackendOutcome{Kind: BackendSuccess, TargetHTTPStatus: &statusCode, Tokens: tokens, Requests: outcome.Requests}
}

func (b *JinaBackend) call(ctx context.Context, endpoint string, body any, headers map[string]string, target any) BackendOutcome {
	api := "reader"
	if endpoint == b.searchEndpoint {
		api = "search"
	}
	tried := map[int]bool{}
	requests := int64(0)
	for {
		index, key, ok := b.acquireKey(tried)
		if !ok {
			return BackendOutcome{Kind: BackendUnavailable, Message: "Web backend is temporarily unavailable."}
		}
		if index >= 0 {
			tried[index] = true
		}
		raw, _ := json.Marshal(body)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
		if err != nil {
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "request_error")
			}
			return BackendOutcome{Kind: BackendRuntimeError, Message: "Web backend request failed."}
		}
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		req.Header["User-Agent"] = []string{""}
		response, err := b.client.Do(req)
		requests++
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				if b.metrics != nil {
					b.metrics.ObserveBackendCall(api, "timeout")
				}
				return BackendOutcome{Kind: BackendRuntimeError, Message: "Web backend request timed out."}
			}
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "transport_error")
			}
			return BackendOutcome{Kind: BackendRuntimeError, Message: "Web backend request failed."}
		}
		responseRaw, readErr := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024))
		_ = response.Body.Close()
		if readErr != nil {
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "response_error")
			}
			return BackendOutcome{Kind: BackendRuntimeError, Message: "Web backend response failed."}
		}
		switch response.StatusCode {
		case http.StatusUnauthorized:
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "401")
			}
			b.disable(index, 0, true)
			if b.metrics != nil {
				b.metrics.ObserveRotation("401")
			}
			continue
		case http.StatusPaymentRequired:
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "402")
			}
			b.disable(index, time.Hour, false)
			if b.metrics != nil {
				b.metrics.ObserveRotation("402")
			}
			continue
		case http.StatusTooManyRequests:
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "429")
			}
			b.disable(index, time.Minute, false)
			if b.metrics != nil {
				b.metrics.ObserveRotation("429")
			}
			continue
		}
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusUnprocessableEntity {
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "tool_error")
			}
			return BackendOutcome{Kind: BackendToolError, Message: "URL could not be fetched."}
		}
		if response.StatusCode >= 500 {
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "runtime_error")
			}
			return BackendOutcome{Kind: BackendRuntimeError, Message: "Web backend failed."}
		}
		if response.StatusCode >= 400 {
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "tool_error")
			}
			if b.logger != nil {
				var taxonomy struct {
					Name   string `json:"name"`
					Status int    `json:"status"`
				}
				_ = json.Unmarshal(responseRaw, &taxonomy)
				b.logger.Warn("web.backend.unmatched_client_error", slog.String("operation", "web.backend.request"), slog.String("backend.api", api), slog.String("backend.error.name", taxonomy.Name), slog.Int("backend.error.status", taxonomy.Status))
			}
			return BackendOutcome{Kind: BackendToolError, Message: "Web backend rejected the request."}
		}
		if json.Unmarshal(responseRaw, target) != nil {
			if b.metrics != nil {
				b.metrics.ObserveBackendCall(api, "invalid_response")
			}
			return BackendOutcome{Kind: BackendRuntimeError, Message: "Web backend returned an invalid response."}
		}
		if b.metrics != nil {
			b.metrics.ObserveBackendCall(api, "success")
		}
		return BackendOutcome{Kind: BackendSuccess, Requests: 1}
	}
}

func (b *JinaBackend) acquireKey(tried map[int]bool) (int, string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.keys) == 0 {
		if tried[-1] {
			return -1, "", false
		}
		return -1, "", true
	}
	for offset := 0; offset < len(b.keys); offset++ {
		index := (b.next + offset) % len(b.keys)
		state := b.keys[index]
		if !tried[index] && !state.dead && !b.now().Before(state.unavailableUntil) {
			b.next = (index + 1) % len(b.keys)
			return index, state.value, true
		}
	}
	return -1, "", false
}
func (b *JinaBackend) disable(index int, duration time.Duration, dead bool) {
	if index < 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keys[index].dead = b.keys[index].dead || dead
	if duration > 0 {
		deadline := b.now().Add(duration)
		if deadline.After(b.keys[index].unavailableUntil) {
			b.keys[index].unavailableUntil = deadline
		}
	}
}
func outcomeWithTotals(outcome, total BackendOutcome) BackendOutcome {
	outcome.Requests = total.Requests
	outcome.Tokens = total.Tokens
	return outcome
}
func searchHeaders(domain string) map[string]string {
	headers := map[string]string{"Accept": "application/json", "Content-Type": "application/json", "X-Respond-With": "no-content"}
	if strings.TrimSpace(domain) != "" {
		headers["X-Site"] = domain
	}
	return headers
}

// readerHeaders sets the fetch headers sent to the reader endpoint.
// X-Token-Budget 262144 is the upstream size assist sized to the 1 MiB local
// snapshot cap (maxSnapshotBytes in types.go): 256*1024 tokens at ~4 bytes per
// token equals 1024*1024 bytes. X-Timeout "30" (seconds) matches the HTTP
// client Timeout of 30*time.Second set in NewJinaBackend.
// UPDATE-WITH: types.go maxSnapshotBytes, NewJinaBackend client timeout.
func readerHeaders() map[string]string {
	return map[string]string{"Accept": "application/json", "Content-Type": "application/json", "X-Return-Format": "markdown", "X-Timeout": "30", "X-Token-Budget": "262144", "X-With-Generated-Alt": "true"}
}
