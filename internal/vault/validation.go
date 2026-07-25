package vault

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	mcpOAuthValidationTimeout  = 5 * time.Second
	mcpOAuthValidationBodyCap  = 1 << 20
	mcpOAuthProtocolVersion    = "2025-06-18"
	mcpOAuthValidationClientID = "tetral-vault-validation"
)

type MCPOAuthValidationInput struct {
	Credential CredentialMetadata
	Auth       CredentialAuth
}

type MCPOAuthValidationResult struct {
	Validation    *CredentialValidation
	RefreshedAuth *CredentialAuth
}

// HTTPMCPOAuthValidator performs the bounded external refresh/probe checks for
// the public MCP OAuth credential diagnostic endpoint.
type HTTPMCPOAuthValidator struct {
	Resolver interface {
		LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
	}
	DialContext func(context.Context, string, string) (net.Conn, error)
	Now         func() time.Time
}

func (v HTTPMCPOAuthValidator) Validate(ctx context.Context, input MCPOAuthValidationInput) (MCPOAuthValidationResult, error) {
	now := v.now().UTC()
	out := &CredentialValidation{
		Type:            "vault_credential_validation",
		CredentialID:    input.Credential.ID,
		VaultID:         input.Credential.VaultID,
		Status:          "valid",
		ValidatedAt:     now,
		HasRefreshToken: hasMCPRefreshToken(input.Auth),
		Refresh:         &CredentialValidationCheck{Status: "no_refresh_token"},
		MCPProbe:        &CredentialValidationCheck{Method: "initialize"},
	}

	authForProbe := input.Auth
	redactionCorpus := credentialValidationRedactionCorpus(input.Auth, nil)
	var refreshed *CredentialAuth
	if hasMCPRefreshToken(input.Auth) && mcpOAuthRefreshDue(input.Auth.ExpiresAt, now) {
		check, refreshResult := v.refresh(ctx, input.Auth, now, redactionCorpus)
		out.Refresh = check
		if check.Status == "connect_error" {
			out.Status = "unknown"
		} else if check.Status != "succeeded" {
			out.Status = "invalid"
		} else if refreshResult != nil {
			refreshedAuth := cloneCredentialAuth(input.Auth)
			refreshedAuth.AccessToken = refreshResult.AccessToken
			refreshedAuth.ExpiresAt = refreshResult.ExpiresAt
			if refreshedAuth.Refresh != nil && refreshResult.RefreshToken != "" {
				refreshedAuth.Refresh.RefreshToken = refreshResult.RefreshToken
			}
			refreshed = &refreshedAuth
			authForProbe = refreshedAuth
			redactionCorpus = credentialValidationRedactionCorpus(input.Auth, []string{refreshResult.AccessToken, refreshResult.RefreshToken})
		}
	} else if hasMCPRefreshToken(input.Auth) {
		out.Refresh = &CredentialValidationCheck{Status: "succeeded"}
	}

	probe, probeOutcome := v.probe(ctx, authForProbe, redactionCorpus)
	out.MCPProbe = probe
	if probeOutcome == "invalid" {
		out.Status = "invalid"
	} else if probeOutcome == "unknown" && out.Status == "valid" {
		out.Status = "unknown"
	}

	return MCPOAuthValidationResult{Validation: out, RefreshedAuth: refreshed}, nil
}

func (v HTTPMCPOAuthValidator) now() time.Time {
	if v.Now != nil {
		return v.Now()
	}
	return time.Now()
}

func cloneCredentialAuth(auth CredentialAuth) CredentialAuth {
	cloned := auth
	if auth.Refresh != nil {
		refresh := *auth.Refresh
		if auth.Refresh.TokenEndpointAuth != nil {
			endpointAuth := *auth.Refresh.TokenEndpointAuth
			refresh.TokenEndpointAuth = &endpointAuth
		}
		cloned.Refresh = &refresh
	}
	return cloned
}

func (v HTTPMCPOAuthValidator) client() *http.Client {
	return &http.Client{
		Timeout:   mcpOAuthValidationTimeout,
		Transport: v.policyTransport(),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (v HTTPMCPOAuthValidator) policyTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           v.dialValidatedDestination,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   mcpOAuthValidationTimeout,
		ResponseHeaderTimeout: mcpOAuthValidationTimeout,
	}
}

func (v HTTPMCPOAuthValidator) dialValidatedDestination(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" {
		return nil, errors.New("credential validation destination is invalid")
	}
	if isClusterInternalHost(host) {
		return nil, errors.New("credential validation destination is not public")
	}
	addresses, err := v.resolveDestination(ctx, host)
	if err != nil || len(addresses) == 0 {
		return nil, errors.New("credential validation destination resolution failed")
	}
	for _, candidate := range addresses {
		if !isPublicCredentialDestination(candidate) {
			return nil, errors.New("credential validation destination is not public")
		}
	}
	selected := addresses[0].Unmap()
	dial := v.DialContext
	if dial == nil {
		dialer := &net.Dialer{Timeout: mcpOAuthValidationTimeout}
		dial = dialer.DialContext
	}
	conn, err := dial(ctx, network, net.JoinHostPort(selected.String(), port))
	if err != nil {
		return nil, err
	}
	remote, err := connectedIP(conn.RemoteAddr())
	if err != nil || remote.Unmap() != selected || !isPublicCredentialDestination(remote) {
		_ = conn.Close()
		return nil, errors.New("credential validation connected destination changed")
	}
	return conn, nil
}

func (v HTTPMCPOAuthValidator) resolveDestination(ctx context.Context, host string) ([]netip.Addr, error) {
	if literal, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{literal.Unmap()}, nil
	}
	resolver := v.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	resolved, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	addresses := make([]netip.Addr, 0, len(resolved))
	seen := make(map[netip.Addr]struct{}, len(resolved))
	for _, value := range resolved {
		candidate, ok := netip.AddrFromSlice(value.IP)
		if !ok {
			return nil, errors.New("credential validation resolver returned an invalid address")
		}
		candidate = candidate.Unmap()
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		addresses = append(addresses, candidate)
	}
	return addresses, nil
}

func connectedIP(address net.Addr) (netip.Addr, error) {
	if tcpAddress, ok := address.(*net.TCPAddr); ok {
		candidate, valid := netip.AddrFromSlice(tcpAddress.IP)
		if valid {
			return candidate.Unmap(), nil
		}
	}
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		return netip.Addr{}, err
	}
	candidate, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, err
	}
	return candidate.Unmap(), nil
}

var disallowedCredentialDestinationPrefixes = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("3fff::/20"),
}

func isPublicCredentialDestination(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsValid() || !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsMulticast() || address.IsUnspecified() {
		return false
	}
	for _, prefix := range disallowedCredentialDestinationPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

func isClusterInternalHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	return host == "localhost" || strings.HasSuffix(host, ".localhost") || host == "svc" || strings.HasSuffix(host, ".svc") ||
		host == "cluster.local" || strings.HasSuffix(host, ".cluster.local")
}

type mcpOAuthRefreshResult struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    string
}

func (v HTTPMCPOAuthValidator) refresh(ctx context.Context, auth CredentialAuth, now time.Time, storedCorpus []string) (*CredentialValidationCheck, *mcpOAuthRefreshResult) {
	refresh := auth.Refresh
	if refresh == nil || refresh.RefreshToken == "" || refresh.ClientID == "" || refresh.TokenEndpoint == "" {
		return &CredentialValidationCheck{Status: "failed", ErrorKind: "invalid_refresh_material"}, nil
	}
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refresh.RefreshToken)
	if refresh.Scope != "" {
		form.Set("scope", refresh.Scope)
	}
	if refresh.Resource != "" {
		form.Set("resource", refresh.Resource)
	}
	endpointAuth := refresh.TokenEndpointAuth
	if endpointAuth == nil || endpointAuth.Type == "" || endpointAuth.Type == "none" {
		form.Set("client_id", refresh.ClientID)
	} else if endpointAuth.Type == "client_secret_post" {
		if endpointAuth.ClientSecret == "" {
			return &CredentialValidationCheck{Status: "failed", ErrorKind: "invalid_refresh_material"}, nil
		}
		form.Set("client_id", refresh.ClientID)
		form.Set("client_secret", endpointAuth.ClientSecret)
	} else if endpointAuth.Type == "client_secret_basic" {
		if endpointAuth.ClientSecret == "" {
			return &CredentialValidationCheck{Status: "failed", ErrorKind: "invalid_refresh_material"}, nil
		}
	} else {
		return &CredentialValidationCheck{Status: "failed", ErrorKind: "invalid_refresh_material"}, nil
	}

	refreshCtx, cancel := context.WithTimeout(ctx, mcpOAuthValidationTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(refreshCtx, http.MethodPost, refresh.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil || validateCredentialValidationURL(request.URL) != nil {
		return &CredentialValidationCheck{Status: "failed", ErrorKind: "invalid_refresh_request"}, nil
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if endpointAuth != nil && endpointAuth.Type == "client_secret_basic" {
		request.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(refresh.ClientID+":"+endpointAuth.ClientSecret)))
	}

	response, err := v.client().Do(request)
	if err != nil {
		return &CredentialValidationCheck{Status: "connect_error", ErrorKind: "refresh_transport_error"}, nil
	}
	defer func() { _ = response.Body.Close() }()
	httpResponse, responseBody, readErr := readCredentialValidationHTTPResponse(response, storedCorpus)
	httpResponse.Body = ""
	httpResponse.ContentType = ""
	check := &CredentialValidationCheck{Status: "failed", HTTPResponse: httpResponse, HTTPStatus: response.StatusCode}
	if readErr != nil {
		check.ErrorKind = "refresh_transport_error"
		check.Status = "connect_error"
		return check, nil
	}
	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		ExpiresAt    string `json:"expires_at"`
	}
	payloadErr := json.Unmarshal(responseBody, &payload)
	if response.StatusCode < 200 || response.StatusCode > 299 {
		check.ErrorKind = "refresh_http_error"
		return check, nil
	}
	if payloadErr != nil {
		check.ErrorKind = "invalid_refresh_response"
		return check, nil
	}
	expiresAt := payload.ExpiresAt
	if expiresAt == "" && payload.ExpiresIn > 0 {
		expiresAt = now.Add(time.Duration(payload.ExpiresIn) * time.Second).UTC().Format(time.RFC3339Nano)
	}
	if payload.AccessToken == "" || expiresAt == "" {
		check.ErrorKind = "invalid_refresh_response"
		return check, nil
	}
	check.Status = "succeeded"
	return check, &mcpOAuthRefreshResult{
		AccessToken:  payload.AccessToken,
		RefreshToken: payload.RefreshToken,
		ExpiresAt:    expiresAt,
	}
}

func (v HTTPMCPOAuthValidator) probe(ctx context.Context, auth CredentialAuth, redactionCorpus []string) (*CredentialValidationCheck, string) {
	check := &CredentialValidationCheck{Method: "initialize"}
	if auth.MCPServerURL == "" || auth.AccessToken == "" {
		check.ErrorKind = "invalid_probe_material"
		return check, "invalid"
	}
	body := []byte(`{"jsonrpc":"2.0","id":"tetral-vault-validation","method":"initialize","params":{"protocolVersion":"` + mcpOAuthProtocolVersion + `","capabilities":{},"clientInfo":{"name":"` + mcpOAuthValidationClientID + `","version":"0"}}}`)
	probeCtx, cancel := context.WithTimeout(ctx, mcpOAuthValidationTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodPost, auth.MCPServerURL, bytes.NewReader(body))
	if err != nil || validateCredentialValidationURL(request.URL) != nil {
		check.ErrorKind = "invalid_probe_request"
		return check, "invalid"
	}
	request.Header.Set("Authorization", "Bearer "+auth.AccessToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("MCP-Protocol-Version", mcpOAuthProtocolVersion)

	response, err := v.client().Do(request)
	if err != nil {
		check.ErrorKind = "probe_transport_error"
		return check, "unknown"
	}
	defer func() { _ = response.Body.Close() }()
	httpResponse, _, readErr := readCredentialValidationHTTPResponse(response, redactionCorpus)
	check.HTTPResponse = httpResponse
	check.HTTPStatus = response.StatusCode
	if readErr != nil {
		check.ErrorKind = "probe_transport_error"
		return check, "unknown"
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		check.ErrorKind = "probe_http_error"
		return check, "invalid"
	}
	return check, "valid"
}

func readCredentialValidationHTTPResponse(response *http.Response, secrets []string) (*CredentialValidationHTTPResponse, []byte, error) {
	maximumEntryBytes := 0
	for _, secret := range secrets {
		if len(secret) > maximumEntryBytes {
			maximumEntryBytes = len(secret)
		}
	}
	readLimit := int64(mcpOAuthValidationBodyCap + maximumEntryBytes + 1)
	body, err := io.ReadAll(io.LimitReader(response.Body, readLimit))
	truncated := int64(len(body)) == readLimit
	httpResponse := sanitizeCredentialValidationHTTPResponse(response, body, secrets, truncated)
	return httpResponse, body, err
}

func credentialValidationRedactionCorpus(auth CredentialAuth, additional []string) []string {
	rawSecrets := []string{auth.AccessToken, auth.RefreshToken, auth.Token}
	if auth.Refresh != nil {
		rawSecrets = append(rawSecrets, auth.Refresh.RefreshToken)
		if auth.Refresh.TokenEndpointAuth != nil {
			rawSecrets = append(rawSecrets, auth.Refresh.TokenEndpointAuth.ClientSecret)
			if auth.Refresh.TokenEndpointAuth.ClientSecret != "" && auth.Refresh.ClientID != "" {
				payload := auth.Refresh.ClientID + ":" + auth.Refresh.TokenEndpointAuth.ClientSecret
				rawSecrets = append(rawSecrets, payload, base64.StdEncoding.EncodeToString([]byte(payload)))
			}
		}
	}
	rawSecrets = append(rawSecrets, additional...)
	corpus := make(map[string]struct{}, len(rawSecrets)*6)
	for _, secret := range rawSecrets {
		if secret == "" {
			continue
		}
		for _, representation := range []string{
			secret,
			"Bearer " + secret,
			"Basic " + secret,
			url.QueryEscape(secret),
			jsonEscapedCredentialSecret(secret),
		} {
			if representation != "" {
				corpus[representation] = struct{}{}
			}
		}
	}
	values := make([]string, 0, len(corpus))
	for value := range corpus {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	return values
}

func jsonEscapedCredentialSecret(secret string) string {
	encoded, err := json.Marshal(secret)
	if err != nil || len(encoded) < 2 {
		return ""
	}
	return string(encoded[1 : len(encoded)-1])
}

func sanitizeCredentialValidationHTTPResponse(response *http.Response, body []byte, secrets []string, truncated bool) *CredentialValidationHTTPResponse {
	scrubbedBody := scrubCredentialValidationBody(body, secrets)
	if len(scrubbedBody) > mcpOAuthValidationBodyCap {
		scrubbedBody = scrubbedBody[:mcpOAuthValidationBodyCap]
		for !utf8.ValidString(scrubbedBody) && len(scrubbedBody) > 0 {
			scrubbedBody = scrubbedBody[:len(scrubbedBody)-1]
		}
		truncated = true
	}
	return &CredentialValidationHTTPResponse{
		StatusCode:    response.StatusCode,
		ContentType:   safeCredentialValidationContentType(response.Header.Get("Content-Type"), secrets),
		Body:          scrubbedBody,
		BodyTruncated: truncated,
	}
}

func safeCredentialValidationContentType(raw string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" && strings.Contains(raw, secret) {
			return ""
		}
	}
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(mediaType)
}

func scrubCredentialValidationBody(body []byte, secrets []string) string {
	var value any
	if json.Unmarshal(body, &value) == nil {
		value = scrubCredentialValidationJSON(value, "")
		if encoded, err := json.Marshal(value); err == nil {
			body = encoded
		}
	}
	scrubbed := string(body)
	for _, secret := range secrets {
		if secret != "" {
			scrubbed = strings.ReplaceAll(scrubbed, secret, "[REDACTED]")
		}
	}
	return scrubbed
}

func validateCredentialValidationURL(parsed *url.URL) error {
	if parsed == nil || parsed.Scheme != "https" || parsed.Opaque != "" || parsed.User != nil || parsed.Hostname() == "" || isClusterInternalHost(parsed.Hostname()) {
		return errors.New("credential validation URL is invalid")
	}
	if port := parsed.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return errors.New("credential validation URL port is invalid")
		}
	}
	return nil
}

func mcpOAuthRefreshDue(expiresAt string, now time.Time) bool {
	if expiresAt == "" {
		return true
	}
	expires, err := time.Parse(time.RFC3339Nano, expiresAt)
	return err != nil || !expires.After(now)
}

func scrubCredentialValidationJSON(value any, field string) any {
	if credentialValidationSecretField(field) {
		return "[REDACTED]"
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			typed[key] = scrubCredentialValidationJSON(child, key)
		}
	case []any:
		for index, child := range typed {
			typed[index] = scrubCredentialValidationJSON(child, field)
		}
	}
	return value
}

func credentialValidationSecretField(field string) bool {
	field = strings.ToLower(field)
	return strings.Contains(field, "token") || strings.Contains(field, "secret") ||
		strings.Contains(field, "password") || strings.Contains(field, "authorization")
}

func credentialRefreshPatch(refreshed CredentialAuth) (*CredentialPatch, error) {
	refreshPatch := map[string]any{}
	if refreshed.Refresh != nil && refreshed.Refresh.RefreshToken != "" {
		refreshPatch["refresh_token"] = refreshed.Refresh.RefreshToken
	}
	authPatch := map[string]any{
		"access_token": refreshed.AccessToken,
		"expires_at":   refreshed.ExpiresAt,
	}
	if len(refreshPatch) > 0 {
		authPatch["refresh"] = refreshPatch
	}
	body, err := json.Marshal(map[string]any{"auth": authPatch})
	if err != nil {
		return nil, err
	}
	patch, err := DecodeUpdateCredentialRequest(body)
	if err != nil {
		return nil, err
	}
	return &patch, nil
}

func hasMCPRefreshToken(auth CredentialAuth) bool {
	return auth.Refresh != nil && auth.Refresh.RefreshToken != ""
}
