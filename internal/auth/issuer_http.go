package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

const issuerHTTPTimeout = 5 * time.Second
const maxIssuerResponseBytes = 1 << 20

func secureURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
		return nil, &ValidationError{Message: "issuer endpoint must be HTTPS"}
	}
	return u, nil
}
func urlOrigin(u *url.URL) string { return u.Scheme + "://" + u.Host }

// ValidateFederationRule rejects invalid transport policy before an import can
// mutate durable authority. Private issuers require explicit CIDR and CA trust.
func ValidateFederationRule(rule FederationRule) error {
	if !boundedIdentity(rule.ID) || !boundedIdentity(rule.OrganizationID) || rule.Audience == "" || len(rule.Audience) > 512 || rule.Algorithm != "RS256" {
		return &ValidationError{Message: "invalid federation rule"}
	}
	issuer, err := secureURL(rule.Issuer)
	if err != nil {
		return err
	}
	if strings.HasSuffix(rule.Issuer, "/") {
		return &ValidationError{Message: "issuer must use its exact canonical URL"}
	}
	if (rule.DiscoveryURL == "") == (rule.JWKSURL == "") {
		return &ValidationError{Message: "configure exactly one discovery or JWKS endpoint"}
	}
	if len(rule.AllowedCIDRs) > 32 || len(rule.AllowedOrigins) > 16 || len(rule.TrustedCAPEM) > 256*1024 {
		return &ValidationError{Message: "issuer trust configuration exceeds bounds"}
	}
	for _, raw := range rule.AllowedCIDRs {
		if _, err := netip.ParsePrefix(raw); err != nil {
			return &ValidationError{Message: "invalid issuer egress CIDR"}
		}
	}
	for _, raw := range rule.AllowedOrigins {
		u, err := secureURL(raw)
		if err != nil || u.Path != "" {
			return &ValidationError{Message: "invalid issuer permitted origin"}
		}
	}
	endpoint := rule.DiscoveryURL
	if endpoint == "" {
		endpoint = rule.JWKSURL
	}
	u, err := secureURL(endpoint)
	if err != nil {
		return &AuthenticationError{Message: "invalid issuer endpoint"}
	}
	if !permittedOrigin(rule, u, urlOrigin(issuer)) {
		return &ValidationError{Message: "issuer endpoint origin is not permitted"}
	}
	if rule.TrustedCAPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(rule.TrustedCAPEM)) {
			return &ValidationError{Message: "invalid issuer CA trust"}
		}
	}
	return nil
}
func permittedOrigin(rule FederationRule, u *url.URL, issuerOrigin string) bool {
	if urlOrigin(u) == issuerOrigin {
		return true
	}
	for _, origin := range rule.AllowedOrigins {
		if origin == urlOrigin(u) {
			return true
		}
	}
	return false
}

var deniedPublicRanges = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001:db8::/32"),
}

func permittedAddress(rule FederationRule, address netip.Addr) bool {
	address = address.Unmap()
	// Explicit ranges permit self-hosted issuers; there is no hostname bypass.
	for _, raw := range rule.AllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err == nil && prefix.Contains(address) {
			return true
		}
	}
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range deniedPublicRanges {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

// fetchIssuerJSON performs native TLS validation and resolves/dials only an
// allowed destination. Proxy environment and redirects cannot widen the policy.
func fetchIssuerJSON(ctx context.Context, rule FederationRule, endpoint string, target any) error {
	u, err := secureURL(endpoint)
	if err != nil {
		return &AuthenticationError{Message: "invalid issuer endpoint"}
	}
	issuer, err := secureURL(rule.Issuer)
	if err != nil {
		return err
	}
	if !permittedOrigin(rule, u, urlOrigin(issuer)) {
		return &AuthenticationError{Message: "invalid issuer configuration"}
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return &UnavailableError{}
	}
	if rule.TrustedCAPEM != "" && !roots.AppendCertsFromPEM([]byte(rule.TrustedCAPEM)) {
		return &AuthenticationError{Message: "invalid issuer configuration"}
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true,
		DialContext: func(dialCtx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, &UnavailableError{}
			}
			addresses, err := net.DefaultResolver.LookupNetIP(dialCtx, "ip", host)
			if err != nil {
				return nil, &UnavailableError{}
			}
			if len(addresses) == 0 || len(addresses) > 32 {
				return nil, &UnavailableError{}
			}
			// Reject a mixed safe/unsafe DNS answer rather than opportunistically using
			// one safe address. Refreshes repeat this check and dial a verified IP.
			for _, ip := range addresses {
				if !permittedAddress(rule, ip) {
					return nil, &AuthenticationError{Message: "issuer destination denied"}
				}
			}
			dialer := net.Dialer{Timeout: issuerHTTPTimeout}
			for _, ip := range addresses {
				conn, err := dialer.DialContext(dialCtx, network, net.JoinHostPort(ip.String(), port))
				if err == nil {
					return conn, nil
				}
			}
			return nil, &UnavailableError{}
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: issuerHTTPTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return &UnavailableError{}
	}
	response, err := client.Do(req)
	if err != nil {
		var denied *AuthenticationError
		if errors.As(err, &denied) {
			return denied
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &UnavailableError{}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		return &AuthenticationError{Message: "issuer redirect denied"}
	}
	if response.StatusCode != http.StatusOK {
		return &UnavailableError{}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxIssuerResponseBytes+1))
	if err != nil {
		return &UnavailableError{}
	}
	if len(body) > maxIssuerResponseBytes {
		return &UnavailableError{}
	}
	// Discovery/JWKS have extensible standards fields. Duplicate fields remain
	// rejected; the caller's schema selectively reads the fields it owns.
	if err := decodeIssuerJSON(body, target); err != nil {
		return &AuthenticationError{Message: "invalid issuer response"}
	}
	return nil
}
