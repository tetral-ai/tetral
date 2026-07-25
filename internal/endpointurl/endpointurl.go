// Package endpointurl validates and canonicalizes public HTTPS endpoint URLs.
package endpointurl

import (
	"fmt"
	"net/netip"
	"net/url"
	"strings"
)

const (
	maxDNSHostnameLength = 253
	maxDNSLabelLength    = 63
)

// ValidationError indicates an unsafe or malformed public endpoint URL.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// CanonicalizePublicURL enforces Engine's public-endpoint rules and
// returns a canonical string form with a lowercased host.
//
// Error messages never echo the raw URL or any secret-bearing component.
// They reference only fieldPath and the offending component class.
func CanonicalizePublicURL(raw string, fieldPath string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", validationErrorf("%s is not a valid URL", fieldPath)
	}
	if parsed.Scheme != "https" {
		return "", validationErrorf("%s must use https scheme", fieldPath)
	}
	if parsed.User != nil {
		return "", validationErrorf("%s must not include embedded credentials", fieldPath)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", validationErrorf("%s must not include query string", fieldPath)
	}
	if parsed.Fragment != "" || parsed.RawFragment != "" {
		return "", validationErrorf("%s must not include fragment", fieldPath)
	}
	host := parsed.Hostname()
	if host == "" {
		return "", validationErrorf("%s host is empty", fieldPath)
	}
	if err := rejectUnsafeHost(host, fieldPath); err != nil {
		return "", err
	}

	parsed.Host = canonicalizeHostForURL(parsed.Host)
	return parsed.String(), nil
}

func validationErrorf(format string, args ...any) error {
	return &ValidationError{Message: fmt.Sprintf(format, args...)}
}

func rejectUnsafeHost(host string, fieldPath string) error {
	normalized := normalizeDNSHost(host)
	if normalized == "localhost" || strings.HasSuffix(normalized, ".localhost") {
		return validationErrorf("%s host must not be localhost", fieldPath)
	}
	if strings.Contains(host, "%") {
		return validationErrorf("%s host must not include IPv6 zone identifier", fieldPath)
	}

	addr, err := parseHostAsIP(host)
	if err != nil {
		if isLegacyIPv4LiteralHost(host) {
			return validationErrorf("%s host must not use legacy IPv4 literal syntax", fieldPath)
		}
		if !isValidDNSHost(host) {
			return validationErrorf("%s host must be a valid DNS name", fieldPath)
		}
		return nil
	}
	if addr.Zone() != "" {
		return validationErrorf("%s host must not include IPv6 zone identifier", fieldPath)
	}
	if addr.Is4In6() {
		addr = addr.Unmap()
	}
	if !isPublicAddr(addr) {
		return validationErrorf("%s host must be a globally reachable destination", fieldPath)
	}
	return nil
}

func normalizeDNSHost(host string) string {
	return strings.TrimSuffix(strings.ToLower(host), ".")
}

func canonicalizeHostForURL(host string) string {
	return strings.ToLower(host)
}

func isValidDNSHost(host string) bool {
	normalized := normalizeDNSHost(host)
	if normalized == "" || len(normalized) > maxDNSHostnameLength {
		return false
	}

	labels := strings.Split(normalized, ".")
	for _, label := range labels {
		if !isValidDNSLabel(label) {
			return false
		}
	}
	return true
}

func isValidDNSLabel(label string) bool {
	if label == "" || len(label) > maxDNSLabelLength {
		return false
	}
	if label[0] == '-' || label[len(label)-1] == '-' {
		return false
	}
	for _, r := range label {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return false
	}
	return true
}

func parseHostAsIP(host string) (netip.Addr, error) {
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, err
	}
	return addr, nil
}

func isLegacyIPv4LiteralHost(host string) bool {
	normalized := strings.TrimSuffix(strings.ToLower(host), ".")
	if normalized == "" || strings.Contains(normalized, ":") {
		return false
	}
	parts := strings.Split(normalized, ".")
	if len(parts) > 4 {
		return false
	}
	for _, part := range parts {
		if !isLegacyIPv4Component(part) {
			return false
		}
	}
	return true
}

func isLegacyIPv4Component(part string) bool {
	if part == "" {
		return false
	}
	if strings.HasPrefix(part, "0x") {
		if len(part) == 2 {
			return false
		}
		for _, r := range part[2:] {
			if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
				return false
			}
		}
		return true
	}
	for _, r := range part {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func globallyReachableSpecialPurposePrefixes() []netip.Prefix {
	return []netip.Prefix{
		netip.MustParsePrefix("192.0.0.9/32"),
		netip.MustParsePrefix("192.0.0.10/32"),
		netip.MustParsePrefix("2001:1::1/128"),
		netip.MustParsePrefix("2001:1::2/128"),
		netip.MustParsePrefix("2001:1::3/128"),
		netip.MustParsePrefix("2001:3::/32"),
		netip.MustParsePrefix("2001:4:112::/48"),
		netip.MustParsePrefix("2001:20::/28"),
		netip.MustParsePrefix("2001:30::/28"),
		netip.MustParsePrefix("2620:4f:8000::/48"),
	}
}

func nonPublicPrefixes() []netip.Prefix {
	return []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("100.64.0.0/10"),
		netip.MustParsePrefix("127.0.0.0/8"),
		netip.MustParsePrefix("169.254.0.0/16"),
		netip.MustParsePrefix("172.16.0.0/12"),
		netip.MustParsePrefix("192.0.0.0/24"),
		netip.MustParsePrefix("192.0.2.0/24"),
		netip.MustParsePrefix("192.88.99.0/24"),
		netip.MustParsePrefix("192.168.0.0/16"),
		netip.MustParsePrefix("198.18.0.0/15"),
		netip.MustParsePrefix("198.51.100.0/24"),
		netip.MustParsePrefix("203.0.113.0/24"),
		netip.MustParsePrefix("224.0.0.0/4"),
		netip.MustParsePrefix("240.0.0.0/4"),
		netip.MustParsePrefix("::/128"),
		netip.MustParsePrefix("::1/128"),
		netip.MustParsePrefix("64:ff9b:1::/48"),
		netip.MustParsePrefix("100::/64"),
		netip.MustParsePrefix("100:0:0:1::/64"),
		netip.MustParsePrefix("2001::/23"),
		netip.MustParsePrefix("2001:db8::/32"),
		netip.MustParsePrefix("2002::/16"),
		netip.MustParsePrefix("3fff::/20"),
		netip.MustParsePrefix("5f00::/16"),
		netip.MustParsePrefix("fc00::/7"),
		netip.MustParsePrefix("fe80::/10"),
		netip.MustParsePrefix("ff00::/8"),
	}
}

func isPublicAddr(addr netip.Addr) bool {
	for _, prefix := range globallyReachableSpecialPurposePrefixes() {
		if prefix.Contains(addr) {
			return true
		}
	}
	for _, prefix := range nonPublicPrefixes() {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}
