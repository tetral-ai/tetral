package webconnector

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
)

type URLClassification struct {
	Allowed bool
	Reason  string
}

var whatwgHostProfile = idna.New(
	idna.MapForLookup(),
	idna.ValidateLabels(false),
	idna.StrictDomainName(false),
)

// internalHostnames matches exactly or by first DNS label. Every entry must be
// distinctive enough to never appear as the first label of a public hostname;
// the retired tetral-*/agent-runtime-* service names stay listed because they
// keep resolving inside the cluster during any rolling cutover.
var internalHostnames = map[string]struct{}{
	"metadata": {}, "metadata.google.internal": {}, "instance-data": {},
	"instance-data.ec2.internal": {}, "metadata.azure.com": {},
	"event-stream": {},
	"tetral-api":   {}, "tetral-auth": {}, "tetral-cleanup": {}, "tetral-queue": {},
	"tetral-sandbox": {}, "tetral-git-proxy": {},
	"agent-runtime-bridge": {}, "agent-runtime-pod": {}, "gateway-service": {},
}

// clusterServiceHostnames are the bare in-cluster Service names. They are
// common words (api, auth, ...), so they block only the dotless short form and
// the namespace-qualified form — never public hostnames like api.github.com.
// Fully-qualified cluster forms (name.ns.svc...) are caught by the suffix
// rules in isInternalHostname.
var clusterServiceHostnames = map[string]struct{}{
	"bridge": {}, "event-stream": {}, "gateway": {},
	"agent-runtime": {}, "git-proxy": {},
	"api": {}, "auth": {}, "cleanup": {}, "queue": {}, "sandbox": {},
}

// clusterNamespaces qualifies bare service names: kube-dns search domains make
// name.<namespace> resolve in-cluster even without the .svc suffix.
var clusterNamespaces = map[string]struct{}{
	"tetral-system": {}, "tetral-agent-runtime": {}, "default": {},
}

func ClassifyURL(raw string) URLClassification {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" {
		return URLClassification{Reason: "invalid_url"}
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return URLClassification{Reason: "unsupported_scheme"}
	}
	if parsed.User != nil {
		return URLClassification{Reason: "userinfo_forbidden"}
	}
	if port := parsed.Port(); port != "" {
		value, portErr := strconv.Atoi(port)
		if portErr != nil || value > 65535 {
			return URLClassification{Reason: "invalid_url"}
		}
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if strings.Contains(host, "%") {
		return URLClassification{Reason: "invalid_url"}
	}
	if net.ParseIP(host) == nil {
		host, err = whatwgHostProfile.ToASCII(host)
		if err != nil || host == "" {
			return URLClassification{Reason: "invalid_url"}
		}
		host = strings.ToLower(strings.TrimSuffix(host, "."))
	}
	if isInternalHostname(host) {
		return URLClassification{Reason: "internal_hostname"}
	}
	if ip := net.ParseIP(host); ip != nil {
		if !isPublicLiteralIP(ip) {
			return URLClassification{Reason: "non_public_ip"}
		}
		return URLClassification{Allowed: true}
	}
	if endsInNumber(host) {
		ip, numeric, parseErr := parseLegacyIPv4(host)
		if !numeric || parseErr != nil {
			return URLClassification{Reason: "invalid_url"}
		}
		if !isPublicLiteralIP(ip) {
			return URLClassification{Reason: "non_public_ip"}
		}
		return URLClassification{Allowed: true}
	}
	if ip, numeric, parseErr := parseLegacyIPv4(host); numeric {
		if parseErr != nil {
			return URLClassification{Reason: "invalid_url"}
		}
		if !isPublicLiteralIP(ip) {
			return URLClassification{Reason: "non_public_ip"}
		}
	}
	return URLClassification{Allowed: true}
}

func endsInNumber(host string) bool {
	last := host
	if dot := strings.LastIndexByte(host, '.'); dot >= 0 {
		last = host[dot+1:]
	}
	if last == "" {
		return false
	}
	if _, numeric, err := parseLegacyIPv4Part(last); numeric && err == nil {
		return true
	}
	for _, char := range last {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func parseLegacyIPv4(host string) (net.IP, bool, error) {
	parts := strings.Split(host, ".")
	values := make([]uint64, 0, len(parts))
	for _, part := range parts {
		value, numeric, err := parseLegacyIPv4Part(part)
		if !numeric {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		values = append(values, value)
	}
	if len(values) == 0 || len(values) > 4 {
		return nil, true, fmt.Errorf("invalid IPv4 component count")
	}
	for _, value := range values[:len(values)-1] {
		if value > 255 {
			return nil, true, fmt.Errorf("invalid IPv4 component")
		}
	}
	lastLimit := uint64(1) << uint(8*(5-len(values)))
	if values[len(values)-1] >= lastLimit {
		return nil, true, fmt.Errorf("invalid IPv4 final component")
	}
	value := values[len(values)-1]
	for index, component := range values[:len(values)-1] {
		value += component << uint(8*(3-index))
	}
	return net.IPv4(byte(value>>24), byte(value>>16), byte(value>>8), byte(value)), true, nil
}

func parseLegacyIPv4Part(part string) (uint64, bool, error) {
	if part == "" {
		return 0, false, nil
	}
	base := 10
	digits := part
	if len(part) >= 2 && part[0] == '0' && (part[1] == 'x' || part[1] == 'X') {
		base = 16
		digits = part[2:]
		if digits == "" {
			return 0, true, fmt.Errorf("empty hexadecimal IPv4 component")
		}
	} else if len(part) >= 2 && part[0] == '0' {
		base = 8
		digits = part[1:]
		if digits == "" {
			return 0, true, nil
		}
	} else {
		for _, char := range part {
			if char < '0' || char > '9' {
				return 0, false, nil
			}
		}
	}
	value, err := strconv.ParseUint(digits, base, 32)
	return value, true, err
}

func isInternalHostname(host string) bool {
	if host == "localhost" || host == "kubernetes" || host == "kubernetes.default" || host == "kubernetes.default.svc" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	if _, ok := internalHostnames[host]; ok {
		return true
	}
	if _, ok := clusterServiceHostnames[host]; ok {
		return true
	}
	first, rest, _ := strings.Cut(host, ".")
	if _, ok := internalHostnames[first]; ok {
		return true
	}
	if _, ok := clusterServiceHostnames[first]; ok {
		if _, nsOK := clusterNamespaces[rest]; nsOK {
			return true
		}
	}
	return strings.HasSuffix(host, ".svc") || strings.Contains(host, ".svc.") || strings.HasSuffix(host, ".cluster.local") || strings.HasSuffix(host, ".local")
}

func isPublicLiteralIP(ip net.IP) bool {
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		first, second := v4[0], v4[1]
		blocked := first == 0 || first >= 224 ||
			(first == 100 && second >= 64 && second <= 127) ||
			(first == 192 && second == 0) ||
			(first == 198 && (second == 18 || second == 19 || second == 51)) ||
			(first == 203 && second == 0)
		return !blocked
	}
	s := strings.ToLower(ip.String())
	return !strings.HasPrefix(s, "fe") && !strings.HasPrefix(s, "2001:db8:")
}
