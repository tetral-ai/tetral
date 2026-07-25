package gitproxy

import (
	"net"
	"net/http"
	"time"
)

func defaultGitHubTransport() http.RoundTripper {
	return &http.Transport{
		DialContext:           (&net.Dialer{Timeout: UpstreamDialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}
}
