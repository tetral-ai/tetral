package workload

import (
	"context"
	"errors"
	"log/slog"
	"strings"
)

// httpServerDiagnostics records what net/http writes to a server's error log.
// The shared process diagnostic policy owns bursts, suppression counts and
// summary intervals.
type httpServerDiagnostics struct{ logger *slog.Logger }

func (w *httpServerDiagnostics) Write(body []byte) (int, error) {
	stage, event, code := "http_server", "workload.http_server.failed", "http_server_failed"
	line, _, _ := strings.Cut(string(body), "\n")
	if rest, found := strings.CutPrefix(line, "http: TLS handshake error from "); found {
		stage, event, code = "tls_handshake", "workload.http_transport.failed", "tls_handshake_failed"
		line = "http: TLS handshake error: " + afterCallerAddress(rest)
	} else if rest, found := strings.CutPrefix(line, "http: panic serving "); found {
		stage, event, code = "handler_panic", "workload.http.panic", "internal_error"
		line = "http: panic serving: " + afterCallerAddress(rest)
	}
	// A diagnostic handler cannot change transport behavior.
	func() {
		defer func() { _ = recover() }()
		WriteFailure(context.Background(), w.logger, event, code, CauseOf(errors.New(line)),
			"operation", "http.listener", "component", "workload", "phase", stage, "transport.stage", stage, "transport.outcome", "failed")
	}()
	return len(body), nil
}

// afterCallerAddress drops the caller's address, which net/http puts before
// ": " in these lines and which a record does not hold.
func afterCallerAddress(rest string) string {
	if _, text, found := strings.Cut(rest, ": "); found {
		return text
	}
	return ""
}
