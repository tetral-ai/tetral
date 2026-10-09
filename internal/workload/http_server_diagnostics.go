package workload

import (
	"bytes"
	"context"
	"log/slog"
)

// net/http's diagnostics contain caller addresses and panic/handshake text.
// Keep the owning stage and fixed failure tuple; the shared process diagnostic
// policy owns bursts, suppression counts and summary intervals.
type httpServerDiagnostics struct{ logger *slog.Logger }

func (w *httpServerDiagnostics) Write(body []byte) (int, error) {
	stage, event, class, code, message := "http_server", "workload.http_server.failed", "internal_error", "http_server_failed", "HTTP server failure"
	level := slog.LevelError
	if bytes.HasPrefix(body, []byte("http: TLS handshake error")) {
		stage, event, class, code, message = "tls_handshake", "workload.http_transport.failed", "transport_error", "tls_handshake_failed", "TLS handshake failed"
		level = slog.LevelWarn
	} else if bytes.HasPrefix(body, []byte("http: panic serving")) {
		stage, event, class, code, message = "handler_panic", "workload.http.panic", "panic", "internal_error", "internal error"
	}
	// A diagnostic handler cannot change transport behavior.
	func() {
		defer func() { _ = recover() }()
		w.logger.Log(context.Background(), level, event, "operation", "http.listener", "component", "workload", "phase", stage, "transport.stage", stage, "transport.outcome", "failed", "error.class", class, "error.code", code, "error.message_safe", message)
	}()
	return len(body), nil
}
