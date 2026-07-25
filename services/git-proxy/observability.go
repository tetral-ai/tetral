package gitproxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"regexp"
	"time"

	"github.com/tetral-ai/tetral/internal/gitticket"
)

const (
	// Transfer bounds. A relayed git operation is bounded at three points and
	// deliberately NOT bounded on total duration:
	//   HeaderReadTimeout   (30s)  inbound request-header read (run.go
	//                              ReadHeaderTimeout).
	//   UpstreamDialTimeout (10s)  TCP connect to github.com (transport.go
	//                              Dialer.Timeout; bounds the TCP connect and DNS
	//                              only -- the TLS handshake is bounded separately
	//                              by the transport's TLSHandshakeTimeout, also 10s).
	//   IdleProgressTimeout (120s) idle-progress kill: a transfer is aborted only
	//                              after this long with zero bytes moved in either
	//                              direction (relay.go idleProgressTracker).
	// There is no total-transfer timeout on purpose, so a multi-minute clone or
	// push that keeps making progress runs to completion.
	// MaxRequestBodyBytes (2 GiB) is the push-pack size cap (not a general body
	// limit): an over-cap declared Content-Length is rejected 413 before any
	// upstream dial, and a chunked push is aborted 413 mid-stream at the cap.
	// UPDATE-WITH: run.go (HeaderReadTimeout), transport.go (UpstreamDialTimeout),
	// relay.go (IdleProgressTimeout, MaxRequestBodyBytes).
	HeaderReadTimeout         = 30 * time.Second
	UpstreamDialTimeout       = 10 * time.Second
	IdleProgressTimeout       = 120 * time.Second
	MaxRequestBodyBytes       = 2 * 1024 * 1024 * 1024
	MaxConnsPerTicket         = 16
	DrainGraceSeconds         = DefaultDrainGraceSeconds
	AccessLogEventKind        = "gitproxy.access"
	AlertTicketRejectionSpike = "ticket_rejection_spike"
	AlertUpstream5xxRatio     = "upstream_5xx_ratio"
)

var safeRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type AccessLogRecord struct {
	ServiceName           string `json:"service.name"`
	ServiceVersion        string `json:"service.version"`
	DeploymentEnvironment string `json:"deployment.environment"`
	RequestID             string `json:"request.id"`
	WorkspaceID           string `json:"workspace.id"`
	SessionID             string `json:"session.id"`
	Operation             string `json:"operation"`
	EventKind             string `json:"event.kind"`
	Component             string `json:"component"`
	TicketID              string `json:"ticket_id"`
	OwnerRepo             string `json:"owner_repo"`
	Decision              string `json:"decision"`
	UpstreamStatus        int    `json:"upstream_status"`
	BytesIn               int64  `json:"bytes_in"`
	BytesOut              int64  `json:"bytes_out"`
	DurationMS            int64  `json:"duration.ms"`
}

type AccessLogger interface {
	LogAccess(context.Context, AccessLogRecord)
}

type NoopAccessLogger struct{}

func (NoopAccessLogger) LogAccess(context.Context, AccessLogRecord) {}

type JSONAccessLogger struct {
	Logger                *slog.Logger
	Writer                io.Writer
	ServiceName           string
	ServiceVersion        string
	DeploymentEnvironment string
}

type AccessLoggerOption func(*JSONAccessLogger)

func WithAccessLogResource(deploymentEnvironment string, serviceVersion string) AccessLoggerOption {
	return func(logger *JSONAccessLogger) {
		logger.DeploymentEnvironment = deploymentEnvironment
		logger.ServiceVersion = serviceVersion
	}
}

func NewJSONAccessLogger(writer io.Writer, options ...AccessLoggerOption) *JSONAccessLogger {
	if writer == nil {
		writer = os.Stderr
	}
	logger := &JSONAccessLogger{
		Logger:                slog.New(slog.NewJSONHandler(writer, nil)),
		Writer:                writer,
		ServiceName:           ServiceName,
		ServiceVersion:        "unknown",
		DeploymentEnvironment: "local",
	}
	for _, option := range options {
		if option != nil {
			option(logger)
		}
	}
	return logger
}

func (l *JSONAccessLogger) LogAccess(ctx context.Context, record AccessLogRecord) {
	if l == nil {
		return
	}
	logger := l.Logger
	if logger == nil {
		writer := l.Writer
		if writer == nil {
			writer = os.Stderr
		}
		logger = slog.New(slog.NewJSONHandler(writer, nil))
	}
	logger.InfoContext(ctx, AccessLogEventKind,
		slog.String("service.name", valueOrDefault(l.ServiceName, ServiceName)),
		slog.String("service.version", valueOrDefault(l.ServiceVersion, "unknown")),
		slog.String("deployment.environment", valueOrDefault(l.DeploymentEnvironment, "local")),
		slog.String("request.id", record.RequestID),
		slog.String("workspace.id", record.WorkspaceID),
		slog.String("session.id", record.SessionID),
		slog.String("operation", record.Operation),
		slog.String("event.kind", AccessLogEventKind),
		slog.String("component", ServiceName),
		slog.String("ticket_id", record.TicketID),
		slog.String("owner_repo", record.OwnerRepo),
		slog.String("decision", record.Decision),
		slog.Int("upstream_status", record.UpstreamStatus),
		slog.Int64("bytes_in", record.BytesIn),
		slog.Int64("bytes_out", record.BytesOut),
		slog.Int64("duration.ms", record.DurationMS),
	)
}

func requestIDForAccessLog(requestID string) string {
	if safeRequestIDPattern.MatchString(requestID) && gitticket.ValidateToken(requestID) != nil {
		return requestID
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "gitreq_unknown"
	}
	return "gitreq_" + hex.EncodeToString(raw[:])
}
