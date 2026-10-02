package workload

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DiagnosticConfig is validated once at startup; changing it requires restart.
type DiagnosticConfig struct {
	Level           slog.Level
	MaxRecordBytes  int
	SummaryInterval time.Duration
	Burst           int
}

func DefaultDiagnosticConfig() DiagnosticConfig {
	return DiagnosticConfig{slog.LevelInfo, 16384, 30 * time.Second, 1}
}
func DiagnosticConfigFromEnv(getenv func(string) string) (DiagnosticConfig, error) {
	cfg := DefaultDiagnosticConfig()
	if getenv == nil {
		return cfg, nil
	}
	switch getenv("TETRAL_LOG_LEVEL") {
	case "", "info":
	case "debug":
		cfg.Level = slog.LevelDebug
	case "warn":
		cfg.Level = slog.LevelWarn
	case "error":
		cfg.Level = slog.LevelError
	default:
		return cfg, NewConfigError("TETRAL_LOG_LEVEL must be debug, info, warn, or error")
	}
	for _, bound := range []struct {
		key      string
		target   *int
		min, max int
	}{{"TETRAL_LOG_MAX_RECORD_BYTES", &cfg.MaxRecordBytes, 1024, 65536}, {"TETRAL_LOG_BURST", &cfg.Burst, 1, 1000}} {
		raw := getenv(bound.key)
		if raw == "" {
			continue
		}
		n, err := strconv.Atoi(raw)
		if err != nil || strconv.Itoa(n) != raw || n < bound.min || n > bound.max {
			return cfg, NewConfigError(bound.key + " outside supported positive integer range")
		}
		*bound.target = n
	}
	if raw := getenv("TETRAL_LOG_SUMMARY_INTERVAL_MS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || strconv.Itoa(n) != raw || n < 100 || n > 3600000 {
			return cfg, NewConfigError("TETRAL_LOG_SUMMARY_INTERVAL_MS must be 100 through 3600000 milliseconds")
		}
		cfg.SummaryInterval = time.Duration(n) * time.Millisecond
	}
	return cfg, nil
}

const diagnosticQueueCapacity = 64
const diagnosticLimiterCapacity = 256
const diagnosticCloseTimeout = time.Second

var diagnosticInstanceID = func() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.Itoa(os.Getpid())
	}
	return hex.EncodeToString(b[:])
}()

// DiagnosticStats accounts for best-effort losses without recursively logging them.
type DiagnosticStats struct {
	Emitted, Dropped, SinkFailures, Suppressed, Filtered uint64
	LimiterEntries, Queued                               int
}
type diagnosticCounters struct{ emitted, dropped, failures, suppressed, filtered atomic.Uint64 }
type diagnosticWindow struct {
	started             time.Time
	first, last         string
	emitted, suppressed int
	level               slog.Level
	sample              map[string]any
}
type diagnosticState struct {
	mu      sync.Mutex
	cfg     DiagnosticConfig
	windows map[string]*diagnosticWindow
	counts  diagnosticCounters
	clock   func() time.Time
}
type diagnosticWriter interface{ Write([]byte) (int, error) }

// ProcessLogger owns exactly one worker and one fixed queue. The producer never
// waits for stderr. Close is budget-limited; an arbitrary io.Writer cannot be
// canceled. If a write never returns, only that single worker remains blocked;
// it cannot retain further per-record workers or unbounded queued data.
type ProcessLogger struct {
	Logger  *slog.Logger
	state   *diagnosticState
	queue   chan []byte
	done    chan struct{}
	mu      sync.Mutex
	closed  bool
	discard atomic.Bool
}

func NewProcessLogger(writer io.Writer, service, environment, version string, cfg DiagnosticConfig) *ProcessLogger {
	if writer == nil {
		writer = os.Stderr
	}
	owner := &ProcessLogger{state: newDiagnosticState(cfg), queue: make(chan []byte, diagnosticQueueCapacity), done: make(chan struct{})}
	owner.Logger = newDiagnosticLogger(owner, service, environment, version, owner.state)
	go func() {
		defer close(owner.done)
		ticker := time.NewTicker(cfg.SummaryInterval)
		defer ticker.Stop()
		for {
			select {
			case line, ok := <-owner.queue:
				if !ok {
					return
				}
				if owner.discard.Load() {
					owner.state.counts.dropped.Add(1)
					continue
				}
				safeDiagnosticWrite(writer, line, &owner.state.counts)
			case <-ticker.C:
				owner.state.flushExpired(owner)
			}
		}
	}()
	return owner
}
func (p *ProcessLogger) Write(line []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(line) > p.state.cfg.MaxRecordBytes {
		p.state.counts.dropped.Add(1)
		return len(line), nil
	}
	select {
	case p.queue <- append([]byte(nil), line...):
	default:
		p.state.counts.dropped.Add(1)
	}
	return len(line), nil
}

// CloseWithBudget is suitable for deferred command cleanup, after business resources close.
func (p *ProcessLogger) CloseWithBudget() {
	ctx, cancel := context.WithTimeout(context.Background(), diagnosticCloseTimeout)
	defer cancel()
	p.Close(ctx)
}
func (p *ProcessLogger) Close(ctx context.Context) {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), diagnosticCloseTimeout)
		defer cancel()
	}
	p.state.flush(p)
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	select {
	case <-p.done:
	case <-ctx.Done():
		p.discard.Store(true)
	}
}
func (p *ProcessLogger) Stats() DiagnosticStats {
	s := p.state.stats()
	s.Queued = len(p.queue)
	return s
}
func (p *ProcessLogger) Metrics() MetricsCollector {
	return func(context.Context) ([]Metric, error) {
		s := p.Stats()
		return []Metric{{Name: "tetral_diagnostic_emitted_total", Type: "counter", Value: float64(s.Emitted)}, {Name: "tetral_diagnostic_dropped_total", Type: "counter", Value: float64(s.Dropped)}, {Name: "tetral_diagnostic_sink_failures_total", Type: "counter", Value: float64(s.SinkFailures)}, {Name: "tetral_diagnostic_suppressed_total", Type: "counter", Value: float64(s.Suppressed)}, {Name: "tetral_diagnostic_filtered_total", Type: "counter", Value: float64(s.Filtered)}, {Name: "tetral_diagnostic_queue_records", Type: "gauge", Value: float64(s.Queued)}, {Name: "tetral_diagnostic_limiter_entries", Type: "gauge", Value: float64(s.LimiterEntries)}}, nil
	}
}
func safeDiagnosticWrite(writer io.Writer, line []byte, c *diagnosticCounters) {
	defer func() {
		if recover() != nil {
			c.failures.Add(1)
			c.dropped.Add(1)
		}
	}()
	n, err := writer.Write(line)
	if err != nil || n != len(line) {
		c.failures.Add(1)
		c.dropped.Add(1)
	} else {
		c.emitted.Add(1)
	}
}
func newDiagnosticState(cfg DiagnosticConfig) *diagnosticState {
	return &diagnosticState{cfg: cfg, windows: map[string]*diagnosticWindow{}, clock: time.Now}
}
func (s *diagnosticState) stats() DiagnosticStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return DiagnosticStats{Emitted: s.counts.emitted.Load(), Dropped: s.counts.dropped.Load(), SinkFailures: s.counts.failures.Load(), Suppressed: s.counts.suppressed.Load(), Filtered: s.counts.filtered.Load(), LimiterEntries: len(s.windows)}
}
func (s *diagnosticState) encode(writer diagnosticWriter, fields map[string]any) {
	line, err := json.Marshal(fields)
	if err != nil || len(line)+1 > s.cfg.MaxRecordBytes {
		s.counts.dropped.Add(1)
		return
	}
	line = append(line, '\n')
	_, _ = writer.Write(line)
}
func (s *diagnosticState) summary(writer diagnosticWriter, w *diagnosticWindow) {
	if w.suppressed == 0 {
		return
	}
	fields := map[string]any{}
	for key, value := range w.sample {
		fields[key] = value
	}
	fields["event.original"] = fields["event"]
	fields["msg"] = "diagnostic.suppressed"
	fields["event"] = "diagnostic.suppressed"
	fields["operation"] = "diagnostic.limiter"
	fields["suppressed.count"] = w.suppressed
	fields["first_seen"] = w.first
	fields["last_seen"] = w.last
	fields["time"] = s.clock().UTC().Format(time.RFC3339Nano)
	s.encode(writer, fields)
}
func (s *diagnosticState) flush(writer diagnosticWriter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, w := range s.windows {
		s.summary(writer, w)
		delete(s.windows, key)
	}
}
func (s *diagnosticState) emit(writer diagnosticWriter, level slog.Level, fields map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, failure := fields["error.class"]
	now := s.clock()
	if recovered, ok := fields["recovery.event"].(string); ok {
		for key, window := range s.windows {
			if window.sample["event"] == recovered && (fields["recovery.resource"] == nil || window.sample["kubernetes.resource"] == fields["recovery.resource"]) {
				s.summary(writer, window)
				delete(s.windows, key)
			}
		}
	}
	if level < slog.LevelWarn && !failure {
		s.encode(writer, fields)
		return
	}
	event, _ := fields["event"].(string)
	reason, _ := fields["error.code"].(string)
	if reason == "" {
		reason, _ = fields["reason"].(string)
	}
	resource, _ := fields["kubernetes.resource"].(string)
	if resource != "pods" && resource != "endpointslices" {
		resource = ""
	}
	key := event + "\x00" + reason + "\x00" + resource
	w := s.windows[key]
	if w != nil && now.Sub(w.started) >= s.cfg.SummaryInterval {
		s.summary(writer, w)
		delete(s.windows, key)
		w = nil
	}
	if w == nil {
		for oldKey, old := range s.windows {
			if now.Sub(old.started) >= s.cfg.SummaryInterval {
				s.summary(writer, old)
				delete(s.windows, oldKey)
			}
		}
		if len(s.windows) >= diagnosticLimiterCapacity {
			s.counts.dropped.Add(1)
			return
		}
		sample := map[string]any{}
		for _, key := range []string{"event", "reason", "level", "service.name", "service.version", "deployment.environment", "service.instance.id", "process.pid", "workspace.id", "session.id", "thread.id", "request.id", "operation.id", "kubernetes.resource", "kubernetes.status", "error.class", "error.code", "error.message_safe", "phase", "component"} {
			if v, ok := fields[key]; ok {
				sample[key] = v
			}
		}
		stamp, _ := fields["time"].(string)
		w = &diagnosticWindow{started: now, first: stamp, last: stamp, level: level, sample: sample}
		s.windows[key] = w
	}
	if _, hadFailure := w.sample["error.class"]; !hadFailure && fields["error.class"] != nil {
		for _, field := range []string{"workspace.id", "session.id", "thread.id", "request.id", "operation.id", "error.class", "error.code", "error.message_safe", "phase", "component"} {
			delete(w.sample, field)
			if value, present := fields[field]; present {
				w.sample[field] = value
			}
		}
	}
	w.last, _ = fields["time"].(string)
	if level > w.level {
		w.level = level
		w.sample["level"] = level.String()
		w.emitted = 0
	}
	if w.emitted >= s.cfg.Burst {
		w.suppressed++
		s.counts.suppressed.Add(1)
		return
	}
	w.emitted++
	s.encode(writer, fields)
}

type diagnosticHandler struct {
	writer diagnosticWriter
	state  *diagnosticState
	base   map[string]any
	attrs  []slog.Attr
	group  string
}

func (h *diagnosticHandler) Enabled(_ context.Context, level slog.Level) bool {
	h.state.mu.Lock()
	threshold := h.state.cfg.Level
	h.state.mu.Unlock()
	ok := level >= threshold
	if !ok {
		h.state.counts.filtered.Add(1)
	}
	return ok
}
func (h *diagnosticHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.attrs = append([]slog.Attr{}, h.attrs...)
	visited := 0
	var add func(slog.Attr, string)
	add = func(attr slog.Attr, prefix string) {
		if visited >= 64 || len(next.attrs) >= 64 {
			return
		}
		visited++
		key := prefix + attr.Key
		if len(key) > 128 {
			return
		}
		value := attr.Value.Resolve()
		if value.Kind() == slog.KindGroup {
			if attr.Key != "" {
				prefix += attr.Key + "."
			}
			for _, child := range value.Group() {
				if visited >= 64 {
					break
				}
				add(child, prefix)
			}
			return
		}
		if safe, ok := diagnosticValue(key, value); ok {
			next.attrs = append(next.attrs, slog.Any(key, safe))
		}
	}
	func() {
		defer func() {
			if recover() != nil {
				h.state.counts.dropped.Add(1)
			}
		}()
		for _, attr := range attrs {
			if visited >= 64 {
				break
			}
			add(attr, h.group)
		}
	}()
	return &next
}
func (h *diagnosticHandler) WithGroup(name string) slog.Handler {
	next := *h
	if name != "" && len(next.group)+len(name)+1 <= 128 {
		next.group += name + "."
	}
	return &next
}
func (h *diagnosticHandler) Handle(_ context.Context, r slog.Record) (err error) {
	defer func() {
		if recover() != nil {
			h.state.counts.dropped.Add(1)
		}
	}()
	fields := map[string]any{}
	count := 0
	visited := 0
	var add func(slog.Attr, string)
	add = func(a slog.Attr, prefix string) {
		if visited >= 64 || len(prefix)+len(a.Key) > 128 {
			return
		}
		visited++
		a.Value = a.Value.Resolve()
		if a.Value.Kind() == slog.KindGroup {
			for _, child := range a.Value.Group() {
				if visited >= 64 {
					break
				}
				groupPrefix := prefix
				if a.Key != "" {
					groupPrefix += a.Key + "."
				}
				add(child, groupPrefix)
			}
			return
		}
		key := prefix + a.Key
		if _, owned := h.base[key]; owned {
			return
		}
		value, ok := diagnosticValue(key, a.Value)
		if ok {
			count++
			fields[key] = value
		}
	}
	for _, a := range h.attrs {
		add(a, "")
	}
	r.Attrs(func(a slog.Attr) bool { add(a, h.group); return visited < 64 })
	for key, value := range h.base {
		fields[key] = value
	}
	event := safeDiagnosticEvent(r.Message)
	if event == "diagnostic.invalid_event" {
		for _, key := range []string{"event", "event.kind"} {
			if candidate, ok := fields[key].(string); ok && safeDiagnosticEvent(candidate) != "diagnostic.invalid_event" {
				event = candidate
				break
			}
		}
	}
	fields["msg"] = event
	fields["event"] = event
	fields["time"] = r.Time.UTC().Format(time.RFC3339Nano)
	fields["level"] = r.Level.String()
	if _, ok := fields["operation"]; !ok {
		fields["operation"] = strings.Split(event, ".")[0]
	}
	_, hasClass := fields["error.class"]
	_, hasCode := fields["error.code"]
	_, hasMessage := fields["error.message_safe"]
	if r.Level >= slog.LevelError || hasClass || hasCode || hasMessage {
		if _, ok := fields["error.class"]; !ok {
			fields["error.class"] = "operation_failed"
		}
		if _, ok := fields["error.code"]; !ok {
			fields["error.code"] = "operation_failed"
		}
		if _, ok := fields["error.message_safe"]; !ok {
			fields["error.message_safe"] = "operation failed"
		}
	}
	h.state.emit(h.writer, r.Level, fields)
	return nil
}

var diagnosticSensitiveKey = regexp.MustCompile(`(?i)(^|[._-])(authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|secret|password|body|content|prompt|reasoning|arguments|payload|stack|headers|sql|exception|tool_result|provider_response)([._-]|$)`)
var diagnosticSensitiveValue = regexp.MustCompile(`(?i)Bearer\s+\S+|(?:sk|ant|ghp|github)[-_][A-Za-z0-9._-]{8,}|(?:access_token|refresh_token|client_secret|token)=|https?://`)

func boundedDiagnosticString(value string) string {
	if len(value) > 1024 {
		return "[TRUNCATED]"
	}
	value = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return ' '
		}
		return r
	}, value)
	if len(value) > 1024 {
		value = value[:1024]
	}
	return value
}
func diagnosticValue(key string, v slog.Value) (any, bool) {
	if diagnosticSensitiveKey.MatchString(key) && (!approvedDiagnosticField(key) || !strings.HasSuffix(key, "_count") || (v.Kind() != slog.KindInt64 && v.Kind() != slog.KindUint64 && v.Kind() != slog.KindFloat64)) {
		return "[REDACTED]", true
	}
	if v.Kind() == slog.KindString && len(v.String()) <= 1024 && diagnosticSensitiveValue.MatchString(v.String()) {
		return "[REDACTED]", true
	}
	if !approvedDiagnosticField(key) {
		return nil, false
	}
	switch v.Kind() {
	case slog.KindString:
		s := v.String()
		if len(s) > 1024 {
			return "[TRUNCATED]", true
		}
		if diagnosticSensitiveValue.MatchString(s) {
			return "[REDACTED]", true
		}
		return boundedDiagnosticString(s), true
	case slog.KindBool:
		return v.Bool(), true
	case slog.KindInt64:
		return v.Int64(), true
	case slog.KindUint64:
		return v.Uint64(), true
	case slog.KindFloat64:
		return v.Float64(), true
	case slog.KindDuration:
		return v.Duration().String(), true
	case slog.KindTime:
		return v.Time().UTC().Format(time.RFC3339Nano), true
	default:
		return "[REDACTED]", true
	}
}
func newDiagnosticLogger(writer diagnosticWriter, service, environment, version string, state *diagnosticState) *slog.Logger {
	return slog.New(&diagnosticHandler{writer: writer, state: state, base: map[string]any{"service.name": safeDiagnosticResource(defaultString(service, "unknown")), "deployment.environment": safeDiagnosticResource(defaultString(environment, DefaultDeploymentEnvironment)), "service.version": safeDiagnosticResource(defaultString(version, DefaultServiceVersion)), "service.instance.id": diagnosticInstanceID, "process.pid": os.Getpid()}})
}

type promptDiagnosticWriter struct {
	writer io.Writer
	counts *diagnosticCounters
}

func (w promptDiagnosticWriter) Write(line []byte) (int, error) {
	safeDiagnosticWrite(w.writer, line, w.counts)
	return len(line), nil
}

// DiagnosticMetrics finds the owned shared handler; unrelated injected loggers expose no series.
func DiagnosticMetrics(logger *slog.Logger) MetricsCollector {
	return func(context.Context) ([]Metric, error) {
		if logger == nil {
			return nil, nil
		}
		h, ok := logger.Handler().(*diagnosticHandler)
		if !ok {
			return nil, nil
		}
		s := h.state.stats()
		return []Metric{{Name: "tetral_diagnostic_emitted_total", Type: "counter", Value: float64(s.Emitted)}, {Name: "tetral_diagnostic_dropped_total", Type: "counter", Value: float64(s.Dropped)}, {Name: "tetral_diagnostic_sink_failures_total", Type: "counter", Value: float64(s.SinkFailures)}, {Name: "tetral_diagnostic_suppressed_total", Type: "counter", Value: float64(s.Suppressed)}, {Name: "tetral_diagnostic_filtered_total", Type: "counter", Value: float64(s.Filtered)}, {Name: "tetral_diagnostic_limiter_entries", Type: "gauge", Value: float64(s.LimiterEntries)}}, nil
	}
}

// DiagnosticMetricsText is suitable for existing service-local metrics renderers.
func DiagnosticMetricsText(logger *slog.Logger) string {
	metrics, _ := DiagnosticMetrics(logger)(context.Background())
	var b strings.Builder
	headers := map[string]bool{}
	for _, metric := range metrics {
		writeMetric(&b, headers, metric)
	}
	return b.String()
}

// SetLevel preserves a service's established boot-only debug switch. Call before serving traffic.
func (p *ProcessLogger) SetLevel(level slog.Level) {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	p.state.cfg.Level = level
}

var diagnosticEventPattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

// Event names are stable owner-controlled identifiers, never arbitrary messages or payloads.
func safeDiagnosticEvent(event string) string {
	if !diagnosticEventPattern.MatchString(event) || diagnosticSensitiveValue.MatchString(event) {
		return "diagnostic.invalid_event"
	}
	return event
}

// InstallDefaultLogger gives legacy package-level diagnostics the process's owned sink.
// Process composition installs it before constructing dependencies and restores after cleanup.
func InstallDefaultLogger(logger *slog.Logger) func() {
	previous := slog.Default()
	slog.SetDefault(logger)
	return func() { slog.SetDefault(previous) }
}

func (s *diagnosticState) flushExpired(writer diagnosticWriter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	for key, w := range s.windows {
		if now.Sub(w.started) >= s.cfg.SummaryInterval {
			s.summary(writer, w)
			delete(s.windows, key)
		}
	}
}

func safeDiagnosticResource(value string) string {
	if len(value) > 253 {
		return "[TRUNCATED]"
	}
	if diagnosticSensitiveValue.MatchString(value) {
		return "[REDACTED]"
	}
	return boundedDiagnosticString(value)
}

// ComponentLogger uses the installed process sink. Embedded components without
// a process owner opt out; callers may inject an explicit prompt test writer.
func ComponentLogger(service string) *slog.Logger {
	logger := slog.Default()
	if _, owned := logger.Handler().(*diagnosticHandler); owned {
		return logger
	}
	return NewLogger(io.Discard, service, "", "")
}
