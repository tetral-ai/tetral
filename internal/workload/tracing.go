package workload

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// spanQueueCapacity bounds the spans waiting for export. A full queue refuses
// the span instead of blocking the request that ended it.
const spanQueueCapacity = 2048

const spanExportBatch = 256

// TraceContext is the only propagator: W3C traceparent, without tracestate.
var TraceContext propagation.TextMapPropagator = traceparentOnly{}

type traceparentOnly struct{ propagation.TraceContext }

func (traceparentOnly) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return
	}
	carrier.Set("traceparent", Traceparent(sc))
}

func (traceparentOnly) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	sc := ParseTraceparent(carrier.Get("traceparent"))
	if !sc.IsValid() {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

func (traceparentOnly) Fields() []string { return []string{"traceparent"} }

// Traceparent formats a span context as a version 00 traceparent value.
func Traceparent(sc trace.SpanContext) string {
	flags := "00"
	if sc.IsSampled() {
		flags = "01"
	}
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-" + flags
}

// ParseTraceparent reads a version 00 traceparent; an invalid value yields an
// invalid span context.
func ParseTraceparent(value string) trace.SpanContext {
	if len(value) != 55 || value[:3] != "00-" {
		return trace.SpanContext{}
	}
	carrier := propagation.MapCarrier{"traceparent": value}
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), carrier))
	return sc.WithTraceState(trace.TraceState{})
}

// tracing owns the process's tracer provider and its bounded export queue.
type tracing struct {
	provider  *sdktrace.TracerProvider
	tracer    trace.Tracer
	processor *boundedSpanProcessor
}

type tracingOptions struct {
	service, environment, version string
	sampleRatio                   float64
	exporter                      sdktrace.SpanExporter
}

func newTracing(options tracingOptions) *tracing {
	res := resource.NewSchemaless(
		attribute.String("service.name", options.service),
		attribute.String("service.version", options.version),
		attribute.String("service.instance.id", diagnosticInstanceID),
		attribute.String("deployment.environment", options.environment),
	)
	providerOptions := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(options.sampleRatio))),
	}
	var processor *boundedSpanProcessor
	if options.exporter != nil {
		processor = newBoundedSpanProcessor(options.exporter)
		providerOptions = append(providerOptions, sdktrace.WithSpanProcessor(processor))
	}
	provider := sdktrace.NewTracerProvider(providerOptions...)
	return &tracing{provider: provider, tracer: provider.Tracer("github.com/tetral-ai/tetral/internal/workload"), processor: processor}
}

// newOTLPExporter dials lazily: an unreachable collector drops spans in the
// export worker and never delays startup or a request.
func newOTLPExporter(endpoint string) (sdktrace.SpanExporter, error) {
	return otlptracegrpc.New(context.Background(), otlptracegrpc.WithEndpointURL(endpoint))
}

func (t *tracing) shutdown(ctx context.Context) {
	if t == nil {
		return
	}
	_ = t.provider.Shutdown(ctx)
}

func (t *tracing) droppedSpans() uint64 {
	if t == nil || t.processor == nil {
		return 0
	}
	return t.processor.dropped.Load()
}

// boundedSpanProcessor hands ended sampled spans to one export worker
// through a fixed queue.
type boundedSpanProcessor struct {
	exporter sdktrace.SpanExporter
	queue    chan sdktrace.ReadOnlySpan
	dropped  atomic.Uint64
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func newBoundedSpanProcessor(exporter sdktrace.SpanExporter) *boundedSpanProcessor {
	p := &boundedSpanProcessor{exporter: exporter, queue: make(chan sdktrace.ReadOnlySpan, spanQueueCapacity), stop: make(chan struct{}), done: make(chan struct{})}
	go p.run()
	return p
}

func (p *boundedSpanProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}

func (p *boundedSpanProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	if !span.SpanContext().IsSampled() {
		return
	}
	select {
	case p.queue <- span:
	default:
		p.dropped.Add(1)
	}
}

func (p *boundedSpanProcessor) run() {
	defer close(p.done)
	batch := make([]sdktrace.ReadOnlySpan, 0, spanExportBatch)
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = p.exporter.ExportSpans(ctx, batch)
		cancel()
		batch = batch[:0]
	}
	for {
		select {
		case span := <-p.queue:
			batch = append(batch, span)
			if len(batch) == spanExportBatch {
				flush()
			}
		case <-timer.C:
			flush()
		case <-p.stop:
			for {
				select {
				case span := <-p.queue:
					batch = append(batch, span)
					if len(batch) == spanExportBatch {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

func (p *boundedSpanProcessor) Shutdown(ctx context.Context) error {
	p.once.Do(func() { close(p.stop) })
	select {
	case <-p.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return p.exporter.Shutdown(ctx)
}

func (p *boundedSpanProcessor) ForceFlush(context.Context) error { return nil }

// Span is a span of the process tracer. Packages outside this one start,
// annotate and end spans through it, so the tracing SDK has one owner.
type Span struct{ span trace.Span }

// StartRequestSpan starts the server span of an external request: a new
// trace whose only relation to an incoming traceparent is a link.
func StartRequestSpan(ctx context.Context, name, incomingTraceparent string) (context.Context, *Span) {
	options := []trace.SpanStartOption{trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(contextAttributes(ctx)...)}
	if incoming := ParseTraceparent(incomingTraceparent); incoming.IsValid() {
		options = append(options, trace.WithLinks(trace.Link{SpanContext: incoming}))
	}
	ctx, span := CurrentTelemetry().Tracer().Start(ctx, name, options...)
	return ctx, &Span{span}
}

// StartServerSpan starts the span of an internal call that continues the
// caller's traceparent, or starts a trace when there is none.
func StartServerSpan(ctx context.Context, name, callerTraceparent string) (context.Context, *Span) {
	if caller := ParseTraceparent(callerTraceparent); caller.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, caller)
	}
	ctx, span := CurrentTelemetry().Tracer().Start(ctx, name, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(contextAttributes(ctx)...))
	return ctx, &Span{span}
}

// StartRemoteChildSpan starts a child span of the span parentTraceparent
// names, in that trace, for work that continues another process's span.
// Without a valid traceparent it starts none and returns a nil Span, whose
// methods do nothing.
func StartRemoteChildSpan(ctx context.Context, name, parentTraceparent string) (context.Context, *Span) {
	parent := ParseTraceparent(parentTraceparent)
	if !parent.IsValid() {
		return ctx, nil
	}
	ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	ctx, span := CurrentTelemetry().Tracer().Start(ctx, name, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(contextAttributes(ctx)...))
	return ctx, &Span{span}
}

// StartSpan starts a child span of ctx's span; it records only when its trace
// is sampled.
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	ctx, span := CurrentTelemetry().Tracer().Start(ctx, name, trace.WithAttributes(contextAttributes(ctx)...))
	return ctx, &Span{span}
}

// ContextTraceparent is the traceparent of ctx's span, or "" without one.
func ContextTraceparent(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return Traceparent(sc)
}

// ContextTraceSampled reports whether ctx's trace is sampled.
func ContextTraceSampled(ctx context.Context) bool {
	return trace.SpanContextFromContext(ctx).IsSampled()
}

func (s *Span) SetName(name string) {
	if s != nil {
		s.span.SetName(name)
	}
}

func (s *Span) SetString(key, value string) {
	if s != nil {
		s.span.SetAttributes(attribute.String(key, value))
	}
}

func (s *Span) SetInt(key string, value int64) {
	if s != nil {
		s.span.SetAttributes(attribute.Int64(key, value))
	}
}

// SetFailure marks a failed span with its error class and code, never an
// error's message.
func (s *Span) SetFailure(class, code string) {
	if s != nil {
		s.span.SetAttributes(attribute.String("error.class", class), attribute.String("error.code", code))
	}
}

func (s *Span) End() {
	if s != nil {
		s.span.End()
	}
}
