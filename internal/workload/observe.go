package workload

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// CodedError is an error that carries an error.code an operation may declare.
type CodedError interface {
	error
	ErrorCode() string
}

// Observation is one running operation: its span, its start and the
// telemetry that records its end.
type Observation struct {
	ctx       context.Context
	telemetry *Telemetry
	operation string
	declared  OperationDeclaration
	span      trace.Span
	started   time.Time
	ended     bool
}

// Observe starts operation, which the service's table must declare. The
// returned context carries the operation's span.
func Observe(ctx context.Context, operation string) (context.Context, *Observation) {
	if ctx == nil {
		ctx = context.Background()
	}
	t := CurrentTelemetry()
	declared, ok := lookupOperation(t.service, operation)
	if !ok {
		undeclared("operation", operation)
		t.logger.LogAttrs(ctx, slog.LevelWarn, "diagnostic.invalid_event", slog.String("component", "workload"), slog.String("reason", "undeclared_operation"))
		operation = "unknown_operation"
	}
	ctx, span := t.tracing.tracer.Start(ctx, operation, trace.WithAttributes(contextAttributes(ctx)...))
	return ctx, &Observation{ctx: ctx, telemetry: t, operation: operation, declared: declared, span: span, started: time.Now()}
}

// End records the operation's duration and outcome. A failure is recorded by
// the component that handles it; one that returns the error calls End.
func (o *Observation) End(err error) { o.end(err, false) }

// EndHandled is End for the component that handles err: it also writes the
// operation's failure record, <operation>.failed.
func (o *Observation) EndHandled(err error) { o.end(err, true) }

func (o *Observation) end(err error, handled bool) {
	if o == nil || o.ended {
		return
	}
	o.ended = true
	elapsed := time.Since(o.started)
	outcome, code := o.outcome(err)
	if outcome != "ok" {
		o.span.SetAttributes(attribute.String("error.class", errorClass(outcome)), attribute.String("error.code", code))
	}
	o.span.End()
	histogram := o.telemetry.operations
	if o.declared.Long {
		histogram = o.telemetry.long
	}
	histogram.Observe(elapsed.Seconds(), o.telemetry.service, o.operation, outcome)
	if handled && err != nil && outcome != "rejected" {
		writeFailureRecord(o.ctx, o.telemetry, o.operation+".failed", code, elapsed)
	}
}

func (o *Observation) outcome(err error) (outcome, code string) {
	if err == nil {
		return "ok", ""
	}
	var rejection *Rejection
	if errors.As(err, &rejection) {
		return "rejected", "rejected"
	}
	var coded CodedError
	if errors.As(err, &coded) {
		for _, declared := range o.declared.Errors {
			if declared == coded.ErrorCode() {
				return declared, declared
			}
		}
	}
	return "error", "operation_failed"
}

func errorClass(outcome string) string {
	if outcome == "error" {
		return "operation_failed"
	}
	return outcome
}

// writeFailureRecord writes event with the error tuple its declaration fixes
// for code, never the error's own text.
func writeFailureRecord(ctx context.Context, t *Telemetry, event, code string, elapsed time.Duration) {
	declared, ok := lookupEvent(t.service, event)
	level := slog.LevelError
	if ok {
		_ = level.UnmarshalText([]byte(declared.Level))
	}
	class, message := "operation_failed", "operation failed"
	if fixed, known := declared.Errors[code]; known {
		class, message = fixed.Class, fixed.MessageSafe
	} else {
		code = "operation_failed"
	}
	t.logger.LogAttrs(ctx, level, event, slog.String("error.class", class), slog.String("error.code", code), slog.String("error.message_safe", message), slog.Int64("duration_ms", elapsed.Milliseconds()))
}

type contextFieldsKey struct{}

// WithFields adds correlation fields, as slog key-value pairs, to ctx. Every
// record written with ctx, and every span and observation started under it,
// carries them.
func WithFields(ctx context.Context, fields ...any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	record := slog.NewRecord(time.Time{}, 0, "", 0)
	record.Add(fields...)
	merged := append([]slog.Attr(nil), ContextFields(ctx)...)
	record.Attrs(func(attr slog.Attr) bool {
		for i := range merged {
			if merged[i].Key == attr.Key {
				merged[i] = attr
				return true
			}
		}
		merged = append(merged, attr)
		return true
	})
	return context.WithValue(ctx, contextFieldsKey{}, merged)
}

// ContextFields returns the correlation fields ctx carries.
func ContextFields(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	fields, _ := ctx.Value(contextFieldsKey{}).([]slog.Attr)
	return fields
}

func contextAttributes(ctx context.Context) []attribute.KeyValue {
	fields := ContextFields(ctx)
	attrs := make([]attribute.KeyValue, 0, len(fields))
	for _, field := range fields {
		if !approvedDiagnosticField(field.Key) {
			continue
		}
		value := field.Value.Resolve()
		switch value.Kind() {
		case slog.KindString:
			attrs = append(attrs, attribute.String(field.Key, boundedDiagnosticString(value.String())))
		case slog.KindInt64:
			attrs = append(attrs, attribute.Int64(field.Key, value.Int64()))
		case slog.KindBool:
			attrs = append(attrs, attribute.Bool(field.Key, value.Bool()))
		}
	}
	return attrs
}
