package workload

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Cause is what a failure record takes for error.message: the text of an
// error, or nothing. Its text comes only from this component's own code, the
// operating system, a linked library (including its view of a remote
// dependency) or a helper process before Session work; never from what the
// work of a Session or a peer product returned.
type Cause struct{ text string }

// CauseOf takes err's text as the cause. An error in err's chain that keeps
// the cause it wraps out of its own text, because that text may be answered
// to a caller, gives it to the record through RecordedCause.
func CauseOf(err error) Cause {
	if err == nil {
		return Cause{}
	}
	text := err.Error()
	var wrapper interface{ RecordedCause() error }
	if errors.As(err, &wrapper) {
		if hidden := wrapper.RecordedCause(); hidden != nil && !strings.Contains(text, hidden.Error()) {
			text += ": " + hidden.Error()
		}
	}
	return Cause{text: shapeCause(text)}
}

// WithoutPaths is err with the configured or mounted paths it names left out
// of its text: the path of every file system error it wraps, and each of
// paths wherever the text holds it, as a library that reads the file may
// quote it in its own words. errors.Is and errors.As still see err.
func WithoutPaths(err error, paths ...string) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	for _, pathError := range pathErrors(err) {
		text = strings.ReplaceAll(text, pathError.Op+" "+pathError.Path+":", pathError.Op+":")
	}
	for _, path := range paths {
		if path == "" {
			continue
		}
		text = strings.ReplaceAll(text, " "+strconv.Quote(path), "")
		text = strings.ReplaceAll(text, strconv.Quote(path), "")
		text = strings.ReplaceAll(text, " "+path, "")
		text = strings.ReplaceAll(text, path, "")
	}
	if text == err.Error() {
		return err
	}
	return pathlessError{text: text, err: err}
}

func pathErrors(err error) []*fs.PathError {
	var found []*fs.PathError
	for err != nil {
		if pathError, ok := err.(*fs.PathError); ok {
			found = append(found, pathError)
		}
		switch wrapper := err.(type) {
		case interface{ Unwrap() []error }:
			for _, inner := range wrapper.Unwrap() {
				found = append(found, pathErrors(inner)...)
			}
			return found
		case interface{ Unwrap() error }:
			err = wrapper.Unwrap()
		default:
			return found
		}
	}
	return found
}

type pathlessError struct {
	text string
	err  error
}

func (e pathlessError) Error() string { return e.text }
func (e pathlessError) Unwrap() error { return e.err }

// NoCause is the cause of a failure whose only text is of a source its code
// declares with no_cause.
var NoCause = Cause{}

// WriteFailure is the one way a failure record is written: event at its
// declared level, with code and the class and safe text the event declares
// for it, and cause as error.message. fields are slog arguments. A code the
// event does not declare, and a cause that disagrees with the code's
// declaration, fail a test binary; production then writes
// diagnostic.invalid_event for the first and follows the declaration for the
// second.
func WriteFailure(ctx context.Context, logger *slog.Logger, event, code string, cause Cause, fields ...any) {
	if logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	handler, owned := logger.Handler().(*diagnosticHandler)
	if !owned {
		// A foreign handler has no table: it gets the code and the cause.
		fields = append(fields, slog.String("error.code", code))
		if cause.text != "" {
			fields = append(fields, slog.String("error.message", cause.text))
		}
		logger.Log(ctx, slog.LevelError, event, fields...)
		return
	}
	handler.writeFailure(ctx, event, code, cause, fields)
}

const causeLimitBytes = 1024

var (
	causeURLPattern     = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>` + "`" + `]+`)
	causeURLHostPattern = regexp.MustCompile(`^(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._~%-]*)(?::[0-9]*)?$`)
)

// shapeCause keeps of each URL its scheme, host and path, turns control
// characters into spaces, and cuts the text at 1,024 UTF-8 bytes. The
// TypeScript library's shapeCause is the same function;
// internal/ts-observability/test-support/cause-shaping.json holds the rows
// both are tested with.
func shapeCause(text string) string {
	text = causeURLPattern.ReplaceAllStringFunc(text, shapeCauseURL)
	text = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return ' '
		}
		return r
	}, text)
	if len(text) <= causeLimitBytes {
		return text
	}
	end := causeLimitBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + "[TRUNCATED]"
}

func shapeCauseURL(raw string) string {
	trimmed := strings.TrimRight(raw, ".,;:!?)]}")
	if strings.Count(trimmed, "[") > strings.Count(trimmed, "]") && raw[len(trimmed)] == ']' {
		// The bracket closes an IPv6 host and is part of the URL.
		trimmed = raw[:len(trimmed)+1]
	}
	scheme, rest, _ := strings.Cut(trimmed, "://")
	authority, tail := cutBeforeAny(rest, "/?#")
	host := authority[strings.LastIndex(authority, "@")+1:]
	if !causeURLHostPattern.MatchString(host) {
		return scheme + "://[REDACTED]" + raw[len(trimmed):]
	}
	path, _ := cutBeforeAny(tail, "?#")
	return scheme + "://" + host + path + raw[len(trimmed):]
}

// cutBeforeAny splits text before the first of chars.
func cutBeforeAny(text, chars string) (before, rest string) {
	if i := strings.IndexAny(text, chars); i >= 0 {
		return text[:i], text[i:]
	}
	return text, ""
}

// failRecordRule fails the test binary on a record that breaks a rule of the
// writer. The panic of a new goroutine ends the binary even where the
// writing site recovers panics around its records.
var failRecordRule = func(problem string) {
	go panic("workload: " + problem)
	select {}
}

// recordRuleBroken fails a test binary on the record of event, and of code
// when it has one, that breaks rule, and names the site that wrote it.
func recordRuleBroken(event, code, rule string) {
	if !failOnUndeclared {
		return
	}
	problem := "event " + strconv.Quote(event)
	if code != "" {
		problem += ", error code " + strconv.Quote(code)
	}
	failRecordRule(problem + ": " + rule + "; written at " + writingSite())
}

// writingSite is the first caller outside this package and log/slog.
func writingSite() string {
	pcs := make([]uintptr, 32)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		library := strings.Contains(frame.File, "/internal/workload/") && !strings.HasSuffix(frame.File, "_test.go") || strings.Contains(frame.File, "/log/slog/")
		if frame.File != "" && !library {
			return frame.File + ":" + strconv.Itoa(frame.Line)
		}
		if !more {
			return "an unknown site"
		}
	}
}

func (h *diagnosticHandler) writeFailure(ctx context.Context, event, code string, cause Cause, args []any) {
	defer func() {
		if recover() != nil {
			h.state.counts.dropped.Add(1)
		}
	}()
	service, _ := h.base["service.name"].(string)
	declared, eventKnown := lookupEvent(service, event)
	fixed, known := declared.Errors[code]
	level := slog.LevelError
	if known {
		_ = level.UnmarshalText([]byte(declared.Level))
	}
	// A test binary checks every failure record, whatever the level set.
	enabled := h.passes(level)
	if !enabled && !failOnUndeclared {
		return
	}
	record := slog.NewRecord(time.Now(), level, event, 0)
	record.Add(args...)
	fields := h.recordFields(ctx, record)
	for _, key := range []string{"error.class", "error.code", "error.message_safe", "error.message"} {
		if _, passed := fields[key]; passed {
			recordRuleBroken(event, code, "the site passes "+key+" as a field; the entry sets it from the code's declaration")
			delete(fields, key)
		}
	}
	switch {
	case !eventKnown && RegisteredDeclarations(service) == nil:
		// A service that registered no table has no declarations to follow,
		// as for its events: the record carries the code and the cause.
		if !enabled {
			return
		}
		fields["error.code"] = code
		if cause.text != "" {
			fields["error.message"] = cause.text
		}
		h.finishRecord(record, fields, event)
		return
	case !eventKnown:
		recordRuleBroken(event, code, "the event is not declared in the observability.json of service "+service)
		if enabled {
			fields["event.original"] = event
			h.finishRecord(record, fields, "diagnostic.invalid_event")
		}
		return
	case !known:
		recordRuleBroken(event, code, "the event does not declare the code in the observability.json of service "+service)
		if enabled {
			h.writeInvalidEvent(record, fields, event, "undeclared_error_code")
		}
		return
	case fixed.Cause && cause.text == "":
		recordRuleBroken(event, code, "the code is declared with a cause and was written without one")
	case !fixed.Cause && cause.text != "":
		recordRuleBroken(event, code, "the code is declared without a cause and was written with one")
	}
	if !enabled {
		return
	}
	fields["error.class"] = fixed.Class
	fields["error.code"] = code
	fields["error.message_safe"] = fixed.MessageSafe
	if fixed.Cause && cause.text != "" {
		fields["error.message"] = cause.text
	}
	h.finishRecord(record, fields, event)
}
