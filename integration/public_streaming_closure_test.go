package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/workspace"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

func TestPostgreSQLPublicStreamingRequestClosure(t *testing.T) {
	for _, delayed := range []string{"request_open", "event_start", "event_delta", "thinking_start"} {
		t.Run("delayed-after-formal-closure/"+delayed, func(t *testing.T) {
			f := newPublicProjectionFixture(t, publicProjectionOptions{})
			f.open(t, "before", []string{"agent.message", "agent.thinking"}, "")
			r := f.start(t, "", "")
			m1 := f.text(t, r, "alpha βeta omega\n", true)
			thinking := f.thinking(t, r)
			f.waitEvent(t, "before", "agent.thinking", 1)
			f.open(t, "during", []string{"agent.message"}, "")
			open := f.frame(r, "request_open", "", "", 0, "")
			start := f.frame(r, "event_start", m1, "agent.message", 0, "")
			delta := f.frame(r, "event_delta", m1, "agent.message", 1, "alpha ")
			var held eventwire.PreviewFrame
			switch delayed {
			case "request_open":
				held = open
			case "event_start":
				f.publish(t, open)
				held = start
			case "event_delta":
				f.publish(t, open, start)
				f.waitEvent(t, "before", "event_start", 1)
				held = delta
			case "thinking_start":
				f.publish(t, open, start, delta)
				f.waitEvent(t, "before", "event_delta", 1)
				held = f.frame(r, "event_start", thinking, "agent.thinking", 0, "")
			}
			m2 := f.text(t, r, "second\n", false)
			f.end(t, r)
			for _, viewer := range []string{"before", "during"} {
				result := f.waitEvent(t, viewer, "span.model_request_end", 1)
				f.assertFormal(t, result, []string{"alpha βeta omega\n", "second\n"})
				if viewer == "during" && (countPublicEvents(result, "event_start") != 0 || countPublicEvents(result, "event_delta") != 0) {
					t.Fatal("mid-request viewer received old preview")
				}
			}
			before := f.snapshot(t, "before")
			f.open(t, "after", []string{"agent.message"}, "")
			// Real NATS carries the delayed batch; a new legal request supplies the
			// formal sentinel after both the old request and old event are closed.
			f.publish(t, held, open, start, delta, f.frame(r, "event_start", m2, "agent.message", 0, ""))
			sentinel := f.start(t, "", "")
			after := f.fence(t, "before", sentinel)
			if countPublicEvents(after, "event_start") != countPublicEvents(before, "event_start") || countPublicEvents(after, "event_delta") != countPublicEvents(before, "event_delta") {
				t.Fatal("late frame reopened a formally closed request/event")
			}
			post := f.fence(t, "after", sentinel)
			if countPublicEvents(post, "agent.message") != 0 || countPublicEvents(post, "span.model_request_end") != 0 || countPublicEvents(post, "event_start") != 0 || countPublicEvents(post, "event_delta") != 0 {
				t.Fatal("post-End viewer replayed an old publication group")
			}
			ids := publicProjectionAllPages(t, f, "", "asc", false)
			if !publicProjectionContains(ids, m1) || !publicProjectionContains(ids, m2) {
				t.Fatal("post-End history failed to recover complete messages")
			}
			f.end(t, sentinel)
			publicLogAssertion(t, "late-frame-closure-and-before-during-after-opening")
		})
	}
	for _, primaryFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("primary-child-opposite-End-order/primary-first=%t", primaryFirst), func(t *testing.T) {
			f := newPublicProjectionFixture(t, publicProjectionOptions{})
			setup := f.start(t, "", "")
			child := f.child(t, setup)
			f.end(t, setup)
			f.open(t, "session", []string{"agent.message"}, "")
			f.open(t, "child", nil, child)
			rp := f.start(t, "", "")
			rc := f.start(t, child, "")
			mp := f.text(t, rp, "alpha βeta omega\n", false)
			f.text(t, rc, "public-child-text", false)
			f.publish(t, f.frame(rp, "request_open", "", "", 0, ""), f.frame(rp, "event_start", mp, "agent.message", 0, ""), f.frame(rp, "event_delta", mp, "agent.message", 1, "alpha "))
			f.waitEvent(t, "session", "event_delta", 1)
			if primaryFirst {
				f.end(t, rp)
				f.waitEvent(t, "session", "span.model_request_end", 1)
				f.end(t, rc)
				f.waitEvent(t, "child", "span.model_request_end", 1)
				f.publish(t, f.frame(rp, "event_delta", mp, "agent.message", 2, "βeta "))
				next := f.start(t, "", "")
				result := f.fence(t, "session", next)
				if countPublicEvents(result, "event_delta") != 1 {
					t.Fatal("primary End failed to close its own preview")
				}
				f.end(t, next)
			} else {
				f.end(t, rc)
				childResult := f.waitEvent(t, "child", "span.model_request_end", 1)
				f.assertFormal(t, childResult, []string{"public-child-text"})
				f.publish(t, f.frame(rp, "event_delta", mp, "agent.message", 2, "βeta "))
				f.waitEvent(t, "session", "event_delta", 2)
				f.end(t, rp)
				result := f.waitEvent(t, "session", "span.model_request_end", 1)
				if countPublicEvents(result, "event_delta") != 2 {
					t.Fatal("child End advanced primary closure watermark")
				}
			}
			publicLogAssertion(t, "exact-primary-request-closure-child-End-independent")
		})
	}
	for _, phase := range []string{"before-subscribe", "after-subscribe-before-highwater", "after-highwater-before-activation"} {
		t.Run("opening-race/"+phase, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			options := publicProjectionOptions{config: func(config *eventstream.StreamConfig) { config.PreviewSetupTimeout = 5 * time.Second }}
			if phase == "before-subscribe" || phase == "after-subscribe-before-highwater" {
				options.transport = func(base eventstream.PreviewTransport) eventstream.PreviewTransport {
					return &publicOpeningSubscriptionGate{PreviewTransport: base, before: phase == "before-subscribe", entered: entered, release: release}
				}
			} else {
				options.reader = func(base eventstream.Reader) eventstream.Reader {
					return &publicOpeningHighwaterGate{Reader: base, entered: entered, release: release}
				}
			}
			f := newPublicProjectionFixture(t, options)
			done := make(chan struct{})
			go func() { defer close(done); f.open(t, "race", []string{"agent.message"}, "") }()
			publicProjectionBarrier(t, entered, "opening boundary")
			r := f.start(t, "", "")
			m := f.text(t, r, "alpha βeta omega\n", false)
			if phase != "before-subscribe" {
				f.publish(t, f.frame(r, "request_open", "", "", 0, ""), f.frame(r, "event_start", m, "agent.message", 0, ""), f.frame(r, "event_delta", m, "agent.message", 1, "alpha "))
			}
			close(release)
			publicProjectionBarrier(t, done, "SDK open joined")
			if phase == "before-subscribe" {
				f.publish(t, f.frame(r, "request_open", "", "", 0, ""), f.frame(r, "event_start", m, "agent.message", 0, ""), f.frame(r, "event_delta", m, "agent.message", 1, "alpha "))
			}
			if phase == "after-highwater-before-activation" {
				f.waitEvent(t, "race", "event_delta", 1)
			}
			f.end(t, r)
			result := f.waitEvent(t, "race", "span.model_request_end", 1)
			f.assertFormal(t, result, []string{"alpha βeta omega\n"})
			if phase != "after-highwater-before-activation" && countPublicEvents(result, "event_delta") != 0 {
				t.Fatal("subscription setup made pre-mark Start preview-eligible")
			}
			publicLogAssertion(t, "paused-opening-highwater-admission")
		})
	}
}

type publicOpeningSubscriptionGate struct {
	eventstream.PreviewTransport
	before           bool
	entered, release chan struct{}
	once             atomic.Bool
}

func (g *publicOpeningSubscriptionGate) Subscribe(ctx context.Context, subject string, receive func(string, []byte), lost func(string)) (eventstream.PreviewSubscription, error) {
	gate := g.once.CompareAndSwap(false, true)
	wait := func() error {
		close(g.entered)
		select {
		case <-g.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if gate && g.before {
		if err := wait(); err != nil {
			return nil, err
		}
	}
	subscription, err := g.PreviewTransport.Subscribe(ctx, subject, receive, lost)
	if err == nil && gate && !g.before {
		if err = wait(); err != nil {
			_ = subscription.Close()
			return nil, err
		}
	}
	return subscription, err
}

type publicOpeningHighwaterGate struct {
	eventstream.Reader
	entered, release chan struct{}
	calls            atomic.Int64
}

func (g *publicOpeningHighwaterGate) CurrentStreamPosition(ctx context.Context, ws workspace.ID, session string) (int64, error) {
	value, err := g.Reader.CurrentStreamPosition(ctx, ws, session)
	if err == nil && g.calls.Add(1) == 2 {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return value, err
}
func publicProjectionBarrier(t *testing.T, barrier <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-barrier:
	case <-time.After(5 * time.Second):
		t.Fatalf("deadline awaiting %s", name)
	}
}
func publicProjectionContains(ids []string, id string) bool {
	for _, value := range ids {
		if value == id {
			return true
		}
	}
	return false
}
