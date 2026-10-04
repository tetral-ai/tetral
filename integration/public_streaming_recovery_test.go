package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

func TestPostgreSQLPublicStreamingRecovery(t *testing.T) {
	for _, mode := range []string{"normal", "provider-error", "public-interrupt", "runtime-process-loss"} {
		t.Run(mode, func(t *testing.T) {
			scenario := "public-text"
			if mode == "provider-error" {
				scenario = "public-error"
			} else if mode != "normal" {
				scenario = "public-incomplete"
			}
			h := newPublicStreamingHarness(t, scenario, publicStreamingOptions{})
			h.open(t, "before", []string{"agent.message"}, "")
			h.open(t, "ordinary", nil, "")
			h.send(t)
			h.waitFragments(t)
			h.releaseFragments(t, 1)
			h.waitEvent(t, "before", "event_delta", 1)
			h.open(t, "during", []string{"agent.message"}, "")
			remaining := 3
			if mode != "normal" {
				remaining = 4
			}
			h.releaseFragments(t, remaining)
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 2)
			if mode != "normal" {
				publicWait(t, "incomplete third block crosses actual broker", func() bool { return publicTappedDelta(h, loadPublicStreamingVectors(t).Content.Incomplete) })
			}
			switch mode {
			case "normal", "provider-error":
				h.finish(t)
			case "public-interrupt":
				h.client.control(t, "interrupt", map[string]any{"sessionId": h.session})
			case "runtime-process-loss":
				h.recoverRuntime(t)
			}
			for _, viewer := range []string{"before", "ordinary", "during"} {
				s := h.waitEvent(t, viewer, "span.model_request_end", 1)
				h.assertFormal(t, s, []string{"alpha βeta omega\n", "second\n"})
				if viewer == "during" && (countPublicEvents(s, "event_start") != 0 || countPublicEvents(s, "event_delta") != 0) {
					t.Fatal("mid-request opening replayed prior preview eligibility")
				}
			}
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 2)
			h.open(t, "after", []string{"agent.message"}, "")
			if countPublicEvents(h.snapshot(t, "after"), "agent.message") != 0 {
				t.Fatal("new live stream claimed old exactly-once delivery")
			}
			assertContentSDKTextHistory(t, h.contentE2E, []string{"alpha βeta omega\n", "second\n"})
			publicLogAssertion(t, "completed-nonempty-blocks-reconcile-after-"+mode)
		})
	}
	for _, scope := range []string{"session-formal", "session-preview", "thread-formal"} {
		for _, cancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("paged-sdk-recovery/scope=%s/cancel=%t", scope, cancel), func(t *testing.T) { runPublicPagedRecovery(t, scope, cancel) })
		}
		t.Run("second-page-query-cancellation/"+scope, func(t *testing.T) { runPublicSecondPageCancellation(t, scope) })
	}
}

type publicFinalPageObserver struct {
	eventstream.Reader
	calls          atomic.Int64
	deferredBytes  atomic.Int64
	deferredCount  atomic.Int64
	changesRelease <-chan struct{}
	secondPage     *publicSecondPageGate
}

type publicSecondPageGate struct {
	entered, proceed, done chan struct{}
	canceled               atomic.Bool
}

func (r *publicFinalPageObserver) ListRequestFinalMessages(ctx context.Context, scope eventstream.ReadScope, end string, after int64, limit int) ([]eventstream.RequestFinalMessage, error) {
	ordinal := r.calls.Add(1)
	if ordinal == 2 && r.secondPage != nil {
		close(r.secondPage.entered)
		defer close(r.secondPage.done)
		select {
		case <-r.secondPage.proceed:
		case <-ctx.Done():
			r.secondPage.canceled.Store(true)
			return nil, ctx.Err()
		}
		defer func() { r.secondPage.canceled.Store(ctx.Err() != nil) }()
	}
	return r.Reader.ListRequestFinalMessages(ctx, scope, end, after, 1)
}

func runPublicSecondPageCancellation(t *testing.T, scope string) {
	gate := &publicSecondPageGate{entered: make(chan struct{}), proceed: make(chan struct{}), done: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.proceed) }) }
	f := newPublicProjectionFixture(t, publicProjectionOptions{reader: func(r eventstream.Reader) eventstream.Reader {
		return &publicFinalPageObserver{Reader: r, secondPage: gate}
	}})
	t.Cleanup(release)
	thread := ""
	var deltas []string
	switch scope {
	case "thread-formal":
		thread = f.thread(t)
	case "session-preview":
		deltas = []string{"agent.message"}
	}
	f.open(t, "cancel-query", deltas, thread)
	r := f.start(t, "", "")
	first := f.text(t, r, "alpha βeta omega\n", false)
	second := f.text(t, r, "second\n", false)
	f.end(t, r)
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second final page not reached")
	}
	f.waitEvent(t, "cancel-query", "agent.message", 1)
	blocker, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err = blocker.ExecContext(t.Context(), `LOCK TABLE session_events IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	release()
	blocked := func() bool {
		var count int
		if err := f.db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT ended.session_thread_id,%'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count > 0
	}
	publicWait(t, "actual PostgreSQL second-page query waiting on table lock", blocked)
	f.client.control(t, "close_viewer", map[string]any{"viewer": "cancel-query"})
	select {
	case <-gate.done:
	case <-time.After(5 * time.Second):
		t.Fatal("SDK cancellation failed to join actual final-page query")
	}
	if !gate.canceled.Load() {
		t.Fatal("reader returned without request context cancellation")
	}
	publicWait(t, "PostgreSQL query canceled before blocking transaction releases", func() bool { return !blocked() })
	if err = blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	f.open(t, "new-after-cancel", deltas, thread)
	ids := publicProjectionAllPages(t, f, thread, "asc", false)
	if !publicProjectionContains(ids, first) || !publicProjectionContains(ids, second) {
		t.Fatal("history failed to recover canceled End-group texts")
	}
	publicLogAssertion(t, "SDK-cancel-reaches-active-PostgreSQL-second-page-query-and-history-recovers")
}
func (r *publicFinalPageObserver) inspect(changes []eventstream.StreamChange) {
	for _, change := range changes {
		if change.DeferredMessage {
			r.deferredBytes.Add(int64(len(change.Event.Payload)))
			r.deferredCount.Add(1)
		}
	}
}
func (r *publicFinalPageObserver) ListSessionEventChanges(ctx context.Context, ws workspace.ID, session string, after int64, limit int) ([]eventstream.StreamChange, error) {
	if r.changesRelease != nil {
		select {
		case <-r.changesRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	changes, err := r.Reader.ListSessionEventChanges(ctx, ws, session, after, limit)
	r.inspect(changes)
	return changes, err
}
func (r *publicFinalPageObserver) ListThreadEventChanges(ctx context.Context, ws workspace.ID, session, thread string, after int64, limit int) ([]eventstream.StreamChange, error) {
	if r.changesRelease != nil {
		select {
		case <-r.changesRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	changes, err := r.Reader.ListThreadEventChanges(ctx, ws, session, thread, after, limit)
	r.inspect(changes)
	return changes, err
}
func runPublicPagedRecovery(t *testing.T, scope string, cancel bool) {
	barrier := newPublicTextWriteBarrier(t)
	changesRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseChanges := func() { releaseOnce.Do(func() { close(changesRelease) }) }
	var observer *publicFinalPageObserver
	f := newPublicProjectionFixture(t, publicProjectionOptions{reader: func(r eventstream.Reader) eventstream.Reader {
		observer = &publicFinalPageObserver{Reader: r, changesRelease: changesRelease}
		return observer
	}, wrapWriter: barrier.wrap})
	t.Cleanup(barrier.unblock)
	t.Cleanup(releaseChanges)
	threadID := ""
	var deltas []string
	switch scope {
	case "thread-formal":
		threadID = f.thread(t)
	case "session-preview":
		deltas = []string{"agent.message"}
	}
	f.open(t, "paged", deltas, threadID)
	r := f.start(t, "", "")
	ids := make([]string, 3)
	hashes := make([][32]byte, 3)
	for i := range ids {
		body := strings.Repeat(string(rune('a'+i)), 256*1024)
		ids[i] = f.text(t, r, body, false)
		hashes[i] = sha256.Sum256([]byte(body))
	}
	barrier.armed.Store(true)
	end := f.end(t, r)
	suffixRequest := f.start(t, "", "")
	suffix := f.thinking(t, suffixRequest)
	// The actual change query now sees the three deferred messages, End and
	// later changes in one ordinary production-sized batch.
	releaseChanges()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("paged production writer barrier not reached")
	}
	if observer.calls.Load() != 1 || observer.deferredBytes.Load() != 0 || observer.deferredCount.Load() != 3 {
		t.Fatal("writer fetched next full body or change reader retained deferred body")
	}
	if cancel {
		f.client.control(t, "close_viewer", map[string]any{"viewer": "paged"})
		barrier.unblock()
		publicWait(t, "canceled page viewer releases ownership", func() bool { return f.metric(t, "event_stream_preview_viewers") == 0 })
		f.open(t, "reconnected", deltas, threadID)
		if countPublicEvents(f.snapshot(t, "reconnected"), "agent.message") != 0 {
			t.Fatal("reconnect replayed End group")
		}
	} else {
		barrier.unblock()
		snapshot := f.waitEvent(t, "paged", "agent.thinking", 1)
		position := 0
		endSeen := false
		suffixSeen := false
		for _, event := range snapshot.Events {
			if publicEventType(event) == "agent.message" {
				if position >= 3 || publicEventID(event) != ids[position] || sha256.Sum256([]byte(publicText(event))) != hashes[position] {
					t.Fatal("SDK final page hash/identity/order changed")
				}
				position++
			}
			if publicEventID(event) == end {
				if position != 3 {
					t.Fatal("End overtook final page")
				}
				endSeen = true
			}
			if publicEventID(event) == suffix {
				if !endSeen {
					t.Fatal("later change overtook End group")
				}
				suffixSeen = true
			}
		}
		if position != 3 || !endSeen || !suffixSeen {
			t.Fatal("incomplete SDK End-group/suffix delivery")
		}
	}
	for i, id := range ids {
		var digest string
		if err := f.db.QueryRow(`SELECT encode(sha256(convert_to(payload_json::jsonb#>>'{content,0,text}','UTF8')),'hex') FROM session_events WHERE session_id=$1 AND event_id=$2`, f.session, id).Scan(&digest); err != nil || digest != fmt.Sprintf("%x", hashes[i]) {
			t.Fatalf("independent SQL body hash: %v", err)
		}
	}
	f.end(t, suffixRequest)
	publicLogAssertion(t, "page-one-original-SDK-hashes-cancel-or-ordered-End-and-suffix")
}
