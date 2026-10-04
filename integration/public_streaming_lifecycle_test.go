package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/testinfra"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

func TestPostgreSQLPublicStreamingSubscribers(t *testing.T) {
	publicIsolatedProcessCase(t, runPublicStreamingSubscribers)
}
func runPublicStreamingSubscribers(t *testing.T) {
	h, group := newPublicStreamingProcesses(t, "public-text", 2, nil)
	group.selectProcess(0)
	h.open(t, "a1", []string{"agent.message"}, "")
	h.open(t, "a2", []string{"agent.message"}, "")
	h.open(t, "ordinary", nil, "")
	h.open(t, "primary-thread", nil, h.thread(t))
	second := h.newSession(t)
	h.client.control(t, "open", map[string]any{"viewer": "second-session", "sessionId": second, "eventDeltas": []string{"agent.message"}})
	group.selectProcess(1)
	h.open(t, "b1", []string{"agent.message"}, "")
	publicWait(t, "two independent process subscriptions", func() bool { return len(publicBrokerSubscriptions(t, h.broker, h.session)) == 2 })
	if group.processes[0].metric(t, "event_stream_preview_subscriptions") != 2 || group.processes[0].metric(t, "event_stream_preview_viewers") != 3 || group.processes[1].metric(t, "event_stream_preview_subscriptions") != 1 {
		t.Fatal("actual processes did not share one local subscription per Session")
	}
	h.send(t)
	h.waitFragments(t)
	h.releaseFragments(t, 1)
	for _, viewer := range []string{"a1", "a2", "b1"} {
		h.waitEvent(t, viewer, "event_delta", 1)
	}
	h.client.control(t, "close_viewer", map[string]any{"viewer": "a1"})
	if group.processes[0].metric(t, "event_stream_preview_subscriptions") != 2 {
		t.Fatal("first close removed shared process subscription")
	}
	// A's owned client/worker exits; B's independent ordinary subscription keeps
	// the same eligible request. The broker and database remain alive.
	group.restart(t, 0)
	publicWait(t, "only B subscription after A exit", func() bool { return len(publicBrokerSubscriptions(t, h.broker, h.session)) == 1 })
	h.releaseFragments(t, 3)
	h.waitEvent(t, "b1", "event_delta", 4)
	h.finish(t)
	result := h.waitEvent(t, "b1", "span.model_request_end", 1)
	h.assertFormal(t, result, []string{"alpha βeta omega\n", "second\n"})
	assertPublicPreviewShapes(t, result, []string{"agent.message"}, nil)
	group.selectProcess(0)
	h.open(t, "a-new", []string{"agent.message"}, "")
	if countPublicEvents(h.snapshot(t, "a-new"), "event_delta") != 0 {
		t.Fatal("replacement process replayed old preview")
	}
	// A second actual admitted request provides a positive recovery control.
	h.send(t)
	h.waitFragments(t)
	h.releaseFragments(t, 1)
	h.waitEvent(t, "a-new", "event_delta", 1)
	h.releaseFragments(t, 3)
	h.finish(t)
	h.waitEvent(t, "a-new", "span.model_request_end", 1)
	h.client.control(t, "close_viewer", map[string]any{"viewer": "a-new"})
	h.client.control(t, "close_viewer", map[string]any{"viewer": "b1"})
	publicAssertProcessBaseline(t, group.processes[0])
	publicAssertProcessBaseline(t, group.processes[1])
	publicWait(t, "broker unsubscribe after last viewers", func() bool { return len(publicBrokerSubscriptions(t, h.broker, h.session)) == 0 })
	publicLogAssertion(t, "actual-independent-process-fanout-refcounts-restart-no-replay")
	t.Run("last-unsubscribe-races-new-viewer", runPublicLastUnsubscribeRace)
}

type publicClosingTransport struct {
	eventstream.PreviewTransport
	installs         atomic.Int64
	hold             atomic.Bool
	entered, release chan struct{}
}
type publicClosingSubscription struct {
	eventstream.PreviewSubscription
	owner *publicClosingTransport
}

func (p *publicClosingTransport) Subscribe(ctx context.Context, subject string, receive func(string, []byte), lost func(string)) (eventstream.PreviewSubscription, error) {
	sub, err := p.PreviewTransport.Subscribe(ctx, subject, receive, lost)
	if sub == nil {
		return nil, err
	}
	p.installs.Add(1)
	return &publicClosingSubscription{PreviewSubscription: sub, owner: p}, err
}
func (s *publicClosingSubscription) Close() error {
	if s.owner.hold.CompareAndSwap(true, false) {
		close(s.owner.entered)
		<-s.owner.release
	}
	return s.PreviewSubscription.Close()
}
func runPublicLastUnsubscribeRace(t *testing.T) {
	var transport *publicClosingTransport
	joinArrived := make(chan struct{})
	var watchJoin atomic.Bool
	f := newPublicProjectionFixture(t, publicProjectionOptions{
		transport: func(native eventstream.PreviewTransport) eventstream.PreviewTransport {
			transport = &publicClosingTransport{PreviewTransport: native, entered: make(chan struct{}), release: make(chan struct{})}
			return transport
		},
		wrapWriter: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if watchJoin.CompareAndSwap(true, false) {
					close(joinArrived)
				}
				next.ServeHTTP(w, r)
			})
		},
	})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(transport.release) }) }
	t.Cleanup(release)
	setup := f.start(t, "", "")
	child := f.child(t, setup)
	f.end(t, setup)
	f.open(t, "old", []string{"agent.message"}, "")
	f.open(t, "ordinary", nil, "")
	f.open(t, "child", nil, child)
	if transport.installs.Load() != 1 || f.metric(t, "event_stream_preview_viewers") != 1 {
		t.Fatal("ordinary Session or child formal viewers changed native refcount")
	}
	transport.hold.Store(true)
	f.client.control(t, "close_viewer", map[string]any{"viewer": "old"})
	select {
	case <-transport.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("last viewer did not reach actual native unsubscribe boundary")
	}
	watchJoin.Store(true)
	joined := make(chan struct{})
	go func() { defer close(joined); f.open(t, "new", []string{"agent.message"}, "") }()
	select {
	case <-joinArrived:
	case <-time.After(5 * time.Second):
		t.Fatal("new SDK join did not reach production hub")
	}
	if transport.installs.Load() != 1 {
		t.Fatal("new viewer installed against a closing native registration")
	}
	release()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("serialized native unsubscribe/join did not complete")
	}
	if transport.installs.Load() != 2 || f.metric(t, "event_stream_preview_subscriptions") != 1 {
		t.Fatal("new join reused removed subscription")
	}
	r := f.start(t, "", "")
	message := f.text(t, r, "alpha βeta omega\n", false)
	f.publish(t, f.frame(r, "request_open", "", "", 0, ""), f.frame(r, "event_start", message, "agent.message", 0, ""), f.frame(r, "event_delta", message, "agent.message", 1, "alpha "))
	f.waitEvent(t, "new", "event_delta", 1)
	f.end(t, r)
	f.assertFormal(t, f.waitEvent(t, "new", "span.model_request_end", 1), []string{"alpha βeta omega\n"})
	f.client.control(t, "close_viewer", map[string]any{"viewer": "new"})
	publicWait(t, "race final native subscription removed", func() bool {
		return f.metric(t, "event_stream_preview_subscriptions") == 0 && len(publicBrokerSubscriptions(t, f.broker, f.session)) == 0
	})
	publicLogAssertion(t, "actual-native-teardown-barrier-serializes-new-SDK-join-and-formal-child-has-no-refcount")
}

type publicBrokerSubscription struct {
	Subject string `json:"subject"`
	Queue   string `json:"qgroup"`
	CID     uint64 `json:"cid"`
}

func publicBrokerSubscriptions(t *testing.T, broker testinfra.NATSFixture, session string) []publicBrokerSubscription {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "port", broker.Container, "8222/tcp") //nolint:gosec // The run-owned broker descriptor is infrastructure discovery.
	address, err := command.Output()
	if err != nil {
		t.Fatal("discover broker monitoring endpoint")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+strings.TrimSpace(string(address))+"/subsz?subs=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("read broker subscription observations")
	}
	defer func() { _ = response.Body.Close() }()
	var state struct {
		Subscriptions []publicBrokerSubscription `json:"subscriptions_list"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 1024*1024)).Decode(&state) != nil {
		t.Fatal("invalid broker subscription observation")
	}
	subject := eventwire.PreviewSubject("default", session)
	var result []publicBrokerSubscription
	seen := map[uint64]bool{}
	for _, entry := range state.Subscriptions {
		if entry.Subject == subject {
			if entry.Queue != "" || seen[entry.CID] || entry.CID == 0 {
				t.Fatalf("preview subscriptions are not independent ordinary connections: %s", fmt.Sprint(entry))
			}
			seen[entry.CID] = true
			result = append(result, entry)
		}
	}
	return result
}
