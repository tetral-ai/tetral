package eventstream

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/tetral-ai/tetral/internal/eventwire"
)

// This speaks just INFO/CONNECT/PING/SUB/UNSUB/MSG over a real TCP socket.
// It verifies the pinned official client's admission and queue semantics;
// real broker permissions/TLS/SDK behavior belongs to the integration roots.
type nativeProtocolFixture struct {
	listener   net.Listener
	mu         sync.Mutex
	connection net.Conn
	subjects   map[string]string
	writeMu    sync.Mutex
	done       chan struct{}
}

func newNativeProtocolFixture(t *testing.T, ceiling int) *nativeProtocolFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &nativeProtocolFixture{listener: listener, subjects: map[string]string{}, done: make(chan struct{})}
	go func() {
		defer close(f.done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.connection = connection
			f.subjects = map[string]string{}
			f.mu.Unlock()
			f.writeMu.Lock()
			_, _ = fmt.Fprintf(connection, "INFO {\"server_id\":\"owned-native-fixture\",\"max_payload\":%d,\"proto\":1}\r\n", ceiling)
			f.writeMu.Unlock()
			func() {
				scanner := bufio.NewScanner(connection)
				for scanner.Scan() {
					fields := strings.Fields(scanner.Text())
					if len(fields) == 0 {
						continue
					}
					switch fields[0] {
					case "PING":
						f.writeMu.Lock()
						_, _ = io.WriteString(connection, "PONG\r\n")
						f.writeMu.Unlock()
					case "SUB":
						if len(fields) != 3 {
							return
						}
						f.mu.Lock()
						f.subjects[fields[1]] = fields[2]
						f.mu.Unlock()
					case "UNSUB":
						if len(fields) < 2 {
							return
						}
						f.mu.Lock()
						for subject, sid := range f.subjects {
							if sid == fields[1] {
								delete(f.subjects, subject)
							}
						}
						f.mu.Unlock()
					}
				}
			}()
			_ = connection.Close()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		f.mu.Lock()
		connection := f.connection
		f.mu.Unlock()
		if connection != nil {
			_ = connection.Close()
		}
		select {
		case <-f.done:
		case <-time.After(5 * time.Second):
			t.Error("protocol fixture did not join")
		}
	})
	return f
}
func (f *nativeProtocolFixture) send(t *testing.T, subject string, payload []byte, count int) {
	t.Helper()
	f.mu.Lock()
	connection, sid := f.connection, f.subjects[subject]
	f.mu.Unlock()
	if connection == nil || sid == "" {
		t.Fatalf("no admitted native subscription for %s", subject)
	}
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	for i := 0; i < count; i++ {
		if _, err := fmt.Fprintf(connection, "MSG %s %s %d\r\n", subject, sid, len(payload)); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Write(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(connection, "\r\n"); err != nil {
			t.Fatal(err)
		}
	}
}
func nativeFixtureTransport(t *testing.T, fixture *nativeProtocolFixture, config StreamConfig) *NATSPreviewTransport {
	t.Helper()
	credentials := fixtureNATSCredentials(t)
	credentials.Servers = []string{"nats://" + fixture.listener.Addr().String()}
	credentials.ConnectTimeout = time.Second
	transport, err := NewNATSPreviewTransport(t.Context(), credentials, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.Close)
	return transport
}
func flushNativeFixture(t *testing.T, transport *NATSPreviewTransport) {
	t.Helper()
	transport.mu.Lock()
	connection := transport.connection
	transport.mu.Unlock()
	if connection == nil {
		t.Fatal("native connection not established")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := connection.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
}
func nativeSubscribe(t *testing.T, transport *NATSPreviewTransport, subject string, frame func(string, []byte), loss func(string)) *natsPreviewSubscription {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sub, err := transport.Subscribe(ctx, subject, frame, loss)
	if err != nil {
		t.Fatal(err)
	}
	s := sub.(*natsPreviewSubscription)
	if s.subscription.Type() != nats.ChanSubscription {
		t.Fatal("native subscription unexpectedly owns an async callback queue")
	}
	return s
}

func TestNATSNativeProcessBudgetDerivationAndEmptyConnection(t *testing.T) {
	for _, item := range []struct{ bytes, frames, capacity int }{{8388608, 2048, 5}, {4194304, 2048, 1}, {8388608, 4, 1}, {3145728, 3, 0}} {
		config := DefaultStreamConfig()
		config.SubscriptionMaxBytes = item.bytes
		config.SubscriptionMaxFrames = item.frames
		capacity, err := nativePreviewChannelCapacity(config)
		if err != nil || capacity != item.capacity {
			t.Fatalf("budget %d/%d => capacity %d,%v; want %d", item.bytes, item.frames, capacity, err, item.capacity)
		}
	}
	for _, item := range []struct{ bytes, frames int }{{3145727, 2048}, {8388608, 2}} {
		config := DefaultStreamConfig()
		config.SubscriptionMaxBytes = item.bytes
		config.SubscriptionMaxFrames = item.frames
		if _, err := nativePreviewChannelCapacity(config); err == nil {
			t.Fatalf("under-reserved native budget accepted=%+v", item)
		}
	}
	fixture := newNativeProtocolFixture(t, 1048576)
	transport := nativeFixtureTransport(t, fixture, DefaultStreamConfig())
	waitCondition(t, func() bool { transport.mu.Lock(); defer transport.mu.Unlock(); return transport.connection != nil })
	if len(transport.subscriptions) != 0 || transport.nativeMetrics.reservedBytes != 8388608 || transport.nativeMetrics.reservedFrames != 8 || transport.nativeMetrics.currentBytes.Load() != 0 || cap(transport.nativeQueue) != 5 {
		t.Fatal("empty connected process lacks explicit bounded parser/channel ownership")
	}
	transport.Close()
	if transport.metrics.native.Load() != nil || len(transport.nativeQueue) != 0 {
		t.Fatal("empty native process ownership survived joined shutdown")
	}
}

func TestNATSNativeHeartbeatOptionsConsumeTypedEnvironment(t *testing.T) {
	for _, item := range []struct {
		name     string
		env      map[string]string
		interval time.Duration
		out      int
	}{
		{"pinned_defaults", nil, 120000 * time.Millisecond, 2},
		{"overrides", map[string]string{"TETRAL_NATS_PING_INTERVAL_MS": "37", "TETRAL_NATS_MAX_PING_OUT": "4"}, 37 * time.Millisecond, 4},
	} {
		t.Run(item.name, func(t *testing.T) {
			fixture := newNativeProtocolFixture(t, 1048576)
			credentials := fixtureNATSCredentials(t)
			env := map[string]string{"TETRAL_NATS_SERVERS": "nats://" + fixture.listener.Addr().String(), "TETRAL_NATS_USER_PATH": credentials.UserPath, "TETRAL_NATS_PASSWORD_PATH": credentials.PasswordPath}
			for key, value := range item.env {
				env[key] = value
			}
			config, err := NATSConfigFromEnv(func(key string) string { return env[key] })
			if err != nil {
				t.Fatal(err)
			}
			transport, err := NewNATSPreviewTransport(t.Context(), config, DefaultStreamConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(transport.Close)
			nativeSubscribe(t, transport, "preview.v1.workspace.heartbeat", func(string, []byte) {}, func(string) {})
			transport.mu.Lock()
			connection := transport.connection
			transport.mu.Unlock()
			if connection.Opts.PingInterval != item.interval || connection.Opts.MaxPingsOut != item.out {
				t.Fatalf("actual native options: interval=%v outstanding=%d", connection.Opts.PingInterval, connection.Opts.MaxPingsOut)
			}
			flushNativeFixture(t, transport)
		})
	}
}

// Two queued broker-ceiling reservations, one current message and two parser
// copies require exactly 5MiB. Independent byte padding around that threshold
// changes admission even though the frame-count budget never binds.
func TestNATSNativeExactProcessByteReservationBoundary(t *testing.T) {
	for _, offset := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("%+d", offset), func(t *testing.T) {
			fixture := newNativeProtocolFixture(t, 1048576)
			config := DefaultStreamConfig()
			config.SubscriptionMaxBytes = 5*1048576 + offset
			transport := nativeFixtureTransport(t, fixture, config)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var delivered atomic.Int64
			sub := nativeSubscribe(t, transport, "preview.v1.workspace.boundary", func(string, []byte) { delivered.Add(1); once.Do(func() { close(entered); <-release }) }, func(string) {})
			// Cleanup is registered after the transport, so a failed assertion also
			// unblocks and joins the held dispatcher before process cleanup.
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			payload := []byte(strings.Repeat("x", 262144))
			fixture.send(t, sub.subject, payload, 1)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("native current-message barrier not reached")
			}
			fixture.send(t, sub.subject, payload, 2)
			flushNativeFixture(t, transport)
			if offset < 0 {
				waitCondition(t, func() bool {
					transport.mu.Lock()
					defer transport.mu.Unlock()
					return sub.subscription == nil && sub.recovering
				})
				if len(transport.nativeQueue) != 1 {
					t.Fatal("below exact reservation did not retain exactly one queued slot")
				}
			} else {
				transport.mu.Lock()
				live := sub.subscription != nil && !sub.recovering
				transport.mu.Unlock()
				if !live || len(transport.nativeQueue) != 2 {
					t.Fatal("at/above exact reservation rejected two queued slots")
				}
			}
			if got := transport.nativeMetrics.reservedBytes; got > int64(config.SubscriptionMaxBytes) || got != int64((len(transport.nativeQueue)+3)*1048576) {
				t.Fatalf("actual reserved bytes=%d exceed budget=%d or disagree with held storage", got, config.SubscriptionMaxBytes)
			}
			releaseOnce.Do(func() { close(release) })
			waitCondition(t, func() bool {
				return len(transport.nativeQueue) == 0 && transport.nativeMetrics.currentFrames.Load() == 0
			})
			want := int64(3)
			if offset < 0 {
				want = 1
			}
			if delivered.Load() != want {
				t.Fatalf("native delivered=%d want=%d", delivered.Load(), want)
			}
			transport.Close()
			if transport.metrics.native.Load() != nil || len(transport.nativeQueue) != 0 {
				t.Fatal("native reservation survived joined cleanup")
			}
		})
	}
}

func TestNATSNativeSharedChannelAtAndOverByteAndCountBounds(t *testing.T) {
	for _, item := range []struct {
		name             string
		frames, capacity int
	}{{"bytes", 2048, 5}, {"count", 5, 2}} {
		t.Run(item.name, func(t *testing.T) {
			fixture := newNativeProtocolFixture(t, 1048576)
			config := DefaultStreamConfig()
			config.SubscriptionMaxFrames = item.frames
			transport := nativeFixtureTransport(t, fixture, config)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var delivered atomic.Int64
			losses := make(chan string, 4)
			a := nativeSubscribe(t, transport, "preview.v1.workspace.sessionA", func(string, []byte) { delivered.Add(1); once.Do(func() { close(entered); <-release }) }, func(reason string) { losses <- reason })
			var healthy atomic.Int64
			b := nativeSubscribe(t, transport, "preview.v1.workspace.sessionB", func(string, []byte) { healthy.Add(1) }, func(string) { t.Error("unaffected native subscription invalidated") })
			defer func() {
				if err := b.Close(); err != nil {
					t.Error(err)
				}
			}()
			// A valid-size frame can hold the dispatcher while every remaining
			// native slot fills, independently of the number of subscriptions.
			payload := []byte(strings.Repeat("x", 262144))
			fixture.send(t, a.subject, payload, 1)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("dispatcher barrier not entered")
			}
			t.Cleanup(func() {
				once.Do(func() {})
				select {
				case <-release:
				default:
					close(release)
				}
			})
			fixture.send(t, a.subject, payload, item.capacity)
			flushNativeFixture(t, transport)
			if len(transport.nativeQueue) != item.capacity || transport.nativeMetrics.currentFrames.Load() != 1 || transport.nativeMetrics.currentBytes.Load() != 262144 {
				t.Fatal("at-bound native queued/current ownership mismatch")
			}
			select {
			case reason := <-losses:
				t.Fatalf("at-bound frames caused loss=%s", reason)
			default:
			}
			if reserved := transport.nativeMetrics.reservedBytes; reserved > int64(config.SubscriptionMaxBytes) || reserved != int64((item.capacity+3)*1048576) {
				t.Fatalf("native reservation exceeds process byte budget=%d", reserved)
			}
			fixture.send(t, a.subject, payload, 1)
			flushNativeFixture(t, transport)
			// The official slow-consumer callback removes the old native
			// identity before waiting for the held application callback.
			waitCondition(t, func() bool {
				transport.mu.Lock()
				defer transport.mu.Unlock()
				return a.subscription == nil && a.recovering
			})
			close(release)
			select {
			case <-losses:
			case <-time.After(5 * time.Second):
				t.Fatal("over-bound native loss not observed")
			}
			waitCondition(t, func() bool {
				transport.mu.Lock()
				defer transport.mu.Unlock()
				return a.subscription != nil && !a.recovering
			})
			flushNativeFixture(t, transport)
			waitCondition(t, func() bool {
				return len(transport.nativeQueue) == 0 && transport.nativeMetrics.currentFrames.Load() == 0
			})
			if delivered.Load() != 1 {
				t.Fatalf("old queued frames survived native loss generation=%d", delivered.Load())
			}
			fixture.send(t, a.subject, []byte("future-open"), 1)
			fixture.send(t, b.subject, []byte("healthy-other-session"), 1)
			flushNativeFixture(t, transport)
			waitCondition(t, func() bool { return delivered.Load() == 2 && healthy.Load() == 1 })
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			transport.Close()
			if len(transport.nativeQueue) != 0 || transport.metrics.native.Load() != nil {
				t.Fatal("native budget credits survived shutdown")
			}
		})
	}
}

func TestNATSNativeCloseJoinsCurrentDeliveryAndDiscardsOldRejoinFrames(t *testing.T) {
	fixture := newNativeProtocolFixture(t, 1048576)
	transport := nativeFixtureTransport(t, fixture, DefaultStreamConfig())
	entered, release := make(chan struct{}), make(chan struct{})
	var delivered atomic.Int64
	sub := nativeSubscribe(t, transport, "preview.v1.workspace.session", func(string, []byte) { delivered.Add(1); close(entered); <-release }, func(string) {})
	fixture.send(t, sub.subject, []byte("held"), 1)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery barrier not entered")
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	fixture.send(t, sub.subject, []byte("old-queued"), 5)
	flushNativeFixture(t, transport)
	joined := make(chan struct{})
	go func() { _ = sub.Close(); close(joined) }()
	waitCondition(t, func() bool { transport.mu.Lock(); defer transport.mu.Unlock(); return sub.closed })
	select {
	case <-joined:
		t.Fatal("subscription close returned before current callback finished")
	default:
	}
	close(release)
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not join")
	}
	var next atomic.Int64
	rejoin := nativeSubscribe(t, transport, sub.subject, func(string, []byte) { next.Add(1) }, func(string) {})
	defer func() {
		if err := rejoin.Close(); err != nil {
			t.Error(err)
		}
	}()
	fixture.send(t, rejoin.subject, []byte("new"), 1)
	flushNativeFixture(t, transport)
	waitCondition(t, func() bool { return next.Load() == 1 && len(transport.nativeQueue) == 0 })
	if delivered.Load() != 1 {
		t.Fatal("closed native callback received old queued frames")
	}
}

func TestNATSNativeRejectsBrokerCeilingAndOversizeFrames(t *testing.T) {
	t.Run("advertised-before-SUB", func(t *testing.T) {
		fixture := newNativeProtocolFixture(t, 1048577)
		config := fixtureNATSCredentials(t)
		config.Servers = []string{"nats://" + fixture.listener.Addr().String()}
		transport := &NATSPreviewTransport{config: config, ctx: t.Context(), metrics: NewPreviewMetrics(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		connection, err := transport.connect()
		if err == nil || connection != nil {
			t.Fatal("over-ceiling broker admitted")
		}
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		if len(fixture.subjects) != 0 {
			t.Fatal("SUB emitted before broker ceiling check")
		}
	})
	t.Run("frame-bound", func(t *testing.T) {
		fixture := newNativeProtocolFixture(t, 1048576)
		transport := nativeFixtureTransport(t, fixture, DefaultStreamConfig())
		var frames atomic.Int64
		loss := make(chan string, 1)
		sub := nativeSubscribe(t, transport, "preview.v1.workspace.session", func(string, []byte) { frames.Add(1) }, func(reason string) { loss <- reason })
		fixture.send(t, sub.subject, []byte(strings.Repeat("x", eventwire.MaxPreviewFrameBytes+1)), 1)
		select {
		case reason := <-loss:
			if reason != "invalid_native_frame" {
				t.Fatal(reason)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("oversize frame loss absent")
		}
		if frames.Load() != 0 || transport.metrics.invalidFrames.Load() != 1 {
			t.Fatal("oversize frame copied into application hub")
		}
	})
}

func TestNATSNativeSharedBudgetDoesNotMultiplyBySessions(t *testing.T) {
	fixture := newNativeProtocolFixture(t, 1048576)
	transport := nativeFixtureTransport(t, fixture, DefaultStreamConfig())
	var delivered atomic.Int64
	for i := 0; i < 32; i++ {
		nativeSubscribe(t, transport, "preview.v1.workspace.session"+strconv.Itoa(i), func(string, []byte) { delivered.Add(1) }, func(string) { t.Error("healthy delivery caused native loss") })
	}
	if cap(transport.nativeQueue) != 5 || transport.nativeMetrics.reservedBytes != 8388608 {
		t.Fatal("native budget multiplied by Session count")
	}
	for i := 0; i < 32; i++ {
		fixture.send(t, "preview.v1.workspace.session"+strconv.Itoa(i), []byte("healthy"), 1)
		flushNativeFixture(t, transport)
		waitCondition(t, func() bool { return delivered.Load() == int64(i+1) })
	}
}

func TestNATSNativeFreshRecoveryInvalidatesBeforeDelayedCloseCallback(t *testing.T) {
	fixture := newNativeProtocolFixture(t, 1048576)
	transport := nativeFixtureTransport(t, fixture, DefaultStreamConfig())
	losses := make(chan string, 8)
	var delivered atomic.Int64
	sub := nativeSubscribe(t, transport, "preview.v1.workspace.session", func(string, []byte) { delivered.Add(1) }, func(reason string) { losses <- reason })
	fixture.send(t, sub.subject, []byte("before-loss"), 1)
	flushNativeFixture(t, transport)
	waitCondition(t, func() bool { return delivered.Load() == 1 })
	transport.mu.Lock()
	old := transport.connection
	oldSub := sub.subscription
	transport.mu.Unlock()
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	// Hold the official client's asynchronous callback queue. A supervisor
	// replacement must invalidate old previews even while this notification
	// and the subsequent ClosedHandler remain delayed.
	old.SetDisconnectErrHandler(func(connection *nats.Conn, _ error) {
		close(entered)
		<-release
		transport.loss(connection, nil, "delayed_native_disconnect")
		close(returned)
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	fixture.mu.Lock()
	socket := fixture.connection
	fixture.mu.Unlock()
	if err := socket.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native callback lag barrier absent")
	}
	select {
	case <-losses:
	case <-time.After(5 * time.Second):
		t.Fatal("fresh supervisor suppressed required old-connection preview loss")
	}
	waitCondition(t, func() bool {
		transport.mu.Lock()
		defer transport.mu.Unlock()
		return transport.connection != nil && transport.connection != old && transport.connection.IsConnected() && sub.subscription != nil && sub.subscription != oldSub
	})
	flushNativeFixture(t, transport)
	close(release)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("old native callback did not join")
	}
	fixture.send(t, sub.subject, []byte("future-eligible"), 1)
	flushNativeFixture(t, transport)
	waitCondition(t, func() bool { return delivered.Load() == 2 })
}
