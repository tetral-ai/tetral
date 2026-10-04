package eventstream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
)

const natsConnectTimeout = time.Second
const natsReconnectWait = time.Second

// The broker deployment and parser/admission reservation use the same ceiling.
// A fresh connection is checked before any subscription is admitted.
const natsBrokerPayloadCeiling = 1024 * 1024
const maxNATSDiscoveredServers = 64
const maxNATSServerAddressBytes = 2048

// Each shared-channel slot is charged at the broker ceiling, independently of
// the smaller valid private frame bound. Reserve two additional payloads for
// the pinned parser/admission copy and one for the current dispatcher message.
func nativePreviewChannelCapacity(c StreamConfig) (int, error) {
	if c.SubscriptionMaxBytes < 3*natsBrokerPayloadCeiling || c.SubscriptionMaxFrames < 3 {
		return 0, errors.New("native preview budget must cover parser, admission and dispatcher storage")
	}
	return min(c.SubscriptionMaxBytes/natsBrokerPayloadCeiling-3, c.SubscriptionMaxFrames-3), nil
}

type NATSConfig struct {
	Servers                                           []string
	UserPath, PasswordPath, CAPath, CertPath, KeyPath string
	ConnectTimeout, ReconnectWait                     time.Duration
	PingInterval                                      time.Duration
	MaxPingsOutstanding                               int
}

func (c NATSConfig) heartbeatDefaults() NATSConfig {
	if c.PingInterval == 0 {
		c.PingInterval = nats.DefaultPingInterval
	}
	if c.MaxPingsOutstanding == 0 {
		c.MaxPingsOutstanding = nats.DefaultMaxPingOut
	}
	return c
}

func (c NATSConfig) validateHeartbeat() error {
	if c.PingInterval < time.Millisecond || c.PingInterval > time.Hour || c.MaxPingsOutstanding < 1 || c.MaxPingsOutstanding > 16 {
		return workload.NewConfigError("invalid NATS heartbeat settings")
	}
	return nil
}

func (c NATSConfig) Enabled() bool { return len(c.Servers) > 0 }
func NATSConfigFromEnv(getenv func(string) string) (NATSConfig, error) {
	c := (NATSConfig{ConnectTimeout: natsConnectTimeout, ReconnectWait: natsReconnectWait, UserPath: getenv("TETRAL_NATS_USER_PATH"), PasswordPath: getenv("TETRAL_NATS_PASSWORD_PATH"), CAPath: getenv("TETRAL_NATS_TLS_CA_PATH"), CertPath: getenv("TETRAL_NATS_TLS_CERT_PATH"), KeyPath: getenv("TETRAL_NATS_TLS_KEY_PATH")}).heartbeatDefaults()
	for _, item := range []struct {
		key    string
		target *time.Duration
	}{{"TETRAL_NATS_CONNECT_TIMEOUT_MS", &c.ConnectTimeout}, {"TETRAL_NATS_RECONNECT_WAIT_MS", &c.ReconnectWait}} {
		if raw := getenv(item.key); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 || value > 5000 {
				return c, workload.NewConfigError("invalid " + item.key)
			}
			*item.target = time.Duration(value) * time.Millisecond
		}
	}
	for _, item := range []struct {
		key string
		max int
		set func(int)
	}{
		{"TETRAL_NATS_PING_INTERVAL_MS", 3600000, func(value int) { c.PingInterval = time.Duration(value) * time.Millisecond }},
		{"TETRAL_NATS_MAX_PING_OUT", 16, func(value int) { c.MaxPingsOutstanding = value }},
	} {
		if raw := getenv(item.key); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 || value > item.max {
				return c, workload.NewConfigError("invalid " + item.key)
			}
			item.set(value)
		}
	}
	raw := getenv("TETRAL_NATS_SERVERS")
	if raw == "" {
		if c.UserPath != "" || c.PasswordPath != "" || c.CAPath != "" || c.CertPath != "" || c.KeyPath != "" {
			return c, workload.NewConfigError("NATS servers are required with preview credentials")
		}
		return c, nil
	}
	if c.UserPath == "" || c.PasswordPath == "" {
		return c, workload.NewConfigError("NATS credential file paths are required")
	}
	tlsEnabled := c.CAPath != "" || c.CertPath != "" || c.KeyPath != ""
	if tlsEnabled && (c.CAPath == "" || c.CertPath == "" || c.KeyPath == "") {
		return c, workload.NewConfigError("NATS TLS requires complete trust and certificate paths")
	}
	for _, server := range strings.Split(raw, ",") {
		u, err := url.Parse(strings.TrimSpace(server))
		if len(server) > maxNATSServerAddressBytes || err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || (u.Scheme != "nats" && u.Scheme != "tls") {
			return c, workload.NewConfigError("invalid NATS server address")
		}
		if u.Scheme == "tls" && !tlsEnabled {
			return c, workload.NewConfigError("NATS TLS paths are required")
		}
		if tlsEnabled && net.ParseIP(u.Hostname()) != nil {
			return c, workload.NewConfigError("NATS TLS requires broker DNS names")
		}
		if len(c.Servers) == maxNATSDiscoveredServers {
			return c, workload.NewConfigError("too many NATS seed addresses")
		}
		c.Servers = append(c.Servers, u.String())
	}
	return c, nil
}

type NATSPreviewTransport struct {
	config        NATSConfig
	streamConfig  StreamConfig
	metrics       *PreviewMetrics
	logger        *slog.Logger
	trust         *transportsecurity.Owner
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	dispatchDone  chan struct{}
	nativeQueue   chan *nats.Msg
	nativeMetrics *nativePreviewMetrics
	servers       []string
	wake          chan struct{}
	mu            sync.Mutex
	connection    *nats.Conn
	subscriptions map[*natsPreviewSubscription]struct{}
	closed        bool
}
type natsPreviewSubscription struct {
	owner        *NATSPreviewTransport
	subject      string
	onFrame      func(string, []byte)
	onLoss       func(string)
	subscription *nats.Subscription
	ready        chan struct{}
	readyOnce    sync.Once
	closed       bool
	recovering   bool
	callbacks    sync.WaitGroup
	callbackMu   sync.Mutex
	closeDone    chan struct{}
}

func NewNATSPreviewTransport(ctx context.Context, config NATSConfig, streamConfig StreamConfig, metrics *PreviewMetrics, logger *slog.Logger) (*NATSPreviewTransport, error) {
	config = config.heartbeatDefaults()
	if err := config.validateHeartbeat(); err != nil {
		return nil, err
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = natsConnectTimeout
	}
	if config.ReconnectWait <= 0 {
		config.ReconnectWait = natsReconnectWait
	}
	if !config.Enabled() {
		return nil, errors.New("NATS servers are required")
	}
	if err := streamConfig.Validate(); err != nil {
		return nil, err
	}
	capacity, err := nativePreviewChannelCapacity(streamConfig)
	if err != nil {
		return nil, err
	}
	if metrics == nil {
		metrics = NewPreviewMetrics()
	}
	if logger == nil {
		logger = workload.ComponentLogger("event-stream")
	}
	// Read at startup even if the broker is unavailable: invalid local credential
	// configuration must never masquerade as a recoverable broker outage.
	if _, _, err := readNATSCredentials(config); err != nil {
		return nil, err
	}
	var trust *transportsecurity.Owner
	if config.CAPath != "" {
		var err error
		trust, err = transportsecurity.Open(ctx, transportsecurity.Config{CAPath: config.CAPath, CertPath: config.CertPath, KeyPath: config.KeyPath, Purpose: "nats"})
		if err != nil {
			return nil, workload.NewConfigError("invalid NATS TLS material")
		}
	}
	lifetime, cancel := context.WithCancel(context.Background())
	t := &NATSPreviewTransport{config: config, streamConfig: streamConfig, metrics: metrics, logger: logger, trust: trust, ctx: lifetime, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1), subscriptions: map[*natsPreviewSubscription]struct{}{}, nativeQueue: make(chan *nats.Msg, capacity), dispatchDone: make(chan struct{}), servers: append([]string(nil), config.Servers...)}
	if trust != nil {
		if err := trust.SetTrustActivationObserver(func() { t.retire("trust_changed") }); err != nil {
			cancel()
			_ = trust.Close()
			return nil, err
		}
	}
	t.nativeMetrics = &nativePreviewMetrics{queueDepth: func() int { return len(t.nativeQueue) }, reservedBytes: int64((capacity + 3) * natsBrokerPayloadCeiling), reservedFrames: int64(capacity + 3)}
	metrics.native.Store(t.nativeMetrics)
	go t.dispatchNative()
	go t.supervise()
	return t, nil
}
func readNATSCredentials(config NATSConfig) (string, string, error) {
	user, err := os.ReadFile(config.UserPath)
	if err != nil {
		return "", "", workload.NewConfigError("cannot load NATS user file")
	}
	password, err := os.ReadFile(config.PasswordPath)
	if err != nil {
		return "", "", workload.NewConfigError("cannot load NATS password file")
	}
	u, p := strings.TrimSpace(string(user)), strings.TrimSpace(string(password))
	if u == "" || p == "" || strings.ContainsAny(u, "\r\n") || strings.ContainsAny(p, "\r\n") {
		return "", "", workload.NewConfigError("invalid NATS credential files")
	}
	return u, p, nil
}
func (t *NATSPreviewTransport) Subscribe(ctx context.Context, subject string, onFrame func(string, []byte), onLoss func(string)) (PreviewSubscription, error) {
	s := &natsPreviewSubscription{owner: t, subject: subject, onFrame: onFrame, onLoss: onLoss, ready: make(chan struct{}), closeDone: make(chan struct{})}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errors.New("NATS subscriber closed")
	}
	t.subscriptions[s] = struct{}{}
	connection := t.connection
	if connection != nil && connection.IsConnected() {
		if err := t.installLocked(connection, s); err != nil {
			t.mu.Unlock()
			return s, err
		}
	}
	t.mu.Unlock()
	signal(t.wake)
	if connection != nil && connection.IsConnected() {
		if err := connection.FlushWithContext(ctx); err != nil {
			return s, err
		}
		s.readyOnce.Do(func() { close(s.ready) })
	}
	select {
	case <-ctx.Done():
		return s, ctx.Err()
	case <-s.ready:
		return s, nil
	case <-t.ctx.Done():
		return s, errors.New("NATS subscriber closed")
	}
}
func (t *NATSPreviewTransport) installLocked(connection *nats.Conn, s *natsPreviewSubscription) error {
	if s.closed || s.recovering || s.subscription != nil {
		return nil
	}
	if connection.MaxPayload() <= 0 || connection.MaxPayload() > natsBrokerPayloadCeiling {
		return errors.New("NATS broker payload ceiling exceeds native preview reservation")
	}
	// The provided process channel exists before SUB. ChanSubscribe has no
	// per-subscription linked queue or callback worker in the pinned client.
	sub, err := connection.ChanSubscribe(s.subject, t.nativeQueue)
	if err != nil {
		return err
	}
	s.subscription = sub
	return nil
}
func (s *natsPreviewSubscription) Close() error {
	t := s.owner
	t.mu.Lock()
	if s.closed {
		t.mu.Unlock()
		<-s.closeDone
		return nil
	}
	s.closed = true
	delete(t.subscriptions, s)
	subscription := s.subscription
	s.subscription = nil
	t.mu.Unlock()
	var err error
	if subscription != nil {
		err = subscription.Unsubscribe()
	}
	// Adds happen only under owner.mu while the registration is open.
	// Queued messages retain their process credits and are discarded by the
	// dispatcher using the old Subscription identity; they never reach a rejoin.
	s.callbacks.Wait()
	close(s.closeDone)
	return err
}

func (t *NATSPreviewTransport) dispatchNative() {
	defer close(t.dispatchDone)
	defer func() {
		// Connection.Close clears every native channel subscription before this
		// drain. Never close the shared channel: the official client owns sends.
		for {
			select {
			case message := <-t.nativeQueue:
				message.Data = nil
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-t.ctx.Done():
			return
		case message := <-t.nativeQueue:
			t.nativeMetrics.currentBytes.Store(int64(len(message.Data)))
			t.nativeMetrics.currentFrames.Store(1)
			t.deliverNative(message)
			message.Data = nil
			t.nativeMetrics.currentBytes.Store(0)
			t.nativeMetrics.currentFrames.Store(0)
		}
	}
}
func (t *NATSPreviewTransport) deliverNative(message *nats.Msg) {
	t.mu.Lock()
	var target *natsPreviewSubscription
	for s := range t.subscriptions {
		if !s.closed && s.subscription == message.Sub {
			target = s
			s.callbacks.Add(1)
			break
		}
	}
	t.mu.Unlock()
	if target == nil {
		return
	}
	defer target.callbacks.Done()
	target.callbackMu.Lock()
	defer target.callbackMu.Unlock()
	t.mu.Lock()
	valid := !t.closed && !target.closed && target.subscription == message.Sub && t.connection != nil && t.connection.IsConnected()
	t.mu.Unlock()
	if !valid {
		return
	}
	if message.Subject != target.subject || len(message.Data) > eventwire.MaxPreviewFrameBytes || len(message.Header) != 0 || message.Reply != "" {
		t.metrics.invalidFrames.Add(1)
		t.mu.Lock()
		if target.subscription == message.Sub {
			target.subscription = nil
			target.recovering = true
		}
		t.mu.Unlock()
		_ = message.Sub.Unsubscribe()
		target.onLoss("invalid_native_frame")
		t.mu.Lock()
		target.recovering = false
		t.mu.Unlock()
		signal(t.wake)
		return
	}
	target.onFrame(message.Subject, message.Data)
}
func (t *NATSPreviewTransport) loss(connection *nats.Conn, subscription *nats.Subscription, reason string) {
	t.mu.Lock()
	if t.closed || (connection != nil && connection != t.connection) {
		t.mu.Unlock()
		return
	}
	targets := []*natsPreviewSubscription{}
	nativeSubscriptions := []*nats.Subscription{}
	for s := range t.subscriptions {
		if !s.closed && s.subscription != nil && (subscription == nil || s.subscription == subscription) {
			s.callbacks.Add(1)
			if s.subscription != nil {
				nativeSubscriptions = append(nativeSubscriptions, s.subscription)
			}
			// Replace the native identity on loss. Otherwise an old queued
			// request_open could enter the hub after its generation was reset.
			s.subscription = nil
			s.recovering = true
			targets = append(targets, s)
		}
	}
	t.mu.Unlock()
	for _, native := range nativeSubscriptions {
		_ = native.Unsubscribe()
	}
	for _, s := range targets {
		s.callbackMu.Lock()
		t.mu.Lock()
		valid := !t.closed && !s.closed && (connection == nil || t.connection == connection)
		t.mu.Unlock()
		if valid {
			s.onLoss(reason)
		}
		t.mu.Lock()
		s.recovering = false
		t.mu.Unlock()
		s.callbackMu.Unlock()
		s.callbacks.Done()
	}
	signal(t.wake)
}
func (t *NATSPreviewTransport) retire(reason string) {
	t.mu.Lock()
	connection := t.connection
	t.mu.Unlock()
	// Reset native identities and notify the hub before Close: the supervisor
	// may otherwise replace the connection before its async close callback runs.
	t.loss(connection, nil, reason)
	if connection != nil {
		connection.Close()
	}
	signal(t.wake)
}
func (t *NATSPreviewTransport) connect() (*nats.Conn, error) {
	user, password, err := readNATSCredentials(t.config)
	if err != nil {
		return nil, err
	}
	attemptCtx, attemptCancel := context.WithTimeout(t.ctx, t.config.ConnectTimeout)
	defer attemptCancel()
	initial := &natsConnectAttempt{ctx: attemptCtx}
	aborted := make(chan struct{})
	stopAbort := context.AfterFunc(attemptCtx, func() { defer close(aborted); initial.abort() })
	defer func() {
		if !stopAbort() {
			<-aborted
		}
	}()
	dialer := &natsContextDialer{lifetime: t.ctx, attempt: attemptCtx, initial: initial, timeout: t.config.ConnectTimeout}
	var tlsDialer *natsTLSDialer
	heartbeat := t.config.heartbeatDefaults()
	options := []nats.Option{nats.SetCustomDialer(dialer), nats.SkipHostLookup(), nats.Name("tetral-event-stream"), nats.UserInfo(user, password), nats.Timeout(t.config.ConnectTimeout), nats.ReconnectWait(t.config.ReconnectWait), nats.PingInterval(heartbeat.PingInterval), nats.MaxPingsOutstanding(heartbeat.MaxPingsOutstanding), nats.NoReconnect(), nats.ReconnectBufSize(0),
		nats.DisconnectErrHandler(func(connection *nats.Conn, _ error) {
			t.metrics.disconnects.Add(1)
			t.loss(connection, nil, "nats_disconnect")
			t.logger.Warn("preview_subscriber_disconnected", "operation", "event_stream.nats", "reason", "connection_lost", "outcome", "formal_active")
		}),
		nats.ClosedHandler(func(connection *nats.Conn) { t.loss(connection, nil, "nats_closed"); signal(t.wake) }),
		nats.ErrorHandler(func(connection *nats.Conn, sub *nats.Subscription, _ error) {
			t.loss(connection, sub, "nats_subscription_loss")
		}),
	}
	if t.trust != nil {
		tlsDialer = &natsTLSDialer{ctx: t.ctx, attempt: attemptCtx, initial: initial, owner: t.trust, timeout: t.config.ConnectTimeout}
		options = append(options, nats.Secure(&tls.Config{MinVersion: tls.VersionTLS12}), nats.TLSHandshakeFirst(), nats.SetCustomDialer(tlsDialer))
	}
	t.mu.Lock()
	servers := append([]string(nil), t.servers...)
	t.mu.Unlock()
	if len(servers) == 0 { // Direct owning connection-budget fixtures.
		servers = t.config.Servers
	}
	connection, err := nats.Connect(strings.Join(servers, ","), options...)
	if err == nil && (connection.MaxPayload() <= 0 || connection.MaxPayload() > natsBrokerPayloadCeiling) {
		connection.Close()
		connection = nil
		err = errors.New("NATS broker payload ceiling exceeds native preview reservation")
	}
	if budgetErr := initial.finish(); budgetErr != nil {
		if connection != nil {
			connection.Close()
		}
		err = budgetErr
		connection = nil
	}
	dialer.connected.Store(true)
	if tlsDialer != nil {
		tlsDialer.connected.Store(true)
	}
	return connection, err
}
func (t *NATSPreviewTransport) supervise() {
	defer close(t.done)
	ticker := time.NewTicker(t.config.ReconnectWait)
	defer ticker.Stop()
	degraded := false
	for {
		if t.ctx.Err() != nil {
			return
		}
		t.mu.Lock()
		connection := t.connection
		t.mu.Unlock()
		if connection == nil || connection.IsClosed() {
			if connection != nil {
				t.loss(connection, nil, "nats_connection_replaced")
			}
			hadConnection := connection != nil
			fresh, err := t.connect()
			if err != nil {
				if !degraded {
					degraded = true
					t.logger.Warn("preview_subscriber_unavailable", "operation", "event_stream.nats", "reason", "connect_failed", "outcome", "formal_active")
				}
			} else {
				t.mu.Lock()
				if t.closed {
					t.mu.Unlock()
					fresh.Close()
					return
				}
				t.rememberServersLocked(fresh.Servers())
				t.connection = fresh
				for s := range t.subscriptions {
					s.subscription = nil
				}
				connection = fresh
				t.mu.Unlock()
				if degraded || hadConnection {
					t.metrics.reconnects.Add(1)
					t.logger.Info("preview_subscriber_recovered", "operation", "event_stream.nats", "outcome", "new_requests_eligible")
					degraded = false
				}
			}
		}
		if connection != nil && connection.IsConnected() && t.trust != nil {
			server, _ := url.Parse(t.config.Servers[0])
			if _, err := t.trust.ClientTLSConfig(server.Hostname(), ""); err != nil {
				t.retire("credentials_expired")
				connection = nil
			}
		}
		if connection != nil && connection.IsConnected() {
			t.mu.Lock()
			t.rememberServersLocked(connection.Servers())
			waiting := []*natsPreviewSubscription{}
			for s := range t.subscriptions {
				if !s.closed {
					if err := t.installLocked(connection, s); err == nil {
						waiting = append(waiting, s)
					}
				}
			}
			t.mu.Unlock()
			if err := connection.FlushTimeout(t.config.ConnectTimeout); err != nil {
				t.retire("nats_flush_failed")
			} else {
				for _, s := range waiting {
					s.readyOnce.Do(func() { close(s.ready) })
				}
			}
		}
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
		case <-t.wake:
		}
	}
}

// Retain verified discovered DNS addresses for a subsequent fresh connection,
// including when the original seed is unavailable. No URL credentials survive.
func (t *NATSPreviewTransport) rememberServersLocked(advertised []string) {
	seen := make(map[string]struct{}, len(t.servers))
	for _, server := range t.servers {
		seen[server] = struct{}{}
	}
	for _, server := range advertised {
		u, err := url.Parse(server)
		if err != nil || len(server) > maxNATSServerAddressBytes || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || (u.Scheme != "nats" && u.Scheme != "tls") || (t.trust != nil && net.ParseIP(u.Hostname()) != nil) {
			continue
		}
		if _, exists := seen[server]; exists {
			continue
		}
		if len(t.servers) == maxNATSDiscoveredServers {
			break
		}
		t.servers = append(t.servers, server)
		seen[server] = struct{}{}
	}
}
func (t *NATSPreviewTransport) Close() {
	t.mu.Lock()
	t.closed = true
	connection := t.connection
	t.mu.Unlock()
	// Stop native sends first, then cancel and join our supervisor/dispatcher.
	if connection != nil {
		connection.Close()
	}
	t.cancel()
	<-t.done
	t.mu.Lock()
	subscriptions := make([]*natsPreviewSubscription, 0, len(t.subscriptions))
	for s := range t.subscriptions {
		subscriptions = append(subscriptions, s)
	}
	t.mu.Unlock()
	for _, s := range subscriptions {
		_ = s.Close()
	}
	<-t.dispatchDone
	t.metrics.native.CompareAndSwap(t.nativeMetrics, nil)
	if t.trust != nil {
		_ = t.trust.Close()
	}
}

type natsTLSDialer struct {
	ctx       context.Context
	attempt   context.Context
	connected atomic.Bool
	initial   *natsConnectAttempt
	owner     *transportsecurity.Owner
	timeout   time.Duration
}

func (*natsTLSDialer) SkipTLSHandshake() bool { return true }
func (d *natsTLSDialer) Dial(network, address string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid NATS broker address")
	}
	config, _, err := d.owner.ClientTLSConfigSnapshot(host, "")
	if err != nil {
		return nil, err
	}
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: d.timeout}, Config: config}
	parent := d.ctx
	if !d.connected.Load() {
		parent = d.attempt
	}
	ctx, cancel := context.WithTimeout(parent, d.timeout)
	defer cancel()
	connection, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if !d.connected.Load() {
		return d.initial.register(connection)
	}
	return connection, nil
}

type natsContextDialer struct {
	lifetime, attempt context.Context
	timeout           time.Duration
	connected         atomic.Bool
	initial           *natsConnectAttempt
}

func (d *natsContextDialer) Dial(network, address string) (net.Conn, error) {
	parent := d.lifetime
	if !d.connected.Load() {
		parent = d.attempt
	}
	ctx, cancel := context.WithTimeout(parent, d.timeout)
	defer cancel()
	connection, err := (&net.Dialer{Timeout: d.timeout}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if !d.connected.Load() {
		return d.initial.register(connection)
	}
	return connection, nil
}

// NATS resets socket deadlines while reading INFO. Close every provisional
// socket at the single attempt deadline so DNS/TLS/INFO and multiple seeds
// cannot each consume another full budget. Successful sockets leave this owner
// before its context is canceled and use the process lifetime on reconnect.
type natsConnectAttempt struct {
	ctx         context.Context
	mu          sync.Mutex
	connections []net.Conn
	finished    bool
}

func (a *natsConnectAttempt) register(connection net.Conn) (net.Conn, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.ctx.Err(); err != nil {
		_ = connection.Close()
		return nil, err
	}
	a.connections = append(a.connections, connection)
	return connection, nil
}
func (a *natsConnectAttempt) abort() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return
	}
	for _, connection := range a.connections {
		_ = connection.Close()
	}
	a.connections = nil
}
func (a *natsConnectAttempt) finish() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	err := a.ctx.Err()
	a.finished = true
	if err != nil {
		for _, connection := range a.connections {
			_ = connection.Close()
		}
	}
	a.connections = nil
	return err
}
