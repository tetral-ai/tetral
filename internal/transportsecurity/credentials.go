// Package transportsecurity owns native TLS credential generations. Captured
// mesh traffic obtains its credentials from Istio and does not use this loader.
package transportsecurity

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/credentials"
)

// Config refers only to mounted material. Certificate and key are optional
// together for server-authenticated store clients, and required for mutual TLS.
type Config struct {
	CAPath, CertPath, KeyPath string
	Purpose                   string
}
type generation struct {
	roots            *x509.CertPool
	leaf             *tls.Certificate
	expires          time.Time
	fingerprint      [32]byte
	trustFingerprint [32]byte
}

// Owner atomically activates complete generations and joins its observer on Close.
type Owner struct {
	config          Config
	current         atomic.Pointer[generation]
	cancel          context.CancelFunc
	done            chan struct{}
	once            sync.Once
	closed          atomic.Bool
	reloadMutex     sync.Mutex
	activation      atomic.Pointer[func()]
	trustActivation atomic.Pointer[func()]
	reloadFailures  atomic.Uint64
	reloadFailed    atomic.Bool
	reloadSummaryAt time.Time // guarded by reloadMutex
}

// Open validates initial material before it starts observing projected symlinks.
// The caller owns Close; the context controls startup, not the owner's lifetime.
func Open(ctx context.Context, cfg Config) (*Owner, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.CAPath == "" || (cfg.CertPath == "") != (cfg.KeyPath == "") {
		return nil, errors.New("TLS trust and a complete optional certificate/key pair are required")
	}
	if cfg.Purpose != "" && cfg.Purpose != "runtime-direct" && cfg.Purpose != "database" && cfg.Purpose != "blob" && cfg.Purpose != "nats" && cfg.Purpose != "edge-http" && cfg.Purpose != "edge-check" {
		return nil, errors.New("unknown TLS purpose")
	}
	g, err := load(cfg)
	if err != nil {
		return nil, err
	}
	// #nosec G118 -- returned owner retains cancel; Close cancels and joins watcher plus serialized reload callbacks.
	lifetime, cancel := context.WithCancel(context.Background())
	o := &Owner{config: cfg, cancel: cancel, done: make(chan struct{})}
	o.current.Store(g)
	go func() {
		defer close(o.done)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-lifetime.Done():
				return
			case <-ticker.C:
				_ = o.Reload()
			}
		}
	}()
	return o, nil
}

// OpenRuntimeDirectFromEnv opens the hardened direct Runtime client generation
// from its mount references. Database and object-store owners read their own
// trust settings, so no other purpose is opened from the environment here.
func OpenRuntimeDirectFromEnv(ctx context.Context, getenv func(string) string) (*Owner, error) {
	const prefix = "TETRAL_RUNTIME_DIRECT_TLS_"
	return Open(ctx, Config{CAPath: getenv(prefix + "CA_PATH"), CertPath: getenv(prefix + "CERT_PATH"), KeyPath: getenv(prefix + "KEY_PATH"), Purpose: "runtime-direct"})
}

// Reload retains a previous valid generation on malformed updates. All handshake
// constructors still enforce its expiry, so retention cannot extend validity.
// Trust anchors past their validity are left out of a generation rather than
// invalidating it; a not-yet-valid anchor keeps the update invalid, so the
// last-known-good generation stays active and a later observation activates
// the bundle once the anchor is valid.
func (o *Owner) Reload() error {
	o.reloadMutex.Lock()
	defer o.reloadMutex.Unlock()
	if o.closed.Load() {
		return errors.New("TLS credential owner is closed")
	}
	g, err := load(o.config)
	if err != nil {
		count := o.reloadFailures.Add(1)
		if !o.reloadFailed.Swap(true) || time.Since(o.reloadSummaryAt) >= 30*time.Second {
			o.reloadSummaryAt = time.Now()
			o.logReload(slog.LevelWarn, "transport.credential_reload_failed", "invalid_generation", count)
		}
		return err
	}
	if o.reloadFailed.Swap(false) {
		o.logReload(slog.LevelInfo, "transport.credential_reload_recovered", "recovered", o.reloadFailures.Load())
	}
	old := o.current.Load()
	if old == nil || old.fingerprint != g.fingerprint {
		o.current.Store(g)
		if old != nil && old.trustFingerprint != g.trustFingerprint {
			if observer := o.trustActivation.Load(); observer != nil {
				(*observer)()
			}
		}
		if observer := o.activation.Load(); observer != nil {
			(*observer)()
		}
	}
	return nil
}

func (o *Owner) logReload(level slog.Level, event, outcome string, count uint64) {
	// Production installs the process's bounded logger. A throwing diagnostic
	// handler cannot change credential activation or joined transport cleanup.
	defer func() { _ = recover() }()
	purpose := o.config.Purpose
	if purpose == "" {
		purpose = "native-transport"
	}
	slog.Log(context.Background(), level, event, "component", purpose, "transport.stage", "credential_reload", "transport.outcome", outcome, "failed.count", count)
}

// SetActivationObserver installs one owning transport's valid-generation hook.
// Close withdraws it and joins any invocation on the credential observer.
func (o *Owner) SetActivationObserver(observer func()) error {
	o.reloadMutex.Lock()
	defer o.reloadMutex.Unlock()
	if o.closed.Load() {
		return errors.New("TLS credential owner is closed")
	}
	if observer == nil {
		o.activation.Store(nil)
	} else {
		o.activation.Store(&observer)
	}
	return nil
}

// SetTrustActivationObserver owns CA-retirement drain separately from leaf
// renewal, which preserves established RPCs under an unchanged issuer.
func (o *Owner) SetTrustActivationObserver(observer func()) error {
	o.reloadMutex.Lock()
	defer o.reloadMutex.Unlock()
	if o.closed.Load() {
		return errors.New("TLS credential owner is closed")
	}
	if observer == nil {
		o.trustActivation.Store(nil)
	} else {
		o.trustActivation.Store(&observer)
	}
	return nil
}

// IsCurrentTLSConfig compares immutable trust generations, including expiry.
func (o *Owner) IsCurrentTLSConfig(config *tls.Config) bool {
	g, err := o.snapshot()
	return err == nil && config != nil && config.RootCAs == g.roots
}

func (o *Owner) Close() error {
	o.once.Do(func() {
		o.closed.Store(true)
		o.activation.Store(nil)
		o.trustActivation.Store(nil)
		o.cancel()
		<-o.done
		o.reloadMutex.Lock()
		// Release retained material after any admitted reload/callback has joined.
		o.current.Store(nil)
		o.reloadMutex.Unlock()
	})
	return nil
}
func (o *Owner) snapshot() (*generation, error) {
	g := o.current.Load()
	if o.closed.Load() || g == nil || !time.Now().Before(g.expires) {
		return nil, errors.New("TLS credential generation is expired or unavailable")
	}
	return g, nil
}

func load(cfg Config) (*generation, error) {
	paths := []string{cfg.CAPath}
	if cfg.CertPath != "" {
		paths = append(paths, cfg.CertPath, cfg.KeyPath)
	}
	resolved := make([]string, len(paths))
	data := make([][]byte, len(paths))
	h := sha256.New()
	for i, p := range paths {
		var err error
		resolved[i], err = filepath.EvalSymlinks(p)
		if err != nil {
			return nil, errors.New("TLS mounted generation is unavailable")
		}
		data[i], err = os.ReadFile(resolved[i])
		if err != nil {
			return nil, errors.New("TLS mounted generation cannot be read")
		}
		h.Write(data[i])
	}
	for i, p := range paths {
		r, err := filepath.EvalSymlinks(p)
		if err != nil || r != resolved[i] {
			return nil, errors.New("TLS mounted generation changed while reading")
		}
	}
	roots := x509.NewCertPool()
	rest := data[0]
	expiry := time.Time{}
	count, retained := 0, 0
	now := time.Now()
	for len(strings.TrimSpace(string(rest))) > 0 {
		block, remaining := pem.Decode(rest)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, errors.New("TLS trust bundle is malformed")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA {
			return nil, errors.New("TLS trust bundle contains an invalid CA")
		}
		count++
		rest = remaining
		if now.Before(cert.NotBefore) {
			return nil, errors.New("TLS trust bundle contains a CA that is not yet valid")
		}
		// An expired anchor cannot validate any chain. Leaving it out keeps an
		// old CA that expires during a planned overlap from disabling the
		// generation that still holds the current CA.
		if !now.Before(cert.NotAfter) {
			continue
		}
		roots.AddCert(cert)
		retained++
		if cert.NotAfter.After(expiry) {
			expiry = cert.NotAfter
		}
	}
	if count == 0 {
		return nil, errors.New("TLS trust bundle is empty")
	}
	if retained == 0 {
		return nil, errors.New("TLS trust bundle has no currently valid CA")
	}
	// Without a leaf the generation admits handshakes until its last retained
	// anchor expires; a leaf further bounds it by its verified chain below.
	g := &generation{roots: roots, trustFingerprint: sha256.Sum256(data[0]), expires: expiry}
	copy(g.fingerprint[:], h.Sum(nil))
	if len(data) == 3 {
		pair, err := tls.X509KeyPair(data[1], data[2])
		if err != nil {
			return nil, errors.New("TLS certificate/key generation is invalid")
		}
		leaf, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return nil, errors.New("TLS leaf certificate is invalid")
		}
		intermediates := x509.NewCertPool()
		for _, der := range pair.Certificate[1:] {
			c, e := x509.ParseCertificate(der)
			if e != nil {
				return nil, errors.New("TLS intermediate is invalid")
			}
			intermediates.AddCert(c)
		}
		chains, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
		if err != nil {
			return nil, errors.New("TLS leaf generation cannot be verified")
		}
		// The leaf is presentable while at least one verified chain, leaf
		// included, is entirely within its validity period.
		chainExpiry := time.Time{}
		for _, chain := range chains {
			end := chain[0].NotAfter
			for _, member := range chain[1:] {
				if member.NotAfter.Before(end) {
					end = member.NotAfter
				}
			}
			if end.After(chainExpiry) {
				chainExpiry = end
			}
		}
		pair.Leaf = leaf
		g.leaf = &pair
		if chainExpiry.Before(g.expires) {
			g.expires = chainExpiry
		}
	}
	return g, nil
}

// ClientTLSConfig returns a new verified snapshot for one fresh connection.
func (o *Owner) ClientTLSConfig(serverDNS, peerURI string) (*tls.Config, error) {
	config, _, err := o.ClientTLSConfigSnapshot(serverDNS, peerURI)
	return config, err
}

// ClientTLSConfigSnapshot returns one immutable configuration and its validity
// bound together. Pools retaining admitted work must check that same generation
// before admitting a later operation on an established connection.
func (o *Owner) ClientTLSConfigSnapshot(serverDNS, peerURI string) (*tls.Config, time.Time, error) {
	if serverDNS == "" || net.ParseIP(serverDNS) != nil {
		return nil, time.Time{}, errors.New("TLS expected server DNS name is required")
	}
	g, err := o.snapshot()
	if err != nil {
		return nil, time.Time{}, err
	}
	c := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: g.roots, ServerName: serverDNS, SessionTicketsDisabled: true}
	if g.leaf != nil {
		c.Certificates = []tls.Certificate{*g.leaf}
	}
	if peerURI != "" {
		c.NextProtos = []string{"h2"}
		c.MinVersion = tls.VersionTLS13
		c.VerifyConnection = func(s tls.ConnectionState) error { return requireURI(s, peerURI) }
	}
	return c, g.expires, nil
}
func requireURI(s tls.ConnectionState, want string) error {
	if len(s.PeerCertificates) == 0 {
		return errors.New("TLS peer certificate is missing")
	}
	for _, uri := range s.PeerCertificates[0].URIs {
		if uri.String() == want {
			return nil
		}
	}
	return errors.New("TLS peer role is not allowed")
}

// ServerTLSConfig selects the current complete generation for every mutual-TLS
// handshake. Every native listener verifies its caller's certificate and exact
// role identity, so the client role URI is mandatory.
func (o *Owner) ServerTLSConfig(peerURI string) (*tls.Config, error) {
	if peerURI == "" {
		return nil, errors.New("TLS server requires the exact client role identity")
	}
	makeConfig := func() (*tls.Config, error) {
		g, err := o.snapshot()
		if err != nil {
			return nil, err
		}
		if g.leaf == nil {
			return nil, errors.New("TLS server leaf is required")
		}
		return &tls.Config{
			MinVersion:             tls.VersionTLS13,
			Certificates:           []tls.Certificate{*g.leaf},
			SessionTicketsDisabled: true,
			ClientCAs:              g.roots,
			ClientAuth:             tls.RequireAndVerifyClientCert,
			VerifyConnection:       func(s tls.ConnectionState) error { return requireURI(s, peerURI) },
		}, nil
	}
	c, err := makeConfig()
	if err != nil {
		return nil, err
	}
	c.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return makeConfig() }
	return c, nil
}

// GRPCServerCredentials applies ServerTLSConfig, including its mandatory client role URI.
func (o *Owner) GRPCServerCredentials(peerURI string) (credentials.TransportCredentials, error) {
	c, err := o.ServerTLSConfig(peerURI)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(c), nil
}
func (o *Owner) GRPCClientCredentials(serverDNS, peerURI string) (credentials.TransportCredentials, error) {
	if _, err := o.ClientTLSConfig(serverDNS, peerURI); err != nil {
		return nil, err
	}
	return &clientCredentials{owner: o, dns: serverDNS, uri: peerURI}, nil
}

type clientCredentials struct {
	owner    *Owner
	dns, uri string
}

func (c *clientCredentials) ClientHandshake(ctx context.Context, _ string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	cfg, err := c.owner.ClientTLSConfig(c.dns, c.uri)
	if err != nil {
		return nil, nil, err
	}
	return credentials.NewTLS(cfg).ClientHandshake(ctx, c.dns, raw)
}
func (*clientCredentials) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("client credentials cannot accept connections")
}
func (c *clientCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", SecurityVersion: "1.3", ServerName: c.dns}
}
func (c *clientCredentials) Clone() credentials.TransportCredentials { copy := *c; return &copy }
func (c *clientCredentials) OverrideServerName(name string) error {
	if name != c.dns {
		return fmt.Errorf("TLS server name override is not permitted")
	}
	return nil
}
