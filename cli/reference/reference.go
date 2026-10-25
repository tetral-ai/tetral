package reference

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Config is the server's startup input, read as one JSON object.
type Config struct {
	// Listen and ControlListen are the addresses of the protocol listener
	// and the control listener.
	Listen        string `json:"listen"`
	ControlListen string `json:"control_listen"`
	// Credentials are the test instances' credentials.
	Credentials []Credential `json:"credentials"`
	// MinCLIVersion is the oldest cli_version a ready may carry; empty
	// accepts every version.
	MinCLIVersion string `json:"min_cli_version"`
	// MaxStreamAgeMs is the age at which a stream gets a stream-only
	// disconnect.
	MaxStreamAgeMs int64 `json:"max_stream_age_ms"`
	// GitProxyURL is the origin every git ticket names.
	GitProxyURL string `json:"git_proxy_url"`
	// ClockStart is the clock's first time; the zero time starts it at the
	// wall clock.
	ClockStart time.Time `json:"clock_start"`
}

// Credential is one connection credential of a test instance. State is
// `active`, `pending`, `revoked` or `expired`; a pending credential is one
// that a credential.renew offers.
type Credential struct {
	InstanceID    string    `json:"instance_id"`
	CredentialID  string    `json:"credential_id"`
	ConnectionKey string    `json:"connection_key"`
	ExpiresAt     time.Time `json:"expires_at"`
	State         string    `json:"state"`
}

// Addresses is the server's startup output: the two bound addresses.
type Addresses struct {
	Listen        string `json:"listen"`
	ControlListen string `json:"control_listen"`
}

const (
	// DefaultListen is the address of both listeners when the input gives
	// none.
	DefaultListen = "127.0.0.1:0"
	// DefaultMaxStreamAge is the stream age that ends a stream when the
	// input gives none.
	DefaultMaxStreamAge = 30 * time.Minute
	// DefaultGitProxyURL is the git proxy origin when the input gives none.
	DefaultGitProxyURL = "https://git-proxy.test"

	// GrantMax is the longest validity one ACK of a ready or heartbeat
	// grants.
	GrantMax = 45 * time.Second
	// HeartbeatInterval is the time between two heartbeats of a stream.
	HeartbeatInterval = 15 * time.Second
	// StagingLimit is the number of Session instructions the server issues
	// on a connection before it has stored the connection's first Session
	// report.
	StagingLimit = 64
)

// Server is the reference control server. Its state is in memory behind
// mu; every time it reads comes from its Clock, which has its own lock.
type Server struct {
	mu     sync.Mutex
	clock  *Clock
	config Config
	lastID int

	credentials map[string]*credential
	instances   map[string]*instance
	connections map[string]*connection

	faults        []*fault
	eventsModes   []string
	earlyDispatch map[string][]instructionSpec
	held          []*heldAnswer
	tickets       map[computerKey][]string

	log serverLog
}

// New builds a server from its startup input.
func New(config Config) (*Server, error) {
	if config.MaxStreamAgeMs == 0 {
		config.MaxStreamAgeMs = DefaultMaxStreamAge.Milliseconds()
	}
	if config.GitProxyURL == "" {
		config.GitProxyURL = DefaultGitProxyURL
	}
	if config.MinCLIVersion != "" && !validVersion(config.MinCLIVersion) {
		return nil, fmt.Errorf("min_cli_version %q is not a semantic version", config.MinCLIVersion)
	}
	start := config.ClockStart
	if start.IsZero() {
		start = time.Now().UTC()
	}
	server := &Server{
		clock:         NewClock(start),
		config:        config,
		credentials:   map[string]*credential{},
		instances:     map[string]*instance{},
		connections:   map[string]*connection{},
		earlyDispatch: map[string][]instructionSpec{},
		tickets:       map[computerKey][]string{},
	}
	for _, given := range config.Credentials {
		if err := server.addCredential(given); err != nil {
			return nil, err
		}
	}
	return server, nil
}

// Handler serves Engine's side of the client connection.
func (server *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/client/events", server.events)
	mux.HandleFunc("POST /v1/client/ready", server.ready)
	mux.HandleFunc("POST /v1/client/heartbeat", server.heartbeat)
	mux.HandleFunc("POST /v1/client/session-status", server.sessionStatus)
	mux.HandleFunc("POST /v1/client/credential", server.credential)
	mux.HandleFunc("POST /v1/client/git-ticket", server.gitTicket)
	mux.HandleFunc("POST /v1/client/protocol-error", server.protocolError)
	return mux
}

// newID gives a fresh identity with prefix, unique across the server.
func (server *Server) newID(prefix string) string {
	server.lastID++
	return fmt.Sprintf("%s_%d", prefix, server.lastID)
}
