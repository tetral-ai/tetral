package reference

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type credential struct {
	Credential
	// fetched is set once a credential fetch returned the key of a pending
	// credential; only then can the key authenticate.
	fetched bool
	// renewalConnection is the connection whose stream offers it,
	// renewalSent counts the credential.renew events sent for it, and
	// replaces is the credential that connection used, revoked once this one
	// is first used.
	renewalConnection string
	renewalSent       int
	replaces          *credential
	// reuses counts the requests that used it after it was revoked.
	reuses int
}

type instance struct {
	id string
	// live is the instance's active connection, whose lease may have
	// expired.
	live *connection
}

// connection is one connection with its lease. It outlives its streams.
type connection struct {
	id         string
	instance   *instance
	credential *credential
	leaseUntil time.Time
	ended      bool
	stream     *stream
	streams    int

	readyMessageID string
	readyAccepted  bool
	readyRefused   bool

	heartbeatSeq         int64
	acceptedHeartbeatSeq int64

	ledger   map[string]*ledgerEntry
	issued   map[string]*issuedInstruction
	order    []string
	queue    []*issuedInstruction
	bindings map[string]*binding
	// sessionIssued counts the Session instructions issued before the first
	// stored Session report.
	sessionIssued     int
	firstReportStored bool
	// drives holds each session.drives written through send, by message_id.
	drives map[string]sentDrives
}

func (server *Server) addCredential(given Credential) error {
	switch given.State {
	case "active", "pending", "revoked", "expired":
	default:
		return fmt.Errorf("credential %q: state %q is not active, pending, revoked or expired", given.CredentialID, given.State)
	}
	if given.InstanceID == "" || given.CredentialID == "" {
		return fmt.Errorf("a credential needs instance_id and credential_id")
	}
	if given.State != "pending" && given.ConnectionKey == "" {
		return fmt.Errorf("credential %q: connection_key is required", given.CredentialID)
	}
	if _, exists := server.credentials[given.CredentialID]; exists {
		return fmt.Errorf("credential %q exists", given.CredentialID)
	}
	server.credentials[given.CredentialID] = &credential{Credential: given}
	if server.instances[given.InstanceID] == nil {
		server.instances[given.InstanceID] = &instance{id: given.InstanceID}
	}
	return nil
}

// refusal is an error answer with a code of the client error table.
type refusal struct {
	code    string
	message string
	retry   time.Duration
	// status, when not 0, replaces the status the table gives code; only
	// the answer control sets it.
	status int
}

func (r *refusal) Error() string { return r.code + ": " + r.message }

func refuse(code, message string) *refusal { return &refusal{code: code, message: message} }

// authenticate finds the credential that key authenticates. When
// credentialID is not empty the key must belong to that credential of
// instanceID, as on the events request. Only an events request may use a
// fetched pending credential.
func (server *Server) authenticate(key, instanceID, credentialID string, events bool) (*credential, *refusal) {
	var found *credential
	for _, candidate := range server.credentials {
		if candidate.ConnectionKey != "" && candidate.ConnectionKey == key {
			found = candidate
			break
		}
	}
	invalid := refuse("credential_invalid", "the key does not authenticate the instance and credential")
	if found == nil || (found.State == "pending" && !found.fetched) {
		return nil, invalid
	}
	if credentialID != "" && (found.CredentialID != credentialID || found.InstanceID != instanceID) {
		return nil, invalid
	}
	switch {
	case found.State == "revoked":
		found.reuses++
		return nil, refuse("credential_revoked", "the credential has been revoked")
	case found.State == "expired" || !server.clock.Now().Before(found.ExpiresAt):
		return nil, refuse("credential_expired", "the credential has expired")
	case found.State == "pending" && !events:
		return nil, invalid
	}
	return found, nil
}

// credentialOfKey names the credential whose key is key, in any state.
func (server *Server) credentialOfKey(key string) string {
	for _, candidate := range server.credentials {
		if candidate.ConnectionKey != "" && candidate.ConnectionKey == key {
			return candidate.CredentialID
		}
	}
	return ""
}

// live is the instance's connection while its lease lasts.
func (server *Server) live(instanceID string) *connection {
	instance := server.instances[instanceID]
	if instance == nil || instance.live == nil || instance.live.ended {
		return nil
	}
	if !server.clock.Now().Before(instance.live.leaseUntil) {
		server.endConnection(instance.live)
		return nil
	}
	return instance.live
}

// endConnection ends a connection: its stream closes and it is no longer
// any instance's live connection.
func (server *Server) endConnection(conn *connection) {
	if conn.ended {
		return
	}
	conn.ended = true
	if conn.stream != nil {
		conn.stream.close()
		conn.stream = nil
	}
	if conn.instance.live == conn {
		conn.instance.live = nil
	}
}

// grant is the validity an accepted ready or heartbeat grants now: at most
// GrantMax, capped by the credential's expiry.
func (server *Server) grant(conn *connection) (time.Duration, *refusal) {
	remaining := conn.credential.ExpiresAt.Sub(server.clock.Now())
	if remaining <= 0 {
		return 0, refuse("credential_expired", "no lease grant remains before the credential expires")
	}
	return min(remaining, GrantMax), nil
}

// connect applies the predecessor rules to an events request and returns
// the stream's connection and whether it resumed.
func (server *Server) connect(cred *credential, previous string, resume bool) (*connection, bool, *refusal) {
	now := server.clock.Now()
	active := server.live(cred.InstanceID)
	switch {
	case active == nil:
		conn := &connection{
			id:         server.newID("conn"),
			instance:   server.instances[cred.InstanceID],
			credential: cred,
			ledger:     map[string]*ledgerEntry{},
			issued:     map[string]*issuedInstruction{},
			bindings:   map[string]*binding{},
		}
		conn.leaseUntil = now.Add(min(GrantMax, cred.ExpiresAt.Sub(now)))
		conn.instance.live = conn
		server.connections[conn.id] = conn
		server.commitRenewal(conn, cred)
		return conn, false, nil
	case previous == active.id && resume:
		server.commitRenewal(active, cred)
		return active, true, nil
	case previous == active.id:
		return nil, false, &refusal{
			code: "previous_connection_active", message: "the previous connection's lease is live",
			retry: active.leaseUntil.Sub(now),
		}
	default:
		return nil, false, refuse("connection_conflict", "another connection of the instance is live")
	}
}

// validVersion says whether text is a semantic version.
func validVersion(text string) bool {
	_, ok := parseVersion(text)
	return ok
}

type version struct {
	core [3]int64
	pre  []string
}

func parseVersion(text string) (version, bool) {
	text, _, _ = strings.Cut(text, "+")
	core, pre, hasPre := strings.Cut(text, "-")
	parts := strings.Split(core, ".")
	var parsed version
	if len(parts) != 3 {
		return parsed, false
	}
	for index, part := range parts {
		number, err := strconv.ParseInt(part, 10, 64)
		if err != nil || number < 0 {
			return parsed, false
		}
		parsed.core[index] = number
	}
	if hasPre {
		parsed.pre = strings.Split(pre, ".")
	}
	return parsed, true
}

// older says whether a precedes b in semantic version precedence.
func older(a, b string) bool {
	left, _ := parseVersion(a)
	right, _ := parseVersion(b)
	for index := range left.core {
		if left.core[index] != right.core[index] {
			return left.core[index] < right.core[index]
		}
	}
	switch {
	case len(left.pre) == 0:
		return false
	case len(right.pre) == 0:
		return true
	}
	for index := 0; index < len(left.pre) && index < len(right.pre); index++ {
		x, y := left.pre[index], right.pre[index]
		if x == y {
			continue
		}
		xNumber, xErr := strconv.ParseInt(x, 10, 64)
		yNumber, yErr := strconv.ParseInt(y, 10, 64)
		switch {
		case xErr == nil && yErr == nil:
			return xNumber < yNumber
		case xErr == nil:
			return true
		case yErr == nil:
			return false
		}
		return x < y
	}
	return len(left.pre) < len(right.pre)
}
