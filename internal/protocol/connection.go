package protocol

import (
	"net/url"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/tetral-ai/tetral/internal/pathvalidation"
)

// Byte limits of connection-v1.
const (
	// McpToolsJSONBytes bounds a local MCP tool-list report.
	McpToolsJSONBytes   = 4194304
	mcpToolsMaxTools    = 2048
	lostChangePathsMax  = 100
	drivePathMaxBytes   = 1024
	driveSegmentMaxSize = 255
	retryAfterMaxMS     = 45000
	grantValidityMaxMS  = 45000
	missingHashesMax    = 10000
	startupMaxMS        = 2147483647
	connectionKeyPrefix = "tetral_ck_"
)

// safeID is SafeId: an identity the CLI uses as a folder name, so no "/" and
// neither "." nor "..".
var safeID = stringWith(minLength(1), noNUL, func(s string) string {
	if strings.Contains(s, "/") || s == "." || s == ".." {
		return "not a safe folder name"
	}
	return ""
})

// relativePath checks the drive path pattern of every schema file: segments
// separated by "/", none empty, "." or "..", and no C0 or C1 control
// character. A trailing "/" is allowed only where trailingSlash says so.
func relativePath(trailingSlash bool) stringCheck {
	return func(s string) string {
		if trailingSlash {
			s = strings.TrimSuffix(s, "/")
		}
		for _, segment := range strings.Split(s, "/") {
			if segment == "" || segment == "." || segment == ".." {
				return "not a relative path of named segments"
			}
			if strings.IndexFunc(segment, isControl) >= 0 {
				return "control character in a path"
			}
		}
		return ""
	}
}

func isControl(r rune) bool { return r <= 0x1f || 0x7f <= r && r <= 0x9f }

var (
	drivePath  = stringWith(minLength(1), maxLength(drivePathMaxBytes), relativePath(true))
	changePath = stringWith(minLength(1), maxLength(drivePathMaxBytes), relativePath(false))
)

// checkDrivePathRules applies the path rules beyond the pattern: at most
// 1,024 UTF-8 bytes and 255 per segment, NFC in the Stream-Safe Text Format
// (no run of more than 30 non-starters), and no control or format character.
func checkDrivePathRules(ptr, path string) error {
	if len(path) > drivePathMaxBytes {
		return invalid(ptr, "drive path over 1,024 bytes")
	}
	for _, segment := range strings.Split(strings.TrimSuffix(path, "/"), "/") {
		if len(segment) > driveSegmentMaxSize {
			return invalid(ptr, "drive path segment over 255 bytes")
		}
	}
	if !pathvalidation.IsNFC(path) {
		return invalid(ptr, "drive path not NFC")
	}
	if strings.IndexFunc(path, func(r rune) bool { return unicode.Is(unicode.Cc, r) || unicode.Is(unicode.Cf, r) }) >= 0 {
		return invalid(ptr, "drive path has a control or format character")
	}
	return nil
}

// ConnectedPayload opens a stream.
type ConnectedPayload struct {
	Resumed bool
}

func (ConnectedPayload) MessageType() string { return "connected" }

func connectedPayload(d *decoder, ptr string, v *value) ConnectedPayload {
	o := d.object(ptr, v)
	p := ConnectedPayload{Resumed: required(o, "resumed", boolean)}
	o.open()
	return p
}

func (p ConnectedPayload) encode(e *encoder) {
	e.beginObject()
	e.boolMember("resumed", p.Resumed)
	e.endObject()
}

// HeartbeatPayload asks the CLI for a heartbeat report.
type HeartbeatPayload struct {
	HeartbeatSeq int64
}

func (HeartbeatPayload) MessageType() string { return "heartbeat" }

func heartbeatPayload(d *decoder, ptr string, v *value) HeartbeatPayload {
	o := d.object(ptr, v)
	p := HeartbeatPayload{HeartbeatSeq: required(o, "heartbeat_seq", positiveInteger)}
	o.open()
	return p
}

func (p HeartbeatPayload) encode(e *encoder) {
	e.beginObject()
	e.integerMember("heartbeat_seq", p.HeartbeatSeq)
	e.endObject()
}

// DriveBinding is one drive of a binding's drive set.
type DriveBinding struct {
	DriveID string
	Access  string // read_only or read_write
}

func driveBinding(d *decoder, ptr string, v *value) DriveBinding {
	o := d.object(ptr, v)
	b := DriveBinding{
		DriveID: required(o, "drive_id", safeID),
		Access:  required(o, "access", enum("read_only", "read_write")),
	}
	o.open()
	return b
}

func (b DriveBinding) encode(e *encoder) {
	e.beginObject()
	e.stringMember("drive_id", b.DriveID)
	e.stringMember("access", b.Access)
	e.endObject()
}

// DriveExclude lists the paths of a drive that a machine does not sync.
type DriveExclude struct {
	DriveID string
	Paths   []string
}

func driveExclude(d *decoder, ptr string, v *value) DriveExclude {
	o := d.object(ptr, v)
	x := DriveExclude{
		DriveID: required(o, "drive_id", identity),
		Paths:   required(o, "paths", arrayOf(drivePath, 0, -1)),
	}
	o.open()
	return x
}

func (x DriveExclude) encode(e *encoder) {
	e.beginObject()
	e.stringMember("drive_id", x.DriveID)
	e.key("paths")
	e.strings(x.Paths)
	e.endObject()
}

// Checkout is the branch or commit a repository is checked out at.
type Checkout struct {
	Type string           // branch or commit
	Name Optional[string] // branch
	SHA  Optional[string] // commit
}

func checkout(d *decoder, ptr string, v *value) Checkout {
	o := d.object(ptr, v)
	c := Checkout{Type: required(o, "type", enum("branch", "commit"))}
	switch c.Type {
	case "branch":
		c.Name = Some(required(o, "name", stringWith(minLength(1))))
		o.absent("sha")
	case "commit":
		c.SHA = Some(required(o, "sha", stringWith(lowerHex(40))))
		o.absent("name")
	}
	o.open()
	return c
}

func (c Checkout) encode(e *encoder) {
	e.beginObject()
	e.stringMember("type", c.Type)
	optionalMember(e, "name", c.Name, writeString)
	optionalMember(e, "sha", c.SHA, writeString)
	e.endObject()
}

// GitIdentity is the author of a repository's commits.
type GitIdentity struct {
	Name  string
	Email string
}

func gitIdentity(d *decoder, ptr string, v *value) GitIdentity {
	o := d.object(ptr, v)
	g := GitIdentity{Name: required(o, "name", anyString), Email: required(o, "email", anyString)}
	o.open()
	return g
}

func (g GitIdentity) encode(e *encoder) {
	e.beginObject()
	e.stringMember("name", g.Name)
	e.stringMember("email", g.Email)
	e.endObject()
}

// Repository is a repository a cloud computer clones for a Session.
type Repository struct {
	URL         string
	Checkout    Nullable[Checkout] // null for the default branch
	Path        string
	GitIdentity GitIdentity
}

var absolute = stringWith(absolutePath)

func repository(d *decoder, ptr string, v *value) Repository {
	o := d.object(ptr, v)
	r := Repository{
		URL:         required(o, "url", stringWith(minLength(1))),
		Checkout:    requiredNullable(o, "checkout", checkout),
		Path:        required(o, "path", absolute),
		GitIdentity: required(o, "git_identity", gitIdentity),
	}
	o.absent("authorization_token")
	o.open()
	return r
}

func (r Repository) encode(e *encoder) {
	e.beginObject()
	e.stringMember("url", r.URL)
	if r.Checkout.Present {
		e.valueMember("checkout", r.Checkout.Value)
	} else {
		e.key("checkout")
		e.null()
	}
	e.stringMember("path", r.Path)
	e.valueMember("git_identity", r.GitIdentity)
	e.endObject()
}

// SessionPreparePayload prepares a binding of a Session on the machine. Only
// a cloud computer, which creates its cwd, has repositories.
type SessionPreparePayload struct {
	SessionID    string
	BindingID    string
	Cwd          string
	CreateCwd    Optional[bool]
	Drives       []DriveBinding
	DriveExclude []DriveExclude
	Repositories []Repository
}

func (SessionPreparePayload) MessageType() string { return "session.prepare" }

func sessionPreparePayload(d *decoder, ptr string, v *value) SessionPreparePayload {
	o := d.object(ptr, v)
	p := SessionPreparePayload{
		SessionID:    required(o, "session_id", safeID),
		BindingID:    required(o, "binding_id", identity),
		Cwd:          required(o, "cwd", absolute),
		CreateCwd:    optional(o, "create_cwd", boolean),
		Drives:       required(o, "drives", arrayOf(driveBinding, 0, -1)),
		DriveExclude: required(o, "drive_exclude", arrayOf(driveExclude, 0, -1)),
	}
	maxRepositories := 0
	if p.CreateCwd.Value {
		maxRepositories = -1
	}
	p.Repositories = required(o, "repositories", arrayOf(repository, 0, maxRepositories))
	o.absent("ticket")
	o.open()
	return p
}

func (p SessionPreparePayload) encode(e *encoder) {
	e.beginObject()
	e.stringMember("session_id", p.SessionID)
	e.stringMember("binding_id", p.BindingID)
	e.stringMember("cwd", p.Cwd)
	optionalMember(e, "create_cwd", p.CreateCwd, writeBool)
	e.key("drives")
	list(e, p.Drives)
	e.key("drive_exclude")
	list(e, p.DriveExclude)
	e.key("repositories")
	list(e, p.Repositories)
	e.endObject()
}

// SessionClosePayload closes a binding.
type SessionClosePayload struct {
	SessionID string
	BindingID string
}

func (SessionClosePayload) MessageType() string { return "session.close" }

func sessionClosePayload(d *decoder, ptr string, v *value) SessionClosePayload {
	o := d.object(ptr, v)
	p := SessionClosePayload{
		SessionID: required(o, "session_id", safeID),
		BindingID: required(o, "binding_id", identity),
	}
	o.open()
	return p
}

func (p SessionClosePayload) encode(e *encoder) {
	e.beginObject()
	e.stringMember("session_id", p.SessionID)
	e.stringMember("binding_id", p.BindingID)
	e.endObject()
}

// SessionDrivesPayload replaces a binding's drive set.
type SessionDrivesPayload struct {
	SessionID string
	BindingID string
	Drives    []DriveBinding
}

func (SessionDrivesPayload) MessageType() string { return "session.drives" }

func sessionDrivesPayload(d *decoder, ptr string, v *value) SessionDrivesPayload {
	o := d.object(ptr, v)
	p := SessionDrivesPayload{
		SessionID: required(o, "session_id", safeID),
		BindingID: required(o, "binding_id", identity),
		Drives:    required(o, "drives", arrayOf(driveBinding, 0, -1)),
	}
	o.open()
	return p
}

func (p SessionDrivesPayload) encode(e *encoder) {
	e.beginObject()
	e.stringMember("session_id", p.SessionID)
	e.stringMember("binding_id", p.BindingID)
	e.key("drives")
	list(e, p.Drives)
	e.endObject()
}

// CredentialRenewPayload names the pending credential the CLI fetches; it
// never carries a connection key.
type CredentialRenewPayload struct {
	CredentialID string
}

func (CredentialRenewPayload) MessageType() string { return "credential.renew" }

func credentialRenewPayload(d *decoder, ptr string, v *value) CredentialRenewPayload {
	o := d.object(ptr, v)
	p := CredentialRenewPayload{CredentialID: required(o, "credential_id", identity)}
	o.absent("connection_key")
	o.open()
	return p
}

func (p CredentialRenewPayload) encode(e *encoder) {
	e.beginObject()
	e.stringMember("credential_id", p.CredentialID)
	e.endObject()
}

// DisconnectPayload ends a stream, or the whole connection with a code.
type DisconnectPayload struct {
	Scope     string           // stream or connection
	Retryable Optional[bool]   // connection
	Code      Optional[string] // connection: an ErrorCode
}

func (DisconnectPayload) MessageType() string { return "disconnect" }

func errorCode(d *decoder, ptr string, v *value) string {
	return enum(errorCodeNames()...)(d, ptr, v)
}

func disconnectPayload(d *decoder, ptr string, v *value) DisconnectPayload {
	o := d.object(ptr, v)
	p := DisconnectPayload{Scope: required(o, "scope", enum("stream", "connection"))}
	switch p.Scope {
	case "stream":
		o.absent("retryable", "code")
	case "connection":
		p.Retryable = Some(required(o, "retryable", boolean))
		p.Code = Some(required(o, "code", errorCode))
	}
	o.open()
	return p
}

func (p DisconnectPayload) encode(e *encoder) {
	e.beginObject()
	e.stringMember("scope", p.Scope)
	optionalMember(e, "retryable", p.Retryable, writeBool)
	optionalMember(e, "code", p.Code, writeString)
	e.endObject()
}

// DriveChangedPayload tells the CLI that a drive has a newer commit.
type DriveChangedPayload struct {
	DriveID string
	Seq     int64
}

func (DriveChangedPayload) MessageType() string { return "drive.changed" }

func driveChangedPayload(d *decoder, ptr string, v *value) DriveChangedPayload {
	o := d.object(ptr, v)
	p := DriveChangedPayload{
		DriveID: required(o, "drive_id", identity),
		Seq:     required(o, "seq", positiveInteger),
	}
	o.open()
	return p
}

func (p DriveChangedPayload) encode(e *encoder) {
	e.beginObject()
	e.stringMember("drive_id", p.DriveID)
	e.integerMember("seq", p.Seq)
	e.endObject()
}

// connectionPayloads are every SSE envelope's payloads: the connection's own
// and the tool instructions.
var connectionPayloads = func() map[string]decodeFunc[DownstreamPayload] {
	payloads := map[string]decodeFunc[DownstreamPayload]{
		"connected":        payloadOf(connectedPayload),
		"heartbeat":        payloadOf(heartbeatPayload),
		"session.prepare":  payloadOf(sessionPreparePayload),
		"session.close":    payloadOf(sessionClosePayload),
		"session.drives":   payloadOf(sessionDrivesPayload),
		"credential.renew": payloadOf(credentialRenewPayload),
		"disconnect":       payloadOf(disconnectPayload),
		"drive.changed":    payloadOf(driveChangedPayload),
	}
	for t, f := range toolPayloads {
		payloads[t] = f
	}
	return payloads
}()

var connectionDownstream = downstream(connectionPayloads)

// checkDownstream applies the relations of an SSE envelope: a tool
// instruction's own budgets, and otherwise at most 64 KiB, each drive once in
// a drive set, each repository path once, excluded paths under the path
// rules, and no connection key in a credential.renew.
func checkDownstream(m Downstream, v *value) error {
	if _, ok := toolPayloads[m.Payload.MessageType()]; ok {
		return checkToolDownstream(m, v)
	}
	if compactSize(v) > DefaultMessageJSONBytes {
		return invalid("", "SSE event over 64 KiB")
	}
	switch p := m.Payload.(type) {
	case SessionPreparePayload:
		if err := uniqueDrives("/payload/drives", p.Drives); err != nil {
			return err
		}
		paths := make(map[string]bool, len(p.Repositories))
		for i, r := range p.Repositories {
			if paths[r.Path] {
				return invalid("/payload/repositories/"+strconv.Itoa(i)+"/path", "repository path twice")
			}
			paths[r.Path] = true
		}
		for i, x := range p.DriveExclude {
			for j, path := range x.Paths {
				if err := checkDrivePathRules("/payload/drive_exclude/"+strconv.Itoa(i)+"/paths/"+strconv.Itoa(j), path); err != nil {
					return err
				}
			}
		}
	case SessionDrivesPayload:
		return uniqueDrives("/payload/drives", p.Drives)
	case CredentialRenewPayload:
		payload, _ := v.get("payload")
		if holdsString(payload, connectionKeyPrefix) {
			return invalid("/payload", "credential.renew carries a connection key")
		}
	}
	return nil
}

func uniqueDrives(ptr string, drives []DriveBinding) error {
	seen := make(map[string]bool, len(drives))
	for i, b := range drives {
		if seen[b.DriveID] {
			return invalid(ptr+"/"+strconv.Itoa(i)+"/drive_id", "drive_id twice in drives")
		}
		seen[b.DriveID] = true
	}
	return nil
}

// holdsString reports whether any string inside v contains part.
func holdsString(v *value, part string) bool {
	switch v.kind {
	case kindString:
		return strings.Contains(v.text, part)
	case kindArray:
		return slices.ContainsFunc(v.items, func(item *value) bool { return holdsString(item, part) })
	case kindObject:
		return slices.ContainsFunc(v.members, func(m member) bool { return holdsString(m.value, part) })
	}
	return false
}

// EventsQuery is the query of GET /v1/client/events.
type EventsQuery struct {
	InstanceID           string
	CredentialID         string
	PreviousConnectionID Optional[string]
	Resume               Optional[string] // "true" or "false"
}

func eventsQuery(d *decoder, ptr string, v *value) EventsQuery {
	o := d.object(ptr, v)
	required(o, "protocol_version", constString("1"))
	q := EventsQuery{
		InstanceID:           required(o, "instance_id", identity),
		CredentialID:         required(o, "credential_id", identity),
		PreviousConnectionID: optional(o, "previous_connection_id", identity),
		Resume:               optional(o, "resume", enum("true", "false")),
	}
	o.open()
	return q
}

func (q EventsQuery) Query() map[string][]string {
	values := map[string][]string{"protocol_version": {"1"}, "instance_id": {q.InstanceID}, "credential_id": {q.CredentialID}}
	setOptional(values, "previous_connection_id", q.PreviousConnectionID)
	setOptional(values, "resume", q.Resume)
	return values
}

func setOptional(values map[string][]string, name string, o Optional[string]) {
	if o.Set {
		values[name] = []string{o.Value}
	}
}

// DecodeEventsQuery decodes the query of GET /v1/client/events.
func DecodeEventsQuery(query url.Values) (EventsQuery, error) {
	return decodeParameters(query, eventsQuery)
}

// DriveCapability declares whether the machine mounts drives.
type DriveCapability struct {
	Mount bool
}

func driveCapability(d *decoder, ptr string, v *value) DriveCapability {
	o := d.object(ptr, v)
	c := DriveCapability{Mount: required(o, "mount", boolean)}
	o.open()
	return c
}

func (c DriveCapability) encode(e *encoder) {
	e.beginObject()
	e.boolMember("mount", c.Mount)
	e.endObject()
}

// Capabilities is what a ready declares; Session preparation is always
// supported.
type Capabilities struct {
	Drives        DriveCapability
	ToolExecution Optional[ToolExecutionCapability]
	LocalMcp      Optional[LocalMcpCapability] // needs ToolExecution with cancel
}

func capabilities(d *decoder, ptr string, v *value) Capabilities {
	o := d.object(ptr, v)
	required(o, "session_preparation", constBool(true))
	c := Capabilities{
		Drives:        required(o, "drives", driveCapability),
		ToolExecution: optional(o, "tool_execution", toolExecutionCapability),
		LocalMcp:      optional(o, "local_mcp", localMcpCapability),
	}
	if c.LocalMcp.Set && !d.failed() {
		switch {
		case !c.ToolExecution.Set:
			d.fail(ptr, "missing required member %q", "tool_execution")
		case !slices.Contains(c.ToolExecution.Value.Controls, ToolCancel):
			d.fail(ptr+"/tool_execution/controls", "local MCP requires the cancel control")
		}
	}
	o.open()
	return c
}

func (c Capabilities) encode(e *encoder) {
	e.beginObject()
	e.boolMember("session_preparation", true)
	e.valueMember("drives", c.Drives)
	optionalMember(e, "tool_execution", c.ToolExecution, writeValue)
	optionalMember(e, "local_mcp", c.LocalMcp, writeValue)
	e.endObject()
}

// LostChange lists the changes of one drive that a machine dropped when a
// binding closed or its drive set changed.
type LostChange struct {
	RequestMessageID string
	DriveID          string
	Paths            []string // at most 100, in UTF-8 byte order
	PathsTotal       int64
}

func lostChange(d *decoder, ptr string, v *value) LostChange {
	o := d.object(ptr, v)
	l := LostChange{
		RequestMessageID: required(o, "request_message_id", identity),
		DriveID:          required(o, "drive_id", identity),
		Paths:            required(o, "paths", uniqueStrings(arrayOf(changePath, 0, lostChangePathsMax))),
		PathsTotal:       required(o, "paths_total", positiveInteger),
	}
	o.open()
	return l
}

func (l LostChange) encode(e *encoder) {
	e.beginObject()
	e.stringMember("request_message_id", l.RequestMessageID)
	e.stringMember("drive_id", l.DriveID)
	e.key("paths")
	e.strings(l.Paths)
	e.integerMember("paths_total", l.PathsTotal)
	e.endObject()
}

var lostChanges = arrayOf(lostChange, 1, -1)

// checkLostChanges applies the relations of lost changes: path rules, paths
// in UTF-8 byte order, paths_total not below the listed paths. A report lists
// one entry per drive in drive_id order and stops listing paths after the
// first entry it cut; a ready keeps each report's entries in that order.
func checkLostChanges(ptr string, entries []LostChange, inReport bool) error {
	for i, entry := range entries {
		at := ptr + "/" + strconv.Itoa(i)
		for j, path := range entry.Paths {
			if err := checkDrivePathRules(at+"/paths/"+strconv.Itoa(j), path); err != nil {
				return err
			}
			if j > 0 && entry.Paths[j-1] >= path {
				return invalid(at+"/paths/"+strconv.Itoa(j), "lost_changes paths out of UTF-8 byte order")
			}
		}
		if entry.PathsTotal < int64(len(entry.Paths)) {
			return invalid(at+"/paths_total", "paths_total below listed paths")
		}
	}
	if inReport {
		cut := false
		for i, entry := range entries {
			at := ptr + "/" + strconv.Itoa(i)
			if i > 0 && entries[i-1].DriveID >= entry.DriveID {
				return invalid(at+"/drive_id", "lost_changes entries out of drive_id order")
			}
			if cut && len(entry.Paths) > 0 {
				return invalid(at+"/paths", "paths listed after a cut entry")
			}
			cut = cut || int64(len(entry.Paths)) < min(entry.PathsTotal, lostChangePathsMax)
		}
		return nil
	}
	last := make(map[string]string, len(entries))
	for i, entry := range entries {
		if previous, ok := last[entry.RequestMessageID]; ok && previous >= entry.DriveID {
			return invalid(ptr+"/"+strconv.Itoa(i)+"/drive_id", "kept lost_changes entry repeated or reordered")
		}
		last[entry.RequestMessageID] = entry.DriveID
	}
	return nil
}

// ReadyStartup is the CLI's startup timing a ready may carry.
type ReadyStartup struct {
	ConfigMS  int64
	ConnectMS int64
	ReadyMS   int64
}

var startupMilliseconds = integer(0, startupMaxMS)

func readyStartup(d *decoder, ptr string, v *value) ReadyStartup {
	o := d.object(ptr, v)
	s := ReadyStartup{
		ConfigMS:  required(o, "config_ms", startupMilliseconds),
		ConnectMS: required(o, "connect_ms", startupMilliseconds),
		ReadyMS:   required(o, "ready_ms", startupMilliseconds),
	}
	o.open()
	return s
}

func (s ReadyStartup) encode(e *encoder) {
	e.beginObject()
	e.integerMember("config_ms", s.ConfigMS)
	e.integerMember("connect_ms", s.ConnectMS)
	e.integerMember("ready_ms", s.ReadyMS)
	e.endObject()
}

// Ready is the CLI's first report on a connection.
type Ready struct {
	ConnectionID string
	MessageID    string
	OS           string // linux or macos
	Arch         string // x86_64 or aarch64
	CLIVersion   string // semantic version
	Capabilities Capabilities
	LostChanges  Optional[[]LostChange]
	Startup      Optional[ReadyStartup]
}

func ready(d *decoder, ptr string, v *value) Ready {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	r := Ready{
		ConnectionID: required(o, "connection_id", identity),
		MessageID:    required(o, "message_id", identity),
		OS:           required(o, "os", enum("linux", "macos")),
		Arch:         required(o, "arch", enum("x86_64", "aarch64")),
		CLIVersion:   required(o, "cli_version", stringWith(semanticVersion)),
		Capabilities: required(o, "capabilities", capabilities),
		LostChanges:  optional(o, "lost_changes", lostChanges),
		Startup:      optional(o, "startup", readyStartup),
	}
	o.open()
	return r
}

func (r Ready) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", r.ConnectionID)
	e.stringMember("message_id", r.MessageID)
	e.stringMember("os", r.OS)
	e.stringMember("arch", r.Arch)
	e.stringMember("cli_version", r.CLIVersion)
	e.valueMember("capabilities", r.Capabilities)
	optionalMember(e, "lost_changes", r.LostChanges, writeList)
	optionalMember(e, "startup", r.Startup, writeValue)
	e.endObject()
}

// checkReady applies the relations of a ready: at most 64 KiB, the tool
// capability's relations, tool execution only with tools or local MCP, and
// the kept lost changes in order.
func checkReady(r Ready, v *value) error {
	if err := checkBodySize(v, DefaultMessageJSONBytes, "body over 64 KiB"); err != nil {
		return err
	}
	if c := r.Capabilities; c.ToolExecution.Set {
		if err := checkCapabilities("/capabilities/tool_execution", c.ToolExecution.Value, c.LocalMcp.Set); err != nil {
			return err
		}
		if len(c.ToolExecution.Value.Tools) == 0 && !c.LocalMcp.Set {
			return invalid("/capabilities/tool_execution/tools", "tool_execution without tools or local_mcp")
		}
	}
	if r.LostChanges.Set {
		return checkLostChanges("/lost_changes", r.LostChanges.Value, false)
	}
	return nil
}

// semanticVersion is the SemVer 2.0.0 grammar.
func semanticVersion(s string) string {
	core, build, hasBuild := strings.Cut(s, "+")
	core, pre, hasPre := strings.Cut(core, "-")
	numbers := strings.Split(core, ".")
	ok := len(numbers) == 3 && !slices.ContainsFunc(numbers, func(n string) bool { return !numericIdentifier(n) })
	if ok && hasPre {
		ok = !slices.ContainsFunc(strings.Split(pre, "."), func(id string) bool {
			return !alphanumericIdentifier(id) || isDigits(id) && !numericIdentifier(id)
		})
	}
	if ok && hasBuild {
		ok = !slices.ContainsFunc(strings.Split(build, "."), func(id string) bool { return !alphanumericIdentifier(id) })
	}
	if !ok {
		return "not a semantic version"
	}
	return ""
}

func isDigits(s string) bool {
	return s != "" && strings.Trim(s, "0123456789") == ""
}

func numericIdentifier(s string) bool {
	return isDigits(s) && (s == "0" || s[0] != '0')
}

func alphanumericIdentifier(s string) bool {
	return s != "" && strings.IndexFunc(s, func(r rune) bool { return !identifierRune(r) }) < 0
}

func identifierRune(r rune) bool {
	return '0' <= r && r <= '9' || 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || r == '-'
}

// Heartbeat reports a heartbeat instruction.
type Heartbeat struct {
	ConnectionID     string
	MessageID        string
	RequestMessageID string
	HeartbeatSeq     int64
}

func heartbeat(d *decoder, ptr string, v *value) Heartbeat {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	h := Heartbeat{
		ConnectionID:     required(o, "connection_id", identity),
		MessageID:        required(o, "message_id", identity),
		RequestMessageID: required(o, "request_message_id", identity),
		HeartbeatSeq:     required(o, "heartbeat_seq", positiveInteger),
	}
	o.open()
	return h
}

func (h Heartbeat) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", h.ConnectionID)
	e.stringMember("message_id", h.MessageID)
	e.stringMember("request_message_id", h.RequestMessageID)
	e.integerMember("heartbeat_seq", h.HeartbeatSeq)
	e.endObject()
}

// GrantAcknowledgement is the ACK of a ready or heartbeat, with the lease it
// grants.
type GrantAcknowledgement struct {
	ConnectionID string
	MessageID    string
	ValidForMS   int64
}

func grantAcknowledgement(d *decoder, ptr string, v *value) GrantAcknowledgement {
	o := d.object(ptr, v)
	a := readAcknowledgement(o)
	g := GrantAcknowledgement{
		ConnectionID: a.ConnectionID,
		MessageID:    a.MessageID,
		ValidForMS:   required(o, "valid_for_ms", integer(1, grantValidityMaxMS)),
	}
	o.open()
	return g
}

func (g GrantAcknowledgement) encode(e *encoder) {
	e.beginObject()
	Acknowledgement{g.ConnectionID, g.MessageID}.members(e)
	e.integerMember("valid_for_ms", g.ValidForMS)
	e.endObject()
}

// PreparationError is why a binding could not be prepared.
type PreparationError struct {
	Code    string
	Message string
}

func preparationError(d *decoder, ptr string, v *value) PreparationError {
	o := d.object(ptr, v)
	p := PreparationError{
		Code: required(o, "code", enum("cwd_not_found", "cwd_not_directory", "cwd_forbidden", "cwd_inaccessible",
			"cwd_check_timeout", "invalid_input", "stale_connection", "binding_conflict", "cleanup_pending",
			"resource_exhausted", "repository_path_exists", "repository_path_forbidden")),
		Message: required(o, "message", anyString),
	}
	o.open()
	return p
}

func (p PreparationError) encode(e *encoder) {
	e.beginObject()
	e.stringMember("code", p.Code)
	e.stringMember("message", p.Message)
	e.endObject()
}

// RepositoryFailure is a repository a prepared binding could not provide.
type RepositoryFailure struct {
	URL     string
	Path    string
	Cause   string // clone, not_found, authentication, forbidden or checkout
	Message string
}

func repositoryFailure(d *decoder, ptr string, v *value) RepositoryFailure {
	o := d.object(ptr, v)
	f := RepositoryFailure{
		URL:     required(o, "url", stringWith(minLength(1))),
		Path:    required(o, "path", absolute),
		Cause:   required(o, "cause", enum("clone", "not_found", "authentication", "forbidden", "checkout")),
		Message: required(o, "message", anyString),
	}
	o.open()
	return f
}

func (f RepositoryFailure) encode(e *encoder) {
	e.beginObject()
	e.stringMember("url", f.URL)
	e.stringMember("path", f.Path)
	e.stringMember("cause", f.Cause)
	e.stringMember("message", f.Message)
	e.endObject()
}

// SessionStatus answers a Session instruction: prepared or failed answers
// session.prepare, closed answers session.close and applied session.drives.
type SessionStatus struct {
	ConnectionID       string
	MessageID          string
	RequestMessageID   string
	SessionID          string
	BindingID          string
	Status             string                        // prepared, failed, closed or applied
	Cwd                Optional[string]              // prepared
	DriveMountFailures Optional[[]DriveMountFailure] // prepared, applied
	RepositoryFailures Optional[[]RepositoryFailure] // prepared
	Error              Optional[PreparationError]    // failed
	LostChanges        Optional[[]LostChange]        // closed, applied
}

func sessionStatus(d *decoder, ptr string, v *value) SessionStatus {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	s := SessionStatus{
		ConnectionID:     required(o, "connection_id", identity),
		MessageID:        required(o, "message_id", identity),
		RequestMessageID: required(o, "request_message_id", identity),
		SessionID:        required(o, "session_id", identity),
		BindingID:        required(o, "binding_id", identity),
		Status:           required(o, "status", enum("prepared", "failed", "closed", "applied")),
	}
	mountFailures := arrayOf(driveMountFailure, 1, -1)
	switch s.Status {
	case "prepared":
		s.Cwd = Some(required(o, "cwd", absolute))
		s.DriveMountFailures = optional(o, "drive_mount_failures", mountFailures)
		s.RepositoryFailures = optional(o, "repository_failures", arrayOf(repositoryFailure, 1, -1))
		o.absent("error", "lost_changes")
	case "failed":
		s.Error = Some(required(o, "error", preparationError))
		o.absent("cwd", "drive_mount_failures", "repository_failures", "lost_changes")
	case "closed":
		s.LostChanges = optional(o, "lost_changes", lostChanges)
		o.absent("cwd", "error", "drive_mount_failures", "repository_failures")
	case "applied":
		s.DriveMountFailures = optional(o, "drive_mount_failures", mountFailures)
		s.LostChanges = optional(o, "lost_changes", lostChanges)
		o.absent("cwd", "error", "repository_failures")
	}
	o.open()
	return s
}

func (s SessionStatus) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", s.ConnectionID)
	e.stringMember("message_id", s.MessageID)
	e.stringMember("request_message_id", s.RequestMessageID)
	e.stringMember("session_id", s.SessionID)
	e.stringMember("binding_id", s.BindingID)
	e.stringMember("status", s.Status)
	optionalMember(e, "cwd", s.Cwd, writeString)
	optionalMember(e, "drive_mount_failures", s.DriveMountFailures, writeList)
	optionalMember(e, "repository_failures", s.RepositoryFailures, writeList)
	optionalMember(e, "error", s.Error, writeValue)
	optionalMember(e, "lost_changes", s.LostChanges, writeList)
	e.endObject()
}

func checkSessionStatus(s SessionStatus, v *value) error {
	if err := checkBodySize(v, DefaultMessageJSONBytes, "body over 64 KiB"); err != nil {
		return err
	}
	drives := make(map[string]bool)
	for i, f := range s.DriveMountFailures.Value {
		if drives[f.DriveID] {
			return invalid("/drive_mount_failures/"+strconv.Itoa(i)+"/drive_id", "drive listed twice in drive_mount_failures")
		}
		drives[f.DriveID] = true
	}
	paths := make(map[string]bool)
	for i, f := range s.RepositoryFailures.Value {
		if paths[f.Path] {
			return invalid("/repository_failures/"+strconv.Itoa(i)+"/path", "repository listed twice in repository_failures")
		}
		paths[f.Path] = true
	}
	if s.LostChanges.Set {
		return checkLostChanges("/lost_changes", s.LostChanges.Value, true)
	}
	return nil
}

// McpServerInfo is a local MCP server's initialize result.
type McpServerInfo struct {
	Name    string
	Title   Nullable[string]
	Version string
}

func mcpServerInfo(d *decoder, ptr string, v *value) McpServerInfo {
	o := d.object(ptr, v)
	i := McpServerInfo{
		Name:    required(o, "name", anyString),
		Title:   requiredNullable(o, "title", anyString),
		Version: required(o, "version", anyString),
	}
	o.open()
	return i
}

func (i McpServerInfo) encode(e *encoder) {
	e.beginObject()
	e.stringMember("name", i.Name)
	if i.Title.Present {
		e.stringMember("title", i.Title.Value)
	} else {
		e.key("title")
		e.null()
	}
	e.stringMember("version", i.Version)
	e.endObject()
}

// McpTool is one tool of a local MCP server, with the six fields the CLI
// keeps.
type McpTool struct {
	Name         string
	Title        Optional[string]
	Description  Optional[string]
	InputSchema  RawObject
	OutputSchema Optional[RawObject]
	Annotations  Optional[RawObject]
}

func mcpTool(d *decoder, ptr string, v *value) McpTool {
	o := d.object(ptr, v)
	t := McpTool{
		Name:         required(o, "name", stringWith(minLength(1))),
		Title:        optional(o, "title", anyString),
		Description:  optional(o, "description", anyString),
		InputSchema:  required(o, "inputSchema", rawObject),
		OutputSchema: optional(o, "outputSchema", rawObject),
		Annotations:  optional(o, "annotations", rawObject),
	}
	o.closed()
	return t
}

func (t McpTool) encode(e *encoder) {
	e.beginObject()
	e.stringMember("name", t.Name)
	optionalMember(e, "title", t.Title, writeString)
	optionalMember(e, "description", t.Description, writeString)
	e.key("inputSchema")
	e.raw(t.InputSchema)
	optionalMember(e, "outputSchema", t.OutputSchema, writeRaw)
	optionalMember(e, "annotations", t.Annotations, writeRaw)
	e.endObject()
}

// McpListError is why a server's tool list is unavailable.
type McpListError struct {
	Code    string // mcp_start_failed, mcp_list_failed or mcp_server_exited
	Message string
}

func mcpListError(d *decoder, ptr string, v *value) McpListError {
	o := d.object(ptr, v)
	l := McpListError{
		Code:    required(o, "code", enum("mcp_start_failed", "mcp_list_failed", "mcp_server_exited")),
		Message: required(o, "message", anyString),
	}
	o.open()
	return l
}

func (l McpListError) encode(e *encoder) {
	e.beginObject()
	e.stringMember("code", l.Code)
	e.stringMember("message", l.Message)
	e.endObject()
}

// McpTools reports the tool list of one local MCP server. A list with an
// error, or without server info, holds no tools.
type McpTools struct {
	ConnectionID string
	MessageID    string
	Server       string
	Seq          int64
	ServerInfo   Nullable[McpServerInfo]
	Tools        []McpTool
	Error        Nullable[McpListError]
}

func mcpTools(d *decoder, ptr string, v *value) McpTools {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	t := McpTools{
		ConnectionID: required(o, "connection_id", identity),
		MessageID:    required(o, "message_id", identity),
		Server:       required(o, "server", stringWith(minLength(1))),
		Seq:          required(o, "seq", positiveInteger),
	}
	failed := false
	if !d.failed() {
		errorValue, _ := o.v.get("error")
		failed = errorValue != nil && errorValue.kind == kindObject
	}
	if failed {
		t.ServerInfo = requiredNullable(o, "server_info", func(d *decoder, ptr string, v *value) McpServerInfo {
			d.fail(ptr, "expected null, got %s", v.kind)
			return McpServerInfo{}
		})
	} else {
		t.ServerInfo = requiredNullable(o, "server_info", mcpServerInfo)
	}
	maxTools := mcpToolsMaxTools
	if failed || t.ServerInfo.Null {
		maxTools = 0
	}
	t.Tools = required(o, "tools", arrayOf(mcpTool, 0, mcpToolsMaxTools))
	if !d.failed() && len(t.Tools) > maxTools {
		d.fail(pointer(ptr, "tools"), "more than %d items", maxTools)
	}
	t.Error = requiredNullable(o, "error", mcpListError)
	o.open()
	return t
}

func (t McpTools) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", t.ConnectionID)
	e.stringMember("message_id", t.MessageID)
	e.stringMember("server", t.Server)
	e.integerMember("seq", t.Seq)
	e.key("server_info")
	if t.ServerInfo.Present {
		t.ServerInfo.Value.encode(e)
	} else {
		e.null()
	}
	e.key("tools")
	list(e, t.Tools)
	e.key("error")
	if t.Error.Present {
		t.Error.Value.encode(e)
	} else {
		e.null()
	}
	e.endObject()
}

func checkMcpTools(t McpTools, v *value) error {
	if err := checkBodySize(v, McpToolsJSONBytes, "tool-list report over 4 MiB"); err != nil {
		return err
	}
	names := make(map[string]bool, len(t.Tools))
	for i, tool := range t.Tools {
		if names[tool.Name] {
			return invalid("/tools/"+strconv.Itoa(i)+"/name", "tool name twice")
		}
		names[tool.Name] = true
	}
	return nil
}

// CredentialRequest fetches the pending credential.
type CredentialRequest struct {
	ConnectionID string
	MessageID    string
	CredentialID string
}

func credentialRequest(d *decoder, ptr string, v *value) CredentialRequest {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	r := CredentialRequest{
		ConnectionID: required(o, "connection_id", identity),
		MessageID:    required(o, "message_id", identity),
		CredentialID: required(o, "credential_id", identity),
	}
	o.open()
	return r
}

func (r CredentialRequest) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", r.ConnectionID)
	e.stringMember("message_id", r.MessageID)
	e.stringMember("credential_id", r.CredentialID)
	e.endObject()
}

// CredentialResponse carries the pending credential's connection key.
type CredentialResponse struct {
	CredentialID  string
	ConnectionKey string
	ExpiresAt     string // RFC 3339 UTC
}

func credentialResponse(d *decoder, ptr string, v *value) CredentialResponse {
	o := d.object(ptr, v)
	r := CredentialResponse{
		CredentialID: required(o, "credential_id", identity),
		ConnectionKey: required(o, "connection_key", stringWith(func(s string) string {
			if !strings.HasPrefix(s, connectionKeyPrefix) || len(s) == len(connectionKeyPrefix) {
				return "not a connection key"
			}
			return noNUL(s)
		})),
		ExpiresAt: required(o, "expires_at", stringWith(utcTimestamp)),
	}
	o.open()
	return r
}

func (r CredentialResponse) encode(e *encoder) {
	e.beginObject()
	e.stringMember("credential_id", r.CredentialID)
	e.stringMember("connection_key", r.ConnectionKey)
	e.stringMember("expires_at", r.ExpiresAt)
	e.endObject()
}

// utcTimestamp is the pattern of an RFC 3339 UTC time:
// YYYY-MM-DDTHH:MM:SS, optional fractional seconds, then Z.
func utcTimestamp(s string) string {
	ok := len(s) >= 20 && s[len(s)-1] == 'Z'
	for i := 0; ok && i < 19; i++ {
		switch i {
		case 4, 7:
			ok = s[i] == '-'
		case 10:
			ok = s[i] == 'T'
		case 13, 16:
			ok = s[i] == ':'
		default:
			ok = '0' <= s[i] && s[i] <= '9'
		}
	}
	if ok && len(s) > 20 {
		ok = s[19] == '.' && isDigits(s[20:len(s)-1])
	}
	if !ok {
		return "not an RFC 3339 UTC time"
	}
	return ""
}

// checkCalendarTime applies the calendar to a time that passed utcTimestamp;
// a leap second is allowed.
func checkCalendarTime(ptr, s string) error {
	number := func(from, to int) int { n, _ := strconv.Atoi(s[from:to]); return n }
	year, month, day := number(0, 4), number(5, 7), number(8, 10)
	days := [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	if month >= 1 && month <= 12 {
		if month == 2 && (year%4 == 0 && year%100 != 0 || year%400 == 0) {
			days[1] = 29
		}
	}
	if month < 1 || month > 12 || day < 1 || day > days[month-1] || number(11, 13) > 23 || number(14, 16) > 59 || number(17, 19) > 60 {
		return invalid(ptr, "not a calendar time")
	}
	return nil
}

func checkCredentialResponse(r CredentialResponse, _ *value) error {
	return checkCalendarTime("/expires_at", r.ExpiresAt)
}

// GitTicketRequest asks for a git ticket for a binding being prepared.
type GitTicketRequest struct {
	ConnectionID string
	MessageID    string
	BindingID    string
}

func gitTicketRequest(d *decoder, ptr string, v *value) GitTicketRequest {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	r := GitTicketRequest{
		ConnectionID: required(o, "connection_id", identity),
		MessageID:    required(o, "message_id", identity),
		BindingID:    required(o, "binding_id", identity),
	}
	o.open()
	return r
}

func (r GitTicketRequest) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", r.ConnectionID)
	e.stringMember("message_id", r.MessageID)
	e.stringMember("binding_id", r.BindingID)
	e.endObject()
}

// GitTicketResponse carries a git ticket and the git proxy's origin.
type GitTicketResponse struct {
	GitProxyURL string // an https origin without path or trailing slash
	Ticket      string
}

func httpsOrigin(s string) string {
	host, ok := strings.CutPrefix(s, "https://")
	if !ok || host == "" || strings.ContainsAny(host, "/?#@") || strings.IndexFunc(host, isECMASpace) >= 0 {
		return "not an https origin"
	}
	return ""
}

func gitTicketResponse(d *decoder, ptr string, v *value) GitTicketResponse {
	o := d.object(ptr, v)
	r := GitTicketResponse{
		GitProxyURL: required(o, "git_proxy_url", stringWith(httpsOrigin)),
		Ticket:      required(o, "ticket", stringWith(minLength(1))),
	}
	o.open()
	return r
}

func (r GitTicketResponse) encode(e *encoder) {
	e.beginObject()
	e.stringMember("git_proxy_url", r.GitProxyURL)
	e.stringMember("ticket", r.Ticket)
	e.endObject()
}

// ErrorBody is the body of every /v1/client/* error response.
type ErrorBody struct {
	Code          ErrorCode
	Message       string
	RetryAfterMS  Optional[int64]    // previous_connection_active only
	MissingSHA256 Optional[[]string] // content_missing only
}

func errorBody(d *decoder, ptr string, v *value) ErrorBody {
	o := d.object(ptr, v)
	b := ErrorBody{
		Code:    ErrorCode(required(o, "code", errorCode)),
		Message: required(o, "message", anyString),
	}
	if b.Code == ErrorPreviousConnectionActive {
		b.RetryAfterMS = Some(required(o, "retry_after_ms", integer(0, retryAfterMaxMS)))
	} else {
		o.absent("retry_after_ms")
	}
	if b.Code == ErrorContentMissing {
		b.MissingSHA256 = Some(required(o, "missing_sha256", uniqueStrings(arrayOf(sha256Hex, 1, missingHashesMax))))
	} else {
		o.absent("missing_sha256")
	}
	o.open()
	return b
}

func (b ErrorBody) encode(e *encoder) {
	e.beginObject()
	e.stringMember("code", string(b.Code))
	e.stringMember("message", b.Message)
	optionalMember(e, "retry_after_ms", b.RetryAfterMS, writeInteger)
	optionalMember(e, "missing_sha256", b.MissingSHA256, writeStrings)
	e.endObject()
}

// checkErrorBody applies the relations of an error body: its message never
// holds a connection key, and its code is the one the client error table
// sends with status.
func checkErrorBody(status int) func(ErrorBody, *value) error {
	return func(b ErrorBody, _ *value) error {
		if strings.Contains(b.Message, connectionKeyPrefix) {
			return invalid("/message", "error message holds a connection key")
		}
		if ClientErrors[b.Code] != status {
			return invalid("/code", "code "+string(b.Code)+" is not sent with HTTP "+strconv.Itoa(status))
		}
		return nil
	}
}

// DecodeDownstream decodes an SSE envelope of any type.
func DecodeDownstream(data []byte) (Downstream, error) {
	return decodeChecked(data, connectionDownstream, checkDownstream)
}

func DecodeReady(data []byte) (Ready, error) {
	return decodeChecked(data, ready, checkReady)
}

func DecodeHeartbeat(data []byte) (Heartbeat, error) {
	return decodeChecked(data, heartbeat, bodySizeCheck[Heartbeat](DefaultMessageJSONBytes, "body over 64 KiB"))
}

// DecodeGrantAcknowledgement decodes the ACK of a ready or heartbeat.
func DecodeGrantAcknowledgement(data []byte) (GrantAcknowledgement, error) {
	return decodeChecked(data, grantAcknowledgement, messageSizeCheck[GrantAcknowledgement](DefaultMessageJSONBytes, "body over 64 KiB"))
}

func DecodeSessionStatus(data []byte) (SessionStatus, error) {
	return decodeChecked(data, sessionStatus, checkSessionStatus)
}

// DecodeMcpTools decodes a local MCP tool-list report.
func DecodeMcpTools(data []byte) (McpTools, error) {
	return decodeChecked(data, mcpTools, checkMcpTools)
}

func DecodeCredentialRequest(data []byte) (CredentialRequest, error) {
	return decodeChecked(data, credentialRequest, bodySizeCheck[CredentialRequest](DefaultMessageJSONBytes, "body over 64 KiB"))
}

func DecodeCredentialResponse(data []byte) (CredentialResponse, error) {
	return decodeChecked(data, credentialResponse, checkCredentialResponse)
}

func DecodeGitTicketRequest(data []byte) (GitTicketRequest, error) {
	return decodeChecked(data, gitTicketRequest, bodySizeCheck[GitTicketRequest](DefaultMessageJSONBytes, "body over 64 KiB"))
}

func DecodeGitTicketResponse(data []byte) (GitTicketResponse, error) {
	return decodeChecked(data, gitTicketResponse, nil)
}

// DecodeErrorBody decodes the body of an error response with its HTTP
// status, rejecting a code that the client error table does not send with it.
func DecodeErrorBody(status int, data []byte) (ErrorBody, error) {
	return decodeChecked(data, errorBody, checkErrorBody(status))
}
