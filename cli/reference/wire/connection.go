package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
)

// Connection message limits.
const (
	// MessageMaxBytes bounds every connection message not given a larger
	// limit: a request or response body, an ACK, a ready and a downstream
	// event other than a tool instruction.
	MessageMaxBytes = 65_536
	// ToolListMaxBytes bounds a local MCP tool-list report.
	ToolListMaxBytes  = 4_194_304
	LostChangePathMax = 100
	ToolsMax          = 2_048
)

// ConnectionKeyPrefix starts every connection key, which no message but a
// credential response may carry.
const ConnectionKeyPrefix = "tetral_ck_"

// EventsQuery is the query of GET /v1/client/events, as its members'
// strings.
type EventsQuery struct {
	ProtocolVersion      string      `json:"protocol_version" wire:"const=1"`
	InstanceID           ID          `json:"instance_id"`
	CredentialID         ID          `json:"credential_id"`
	PreviousConnectionID Opt[ID]     `json:"previous_connection_id,omitzero"`
	Resume               Opt[string] `json:"resume,omitzero"`
}

func (EventsQuery) MaxBytes() int { return MessageMaxBytes }

func (query *EventsQuery) check() error {
	if query.Resume.Set {
		return oneOf(query.Resume.Value, "true", "false")
	}
	return nil
}

type ConnectedPayload struct {
	Resumed bool `json:"resumed"`
}

type HeartbeatPayload struct {
	HeartbeatSeq int64 `json:"heartbeat_seq" wire:"min=1"`
}

type DriveBinding struct {
	DriveID SafeID `json:"drive_id"`
	Access  string `json:"access"`
}

func (binding *DriveBinding) check() error { return oneOf(binding.Access, "read_only", "read_write") }

type DriveExclude struct {
	DriveID ID            `json:"drive_id"`
	Paths   []ExcludePath `json:"paths"`
}

// Checkout is the branch or the commit a repository is cloned at.
type Checkout struct{ Ref checkoutRef }

type checkoutRef interface{ variant }

type BranchCheckout struct {
	Name NonEmpty `json:"name"`
	Sha  Absent   `json:"sha,omitzero"`
}

type CommitCheckout struct {
	Sha  CommitSha `json:"sha"`
	Name Absent    `json:"name,omitzero"`
}

func (*BranchCheckout) variantName() string { return "branch" }
func (*CommitCheckout) variantName() string { return "commit" }

func (checkout *Checkout) decodeTree(tree any) (err error) {
	checkout.Ref, err = decodeVariant[checkoutRef](tree, "type", &BranchCheckout{}, &CommitCheckout{})
	return err
}

func (checkout Checkout) MarshalJSON() ([]byte, error) { return encodeVariant("type", checkout.Ref) }

type GitIdentity struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Repository struct {
	URL NonEmpty `json:"url"`
	// Checkout is null for the repository's default branch.
	Checkout           Nullable[Checkout] `json:"checkout"`
	Path               AbsolutePath       `json:"path"`
	GitIdentity        GitIdentity        `json:"git_identity"`
	AuthorizationToken Absent             `json:"authorization_token,omitzero"`
}

type SessionPreparePayload struct {
	SessionID    SafeID         `json:"session_id"`
	BindingID    ID             `json:"binding_id"`
	Cwd          AbsolutePath   `json:"cwd"`
	CreateCwd    Opt[bool]      `json:"create_cwd,omitzero"`
	Drives       []DriveBinding `json:"drives"`
	DriveExclude []DriveExclude `json:"drive_exclude"`
	Repositories []Repository   `json:"repositories"`
	Ticket       Absent         `json:"ticket,omitzero"`
}

func (payload *SessionPreparePayload) check() error {
	if err := want(payload.CreateCwd.Value || len(payload.Repositories) == 0,
		"repositories only with create_cwd"); err != nil {
		return err
	}
	if err := rule(uniqueBy(payload.Drives, func(drive DriveBinding) SafeID { return drive.DriveID }),
		"drive_id twice in drives"); err != nil {
		return err
	}
	return rule(uniqueBy(payload.Repositories, func(repository Repository) AbsolutePath { return repository.Path }),
		"repository path twice")
}

type SessionClosePayload struct {
	SessionID SafeID `json:"session_id"`
	BindingID ID     `json:"binding_id"`
}

type SessionDrivesPayload struct {
	SessionID SafeID         `json:"session_id"`
	BindingID ID             `json:"binding_id"`
	Drives    []DriveBinding `json:"drives"`
}

func (payload *SessionDrivesPayload) check() error {
	return rule(uniqueBy(payload.Drives, func(drive DriveBinding) SafeID { return drive.DriveID }),
		"drive_id twice in drives")
}

type CredentialRenewPayload struct {
	CredentialID  ID     `json:"credential_id"`
	ConnectionKey Absent `json:"connection_key,omitzero"`
}

// DisconnectPayload ends the event stream, which may resume, or with scope
// `connection` the connection itself.
type DisconnectPayload struct {
	Scope     string           `json:"scope"`
	Retryable Maybe[bool]      `json:"retryable,omitzero"`
	Code      Maybe[ErrorCode] `json:"code,omitzero"`
}

func (payload *DisconnectPayload) check() error {
	switch payload.Scope {
	case "stream":
		return want(!payload.Retryable.Present && !payload.Code.Present, "a stream disconnect without retryable or code")
	case "connection":
		return want(payload.Retryable.Valid && payload.Code.Valid, "a connection disconnect with retryable and code")
	}
	return oneOf(payload.Scope, "stream", "connection")
}

type DriveChangedPayload struct {
	DriveID ID    `json:"drive_id"`
	Seq     int64 `json:"seq" wire:"min=1"`
}

// Downstream is one event of the stream: Engine's envelope around a payload
// of the type it names.
type Downstream struct {
	ConnectionID ID
	MessageID    ID
	Type         string
	// Payload is a pointer to the payload struct of Type.
	Payload any
	// Traceparent is the W3C traceparent of the work the event belongs to,
	// as sent.
	Traceparent Opt[string]
}

type downstreamFields struct {
	ProtocolVersion int64       `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID          `json:"connection_id"`
	MessageID       ID          `json:"message_id"`
	Type            string      `json:"type"`
	Payload         any         `json:"payload"`
	Traceparent     Opt[string] `json:"traceparent,omitzero"`
}

func newPayload(kind string) (any, bool) {
	payloads := map[string]func() any{
		"connected":        func() any { return &ConnectedPayload{} },
		"heartbeat":        func() any { return &HeartbeatPayload{} },
		"session.prepare":  func() any { return &SessionPreparePayload{} },
		"session.close":    func() any { return &SessionClosePayload{} },
		"session.drives":   func() any { return &SessionDrivesPayload{} },
		"credential.renew": func() any { return &CredentialRenewPayload{} },
		"disconnect":       func() any { return &DisconnectPayload{} },
		"drive.changed":    func() any { return &DriveChangedPayload{} },
		"tool.execute":     func() any { return &ExecutePayload{} },
		"tool.stdin":       func() any { return &StdinPayload{} },
		"tool.cancel":      func() any { return &CancelPayload{} },
		"mcp.call":         func() any { return &MCPCallPayload{} },
	}
	newPayloadOf, known := payloads[kind]
	if !known {
		return nil, false
	}
	return newPayloadOf(), true
}

func (event *Downstream) decodeTree(tree any) error {
	var fields downstreamFields
	if err := decodeAt(&fields, tree, ""); err != nil {
		return err
	}
	payload, known := newPayload(fields.Type)
	if !known {
		return fmt.Errorf("/type: unknown type %q", fields.Type)
	}
	if _, isObject := fields.Payload.(map[string]any); !isObject {
		return errors.New("/payload: want an object")
	}
	if err := decodeAt(payload, fields.Payload, "/payload"); err != nil {
		return err
	}
	*event = Downstream{
		ConnectionID: fields.ConnectionID, MessageID: fields.MessageID, Type: fields.Type,
		Payload: payload, Traceparent: fields.Traceparent,
	}
	return nil
}

func (event Downstream) MarshalJSON() ([]byte, error) {
	return json.Marshal(downstreamFields{
		ProtocolVersion: ProtocolVersion, ConnectionID: event.ConnectionID, MessageID: event.MessageID,
		Type: event.Type, Payload: event.Payload, Traceparent: event.Traceparent,
	})
}

func (Downstream) MaxBytes() int { return ExecuteSSEJSONBytes }

func (event *Downstream) checkReceived(received Received) error {
	if limit, isTool := toolInstructionLimit(event.Type); isTool {
		return rule(received.Bytes <= limit, "tool instruction over its limit")
	}
	if err := rule(received.Bytes <= MessageMaxBytes, "event over 65,536 bytes"); err != nil {
		return err
	}
	if event.Type == "credential.renew" {
		payload := received.Tree.(map[string]any)["payload"]
		return rule(!holdsKey(payload), "credential.renew carries a connection key")
	}
	return nil
}

// holdsKey says whether any string in tree, a member name or a string
// value, holds a connection key.
func holdsKey(tree any) bool {
	switch tree := tree.(type) {
	case string:
		return strings.Contains(tree, ConnectionKeyPrefix)
	case []any:
		return slices.ContainsFunc(tree, holdsKey)
	case map[string]any:
		for name, member := range tree {
			if strings.Contains(name, ConnectionKeyPrefix) || holdsKey(member) {
				return true
			}
		}
	}
	return false
}

// ToolDownstream is a downstream event of tool execution: a tool
// instruction.
type ToolDownstream struct{ Downstream }

func (event *ToolDownstream) decodeTree(tree any) error {
	if err := event.Downstream.decodeTree(tree); err != nil {
		return err
	}
	_, isTool := toolInstructionLimit(event.Type)
	return want(isTool, "a tool instruction")
}

func (event ToolDownstream) MarshalJSON() ([]byte, error) { return event.Downstream.MarshalJSON() }

type DriveCapability struct {
	Mount bool `json:"mount"`
}

type Capabilities struct {
	SessionPreparation bool                         `json:"session_preparation" wire:"const=true"`
	Drives             DriveCapability              `json:"drives"`
	ToolExecution      Opt[ToolExecutionCapability] `json:"tool_execution,omitzero"`
	LocalMCP           Opt[LocalMCPCapability]      `json:"local_mcp,omitzero"`
}

func (capabilities *Capabilities) check() error {
	if err := rule(!capabilities.LocalMCP.Set || capabilities.ToolExecution.Set,
		"local_mcp without tool_execution"); err != nil {
		return err
	}
	if !capabilities.ToolExecution.Set {
		return nil
	}
	return capabilities.ToolExecution.Value.checkDeclarations(capabilities.LocalMCP.Set)
}

// LostChange is one drive's changes that a machine abandoned, for the
// change it was the answer to.
type LostChange struct {
	RequestMessageID ID          `json:"request_message_id"`
	DriveID          ID          `json:"drive_id"`
	Paths            []DrivePath `json:"paths" wire:"max=100,unique"`
	PathsTotal       int64       `json:"paths_total" wire:"min=1"`
}

func (entry *LostChange) check() error {
	if err := rule(slices.IsSortedFunc(entry.Paths, func(a, b DrivePath) int { return strings.Compare(string(a), string(b)) }),
		"lost_changes paths out of UTF-8 byte order"); err != nil {
		return err
	}
	return rule(entry.PathsTotal >= int64(len(entry.Paths)), "paths_total below listed paths")
}

// checkReportOrder holds a Session report's lost_changes to its order: one
// entry per drive in drive_id order, paths fitted entry after entry until one
// entry is cut.
func checkReportOrder(entries []LostChange) error {
	cut := false
	for index, entry := range entries {
		if index > 0 && entries[index-1].DriveID >= entry.DriveID {
			return rule(false, "lost_changes entries out of drive_id order")
		}
		if cut && len(entry.Paths) > 0 {
			return rule(false, "paths listed after a cut entry")
		}
		cut = cut || int64(len(entry.Paths)) < min(entry.PathsTotal, LostChangePathMax)
	}
	return nil
}

// checkKeptOrder holds a ready's lost_changes to the order of the reports it
// repeats: within each report, entries in drive_id order, none twice.
func checkKeptOrder(entries []LostChange) error {
	last := map[ID]ID{}
	for _, entry := range entries {
		previous, seen := last[entry.RequestMessageID]
		if seen && previous >= entry.DriveID {
			return rule(false, "kept lost_changes entry repeated or reordered")
		}
		last[entry.RequestMessageID] = entry.DriveID
	}
	return nil
}

type Startup struct {
	ConfigMs  int64 `json:"config_ms" wire:"min=0,max=2147483647"`
	ConnectMs int64 `json:"connect_ms" wire:"min=0,max=2147483647"`
	ReadyMs   int64 `json:"ready_ms" wire:"min=0,max=2147483647"`
}

// Ready is the message that opens or resumes a connection's report side.
type Ready struct {
	ProtocolVersion int64             `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID                `json:"connection_id"`
	MessageID       ID                `json:"message_id"`
	OS              string            `json:"os"`
	Arch            string            `json:"arch"`
	CLIVersion      SemVer            `json:"cli_version"`
	Capabilities    Capabilities      `json:"capabilities"`
	LostChanges     Opt[[]LostChange] `json:"lost_changes,omitzero" wire:"min=1"`
	Startup         Opt[Startup]      `json:"startup,omitzero"`
}

func (Ready) MaxBytes() int { return MessageMaxBytes }

func (ready *Ready) check() error {
	if err := oneOf(ready.OS, "linux", "macos"); err != nil {
		return fmt.Errorf("/os: %w", err)
	}
	if err := oneOf(ready.Arch, "x86_64", "aarch64"); err != nil {
		return fmt.Errorf("/arch: %w", err)
	}
	return checkKeptOrder(ready.LostChanges.Value)
}

// SemVer is a semantic version: MAJOR.MINOR.PATCH with an optional
// pre-release and build part.
type SemVer string

func (version *SemVer) check() error {
	return want(validSemVer(string(*version)), "a semantic version")
}

func validSemVer(text string) bool {
	rest, build, hasBuild := strings.Cut(text, "+")
	core, pre, hasPre := strings.Cut(rest, "-")
	numeric := func(part string) bool {
		return part != "" && strings.Trim(part, "0123456789") == "" && (part == "0" || part[0] != '0')
	}
	alphanumeric := func(part string) bool {
		return part != "" && strings.Trim(part, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-") == ""
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 || !numeric(parts[0]) || !numeric(parts[1]) || !numeric(parts[2]) {
		return false
	}
	if hasPre {
		for part := range strings.SplitSeq(pre, ".") {
			if !alphanumeric(part) || (strings.Trim(part, "0123456789") == "" && !numeric(part)) {
				return false
			}
		}
	}
	if hasBuild {
		for part := range strings.SplitSeq(build, ".") {
			if !alphanumeric(part) {
				return false
			}
		}
	}
	return true
}

type Heartbeat struct {
	ProtocolVersion  int64 `json:"protocol_version" wire:"const=1"`
	ConnectionID     ID    `json:"connection_id"`
	MessageID        ID    `json:"message_id"`
	RequestMessageID ID    `json:"request_message_id"`
	HeartbeatSeq     int64 `json:"heartbeat_seq" wire:"min=1"`
}

func (Heartbeat) MaxBytes() int { return MessageMaxBytes }

// GrantAcknowledgement is the ACK of a ready or heartbeat: the lease Engine
// grants, from the message's first send time.
type GrantAcknowledgement struct {
	ConnectionID ID    `json:"connection_id"`
	MessageID    ID    `json:"message_id"`
	Accepted     bool  `json:"accepted" wire:"const=true"`
	ValidForMs   int64 `json:"valid_for_ms" wire:"min=1,max=45000"`
}

func (GrantAcknowledgement) MaxBytes() int { return MessageMaxBytes }

type PreparationError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (preparationError *PreparationError) check() error {
	return oneOf(preparationError.Code, "cwd_not_found", "cwd_not_directory", "cwd_forbidden", "cwd_inaccessible",
		"cwd_check_timeout", "invalid_input", "stale_connection", "binding_conflict", "cleanup_pending",
		"resource_exhausted", "repository_path_exists", "repository_path_forbidden")
}

type RepositoryFailure struct {
	URL     NonEmpty     `json:"url"`
	Path    AbsolutePath `json:"path"`
	Cause   string       `json:"cause"`
	Message string       `json:"message"`
}

func (failure *RepositoryFailure) check() error {
	return oneOf(failure.Cause, "clone", "not_found", "authentication", "forbidden", "checkout")
}

// SessionStatus is a Session report: `prepared` or `failed` answers a
// session.prepare, `closed` a session.close and `applied` a
// session.drives, each with the members its status allows.
type SessionStatus struct {
	ProtocolVersion    int64                    `json:"protocol_version" wire:"const=1"`
	ConnectionID       ID                       `json:"connection_id"`
	MessageID          ID                       `json:"message_id"`
	RequestMessageID   ID                       `json:"request_message_id"`
	SessionID          ID                       `json:"session_id"`
	BindingID          ID                       `json:"binding_id"`
	Status             string                   `json:"status"`
	Cwd                Opt[AbsolutePath]        `json:"cwd,omitzero"`
	DriveMountFailures Opt[[]DriveMountFailure] `json:"drive_mount_failures,omitzero" wire:"min=1"`
	RepositoryFailures Opt[[]RepositoryFailure] `json:"repository_failures,omitzero" wire:"min=1"`
	Error              Opt[PreparationError]    `json:"error,omitzero"`
	LostChanges        Opt[[]LostChange]        `json:"lost_changes,omitzero" wire:"min=1"`
}

func (SessionStatus) MaxBytes() int { return MessageMaxBytes }

func (report *SessionStatus) check() error {
	allowed := map[string][]string{
		"prepared": {"cwd", "drive_mount_failures", "repository_failures"},
		"failed":   {"error"},
		"closed":   {"lost_changes"},
		"applied":  {"drive_mount_failures", "lost_changes"},
	}
	members, known := allowed[report.Status]
	if !known {
		return oneOf(report.Status, "prepared", "failed", "closed", "applied")
	}
	present := map[string]bool{
		"cwd": report.Cwd.Set, "drive_mount_failures": report.DriveMountFailures.Set,
		"repository_failures": report.RepositoryFailures.Set, "error": report.Error.Set,
		"lost_changes": report.LostChanges.Set,
	}
	for member, set := range present {
		if set && !slices.Contains(members, member) {
			return fmt.Errorf("%s is not allowed with status %s", member, report.Status)
		}
	}
	if report.Status == "prepared" && !report.Cwd.Set {
		return errors.New("a prepared report without cwd")
	}
	if report.Status == "failed" && !report.Error.Set {
		return errors.New("a failed report without error")
	}
	if err := rule(uniqueBy(report.DriveMountFailures.Value, func(failure DriveMountFailure) ID { return failure.DriveID }),
		"drive listed twice in drive_mount_failures"); err != nil {
		return err
	}
	if err := rule(uniqueBy(report.RepositoryFailures.Value, func(failure RepositoryFailure) AbsolutePath { return failure.Path }),
		"repository listed twice in repository_failures"); err != nil {
		return err
	}
	return checkReportOrder(report.LostChanges.Value)
}

type MCPServerInfo struct {
	Name    string           `json:"name"`
	Title   Nullable[string] `json:"title"`
	Version string           `json:"version"`
}

type MCPTool struct {
	_            Closed
	Name         NonEmpty    `json:"name"`
	Title        Opt[string] `json:"title,omitzero"`
	Description  Opt[string] `json:"description,omitzero"`
	InputSchema  Object      `json:"inputSchema"`
	OutputSchema Opt[Object] `json:"outputSchema,omitzero"`
	Annotations  Opt[Object] `json:"annotations,omitzero"`
}

type MCPListError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (listError *MCPListError) check() error {
	return oneOf(listError.Code, "mcp_start_failed", "mcp_list_failed", "mcp_server_exited")
}

// MCPTools is the tool list of one local MCP server, numbered per server by
// Seq.
type MCPTools struct {
	ProtocolVersion int64                   `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID                      `json:"connection_id"`
	MessageID       ID                      `json:"message_id"`
	Server          NonEmpty                `json:"server"`
	Seq             int64                   `json:"seq" wire:"min=1"`
	ServerInfo      Nullable[MCPServerInfo] `json:"server_info"`
	Tools           []MCPTool               `json:"tools" wire:"max=2048"`
	Error           Nullable[MCPListError]  `json:"error"`
}

func (MCPTools) MaxBytes() int { return ToolListMaxBytes }

func (list *MCPTools) check() error {
	if err := want(!list.Error.Valid || !list.ServerInfo.Valid, "no server_info with a list error"); err != nil {
		return err
	}
	if err := want((!list.Error.Valid && list.ServerInfo.Valid) || len(list.Tools) == 0, "tools only with a server"); err != nil {
		return err
	}
	return rule(uniqueBy(list.Tools, func(tool MCPTool) NonEmpty { return tool.Name }), "tool name twice")
}

type CredentialRequest struct {
	ProtocolVersion int64 `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID    `json:"connection_id"`
	MessageID       ID    `json:"message_id"`
	CredentialID    ID    `json:"credential_id"`
}

func (CredentialRequest) MaxBytes() int { return MessageMaxBytes }

// ConnectionKey is `tetral_ck_` and the key's text. It formats as
// `<redacted>`, so that no log shows it.
type ConnectionKey string

func (key *ConnectionKey) check() error {
	text, hasPrefix := strings.CutPrefix(string(*key), ConnectionKeyPrefix)
	return want(hasPrefix && text != "" && !strings.ContainsRune(text, 0), "a connection key")
}

func (ConnectionKey) String() string { return "<redacted>" }

// GoString keeps the key out of %#v as well.
func (ConnectionKey) GoString() string { return "<redacted>" }

type CredentialResponse struct {
	CredentialID  ID            `json:"credential_id"`
	ConnectionKey ConnectionKey `json:"connection_key"`
	ExpiresAt     Time          `json:"expires_at"`
}

func (CredentialResponse) MaxBytes() int { return MessageMaxBytes }

type GitTicketRequest struct {
	ProtocolVersion int64 `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID    `json:"connection_id"`
	MessageID       ID    `json:"message_id"`
	BindingID       ID    `json:"binding_id"`
}

func (GitTicketRequest) MaxBytes() int { return MessageMaxBytes }

// HTTPSOrigin is an https origin with no path, query, fragment or user
// information.
type HTTPSOrigin string

func (origin *HTTPSOrigin) check() error {
	host, isHTTPS := strings.CutPrefix(string(*origin), "https://")
	valid := isHTTPS && host != "" && strings.IndexFunc(host, func(character rune) bool {
		return strings.ContainsRune("/?#@", character) || unicode.IsSpace(character)
	}) < 0
	return want(valid, "an https origin")
}

type GitTicketResponse struct {
	GitProxyURL HTTPSOrigin `json:"git_proxy_url"`
	Ticket      NonEmpty    `json:"ticket"`
}

func (GitTicketResponse) MaxBytes() int { return MessageMaxBytes }

// uniqueBy says whether no two items share a key.
func uniqueBy[T any, K comparable](items []T, key func(T) K) bool {
	seen := make(map[K]bool, len(items))
	for _, item := range items {
		if seen[key(item)] {
			return false
		}
		seen[key(item)] = true
	}
	return true
}
