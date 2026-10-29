package protocol

import (
	"encoding/base64"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
)

// Byte limits of tool-execution-v1, each counted on the compact encoding.
const (
	MetadataJSONBytes            = 65536
	ToolInputJSONBytes           = 4194304
	ExecuteSSEJSONBytes          = 4259840
	StdinUTF8Bytes               = 32768
	StdinSSEJSONBytes            = 262144
	DefaultMessageJSONBytes      = 65536
	HelperResultJSONBytes        = 262144
	DriveSyncJSONBytes           = 65536
	OrdinaryToolStatusJSONBytes  = 327680
	DriveSyncReportJSONBytes     = 131072
	ViewImageDecodedBytes        = 10485760
	MediaToolStatusJSONBytes     = 14680064
	mcpCallSSEJSONBytes          = 131072
	mcpArgumentsJSONBytes        = 65536
	driveSyncListedEntries       = 100
	sandboxPolicySchemaVersion   = 1
	toolExecutionSchemaVersion   = 1
	helperEnvelopeSchemaVersion  = 1
	connectionProtocolVersionInt = 1
)

// Tool names of tool.execute, the controls and the result envelope.
const (
	ToolExecCommand = "exec_command"
	ToolEdit        = "edit"
	ToolViewImage   = "view_image"
	ToolMCP         = "mcp"
	ToolStdin       = "stdin"
	ToolCancel      = "cancel"
)

// VisibleLimits lowers the text a reply shows per stream.
type VisibleLimits struct {
	VisibleBytes Nullable[int64]
	VisibleLines Nullable[int64]
}

func visibleLimits(d *decoder, ptr string, v *value) VisibleLimits {
	o := d.object(ptr, v)
	l := VisibleLimits{
		VisibleBytes: nullable(o, "visible_bytes", anyInteger),
		VisibleLines: nullable(o, "visible_lines", anyInteger),
	}
	o.open()
	return l
}

func (l VisibleLimits) encode(e *encoder) {
	e.beginObject()
	nullableMember(e, "visible_bytes", l.VisibleBytes, writeInteger)
	nullableMember(e, "visible_lines", l.VisibleLines, writeInteger)
	e.endObject()
}

// ToolInput is the input of a tool.execute: an ExecInput, an EditInput or a
// ViewImageInput, each of which names its tool.
type ToolInput interface {
	encodable
	Tool() string
}

// ExecInput is the input of exec_command.
type ExecInput struct {
	Cmd            string
	Cwd            Nullable[string]
	Env            Nullable[map[string]string]
	WaitMS         Nullable[int64]
	OnWaitExpiry   Nullable[string]
	TaskLifetimeMS Nullable[int64]
	TaskID         Nullable[string]
}

func (ExecInput) Tool() string { return ToolExecCommand }

// nonBlank is the pattern \S.
func nonBlank(s string) string {
	if strings.IndexFunc(s, func(r rune) bool { return !isECMASpace(r) }) < 0 {
		return "holds only whitespace"
	}
	return ""
}

func execInput(d *decoder, ptr string, v *value) ExecInput {
	o := d.object(ptr, v)
	in := ExecInput{
		Cmd:            required(o, "cmd", stringWith(nonBlank)),
		Cwd:            nullable(o, "cwd", anyString),
		Env:            nullable(o, "env", stringMap),
		WaitMS:         nullable(o, "wait_ms", anyInteger),
		OnWaitExpiry:   nullable(o, "on_wait_expiry", enum("", "kill", "detach")),
		TaskLifetimeMS: nullable(o, "task_lifetime_ms", anyInteger),
		TaskID:         nullable(o, "task_id", identity),
	}
	o.open()
	return in
}

func (in ExecInput) encode(e *encoder) {
	e.beginObject()
	e.stringMember("cmd", in.Cmd)
	nullableMember(e, "cwd", in.Cwd, writeString)
	nullableMember(e, "env", in.Env, writeStringMap)
	nullableMember(e, "wait_ms", in.WaitMS, writeInteger)
	nullableMember(e, "on_wait_expiry", in.OnWaitExpiry, writeString)
	nullableMember(e, "task_lifetime_ms", in.TaskLifetimeMS, writeInteger)
	nullableMember(e, "task_id", in.TaskID, writeString)
	e.endObject()
}

// EditInput is the input of edit.
type EditInput struct {
	Path       string
	OldString  string
	NewString  Nullable[string]
	ReplaceAll Nullable[bool]
}

func (EditInput) Tool() string { return ToolEdit }

func editInput(d *decoder, ptr string, v *value) EditInput {
	o := d.object(ptr, v)
	in := EditInput{
		Path:       required(o, "path", identity),
		OldString:  required(o, "old_string", stringWith(minLength(1))),
		NewString:  nullable(o, "new_string", anyString),
		ReplaceAll: nullable(o, "replace_all", boolean),
	}
	o.open()
	return in
}

func (in EditInput) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", in.Path)
	e.stringMember("old_string", in.OldString)
	nullableMember(e, "new_string", in.NewString, writeString)
	nullableMember(e, "replace_all", in.ReplaceAll, writeBool)
	e.endObject()
}

// ViewImageInput is the input of view_image.
type ViewImageInput struct {
	Path string
}

func (ViewImageInput) Tool() string { return ToolViewImage }

func viewImageInput(d *decoder, ptr string, v *value) ViewImageInput {
	o := d.object(ptr, v)
	in := ViewImageInput{Path: required(o, "path", identity)}
	o.open()
	return in
}

func (in ViewImageInput) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", in.Path)
	e.endObject()
}

// SandboxPolicy is the permission of one execution. It and every object in it
// are closed: an unknown member rejects the whole message, so a misspelled
// member never creates a grant.
type SandboxPolicy struct {
	Filesystem FilesystemPolicy
	Network    NetworkPolicy
}

type FilesystemPolicy struct {
	Rules []FilesystemRule
}

type FilesystemRule struct {
	Path   string // absolute
	Access string // deny, read or read_write
}

// NetworkPolicy is disabled or direct network, or managed network with its
// rules.
type NetworkPolicy struct {
	Mode  string // disabled, direct or managed
	Rules Optional[ManagedNetworkRules]
}

type ManagedNetworkRules struct {
	Allow  []NetworkRule
	Deny   []NetworkRule
	Listen []NetworkListen
}

type NetworkRule struct {
	Destination NetworkDestination
	Transport   string // tcp or udp
	Ports       []PortRange
}

// NetworkDestination is a host name, possibly with a "*." wildcard, or a CIDR.
type NetworkDestination struct {
	Kind  string // host or cidr
	Value string
}

type NetworkListen struct {
	Address   string
	Transport string // tcp or udp
	Ports     []PortRange
}

type PortRange struct {
	From int64
	To   int64
}

func sandboxPolicy(d *decoder, ptr string, v *value) SandboxPolicy {
	o := d.object(ptr, v)
	required(o, "schema_version", constInteger(sandboxPolicySchemaVersion))
	p := SandboxPolicy{
		Filesystem: required(o, "filesystem", filesystemPolicy),
		Network:    required(o, "network", networkPolicy),
	}
	o.closed()
	return p
}

func filesystemPolicy(d *decoder, ptr string, v *value) FilesystemPolicy {
	o := d.object(ptr, v)
	p := FilesystemPolicy{Rules: required(o, "rules", arrayOf(filesystemRule, 0, -1))}
	o.closed()
	return p
}

func filesystemRule(d *decoder, ptr string, v *value) FilesystemRule {
	o := d.object(ptr, v)
	r := FilesystemRule{
		Path:   required(o, "path", stringWith(minLength(1), absolutePath)),
		Access: required(o, "access", enum("deny", "read", "read_write")),
	}
	o.closed()
	return r
}

func networkPolicy(d *decoder, ptr string, v *value) NetworkPolicy {
	o := d.object(ptr, v)
	p := NetworkPolicy{Mode: required(o, "mode", enum("disabled", "direct", "managed"))}
	if p.Mode == "managed" {
		p.Rules = Some(required(o, "rules", managedNetworkRules))
	}
	o.closed()
	return p
}

func managedNetworkRules(d *decoder, ptr string, v *value) ManagedNetworkRules {
	o := d.object(ptr, v)
	r := ManagedNetworkRules{
		Allow:  required(o, "allow", arrayOf(networkRule, 0, -1)),
		Deny:   required(o, "deny", arrayOf(networkRule, 0, -1)),
		Listen: required(o, "listen", arrayOf(networkListen, 0, -1)),
	}
	o.closed()
	return r
}

func networkRule(d *decoder, ptr string, v *value) NetworkRule {
	o := d.object(ptr, v)
	r := NetworkRule{
		Destination: required(o, "destination", networkDestination),
		Transport:   required(o, "transport", enum("tcp", "udp")),
		Ports:       required(o, "ports", arrayOf(portRange, 1, -1)),
	}
	o.closed()
	return r
}

func networkDestination(d *decoder, ptr string, v *value) NetworkDestination {
	o := d.object(ptr, v)
	n := NetworkDestination{
		Kind:  required(o, "kind", enum("host", "cidr")),
		Value: required(o, "value", stringWith(minLength(1))),
	}
	o.closed()
	return n
}

func networkListen(d *decoder, ptr string, v *value) NetworkListen {
	o := d.object(ptr, v)
	l := NetworkListen{
		Address:   required(o, "address", stringWith(minLength(1))),
		Transport: required(o, "transport", enum("tcp", "udp")),
		Ports:     required(o, "ports", arrayOf(portRange, 1, -1)),
	}
	o.closed()
	return l
}

var port = integer(1, 65535)

func portRange(d *decoder, ptr string, v *value) PortRange {
	o := d.object(ptr, v)
	r := PortRange{From: required(o, "from", port), To: required(o, "to", port)}
	o.closed()
	return r
}

func (p SandboxPolicy) encode(e *encoder) {
	e.beginObject()
	e.integerMember("schema_version", sandboxPolicySchemaVersion)
	e.valueMember("filesystem", p.Filesystem)
	e.valueMember("network", p.Network)
	e.endObject()
}

func (p FilesystemPolicy) encode(e *encoder) {
	e.beginObject()
	e.key("rules")
	list(e, p.Rules)
	e.endObject()
}

func (r FilesystemRule) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", r.Path)
	e.stringMember("access", r.Access)
	e.endObject()
}

func (p NetworkPolicy) encode(e *encoder) {
	e.beginObject()
	e.stringMember("mode", p.Mode)
	optionalMember(e, "rules", p.Rules, writeValue)
	e.endObject()
}

func (r ManagedNetworkRules) encode(e *encoder) {
	e.beginObject()
	e.key("allow")
	list(e, r.Allow)
	e.key("deny")
	list(e, r.Deny)
	e.key("listen")
	list(e, r.Listen)
	e.endObject()
}

func (r NetworkRule) encode(e *encoder) {
	e.beginObject()
	e.valueMember("destination", r.Destination)
	e.stringMember("transport", r.Transport)
	e.key("ports")
	list(e, r.Ports)
	e.endObject()
}

func (n NetworkDestination) encode(e *encoder) {
	e.beginObject()
	e.stringMember("kind", n.Kind)
	e.stringMember("value", n.Value)
	e.endObject()
}

func (l NetworkListen) encode(e *encoder) {
	e.beginObject()
	e.stringMember("address", l.Address)
	e.stringMember("transport", l.Transport)
	e.key("ports")
	list(e, l.Ports)
	e.endObject()
}

func (r PortRange) encode(e *encoder) {
	e.beginObject()
	e.integerMember("from", r.From)
	e.integerMember("to", r.To)
	e.endObject()
}

// checkSandboxPolicy applies the relations beyond the schema: one access per
// filesystem path, ordered port ranges, IP listen addresses, canonical CIDRs
// and host names that are DNS names, not URLs or IP addresses.
func checkSandboxPolicy(ptr string, p SandboxPolicy) error {
	seen := make(map[string]string, len(p.Filesystem.Rules))
	for i, rule := range p.Filesystem.Rules {
		if access, ok := seen[rule.Path]; ok && access != rule.Access {
			return invalid(pointer(ptr, "filesystem/rules/"+strconv.Itoa(i)), "conflicting filesystem rules")
		}
		seen[rule.Path] = rule.Access
	}
	rules, ok := p.Network.Rules.Value, p.Network.Rules.Set
	if !ok {
		return nil
	}
	checkPorts := func(at string, ports []PortRange) error {
		for i, r := range ports {
			if r.From > r.To {
				return invalid(at+"/ports/"+strconv.Itoa(i), "reversed port range")
			}
		}
		return nil
	}
	for _, group := range []struct {
		name  string
		rules []NetworkRule
	}{{"allow", rules.Allow}, {"deny", rules.Deny}} {
		for i, rule := range group.rules {
			at := ptr + "/network/rules/" + group.name + "/" + strconv.Itoa(i)
			if err := checkPorts(at, rule.Ports); err != nil {
				return err
			}
			if reason := checkDestination(rule.Destination); reason != "" {
				return invalid(at+"/destination/value", reason)
			}
		}
	}
	for i, listen := range rules.Listen {
		at := ptr + "/network/rules/listen/" + strconv.Itoa(i)
		if err := checkPorts(at, listen.Ports); err != nil {
			return err
		}
		if addr, err := netip.ParseAddr(listen.Address); err != nil || addr.Zone() != "" {
			return invalid(at+"/address", "not an IP address without a zone")
		}
	}
	return nil
}

func checkDestination(dest NetworkDestination) string {
	if dest.Kind == "cidr" {
		// netip reads IPv4 in dotted decimal without leading zeros, IPv6 in
		// RFC 4291 text and a decimal prefix length without leading zeros,
		// and rejects a zone and the netmask form.
		prefix, err := netip.ParsePrefix(dest.Value)
		if err != nil {
			return "not a CIDR"
		}
		if prefix.Masked() != prefix {
			return "CIDR has host bits set"
		}
		return ""
	}
	if dest.Value == "*" {
		return ""
	}
	if strings.ContainsAny(dest.Value, "/:@?#") {
		return "not a host name"
	}
	// idna.Lookup is UTS #46 ToASCII with nontransitional processing,
	// UseSTD3ASCIIRules, CheckHyphens, CheckBidi and CheckJoiners; it maps
	// U+3002, U+FF0E and U+FF61 to '.' and rejects '*'.
	host, err := idna.Lookup.ToASCII(strings.TrimPrefix(dest.Value, "*."))
	if err != nil {
		return "not a host name"
	}
	host = strings.TrimSuffix(host, ".")
	if len(host) > 253 {
		return "invalid DNS length"
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") ||
			strings.IndexFunc(label, func(r rune) bool { return !dnsLabelRune(r) }) >= 0 {
			return "invalid DNS labels"
		}
	}
	if addr, err := netip.ParseAddr(host); err == nil && addr.Is4() {
		return "use CIDR for IP destinations"
	}
	return ""
}

func dnsLabelRune(r rune) bool {
	return r == '-' || 'a' <= r && r <= 'z' || '0' <= r && r <= '9'
}

// DownstreamPayload is the payload of one SSE envelope; its type names the
// envelope's type.
type DownstreamPayload interface {
	encodable
	MessageType() string
}

// Downstream is one SSE envelope from Engine to the CLI.
type Downstream struct {
	ConnectionID string
	MessageID    string
	Payload      DownstreamPayload
	Traceparent  Optional[string]
}

func (m Downstream) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", m.ConnectionID)
	e.stringMember("message_id", m.MessageID)
	e.stringMember("type", m.Payload.MessageType())
	e.valueMember("payload", m.Payload)
	optionalMember(e, "traceparent", m.Traceparent, writeString)
	e.endObject()
}

// downstream reads an envelope whose type has a payload decoder in payloads.
func downstream(payloads map[string]decodeFunc[DownstreamPayload]) decodeFunc[Downstream] {
	types := make([]string, 0, len(payloads))
	for t := range payloads {
		types = append(types, t)
	}
	slices.Sort(types)
	kind := enum(types...)
	return func(d *decoder, ptr string, v *value) Downstream {
		o := d.object(ptr, v)
		required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
		m := Downstream{
			ConnectionID: required(o, "connection_id", identity),
			MessageID:    required(o, "message_id", identity),
		}
		t := required(o, "type", kind)
		if !d.failed() {
			m.Payload = required(o, "payload", payloads[t])
		}
		m.Traceparent = optional(o, "traceparent", anyString)
		o.open()
		return m
	}
}

func payloadOf[T DownstreamPayload](f decodeFunc[T]) decodeFunc[DownstreamPayload] {
	return func(d *decoder, ptr string, v *value) DownstreamPayload {
		p := f(d, ptr, v)
		if d.failed() {
			return nil
		}
		return p
	}
}

// toolPayloads are the instructions of tool-execution-v1.
var toolPayloads = map[string]decodeFunc[DownstreamPayload]{
	"tool.execute": payloadOf(executePayload),
	"tool.stdin":   payloadOf(stdinPayload),
	"tool.cancel":  payloadOf(cancelPayload),
	"mcp.call":     payloadOf(mcpCallPayload),
}

// taskAddress is the identity every tool instruction and report carries.
type taskAddress struct {
	SessionID      string
	BindingID      string
	ToolUseEventID string
	TaskID         string
}

func readTaskAddress(o *object) taskAddress {
	return taskAddress{
		SessionID:      required(o, "session_id", identity),
		BindingID:      required(o, "binding_id", identity),
		ToolUseEventID: required(o, "tool_use_event_id", identity),
		TaskID:         required(o, "task_id", identity),
	}
}

func (a taskAddress) encode(e *encoder) {
	e.stringMember("session_id", a.SessionID)
	e.stringMember("binding_id", a.BindingID)
	e.stringMember("tool_use_event_id", a.ToolUseEventID)
	e.stringMember("task_id", a.TaskID)
}

// ExecutePayload starts one tool execution.
type ExecutePayload struct {
	SessionID      string
	BindingID      string
	ToolUseEventID string
	TaskID         string
	Input          ToolInput
	SandboxPolicy  SandboxPolicy
	Limits         Optional[VisibleLimits]
}

func (ExecutePayload) MessageType() string { return "tool.execute" }

func executePayload(d *decoder, ptr string, v *value) ExecutePayload {
	o := d.object(ptr, v)
	a := readTaskAddress(o)
	p := ExecutePayload{SessionID: a.SessionID, BindingID: a.BindingID, ToolUseEventID: a.ToolUseEventID, TaskID: a.TaskID}
	switch required(o, "tool", enum(ToolExecCommand, ToolEdit, ToolViewImage)) {
	case ToolExecCommand:
		p.Input = required(o, "input", execInput)
	case ToolEdit:
		p.Input = required(o, "input", editInput)
	case ToolViewImage:
		p.Input = required(o, "input", viewImageInput)
	}
	p.SandboxPolicy = required(o, "sandbox_policy", sandboxPolicy)
	p.Limits = optional(o, "limits", visibleLimits)
	o.open()
	return p
}

func (p ExecutePayload) encode(e *encoder) {
	e.beginObject()
	taskAddress{p.SessionID, p.BindingID, p.ToolUseEventID, p.TaskID}.encode(e)
	e.stringMember("tool", p.Input.Tool())
	e.valueMember("input", p.Input)
	e.valueMember("sandbox_policy", p.SandboxPolicy)
	optionalMember(e, "limits", p.Limits, writeValue)
	e.endObject()
}

// StdinPayload writes to or reads from a running task.
type StdinPayload struct {
	SessionID      string
	BindingID      string
	ToolUseEventID string
	TaskID         string
	Chars          string
	WriteSeq       Optional[int64] // required when Chars is not empty
	WaitMS         Nullable[int64]
	Limits         Optional[VisibleLimits]
}

func (StdinPayload) MessageType() string { return "tool.stdin" }

func stdinPayload(d *decoder, ptr string, v *value) StdinPayload {
	o := d.object(ptr, v)
	a := readTaskAddress(o)
	p := StdinPayload{SessionID: a.SessionID, BindingID: a.BindingID, ToolUseEventID: a.ToolUseEventID, TaskID: a.TaskID}
	p.Chars = required(o, "chars", anyString)
	if p.Chars != "" && !d.failed() && !o.has("write_seq") {
		d.fail(ptr, "missing required member %q", "write_seq")
	}
	p.WriteSeq = optional(o, "write_seq", positiveInteger)
	p.WaitMS = nullable(o, "wait_ms", anyInteger)
	p.Limits = optional(o, "limits", visibleLimits)
	o.open()
	return p
}

func (p StdinPayload) encode(e *encoder) {
	e.beginObject()
	taskAddress{p.SessionID, p.BindingID, p.ToolUseEventID, p.TaskID}.encode(e)
	e.stringMember("chars", p.Chars)
	optionalMember(e, "write_seq", p.WriteSeq, writeInteger)
	nullableMember(e, "wait_ms", p.WaitMS, writeInteger)
	optionalMember(e, "limits", p.Limits, writeValue)
	e.endObject()
}

// CancelPayload interrupts a task.
type CancelPayload struct {
	SessionID      string
	BindingID      string
	ToolUseEventID string
	TaskID         string
	Reason         Nullable[string]
	Limits         Optional[VisibleLimits]
}

func (CancelPayload) MessageType() string { return "tool.cancel" }

func cancelPayload(d *decoder, ptr string, v *value) CancelPayload {
	o := d.object(ptr, v)
	a := readTaskAddress(o)
	p := CancelPayload{SessionID: a.SessionID, BindingID: a.BindingID, ToolUseEventID: a.ToolUseEventID, TaskID: a.TaskID}
	p.Reason = nullable(o, "reason", anyString)
	p.Limits = optional(o, "limits", visibleLimits)
	o.open()
	return p
}

func (p CancelPayload) encode(e *encoder) {
	e.beginObject()
	taskAddress{p.SessionID, p.BindingID, p.ToolUseEventID, p.TaskID}.encode(e)
	nullableMember(e, "reason", p.Reason, writeString)
	optionalMember(e, "limits", p.Limits, writeValue)
	e.endObject()
}

// McpCallPayload calls a tool of a local MCP server.
type McpCallPayload struct {
	SessionID      string
	BindingID      string
	ToolUseEventID string
	TaskID         string
	Server         string
	Tool           string
	Arguments      RawObject
}

func (McpCallPayload) MessageType() string { return "mcp.call" }

func mcpCallPayload(d *decoder, ptr string, v *value) McpCallPayload {
	o := d.object(ptr, v)
	a := readTaskAddress(o)
	p := McpCallPayload{SessionID: a.SessionID, BindingID: a.BindingID, ToolUseEventID: a.ToolUseEventID, TaskID: a.TaskID}
	p.Server = required(o, "server", stringWith(minLength(1)))
	p.Tool = required(o, "tool", stringWith(minLength(1)))
	p.Arguments = required(o, "arguments", rawObject)
	o.absent("sandbox_policy", "limits")
	o.open()
	return p
}

func (p McpCallPayload) encode(e *encoder) {
	e.beginObject()
	taskAddress{p.SessionID, p.BindingID, p.ToolUseEventID, p.TaskID}.encode(e)
	e.stringMember("server", p.Server)
	e.stringMember("tool", p.Tool)
	e.key("arguments")
	e.raw(p.Arguments)
	e.endObject()
}

// checkToolDownstream applies the byte budgets and identities of a tool
// instruction: the input, chars or arguments, the metadata around them, and
// the whole envelope.
func checkToolDownstream(m Downstream, v *value) error {
	payload, _ := v.get("payload")
	total := compactSize(v)
	limit, metadata := DefaultMessageJSONBytes, total
	switch p := m.Payload.(type) {
	case ExecutePayload:
		if err := checkSandboxPolicy("/payload/sandbox_policy", p.SandboxPolicy); err != nil {
			return err
		}
		input, _ := payload.get("input")
		if compactSize(input) > ToolInputJSONBytes {
			return invalid("/payload/input", "input JSON over 4 MiB")
		}
		metadata = total - compactSize(input) + 2
		limit = ExecuteSSEJSONBytes
		if id, ok := input.get("task_id"); ok && id.kind != kindNull && (id.kind != kindString || id.text != p.TaskID) {
			return invalid("/payload/input/task_id", "inner task identity conflict")
		}
	case StdinPayload:
		if len(p.Chars) > StdinUTF8Bytes {
			return invalid("/payload/chars", "stdin raw bytes over 32 KiB")
		}
		metadata = total - stringSize(p.Chars) + 2
		limit = StdinSSEJSONBytes
	case McpCallPayload:
		arguments, _ := payload.get("arguments")
		if compactSize(arguments) > mcpArgumentsJSONBytes {
			return invalid("/payload/arguments", "MCP arguments over 64 KiB")
		}
		metadata = total - compactSize(arguments) + 2
		limit = mcpCallSSEJSONBytes
	}
	if metadata > MetadataJSONBytes {
		return invalid("", "metadata over 64 KiB")
	}
	if total > limit {
		return invalid("", fmt.Sprintf("downstream body over %d bytes", limit))
	}
	return nil
}

// StreamSnapshot is the visible part of one output stream.
type StreamSnapshot struct {
	Text          string
	TotalBytes    int64
	TotalLines    int64
	ReturnedBytes int64
	Truncated     bool
}

func streamSnapshot(d *decoder, ptr string, v *value) StreamSnapshot {
	o := d.object(ptr, v)
	s := StreamSnapshot{
		Text:          required(o, "text", anyString),
		TotalBytes:    required(o, "total_bytes", nonNegative),
		TotalLines:    required(o, "total_lines", nonNegative),
		ReturnedBytes: required(o, "returned_bytes", nonNegative),
		Truncated:     required(o, "truncated", boolean),
	}
	o.open()
	return s
}

func (s StreamSnapshot) encode(e *encoder) {
	e.beginObject()
	e.stringMember("text", s.Text)
	e.integerMember("total_bytes", s.TotalBytes)
	e.integerMember("total_lines", s.TotalLines)
	e.integerMember("returned_bytes", s.ReturnedBytes)
	e.boolMember("truncated", s.Truncated)
	e.endObject()
}

// HelperResult is the result of a result envelope: an ExecResult, an
// EditResult, a MediaResult or an McpResult where the envelope's tool, status
// and truncation select one, and otherwise the object as sent.
type HelperResult interface {
	encodable
}

// ExecResult is the result of exec_command, stdin and cancel.
type ExecResult struct {
	TaskID       Nullable[string]
	ExitCode     Nullable[int64]
	Signal       Nullable[string]
	TimedOut     bool
	Cancelled    Optional[bool]
	WasRunning   Optional[bool]
	WrittenBytes Optional[int64]
	Stdout       StreamSnapshot
	Stderr       StreamSnapshot
	DurationMS   int64
}

func execResult(d *decoder, ptr string, v *value) ExecResult {
	o := d.object(ptr, v)
	r := ExecResult{
		TaskID:       requiredNullable(o, "task_id", identity),
		ExitCode:     requiredNullable(o, "exit_code", anyInteger),
		Signal:       requiredNullable(o, "signal", anyString),
		TimedOut:     required(o, "timed_out", boolean),
		Cancelled:    optional(o, "cancelled", boolean),
		WasRunning:   optional(o, "was_running", boolean),
		WrittenBytes: optional(o, "written_bytes", nonNegative),
		Stdout:       required(o, "stdout", streamSnapshot),
		Stderr:       required(o, "stderr", streamSnapshot),
		DurationMS:   required(o, "duration_ms", nonNegative),
	}
	o.open()
	return r
}

func (r ExecResult) encode(e *encoder) {
	e.beginObject()
	nullableMember(e, "task_id", r.TaskID, writeString)
	nullableMember(e, "exit_code", r.ExitCode, writeInteger)
	nullableMember(e, "signal", r.Signal, writeString)
	e.boolMember("timed_out", r.TimedOut)
	optionalMember(e, "cancelled", r.Cancelled, writeBool)
	optionalMember(e, "was_running", r.WasRunning, writeBool)
	optionalMember(e, "written_bytes", r.WrittenBytes, writeInteger)
	e.valueMember("stdout", r.Stdout)
	e.valueMember("stderr", r.Stderr)
	e.integerMember("duration_ms", r.DurationMS)
	e.endObject()
}

// MediaResult is the image of view_image.
type MediaResult struct {
	Mime       string // image/png, image/jpeg, image/gif or image/webp
	SizeBytes  int64
	DataBase64 string
}

func mediaResult(d *decoder, ptr string, v *value) MediaResult {
	o := d.object(ptr, v)
	r := MediaResult{
		Mime:       required(o, "mime", enum("image/png", "image/jpeg", "image/gif", "image/webp")),
		SizeBytes:  required(o, "size_bytes", nonNegative),
		DataBase64: required(o, "data_base64", anyString),
	}
	o.open()
	return r
}

func (r MediaResult) encode(e *encoder) {
	e.beginObject()
	e.stringMember("mime", r.Mime)
	e.integerMember("size_bytes", r.SizeBytes)
	e.stringMember("data_base64", r.DataBase64)
	e.endObject()
}

// EditResult is the result of edit.
type EditResult struct {
	Replacements int64
	BytesWritten int64
}

func editResult(d *decoder, ptr string, v *value) EditResult {
	o := d.object(ptr, v)
	r := EditResult{
		Replacements: required(o, "replacements", nonNegative),
		BytesWritten: required(o, "bytes_written", nonNegative),
	}
	o.open()
	return r
}

func (r EditResult) encode(e *encoder) {
	e.beginObject()
	e.integerMember("replacements", r.Replacements)
	e.integerMember("bytes_written", r.BytesWritten)
	e.endObject()
}

// McpResult is the result of a local MCP call, with its content blocks as
// the server sent them.
type McpResult struct {
	Content           []RawObject
	StructuredContent Optional[RawObject]
	IsError           Optional[bool]
}

func mcpResult(d *decoder, ptr string, v *value) McpResult {
	o := d.object(ptr, v)
	r := McpResult{
		Content:           required(o, "content", arrayOf(rawObject, 0, -1)),
		StructuredContent: optional(o, "structuredContent", rawObject),
		IsError:           optional(o, "isError", boolean),
	}
	o.absent("_meta")
	o.open()
	return r
}

func (r McpResult) encode(e *encoder) {
	e.beginObject()
	e.key("content")
	e.begin('[')
	for _, block := range r.Content {
		e.next()
		e.raw(block)
	}
	e.end(']')
	optionalMember(e, "structuredContent", r.StructuredContent, writeRaw)
	optionalMember(e, "isError", r.IsError, writeBool)
	e.endObject()
}

func (r RawObject) encode(e *encoder) { e.raw(r) }

// ToolError is the error of a result envelope.
type ToolError struct {
	Kind    string
	Message string
	Path    Optional[string]
	Detail  Optional[RawObject]
}

func toolError(d *decoder, ptr string, v *value) ToolError {
	o := d.object(ptr, v)
	t := ToolError{
		Kind:    required(o, "kind", stringWith(minLength(1))),
		Message: required(o, "message", anyString),
		Path:    optional(o, "path", anyString),
		Detail:  optional(o, "detail", rawObject),
	}
	o.open()
	return t
}

// mcpToolError is McpToolError: a ToolError whose kind is an MCP kind and
// whose detail holds that kind's fields.
func mcpToolError(d *decoder, ptr string, v *value) ToolError {
	t := toolError(d, ptr, v)
	checkEnum(d, ptr+"/kind", t.Kind, "mcp_error", "mcp_timeout", "mcp_server_exited", "cancelled", "mcp_result_too_large", "mcp_invalid_result")
	var fields func(*object)
	switch t.Kind {
	case "mcp_error":
		fields = func(o *object) {
			required(o, "code", anyInteger)
			required(o, "message", anyString)
		}
	case "cancelled":
		fields = func(o *object) { required(o, "sent", boolean) }
	case "mcp_result_too_large":
		fields = func(o *object) {
			required(o, "media_bytes", nonNegative)
			required(o, "envelope_bytes", nonNegative)
			required(o, "report_bytes", nonNegative)
		}
	}
	if fields != nil && !d.failed() {
		detail, ok := v.get("detail")
		if !ok {
			d.fail(ptr, "missing required member %q", "detail")
			return t
		}
		fields(d.object(ptr+"/detail", detail))
	}
	return t
}

func (t ToolError) encode(e *encoder) {
	e.beginObject()
	e.stringMember("kind", t.Kind)
	e.stringMember("message", t.Message)
	optionalMember(e, "path", t.Path, writeString)
	optionalMember(e, "detail", t.Detail, writeRaw)
	e.endObject()
}

// ProtocolError is the error of a rejected execution, a control or a
// protocol-error report.
type ProtocolError struct {
	Code    string
	Message string
}

// RejectionCode is the code of a rejected execution report.
type RejectionCode string

const (
	RejectionInvalidInput         RejectionCode = "invalid_input"
	RejectionTaskConflict         RejectionCode = "task_conflict"
	RejectionUnsupportedPolicy    RejectionCode = "unsupported_policy"
	RejectionSandboxUnavailable   RejectionCode = "sandbox_unavailable"
	RejectionBindingUnavailable   RejectionCode = "binding_unavailable"
	RejectionResourceExhausted    RejectionCode = "resource_exhausted"
	RejectionMcpServerUnavailable RejectionCode = "mcp_server_unavailable"
)

// ControlErrorCode is the code of a control report's error.
type ControlErrorCode string

const (
	ControlUnsupportedControl ControlErrorCode = "unsupported_control"
	ControlTaskNotFound       ControlErrorCode = "task_not_found"
	ControlWriteConflict      ControlErrorCode = "write_conflict"
	ControlInvalidInput       ControlErrorCode = "invalid_input"
	ControlResultUnavailable  ControlErrorCode = "result_unavailable"
	ControlStdinClosed        ControlErrorCode = "stdin_closed"
	ControlResourceExhausted  ControlErrorCode = "resource_exhausted"
)

// ProtocolErrorCode is the code of a protocol-error report.
type ProtocolErrorCode string

const (
	ProtocolMessageConflict ProtocolErrorCode = "message_conflict"
	ProtocolInvalidMessage  ProtocolErrorCode = "invalid_message"
)

func protocolError[C ~string](codes ...C) decodeFunc[ProtocolError] {
	names := make([]string, len(codes))
	for i, c := range codes {
		names[i] = string(c)
	}
	return func(d *decoder, ptr string, v *value) ProtocolError {
		o := d.object(ptr, v)
		p := ProtocolError{
			Code:    required(o, "code", stringWith(minLength(1))),
			Message: required(o, "message", anyString),
		}
		checkEnum(d, ptr+"/code", p.Code, names...)
		o.open()
		return p
	}
}

var (
	rejectionError = protocolError(RejectionInvalidInput, RejectionTaskConflict, RejectionUnsupportedPolicy,
		RejectionSandboxUnavailable, RejectionBindingUnavailable, RejectionResourceExhausted, RejectionMcpServerUnavailable)
	controlError = protocolError(ControlUnsupportedControl, ControlTaskNotFound, ControlWriteConflict,
		ControlInvalidInput, ControlResultUnavailable, ControlStdinClosed, ControlResourceExhausted)
	protocolErrorReportError = protocolError(ProtocolMessageConflict, ProtocolInvalidMessage)
)

func (p ProtocolError) encode(e *encoder) {
	e.beginObject()
	e.stringMember("code", p.Code)
	e.stringMember("message", p.Message)
	e.endObject()
}

// DriveWriter is the writer of a drive version: an API key, a user, a service
// account or a Session's computer.
type DriveWriter struct {
	Type             string // api_actor, user_actor, service_account_actor or session_actor
	APIKeyID         string // api_actor
	UserID           string // user_actor
	ServiceAccountID string // service_account_actor
	SessionID        string // session_actor
	ComputerID       string // session_actor
}

func driveWriter(d *decoder, ptr string, v *value) DriveWriter {
	o := d.object(ptr, v)
	w := DriveWriter{Type: required(o, "type", enum("api_actor", "user_actor", "service_account_actor", "session_actor"))}
	switch w.Type {
	case "api_actor":
		w.APIKeyID = required(o, "api_key_id", identity)
	case "user_actor":
		w.UserID = required(o, "user_id", identity)
	case "service_account_actor":
		w.ServiceAccountID = required(o, "service_account_id", identity)
	case "session_actor":
		w.SessionID = required(o, "session_id", identity)
		w.ComputerID = required(o, "computer_id", identity)
	}
	o.closed()
	return w
}

func (w DriveWriter) encode(e *encoder) {
	e.beginObject()
	e.stringMember("type", w.Type)
	switch w.Type {
	case "api_actor":
		e.stringMember("api_key_id", w.APIKeyID)
	case "user_actor":
		e.stringMember("user_id", w.UserID)
	case "service_account_actor":
		e.stringMember("service_account_id", w.ServiceAccountID)
	case "session_actor":
		e.stringMember("session_id", w.SessionID)
		e.stringMember("computer_id", w.ComputerID)
	}
	e.endObject()
}

// DriveRejection is a change of a sync point that the drive rejected; the CLI
// has already restored the drive's state on the machine.
type DriveRejection struct {
	Path             string // absolute machine path
	Change           string // write or delete
	Reason           string // conflict, path_rule or limit
	Rule             Optional[string]
	CurrentVersionID Optional[string]
	CurrentWriter    Optional[DriveWriter]
}

var machinePath = stringWith(minLength(1), absolutePath)

func driveRejection(d *decoder, ptr string, v *value) DriveRejection {
	o := d.object(ptr, v)
	r := DriveRejection{
		Path:   required(o, "path", machinePath),
		Change: required(o, "change", enum("write", "delete")),
		Reason: required(o, "reason", enum("conflict", "path_rule", "limit")),
	}
	if r.Reason == "path_rule" || r.Reason == "limit" {
		r.Rule = Some(required(o, "rule", stringWith(minLength(1))))
	} else {
		r.Rule = optional(o, "rule", stringWith(minLength(1)))
	}
	r.CurrentVersionID = optional(o, "current_version_id", identity)
	r.CurrentWriter = optional(o, "current_writer", driveWriter)
	if r.Reason == "conflict" && !d.failed() && r.CurrentVersionID.Set != r.CurrentWriter.Set {
		d.fail(ptr, "a conflict carries both current_version_id and current_writer or neither")
	}
	o.closed()
	return r
}

func (r DriveRejection) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", r.Path)
	e.stringMember("change", r.Change)
	e.stringMember("reason", r.Reason)
	optionalMember(e, "rule", r.Rule, writeString)
	optionalMember(e, "current_version_id", r.CurrentVersionID, writeString)
	optionalMember(e, "current_writer", r.CurrentWriter, writeValue)
	e.endObject()
}

// DriveNotSynced is a path the sync point did not upload.
type DriveNotSynced struct {
	Path   string
	Reason string // git_repository, symlink, special_file, uploading or upload_pending
}

func driveNotSynced(d *decoder, ptr string, v *value) DriveNotSynced {
	o := d.object(ptr, v)
	n := DriveNotSynced{
		Path:   required(o, "path", machinePath),
		Reason: required(o, "reason", enum("git_repository", "symlink", "special_file", "uploading", "upload_pending")),
	}
	o.closed()
	return n
}

func (n DriveNotSynced) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", n.Path)
	e.stringMember("reason", n.Reason)
	e.endObject()
}

// DriveDownload is a file a mount is still downloading.
type DriveDownload struct {
	Path            string
	DownloadedBytes int64
	SizeBytes       int64
}

func driveDownload(d *decoder, ptr string, v *value) DriveDownload {
	o := d.object(ptr, v)
	dl := DriveDownload{
		Path:            required(o, "path", machinePath),
		DownloadedBytes: required(o, "downloaded_bytes", nonNegative),
		SizeBytes:       required(o, "size_bytes", nonNegative),
	}
	o.closed()
	return dl
}

func (dl DriveDownload) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", dl.Path)
	e.integerMember("downloaded_bytes", dl.DownloadedBytes)
	e.integerMember("size_bytes", dl.SizeBytes)
	e.endObject()
}

// DriveMountFailure is a drive the CLI could not mount.
type DriveMountFailure struct {
	DriveID string
	Error   string
}

func driveMountFailure(d *decoder, ptr string, v *value) DriveMountFailure {
	o := d.object(ptr, v)
	f := DriveMountFailure{
		DriveID: required(o, "drive_id", identity),
		Error:   required(o, "error", stringWith(minLength(1))),
	}
	o.closed()
	return f
}

func (f DriveMountFailure) encode(e *encoder) {
	e.beginObject()
	e.stringMember("drive_id", f.DriveID)
	e.stringMember("error", f.Error)
	e.endObject()
}

// DriveSyncOutcome is what one sync point did to the drives. Each list holds
// at most 100 entries; its total, when present, counts the entries beyond
// them as well.
type DriveSyncOutcome struct {
	Committed        int64
	Deleted          int64
	Rejected         []DriveRejection
	RejectedTotal    Optional[int64]
	NotSynced        []DriveNotSynced
	NotSyncedTotal   Optional[int64]
	Downloading      Optional[[]DriveDownload]
	DownloadingTotal Optional[int64]
	Unmounted        Optional[[]DriveMountFailure]
}

func driveSyncOutcome(d *decoder, ptr string, v *value) DriveSyncOutcome {
	o := d.object(ptr, v)
	s := DriveSyncOutcome{
		Committed:        required(o, "committed", nonNegative),
		Deleted:          required(o, "deleted", nonNegative),
		Rejected:         required(o, "rejected", arrayOf(driveRejection, 0, driveSyncListedEntries)),
		RejectedTotal:    optional(o, "rejected_total", positiveInteger),
		NotSynced:        required(o, "not_synced", arrayOf(driveNotSynced, 0, driveSyncListedEntries)),
		NotSyncedTotal:   optional(o, "not_synced_total", positiveInteger),
		Downloading:      optional(o, "downloading", arrayOf(driveDownload, 0, driveSyncListedEntries)),
		DownloadingTotal: optional(o, "downloading_total", positiveInteger),
		Unmounted:        optional(o, "unmounted", arrayOf(driveMountFailure, 0, -1)),
	}
	o.closed()
	if !d.failed() && s.Committed == 0 && s.Deleted == 0 && len(s.Rejected) == 0 && len(s.NotSynced) == 0 &&
		len(s.Downloading.Value) == 0 && !s.RejectedTotal.Set && !s.NotSyncedTotal.Set && !s.DownloadingTotal.Set &&
		len(s.Unmounted.Value) == 0 {
		d.fail(ptr, "an empty drive sync outcome")
	}
	return s
}

func (s DriveSyncOutcome) encode(e *encoder) {
	e.beginObject()
	e.integerMember("committed", s.Committed)
	e.integerMember("deleted", s.Deleted)
	e.key("rejected")
	list(e, s.Rejected)
	optionalMember(e, "rejected_total", s.RejectedTotal, writeInteger)
	e.key("not_synced")
	list(e, s.NotSynced)
	optionalMember(e, "not_synced_total", s.NotSyncedTotal, writeInteger)
	optionalMember(e, "downloading", s.Downloading, writeList)
	optionalMember(e, "downloading_total", s.DownloadingTotal, writeInteger)
	optionalMember(e, "unmounted", s.Unmounted, writeList)
	e.endObject()
}

// checkDriveSyncOutcome applies the relations of a drive sync outcome: at
// most 64 KiB, each total above its listed entries, no download past its
// file's size.
func checkDriveSyncOutcome(ptr string, s DriveSyncOutcome, v *value) error {
	if compactSize(v) > DriveSyncJSONBytes {
		return invalid(ptr, "drive_sync over 64 KiB")
	}
	totals := []struct {
		name   string
		total  Optional[int64]
		listed int
	}{
		{"rejected_total", s.RejectedTotal, len(s.Rejected)},
		{"not_synced_total", s.NotSyncedTotal, len(s.NotSynced)},
		{"downloading_total", s.DownloadingTotal, len(s.Downloading.Value)},
	}
	for _, t := range totals {
		if t.total.Set && t.total.Value <= int64(t.listed) {
			return invalid(pointer(ptr, t.name), t.name+" must exceed the listed entries")
		}
	}
	for i, dl := range s.Downloading.Value {
		if dl.DownloadedBytes > dl.SizeBytes {
			return invalid(ptr+"/downloading/"+strconv.Itoa(i)+"/downloaded_bytes", "download past file size")
		}
	}
	return nil
}

// HelperMetrics are the timings of a finished execution.
type HelperMetrics struct {
	WallTimeMS  int64
	QueuedMS    int64
	DriveWaitMS int64
	RunMS       int64
	SyncMS      int64
}

func helperMetrics(d *decoder, ptr string, v *value) HelperMetrics {
	o := d.object(ptr, v)
	m := HelperMetrics{
		WallTimeMS:  required(o, "wall_time_ms", nonNegative),
		QueuedMS:    required(o, "queued_ms", nonNegative),
		DriveWaitMS: required(o, "drive_wait_ms", nonNegative),
		RunMS:       required(o, "run_ms", nonNegative),
		SyncMS:      required(o, "sync_ms", nonNegative),
	}
	o.open()
	return m
}

func (m HelperMetrics) encode(e *encoder) {
	e.beginObject()
	e.integerMember("wall_time_ms", m.WallTimeMS)
	e.integerMember("queued_ms", m.QueuedMS)
	e.integerMember("drive_wait_ms", m.DriveWaitMS)
	e.integerMember("run_ms", m.RunMS)
	e.integerMember("sync_ms", m.SyncMS)
	e.endObject()
}

// HelperEnvelope is the result envelope of one tool or control reply.
type HelperEnvelope struct {
	Tool      string // exec_command, edit, view_image, mcp, stdin or cancel
	Status    string // success, error or running
	Truncated bool
	Error     Nullable[ToolError] // set exactly when Status is error
	Result    HelperResult
	Metrics   Optional[HelperMetrics]
	DriveSync Optional[DriveSyncOutcome]
}

func helperEnvelope(d *decoder, ptr string, v *value) HelperEnvelope {
	o := d.object(ptr, v)
	required(o, "schema_version", constInteger(helperEnvelopeSchemaVersion))
	h := HelperEnvelope{
		Tool:      required(o, "tool", enum(ToolExecCommand, ToolEdit, ToolViewImage, ToolMCP, ToolStdin, ToolCancel)),
		Status:    required(o, "status", enum("success", "error", "running")),
		Truncated: required(o, "truncated", boolean),
	}
	execTool := h.Tool == ToolExecCommand || h.Tool == ToolStdin || h.Tool == ToolCancel
	finished := h.Status == "success" || h.Status == "running"
	if h.Status == "running" && !execTool && !d.failed() {
		d.fail(pointer(ptr, "tool"), "%q never reports running", h.Tool)
	}
	if h.Tool == ToolMCP && h.Truncated && !d.failed() {
		d.fail(pointer(ptr, "truncated"), "an MCP result is never truncated")
	}
	switch {
	case h.Status != "error":
		h.Error = requiredNullable(o, "error", func(d *decoder, ptr string, v *value) ToolError {
			d.fail(ptr, "expected null, got %s", v.kind)
			return ToolError{}
		})
	case h.Tool == ToolMCP:
		h.Error = Value(required(o, "error", mcpToolError))
	default:
		h.Error = Value(required(o, "error", toolError))
	}
	var result decodeFunc[HelperResult]
	switch {
	case execTool && finished:
		result = resultOf(execResult)
	case h.Tool == ToolEdit && finished && !h.Truncated:
		result = resultOf(editResult)
	case h.Tool == ToolViewImage && finished && !h.Truncated:
		result = resultOf(mediaResult)
	case h.Tool == ToolMCP && h.Status == "success":
		result = resultOf(mcpResult)
	case h.Tool == ToolMCP:
		result = resultOf(emptyObject)
	default:
		result = resultOf(rawObject)
	}
	h.Result = required(o, "result", result)
	h.Metrics = optional(o, "metrics", helperMetrics)
	if h.Tool == ToolMCP {
		o.absent("drive_sync")
	}
	h.DriveSync = optional(o, "drive_sync", driveSyncOutcome)
	o.open()
	return h
}

func resultOf[T HelperResult](f decodeFunc[T]) decodeFunc[HelperResult] {
	return func(d *decoder, ptr string, v *value) HelperResult {
		r := f(d, ptr, v)
		if d.failed() {
			return nil
		}
		return r
	}
}

func emptyObject(d *decoder, ptr string, v *value) RawObject {
	r := rawObject(d, ptr, v)
	if !d.failed() && len(v.members) > 0 {
		d.fail(ptr, "expected an empty object")
	}
	return r
}

func (h HelperEnvelope) encode(e *encoder) {
	e.beginObject()
	e.integerMember("schema_version", helperEnvelopeSchemaVersion)
	e.stringMember("tool", h.Tool)
	e.stringMember("status", h.Status)
	e.boolMember("truncated", h.Truncated)
	if h.Error.Present {
		e.valueMember("error", h.Error.Value)
	} else {
		e.key("error")
		e.null()
	}
	e.valueMember("result", h.Result)
	optionalMember(e, "metrics", h.Metrics, writeValue)
	optionalMember(e, "drive_sync", h.DriveSync, writeValue)
	e.endObject()
}

// ToolStatus is one report on a tool instruction: an ExecutionReport or a
// ControlReport.
type ToolStatus interface {
	encodable
	toolStatus()
}

// reportAddress is the identity every tool report carries.
type reportAddress struct {
	ConnectionID     string
	MessageID        string
	SessionID        string
	BindingID        string
	ToolUseEventID   string
	TaskID           string
	RequestMessageID string
}

func readReportAddress(o *object) reportAddress {
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	return reportAddress{
		ConnectionID:     required(o, "connection_id", identity),
		MessageID:        required(o, "message_id", identity),
		SessionID:        required(o, "session_id", identity),
		BindingID:        required(o, "binding_id", identity),
		ToolUseEventID:   required(o, "tool_use_event_id", identity),
		TaskID:           required(o, "task_id", identity),
		RequestMessageID: required(o, "request_message_id", identity),
	}
}

func (a reportAddress) encode(e *encoder) {
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", a.ConnectionID)
	e.stringMember("message_id", a.MessageID)
	e.stringMember("session_id", a.SessionID)
	e.stringMember("binding_id", a.BindingID)
	e.stringMember("tool_use_event_id", a.ToolUseEventID)
	e.stringMember("task_id", a.TaskID)
	e.stringMember("request_message_id", a.RequestMessageID)
}

// ExecutionReport reports an execution: running or terminal with a result
// envelope and its sequence, or rejected with an error.
type ExecutionReport struct {
	ConnectionID     string
	MessageID        string
	SessionID        string
	BindingID        string
	ToolUseEventID   string
	TaskID           string
	RequestMessageID string
	Phase            string                   // running, terminal or rejected
	ReportSeq        Optional[int64]          // running and terminal
	Result           Optional[HelperEnvelope] // running and terminal
	Error            Optional[ProtocolError]  // rejected, with a RejectionCode
}

func (ExecutionReport) toolStatus() {}

func executionReport(d *decoder, ptr string, o *object) ExecutionReport {
	a := readReportAddress(o)
	r := ExecutionReport{ConnectionID: a.ConnectionID, MessageID: a.MessageID, SessionID: a.SessionID, BindingID: a.BindingID,
		ToolUseEventID: a.ToolUseEventID, TaskID: a.TaskID, RequestMessageID: a.RequestMessageID}
	r.Phase = required(o, "phase", enum("running", "terminal", "rejected"))
	if r.Phase == "rejected" {
		r.Error = Some(required(o, "error", rejectionError))
		o.absent("report_seq", "result")
	} else {
		r.ReportSeq = Some(required(o, "report_seq", positiveInteger))
		r.Result = Some(required(o, "result", helperEnvelope))
		o.absent("error")
		if r.Phase == "terminal" && !d.failed() && !r.Result.Value.Metrics.Set {
			d.fail(ptr+"/result", "missing required member %q", "metrics")
		}
	}
	return r
}

func (r ExecutionReport) encode(e *encoder) {
	e.beginObject()
	reportAddress{r.ConnectionID, r.MessageID, r.SessionID, r.BindingID, r.ToolUseEventID, r.TaskID, r.RequestMessageID}.encode(e)
	e.stringMember("kind", "execution")
	e.stringMember("phase", r.Phase)
	optionalMember(e, "report_seq", r.ReportSeq, writeInteger)
	optionalMember(e, "result", r.Result, writeValue)
	optionalMember(e, "error", r.Error, writeValue)
	e.endObject()
}

// ControlReport answers a stdin or cancel control with a result envelope, an
// error, or both.
type ControlReport struct {
	ConnectionID     string
	MessageID        string
	SessionID        string
	BindingID        string
	ToolUseEventID   string
	TaskID           string
	RequestMessageID string
	Result           Optional[HelperEnvelope]
	Error            Optional[ProtocolError] // with a ControlErrorCode
}

func (ControlReport) toolStatus() {}

func controlReport(d *decoder, ptr string, o *object) ControlReport {
	a := readReportAddress(o)
	r := ControlReport{ConnectionID: a.ConnectionID, MessageID: a.MessageID, SessionID: a.SessionID, BindingID: a.BindingID,
		ToolUseEventID: a.ToolUseEventID, TaskID: a.TaskID, RequestMessageID: a.RequestMessageID}
	r.Result = optional(o, "result", helperEnvelope)
	r.Error = optional(o, "error", controlError)
	if !d.failed() && !r.Result.Set && !r.Error.Set {
		d.fail(ptr, "a control report carries a result or an error")
	}
	o.absent("phase", "report_seq")
	return r
}

func (r ControlReport) encode(e *encoder) {
	e.beginObject()
	reportAddress{r.ConnectionID, r.MessageID, r.SessionID, r.BindingID, r.ToolUseEventID, r.TaskID, r.RequestMessageID}.encode(e)
	e.stringMember("kind", "control")
	optionalMember(e, "result", r.Result, writeValue)
	optionalMember(e, "error", r.Error, writeValue)
	e.endObject()
}

func toolStatus(d *decoder, ptr string, v *value) ToolStatus {
	o := d.object(ptr, v)
	var s ToolStatus
	switch required(o, "kind", enum("execution", "control")) {
	case "execution":
		s = executionReport(d, ptr, o)
	case "control":
		s = controlReport(d, ptr, o)
	}
	o.open()
	if d.failed() {
		return nil
	}
	return s
}

// checkToolStatus applies the relations of a report: first its complete
// limit, then its metadata budget, its phase, tool and sequence against its
// result envelope, the result's task, the drive sync outcome, and the budgets
// of media and of the envelope.
func checkToolStatus(s ToolStatus, v *value) error {
	total := compactSize(v)
	helperValue, hasResult := v.get("result")
	var helper HelperEnvelope
	var taskID string
	switch r := s.(type) {
	case ExecutionReport:
		helper, taskID = r.Result.Value, r.TaskID
	case ControlReport:
		helper, taskID = r.Result.Value, r.TaskID
	}
	if limit := toolStatusLimit(helper.Tool, helperValue); total > limit {
		return tooLarge("", fmt.Sprintf("report over %d bytes", limit))
	}
	metadata := total
	if hasResult {
		metadata = total - compactSize(helperValue) + 2
	}
	if metadata > MetadataJSONBytes {
		return invalid("", "report metadata over 64 KiB")
	}
	switch r := s.(type) {
	case ExecutionReport:
		if !r.Result.Set {
			break
		}
		if (r.Phase == "running") != (helper.Status == "running") {
			return invalid("/phase", "phase and status disagree")
		}
		if helper.Tool == ToolStdin || helper.Tool == ToolCancel {
			return invalid("/result/tool", "control tool in an execution report")
		}
		if helper.Tool == ToolMCP && r.ReportSeq.Value != 1 {
			return invalid("/report_seq", "second report for an MCP call")
		}
	case ControlReport:
		if !r.Result.Set {
			break
		}
		if helper.Tool != ToolStdin && helper.Tool != ToolCancel {
			return invalid("/result/tool", "execution tool in a control report")
		}
	}
	if !hasResult {
		return nil
	}
	if helper.DriveSync.Set {
		sync := helper.DriveSync.Value
		syncValue, _ := helperValue.get("drive_sync")
		if err := checkDriveSyncOutcome("/result/drive_sync", sync, syncValue); err != nil {
			return err
		}
		if (sync.Downloading.Set || sync.DownloadingTotal.Set) && helper.Status != "running" {
			return invalid("/result/drive_sync", "downloads listed in a finished result")
		}
		if sync.Unmounted.Set && helper.Tool != ToolExecCommand && helper.Tool != ToolViewImage {
			return invalid("/result/drive_sync/unmounted", "mount failures outside a mount check")
		}
	}
	result, _ := helperValue.get("result")
	if id, ok := result.get("task_id"); ok && id.kind != kindNull && (id.kind != kindString || id.text != taskID) {
		return invalid("/result/result/task_id", "result task identity mismatch")
	}
	helperSize := compactSize(helperValue)
	if helper.Tool == ToolMCP {
		return checkMcpResultSizes(result, helperSize)
	}
	if data, ok := result.get("data_base64"); ok {
		if helper.Status != "success" || helper.Truncated {
			return invalid("/result/status", "media must be a complete success")
		}
		if helper.Tool != ToolViewImage {
			return invalid("/result/tool", "unexpected media tool")
		}
		if data.kind != kindString {
			return invalid("/result/result/data_base64", "expected a string")
		}
		decoded, err := base64.StdEncoding.DecodeString(data.text)
		if err != nil || !isBase64Text(data.text) || base64.StdEncoding.EncodeToString(decoded) != data.text {
			return invalid("/result/result/data_base64", "noncanonical base64")
		}
		if media, ok := helper.Result.(MediaResult); !ok || int64(len(decoded)) != media.SizeBytes {
			return invalid("/result/result/size_bytes", "false decoded media size")
		}
		if len(decoded) > ViewImageDecodedBytes {
			return invalid("/result/result/data_base64", "decoded media over 10 MiB")
		}
		if helperSize-compactSize(data)+2 > HelperResultJSONBytes {
			return invalid("/result", "non-media helper fields over 256 KiB")
		}
		return nil
	}
	if helperSize > HelperResultJSONBytes {
		return invalid("/result", "ordinary helper envelope over 256 KiB")
	}
	return nil
}

// toolStatusLimit is a report's complete limit: 14 MiB for a view_image
// result that holds its image or a local MCP result that holds media,
// 320 KiB for every other report. The result's tool and type alone choose
// it, so no other field enlarges the bound.
func toolStatusLimit(tool string, helperValue *value) int {
	if helperValue == nil {
		return OrdinaryToolStatusJSONBytes
	}
	result, ok := helperValue.get("result")
	if !ok {
		return OrdinaryToolStatusJSONBytes
	}
	switch tool {
	case ToolViewImage:
		if _, ok := result.get("data_base64"); ok {
			return MediaToolStatusJSONBytes
		}
	case ToolMCP:
		if media, err := mcpMedia(result); err == nil && len(media) > 0 {
			return MediaToolStatusJSONBytes
		}
	}
	return OrdinaryToolStatusJSONBytes
}

// mcpMediaString is one base64 string of a local MCP result: an image or
// audio block's data, or a resource block's blob.
type mcpMediaString struct {
	ptr  string
	data *value
}

// mcpMedia lists the media strings of a local MCP result; a resource block
// whose resource is not an object fails.
func mcpMedia(result *value) ([]mcpMediaString, error) {
	content, _ := result.get("content")
	if content == nil {
		return nil, nil
	}
	var media []mcpMediaString
	for i, block := range content.items {
		at := "/result/result/content/" + strconv.Itoa(i)
		t, _ := block.get("type")
		if t == nil || t.kind != kindString {
			continue
		}
		holder, key := block, "data"
		switch t.text {
		case "image", "audio":
		case "resource":
			resource, ok := block.get("resource")
			if !ok {
				continue
			}
			if resource.kind != kindObject {
				return nil, invalid(at+"/resource", "resource is not an object")
			}
			holder, key, at = resource, "blob", at+"/resource"
		default:
			continue
		}
		if data, ok := holder.get(key); ok && data.kind == kindString {
			media = append(media, mcpMediaString{at + "/" + key, data})
		}
	}
	return media, nil
}

// checkMcpResultSizes applies the budgets of a local MCP result: at most
// 10 MiB of decoded media across its image, audio and resource blocks and an
// envelope of at most 256 KiB without that media.
func checkMcpResultSizes(result *value, helperSize int) error {
	media, err := mcpMedia(result)
	if err != nil {
		return err
	}
	decoded, stripped := 0, helperSize
	for _, m := range media {
		n, ok := decodedBase64Len(m.data.text)
		if !ok {
			return invalid(m.ptr, "invalid base64")
		}
		decoded += n
		stripped -= compactSize(m.data) - 2
	}
	if decoded > ViewImageDecodedBytes {
		return invalid("/result/result/content", "decoded MCP media over 10 MiB")
	}
	if stripped > HelperResultJSONBytes {
		return invalid("/result", "MCP result envelope over 256 KiB")
	}
	return nil
}

// isBase64Text reports whether s holds only the standard alphabet and its
// padding; the decoder of encoding/base64 would also skip line breaks.
func isBase64Text(s string) bool {
	return strings.IndexFunc(s, func(r rune) bool { return !base64Rune(r) }) < 0
}

func base64Rune(r rune) bool {
	return 'A' <= r && r <= 'Z' || 'a' <= r && r <= 'z' || '0' <= r && r <= '9' || r == '+' || r == '/' || r == '='
}

func decodedBase64Len(s string) (int, bool) {
	if !isBase64Text(s) {
		return 0, false
	}
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return 0, false
	}
	return len(decoded), true
}

// DriveSyncReport reports a sync point outside any tool result.
type DriveSyncReport struct {
	ConnectionID string
	MessageID    string
	SessionID    string
	BindingID    string
	TaskID       Optional[string]
	DriveSync    DriveSyncOutcome
}

func driveSyncReport(d *decoder, ptr string, v *value) DriveSyncReport {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	r := DriveSyncReport{
		ConnectionID: required(o, "connection_id", identity),
		MessageID:    required(o, "message_id", identity),
		SessionID:    required(o, "session_id", identity),
		BindingID:    required(o, "binding_id", identity),
		TaskID:       optional(o, "task_id", identity),
		DriveSync:    required(o, "drive_sync", driveSyncOutcome),
	}
	o.open()
	return r
}

func (r DriveSyncReport) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", r.ConnectionID)
	e.stringMember("message_id", r.MessageID)
	e.stringMember("session_id", r.SessionID)
	e.stringMember("binding_id", r.BindingID)
	optionalMember(e, "task_id", r.TaskID, writeString)
	e.valueMember("drive_sync", r.DriveSync)
	e.endObject()
}

func checkDriveSyncReport(r DriveSyncReport, v *value) error {
	if err := checkBodySize(v, DriveSyncReportJSONBytes, "drive sync report over 128 KiB"); err != nil {
		return err
	}
	syncValue, _ := v.get("drive_sync")
	if err := checkDriveSyncOutcome("/drive_sync", r.DriveSync, syncValue); err != nil {
		return err
	}
	if r.DriveSync.Downloading.Set || r.DriveSync.DownloadingTotal.Set {
		return invalid("/drive_sync", "downloads in a drive sync report")
	}
	if r.DriveSync.Unmounted.Set {
		return invalid("/drive_sync/unmounted", "mount failures in a drive sync report")
	}
	return nil
}

// Acknowledgement is Engine's ACK of a report or ready.
type Acknowledgement struct {
	ConnectionID string
	MessageID    string
}

func acknowledgement(d *decoder, ptr string, v *value) Acknowledgement {
	o := d.object(ptr, v)
	a := readAcknowledgement(o)
	o.open()
	return a
}

func readAcknowledgement(o *object) Acknowledgement {
	a := Acknowledgement{
		ConnectionID: required(o, "connection_id", identity),
		MessageID:    required(o, "message_id", identity),
	}
	required(o, "accepted", constBool(true))
	return a
}

func (a Acknowledgement) encode(e *encoder) {
	e.beginObject()
	a.members(e)
	e.endObject()
}

func (a Acknowledgement) members(e *encoder) {
	e.stringMember("connection_id", a.ConnectionID)
	e.stringMember("message_id", a.MessageID)
	e.boolMember("accepted", true)
}

// ProtocolErrorReport reports a downstream message the CLI could not accept.
type ProtocolErrorReport struct {
	ConnectionID     string
	MessageID        string
	RequestMessageID Optional[string]
	Error            ProtocolError // with a ProtocolErrorCode
}

func protocolErrorReport(d *decoder, ptr string, v *value) ProtocolErrorReport {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	r := ProtocolErrorReport{
		ConnectionID:     required(o, "connection_id", identity),
		MessageID:        required(o, "message_id", identity),
		RequestMessageID: optional(o, "request_message_id", identity),
		Error:            required(o, "error", protocolErrorReportError),
	}
	o.open()
	return r
}

func (r ProtocolErrorReport) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", r.ConnectionID)
	e.stringMember("message_id", r.MessageID)
	optionalMember(e, "request_message_id", r.RequestMessageID, writeString)
	e.valueMember("error", r.Error)
	e.endObject()
}

// TransportLimits is the fixed limits table a ready declares; every member is
// one of this file's byte limit constants.
type TransportLimits struct{}

var transportLimits = []struct {
	name  string
	limit int64
}{
	{"metadata_json_bytes", MetadataJSONBytes},
	{"tool_input_json_bytes", ToolInputJSONBytes},
	{"execute_sse_json_bytes", ExecuteSSEJSONBytes},
	{"stdin_utf8_bytes", StdinUTF8Bytes},
	{"stdin_sse_json_bytes", StdinSSEJSONBytes},
	{"default_message_json_bytes", DefaultMessageJSONBytes},
	{"helper_result_json_bytes", HelperResultJSONBytes},
	{"drive_sync_json_bytes", DriveSyncJSONBytes},
	{"ordinary_tool_status_json_bytes", OrdinaryToolStatusJSONBytes},
	{"drive_sync_report_json_bytes", DriveSyncReportJSONBytes},
	{"view_image_decoded_bytes", ViewImageDecodedBytes},
	{"media_tool_status_json_bytes", MediaToolStatusJSONBytes},
}

func transportLimitsTable(d *decoder, ptr string, v *value) TransportLimits {
	o := d.object(ptr, v)
	for _, l := range transportLimits {
		required(o, l.name, constInteger(l.limit))
	}
	o.closed()
	return TransportLimits{}
}

func (TransportLimits) encode(e *encoder) {
	e.beginObject()
	for _, l := range transportLimits {
		e.integerMember(l.name, l.limit)
	}
	e.endObject()
}

// ManagedNetworkCapability is what managed network supports on a machine.
type ManagedNetworkCapability struct {
	Transports []string // tcp, udp
	Listen     bool
}

func managedNetworkCapability(d *decoder, ptr string, v *value) ManagedNetworkCapability {
	o := d.object(ptr, v)
	c := ManagedNetworkCapability{
		Transports: required(o, "transports", uniqueStrings(arrayOf(enum("tcp", "udp"), 0, -1))),
		Listen:     required(o, "listen", boolean),
	}
	o.closed()
	return c
}

func (c ManagedNetworkCapability) encode(e *encoder) {
	e.beginObject()
	e.key("transports")
	e.strings(c.Transports)
	e.boolMember("listen", c.Listen)
	e.endObject()
}

// ToolExecutionCapability is the tool contract a ready declares.
type ToolExecutionCapability struct {
	Tools                 []string // exec_command, edit, view_image
	Controls              []string // stdin, cancel
	SandboxPolicyVersions []int64
	NetworkModes          []string                           // disabled, direct, managed
	ManagedNetwork        Optional[ManagedNetworkCapability] // set exactly when NetworkModes holds managed
	Limits                TransportLimits
}

func toolExecutionCapability(d *decoder, ptr string, v *value) ToolExecutionCapability {
	o := d.object(ptr, v)
	required(o, "schema_version", constInteger(toolExecutionSchemaVersion))
	c := ToolExecutionCapability{
		Tools:                 required(o, "tools", uniqueStrings(arrayOf(enum(ToolExecCommand, ToolEdit, ToolViewImage), 0, -1))),
		Controls:              required(o, "controls", uniqueStrings(arrayOf(enum(ToolStdin, ToolCancel), 0, -1))),
		SandboxPolicyVersions: required(o, "sandbox_policy_versions", arrayOf(constInteger(sandboxPolicySchemaVersion), 0, 1)),
		NetworkModes:          required(o, "network_modes", uniqueStrings(arrayOf(enum("disabled", "direct", "managed"), 0, -1))),
	}
	if slices.Contains(c.NetworkModes, "managed") {
		c.ManagedNetwork = Some(required(o, "managed_network", managedNetworkCapability))
	} else {
		o.absent("managed_network")
	}
	c.Limits = required(o, "limits", transportLimitsTable)
	o.open()
	return c
}

func (c ToolExecutionCapability) encode(e *encoder) {
	e.beginObject()
	e.integerMember("schema_version", toolExecutionSchemaVersion)
	e.key("tools")
	e.strings(c.Tools)
	e.key("controls")
	e.strings(c.Controls)
	e.key("sandbox_policy_versions")
	e.begin('[')
	for _, version := range c.SandboxPolicyVersions {
		e.next()
		e.integer(version)
	}
	e.end(']')
	e.key("network_modes")
	e.strings(c.NetworkModes)
	optionalMember(e, "managed_network", c.ManagedNetwork, writeValue)
	e.valueMember("limits", c.Limits)
	e.endObject()
}

// LocalMcpCapability declares local MCP servers on a machine.
type LocalMcpCapability struct{}

func localMcpCapability(d *decoder, ptr string, v *value) LocalMcpCapability {
	o := d.object(ptr, v)
	required(o, "schema_version", constInteger(toolExecutionSchemaVersion))
	o.open()
	return LocalMcpCapability{}
}

func (LocalMcpCapability) encode(e *encoder) {
	e.beginObject()
	e.integerMember("schema_version", toolExecutionSchemaVersion)
	e.endObject()
}

// ReadyExtension is the part of a ready that tool-execution-v1 defines.
type ReadyExtension struct {
	ToolExecution ToolExecutionCapability
	LocalMcp      Optional[LocalMcpCapability]
}

func readyExtension(d *decoder, ptr string, v *value) ReadyExtension {
	o := d.object(ptr, v)
	r := ReadyExtension{
		ToolExecution: required(o, "tool_execution", toolExecutionCapability),
		LocalMcp:      optional(o, "local_mcp", localMcpCapability),
	}
	o.open()
	return r
}

func (r ReadyExtension) encode(e *encoder) {
	e.beginObject()
	e.valueMember("tool_execution", r.ToolExecution)
	optionalMember(e, "local_mcp", r.LocalMcp, writeValue)
	e.endObject()
}

// checkCapabilities applies the relations between the declared tools,
// controls and local MCP: every executable tool needs cancel, exec_command
// also stdin, and local MCP cancel.
func checkCapabilities(ptr string, c ToolExecutionCapability, localMcp bool) error {
	cancel, stdin := slices.Contains(c.Controls, ToolCancel), slices.Contains(c.Controls, ToolStdin)
	switch {
	case len(c.Tools) > 0 && !cancel:
		return invalid(ptr+"/controls", "executable tools require cancel")
	case slices.Contains(c.Tools, ToolExecCommand) && (!stdin || !cancel):
		return invalid(ptr+"/controls", "exec_command requires stdin and cancel")
	case localMcp && !cancel:
		return invalid(ptr+"/controls", "local MCP requires cancel")
	}
	return nil
}

func checkReadyExtension(r ReadyExtension, v *value) error {
	if err := checkCapabilities("/tool_execution", r.ToolExecution, r.LocalMcp.Set); err != nil {
		return err
	}
	return checkBodySize(v, DefaultMessageJSONBytes, "ready capability over 64 KiB")
}

var toolDownstream = downstream(toolPayloads)

// toolTokens runs check after requiring protocol_version and report_seq to be
// integer tokens wherever a tool execution message carries them.
func toolTokens[T any](check func(T, *value) error) func(T, *value) error {
	return integerTokens(check, "protocol_version", "report_seq")
}

// toolChecked decodes a tool execution message with f and check under
// toolTokens.
func toolChecked[T any](data []byte, f decodeFunc[T], check func(T, *value) error) (T, error) {
	return decodeChecked(data, f, toolTokens(check))
}

// DecodeToolDownstream decodes a tool or local MCP instruction, the
// Downstream of tool-execution-v1.
func DecodeToolDownstream(data []byte) (Downstream, error) {
	return toolChecked(data, toolDownstream, checkToolDownstream)
}

func DecodeToolStatus(data []byte) (ToolStatus, error) {
	return toolChecked(data, toolStatus, checkToolStatus)
}

func DecodeDriveSyncReport(data []byte) (DriveSyncReport, error) {
	return toolChecked(data, driveSyncReport, checkDriveSyncReport)
}

func DecodeAcknowledgement(data []byte) (Acknowledgement, error) {
	return toolChecked(data, acknowledgement, messageSizeCheck[Acknowledgement](DefaultMessageJSONBytes, "body over 64 KiB"))
}

func DecodeProtocolErrorReport(data []byte) (ProtocolErrorReport, error) {
	return toolChecked(data, protocolErrorReport, bodySizeCheck[ProtocolErrorReport](DefaultMessageJSONBytes, "body over 64 KiB"))
}

// DecodeReadyExtension decodes the tool part of a ready.
func DecodeReadyExtension(data []byte) (ReadyExtension, error) {
	return toolChecked(data, readyExtension, checkReadyExtension)
}

// DecodeSandboxPolicy decodes a sandbox policy on its own.
func DecodeSandboxPolicy(data []byte) (SandboxPolicy, error) {
	return toolChecked(data, sandboxPolicy, checkPolicy)
}

func checkPolicy(p SandboxPolicy, _ *value) error { return checkSandboxPolicy("", p) }
