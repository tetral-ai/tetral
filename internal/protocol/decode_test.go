package protocol

import (
	"bytes"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

const executeTemplate = `{"protocol_version":1,"connection_id":"conn","message_id":"msg","type":"tool.execute",` +
	`"payload":{"session_id":"sesn","binding_id":"bind","tool_use_event_id":"use","task_id":"task","tool":"exec_command",` +
	`"input":{"cmd":"true"INPUT},"sandbox_policy":{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"disabledNETWORK"}}}TOP}`

func execute(input, network, top string) []byte {
	return []byte(strings.NewReplacer("INPUT", input, "NETWORK", network, "TOP", top).Replace(executeTemplate))
}

// statusTemplate is an ordinary terminal report whose result envelope holds
// an unknown member PAD long.
const statusTemplate = `{"connection_id":"conn","message_id":"msg","protocol_version":1,"session_id":"sesn","binding_id":"bind",` +
	`"tool_use_event_id":"use","task_id":"task","request_message_id":"execute_1","kind":"execution","phase":"terminal","report_seq":1,` +
	`"result":{"schema_version":1,"tool":"exec_command","status":"success","truncated":false,"error":null,"result":{"task_id":"task",` +
	`"exit_code":0,"signal":null,"timed_out":false,"stdout":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false},` +
	`"stderr":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false},"duration_ms":12},` +
	`"metrics":{"wall_time_ms":1250,"queued_ms":3,"drive_wait_ms":40,"run_ms":1180,"sync_ms":20},"padding":"PAD"}}`

// ordinaryReport is that report padded to total bytes.
func ordinaryReport(total int) []byte {
	return []byte(strings.Replace(statusTemplate, "PAD", strings.Repeat("v", total-len(statusTemplate)+3), 1))
}

// escapedHeartbeat is a heartbeat of written bytes whose unknown member is
// written as \u0061 escapes, six bytes for each one-byte character.
func escapedHeartbeat(written int) []byte {
	const template = `{"protocol_version":1,"connection_id":"conn","message_id":"hb","request_message_id":"msg_hb","heartbeat_seq":7,"padding":"PAD"}`
	n := written - len(template) + 3
	return []byte(strings.Replace(template, "PAD", strings.Repeat(`\u0061`, n/6)+strings.Repeat("a", n%6), 1))
}

// managedPolicy is a managed network policy with rules.
func managedPolicy(rules string) []byte {
	return []byte(`{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"managed","rules":` + rules + `}}`)
}

func allowRule(kind, value string) []byte {
	return managedPolicy(`{"allow":[{"destination":{"kind":"` + kind + `","value":"` + value + `"},"transport":"tcp","ports":[{"from":443,"to":443}]}],"deny":[],"listen":[]}`)
}

func TestDecodingRulesBeyondTheFixtures(t *testing.T) {
	tool := func(data []byte) func() error {
		return func() error {
			_, err := DecodeToolDownstream(data)
			return err
		}
	}
	events := func(query url.Values) func() error {
		return func() error {
			_, err := DecodeEventsQuery(query)
			return err
		}
	}
	waitMS := func(t *testing.T, data []byte) Nullable[int64] {
		t.Helper()
		m, err := DecodeToolDownstream(data)
		if err != nil {
			t.Fatal(err)
		}
		return m.Payload.(ExecutePayload).Input.(ExecInput).WaitMS
	}
	sandbox := func(data []byte) func() error {
		return func() error {
			_, err := DecodeSandboxPolicy(data)
			return err
		}
	}
	accepted := func(decode func() error) func(t *testing.T) {
		return func(t *testing.T) {
			if err := decode(); err != nil {
				t.Fatal(err)
			}
		}
	}
	tests := []struct {
		name string
		// decode, pointer and status name a rejected input, the pointer of
		// its failing value and its answer when that is not 400
		// invalid_message; check inspects an accepted one.
		decode  func() error
		pointer string
		status  int
		code    ErrorCode
		check   func(t *testing.T)
	}{
		{
			name: "an integer above 2^53 keeps its exact text",
			check: func(t *testing.T) {
				m, err := DecodeToolDownstream(execute(`,"wait_ms":9007199254740993`, "", ""))
				if err != nil {
					t.Fatal(err)
				}
				if got := Encode(m); !bytes.Contains(got, []byte(`"wait_ms":9007199254740993`)) {
					t.Fatalf("encoding lost the integer: %s", got)
				}
			},
		},
		{name: "an integer above the signed 64-bit range", decode: tool(execute(`,"wait_ms":9223372036854775808`, "", "")), pointer: "/payload/input/wait_ms"},
		{name: "a duplicate key inside payload, equal only after unescaping", decode: tool(execute(`,"\u0063md":"false"`, "", "")), pointer: "/payload/input/cmd"},
		{name: "a lone surrogate escape", decode: tool(execute(`,"cwd":"\ud800"`, "", "")), pointer: "/payload/input/cwd"},
		{name: "invalid UTF-8 in a string", decode: tool(execute(",\"cwd\":\"\xff\"", "", "")), pointer: "/payload/input/cwd"},
		{name: "a number beyond the float64 range", decode: tool(execute("", "", `,"priority":1e400`)), pointer: "/priority"},
		{name: "an unknown member of the network policy", decode: tool(execute("", `","allow_all":"yes`, "")), pointer: "/payload/sandbox_policy/network/allow_all"},
		{
			name: "an unknown envelope member",
			check: func(t *testing.T) {
				if _, err := DecodeToolDownstream(execute("", "", `,"priority":"high"`)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "an omitted wait_ms is absent",
			check: func(t *testing.T) {
				if got := waitMS(t, execute("", "", "")); got.Present || got.Null {
					t.Fatalf("wait_ms = %+v, want absent", got)
				}
			},
		},
		{
			name: "a null wait_ms is null",
			check: func(t *testing.T) {
				if got := waitMS(t, execute(`,"wait_ms":null`, "", "")); got.Present || !got.Null {
					t.Fatalf("wait_ms = %+v, want null", got)
				}
			},
		},
		{
			name: "a protocol_version string where the definition does not model the member",
			decode: func() error {
				_, err := DecodeAcknowledgement([]byte(`{"connection_id":"conn","message_id":"msg","accepted":true,"protocol_version":"1"}`))
				return err
			},
			pointer: "/protocol_version",
		},
		{name: "a report_seq fraction where the definition does not model the member", decode: tool(execute("", "", `,"report_seq":1.0`)), pointer: "/report_seq"},
		{
			name:   "an ordinary report over 320 KiB, whose envelope is also over its own limit",
			decode: func() error { _, err := DecodeToolStatus(ordinaryReport(OrdinaryToolStatusJSONBytes + 1)); return err },
			status: http.StatusRequestEntityTooLarge,
			code:   ErrorRequestTooLarge,
		},
		{
			name:    "an ordinary report of 320 KiB whose envelope is over 256 KiB",
			decode:  func() error { _, err := DecodeToolStatus(ordinaryReport(OrdinaryToolStatusJSONBytes)); return err },
			pointer: "/result",
		},
		{name: "a host converted as a whole, its ideographic full stop and one trailing dot included", check: accepted(sandbox(allowRule("host", "B\u00fccher\u3002DE.")))},
		{name: "a host holding U+200D between two letters", decode: sandbox(allowRule("host", "a\u200db.example")), pointer: "/network/rules/allow/0/destination/value"},
		{name: "a CIDR in netmask form", decode: sandbox(allowRule("cidr", "10.0.0.0/255.0.0.0")), pointer: "/network/rules/allow/0/destination/value"},
		{name: "a CIDR prefix length with a leading zero", decode: sandbox(allowRule("cidr", "10.0.0.0/08")), pointer: "/network/rules/allow/0/destination/value"},
		{
			name:    "a listen address with a zone",
			decode:  sandbox(managedPolicy(`{"allow":[],"deny":[],"listen":[{"address":"fe80::1%a","transport":"tcp","ports":[{"from":8080,"to":8080}]}]}`)),
			pointer: "/network/rules/listen/0/address",
		},
		{
			name:    "a query parameter given twice",
			decode:  events(url.Values{"protocol_version": {"1", "1"}, "instance_id": {"inst_1"}, "credential_id": {"ccred_1"}}),
			pointer: "/protocol_version",
		},
		{
			name:    "invalid UTF-8 in a query parameter",
			decode:  events(url.Values{"protocol_version": {"1"}, "instance_id": {"inst_\xff"}, "credential_id": {"ccred_1"}}),
			pointer: "/instance_id",
		},
		{
			name:    "a query parameter given twice is reported before a value an earlier parameter breaks",
			decode:  events(url.Values{"protocol_version": {"2"}, "instance_id": {"inst_1", "inst_2"}, "credential_id": {"ccred_1"}}),
			pointer: "/instance_id",
		},
		{
			name: "a query parameter the definition does not model, given twice and not in UTF-8, is not read",
			check: func(t *testing.T) {
				if _, err := DecodeEventsQuery(url.Values{"protocol_version": {"1"}, "instance_id": {"inst_1"}, "credential_id": {"ccred_1"}, "trace": {"\xff", "b"}}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "a heartbeat over 64 KiB as written, though not with its escapes undone",
			decode: func() error { _, err := DecodeHeartbeat(escapedHeartbeat(DefaultMessageJSONBytes + 1)); return err },
			status: http.StatusRequestEntityTooLarge,
			code:   ErrorRequestTooLarge,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.check != nil {
				test.check(t)
				return
			}
			status, code := test.status, test.code
			if status == 0 {
				status, code = http.StatusBadRequest, ErrorInvalidMessage
			}
			err := test.decode()
			var decodeErr *DecodeError
			if !errors.As(err, &decodeErr) {
				t.Fatalf("decoded: %v", err)
			}
			if decodeErr.Pointer != test.pointer || decodeErr.Status != status || decodeErr.Code != code {
				t.Fatalf("got %+v, want %d %s at %q", decodeErr, status, code, test.pointer)
			}
		})
	}
}
