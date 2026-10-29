package protocol

import (
	"bytes"
	"errors"
	"net/http"
	"net/url"
	"strconv"
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
	envelope := func(data []byte) func() error {
		return func() error {
			_, err := DecodeDownstream(data)
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
	long := func(n int) string { return strings.Repeat("v", n) }
	files := func(paths ...string) func() error {
		entries := make([]string, len(paths))
		for i, path := range paths {
			entries[i] = `{"path":"` + path + `","version_id":"v1","content_sha256":"331d26d6d8f862e46ba900811be8a7a1e4dbaa229b14c99becfd5e5151490d95","size_bytes":7,"executable":false}`
		}
		return func() error {
			_, err := DecodeListFilesResponse([]byte(`{"files":[`+strings.Join(entries, ",")+`],"has_more":false}`), ListFilesResponseContext{})
			return err
		}
	}
	upload := func(length ...string) func() error {
		header := http.Header{"Content-Type": {"application/octet-stream"}}
		if len(length) > 0 {
			header["Content-Length"] = length
		}
		return func() error {
			h, err := DecodeUploadContentRequestHeaders(header)
			if err != nil {
				return errors.New("the headers did not decode: " + err.Error())
			}
			return CheckUploadLength(h)
		}
	}
	commit := func(n int) []byte {
		changes := make([]string, n)
		for i := range changes {
			changes[i] = `{"path":"f` + strconv.Itoa(i) + `","change":"delete","base_sha256":"331d26d6d8f862e46ba900811be8a7a1e4dbaa229b14c99becfd5e5151490d95"}`
		}
		return []byte(`{"protocol_version":1,"connection_id":"conn","binding_id":"bind","changes":[` + strings.Join(changes, ",") + `]}`)
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
		{name: "a report_seq string in a tool instruction decoded as any SSE envelope", decode: envelope(execute("", "", `,"report_seq":"x"`)), pointer: "/report_seq"},
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
		{
			name: "a files page over 4 MiB",
			decode: func() error {
				_, err := DecodeListFilesResponse([]byte(`{"files":[{"path":"a.md","version_id":"`+long(ListPageMaxJSONBytes)+
					`","content_sha256":"331d26d6d8f862e46ba900811be8a7a1e4dbaa229b14c99becfd5e5151490d95","size_bytes":7,"executable":false}],"has_more":false}`), ListFilesResponseContext{})
				return err
			},
		},
		{
			name: "a protocol_version fraction on a files page",
			decode: func() error {
				_, err := DecodeListFilesResponse([]byte(`{"files":[],"has_more":false,"protocol_version":1.0}`), ListFilesResponseContext{})
				return err
			},
			pointer: "/protocol_version",
		},
		{name: "paths that differ only in the case of a Cherokee letter from U+13A0 to U+13EF", decode: files("\u13a0", "\uab70"), pointer: "/files/1/path"},
		{name: "paths that differ only in the case of a Cherokee letter from U+13F0 to U+13F5", decode: files("\u13f0", "\u13f8"), pointer: "/files/1/path"},
		{
			name: "a changes page over 4 MiB",
			decode: func() error {
				_, err := DecodeListChangesResponse([]byte(`{"records":[{"seq":42,"index":0,"path":"a.md","operation":"deleted","version_id":"`+
					long(ListPageMaxJSONBytes)+`","content_sha256":null,"size_bytes":0,"executable":false}],"has_more":false}`), ListChangesResponseContext{})
				return err
			},
		},
		{
			name: "a commit response over 16 MiB",
			decode: func() error {
				_, err := DecodeCommitResponse([]byte(`{"seq":43,"results":[{"status":"accepted","version_id":"`+
					long(CommitResponseMaxJSONBytes)+`"}]}`), CommitResponseContext{})
				return err
			},
		},
		{
			name: "an accepted commit result with a rule over 512 bytes",
			decode: func() error {
				_, err := DecodeCommitResponse([]byte(`{"seq":43,"results":[{"status":"accepted","version_id":"v1","rule":"`+
					long(RuleMaxUTF8Bytes+1)+`"}]}`), CommitResponseContext{})
				return err
			},
			pointer: "/results/0/rule",
		},
		{
			name: "an accepted commit result with a rule of 257 two-byte characters",
			decode: func() error {
				_, err := DecodeCommitResponse([]byte(`{"seq":43,"results":[{"status":"accepted","version_id":"v1","rule":"`+
					strings.Repeat("é", RuleMaxUTF8Bytes/2+1)+`"}]}`), CommitResponseContext{})
				return err
			},
			pointer: "/results/0/rule",
		},
		{
			name: "a header the definition models, given twice",
			decode: func() error {
				_, err := DecodeDownloadContentRequestHeaders(http.Header{"Range": {"bytes=0-1", "bytes=2-3"}})
				return err
			},
			pointer: "/Range",
		},
		{
			name: "a header the definition does not model, given twice, is not read",
			check: func(t *testing.T) {
				if _, err := DecodeDownloadContentRequestHeaders(http.Header{"Range": {"bytes=0-1"}, "Accept": {"a", "b"}}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{name: "an upload without Content-Length decodes and fails the length check", decode: upload()},
		{name: "an upload of 500,000,000 bytes passes the length check", check: accepted(upload("500000000"))},
		{
			name:    "an upload of 500,000,001 bytes decodes and fails the length check",
			decode:  upload("500000001"),
			pointer: "/Content-Length",
			status:  http.StatusRequestEntityTooLarge,
			code:    ErrorFileTooLarge,
		},
		{
			name: "a commit body over 16 MiB, answered before any other rule",
			decode: func() error {
				_, err := DecodeCommitRequest(bytes.Repeat([]byte("x"), CommitRequestMaxJSONBytes+1))
				return err
			},
			status: http.StatusRequestEntityTooLarge,
			code:   ErrorRequestTooLarge,
		},
		{
			name: "a commit of 10,000 changes passes the change count",
			check: func(t *testing.T) {
				r, err := DecodeCommitRequest(commit(CommitMaxChanges))
				if err != nil {
					t.Fatal(err)
				}
				if err := CheckCommitChanges(r); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "a commit of 10,001 changes decodes and fails the change count",
			decode: func() error {
				r, err := DecodeCommitRequest(commit(CommitMaxChanges + 1))
				if err != nil {
					return errors.New("the commit did not decode: " + err.Error())
				}
				return CheckCommitChanges(r)
			},
			pointer: "/changes",
			status:  http.StatusRequestEntityTooLarge,
			code:    ErrorRequestTooLarge,
		},
		{
			name: "a commit of 10,001 changes with one path twice fails as a duplicate before the change count",
			decode: func() error {
				_, err := DecodeCommitRequest(bytes.Replace(commit(CommitMaxChanges+1), []byte(`"path":"f10000"`), []byte(`"path":"f0"`), 1))
				return err
			},
			pointer: "/changes/10000/path",
		},
		{name: "a path of 30 non-starters in its NFKD form", check: accepted(files("\u00e9" + strings.Repeat("\u0301", 29)))},
		{name: "a path of 31 non-starters in its NFKD form", decode: files("\u00e9" + strings.Repeat("\u0301", 30)), pointer: "/files/0/path"},
		{
			name: "a Sha256 followed by a line feed",
			decode: func() error {
				_, err := DecodeListFilesResponse([]byte(`{"files":[{"path":"a.md","version_id":"v1",`+
					`"content_sha256":"331d26d6d8f862e46ba900811be8a7a1e4dbaa229b14c99becfd5e5151490d95\n","size_bytes":7,"executable":false}],"has_more":false}`), ListFilesResponseContext{})
				return err
			},
			pointer: "/files/0/content_sha256",
		},
		{
			name: "a drive.changed notice over 64 KiB",
			decode: func() error {
				_, err := DecodeDriveChanged([]byte(`{"protocol_version":1,"connection_id":"conn","message_id":"msg","type":"drive.changed",` +
					`"payload":{"drive_id":"drive","seq":43},"padding":"` + long(DriveChangedSSEMaxJSONBytes) + `"}`))
				return err
			},
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
