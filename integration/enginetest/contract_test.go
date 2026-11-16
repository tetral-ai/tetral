// Layer 1: the protocol check against an in-process stand-in for Engine.
package enginetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tetral-ai/tetral/internal/protocol"
	contractfiles "github.com/tetral-ai/tetral/protocol"
)

// fixture is one entry of a protocol fixture file, as internal/protocol
// reads it.
type fixture struct {
	Name       string                         `json:"name"`
	Schema     string                         `json:"schema"`
	Valid      bool                           `json:"valid"`
	Value      json.RawMessage                `json:"value"`
	HTTPStatus int                            `json:"http_status"`
	Endpoint   protocol.DriveTransferEndpoint `json:"endpoint"`
	Status     int                            `json:"status"`
	Pair       map[string]string              `json:"pair"`
	Expect     []string                       `json:"expect"`
}

func readFixtures(t *testing.T, file string) []fixture {
	t.Helper()
	data, err := contractfiles.Files.ReadFile(file + ".fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []fixture
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

// TestTheCheckAcceptsEveryValidFixtureAndRefusesEveryInvalidOne sends or
// receives each fixture of protocol/ as the check does a message of a
// request: after the request parts its pair names, under its status and
// endpoint.
func TestTheCheckAcceptsEveryValidFixtureAndRefusesEveryInvalidOne(t *testing.T) {
	for _, file := range []string{connectionFile, toolFile, driveFile} {
		t.Run(file, func(t *testing.T) {
			fixtures := readFixtures(t, file)
			byName := make(map[string]fixture, len(fixtures))
			for _, f := range fixtures {
				byName[f.Name] = f
			}
			for _, f := range fixtures {
				t.Run(f.Name, func(t *testing.T) {
					d := definition{file, f.Schema}
					x := exchange{status: f.HTTPStatus + f.Status, endpoint: f.Endpoint}
					for _, name := range f.Pair {
						request := byName[name]
						if err := x.sent(definition{file, request.Schema}, request.Value); err != nil {
							t.Fatalf("paired fixture %s: %v", name, err)
						}
					}
					var err error
					if parts[d].sent {
						err = x.sent(d, f.Value)
					} else {
						err = x.received(d, f.Value)
					}
					if f.Valid && err != nil {
						t.Fatalf("valid fixture refused: %v", err)
					}
					if !f.Valid && err == nil {
						t.Fatal("invalid fixture accepted")
					}
					// A fixture that names the schema keyword it breaks
					// is refused by the schema itself.
					for _, rule := range f.Expect {
						if !strings.HasPrefix(rule, "semantic:") && !f.Valid && validate(d, f.Value) == nil && !decimalAsInteger[f.Name] {
							t.Fatalf("the schema accepts a fixture that breaks %s", rule)
						}
					}
				})
			}
		})
	}
}

// decimalAsInteger names the fixtures whose number has a zero fraction,
// which JSON Schema counts as an integer and the contract's decoders
// refuse.
var decimalAsInteger = map[string]bool{"list_changes_float_seq": true}

// failures records what a CLI reports.
type failures struct {
	mu       sync.Mutex
	messages []string
}

func (f *failures) Helper() {}

func (f *failures) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, fmt.Sprintf(format, args...))
}

func event(messageID, kind, payload string) string {
	return fmt.Sprintf(`data: {"protocol_version":1,"message_id":%q,"connection_id":"conn_example","type":%q,"payload":%s}`+"\n\n", messageID, kind, payload)
}

const (
	connected = `data: {"protocol_version":1,"message_id":"msg_connected","connection_id":"conn_example","type":"connected","payload":{"resumed":false}}` + "\n\n"
	resumed   = `data: {"protocol_version":1,"message_id":"msg_resumed","connection_id":"conn_example","type":"connected","payload":{"resumed":true}}` + "\n\n"
	prepare   = `{"session_id":"sess_example","binding_id":"bind_example","cwd":"/workspace","drives":[{"drive_id":"drive_team_ai","access":"read_write"}],"drive_exclude":[],"repositories":[]}`
	changed   = `{"drive_id":"drive_team_ai","seq":5}`
	closing   = `{"session_id":"sess_example","binding_id":"bind_example"}`
	// filesPath and contentPath are drive transfer requests of the
	// fixtures' connection and binding.
	filesPath   = "/v1/client/drives/id_drive_id/files?protocol_version=1&connection_id=id_connection_id&binding_id=id_binding_id"
	contentPath = "/v1/client/drives/id_drive_id/content/a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447?protocol_version=1&connection_id=id_connection_id&binding_id=id_binding_id"
)

// acknowledge answers a request with the ACK of its identity, with a
// grant for a ready or heartbeat.
func acknowledge(w http.ResponseWriter, r *http.Request) {
	var identity struct {
		ConnectionID string `json:"connection_id"`
		MessageID    string `json:"message_id"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &identity)
	grant := ""
	if r.URL.Path == "/v1/client/ready" || r.URL.Path == "/v1/client/heartbeat" {
		grant = `,"valid_for_ms":45000`
	}
	_, _ = fmt.Fprintf(w, `{"connection_id":%q,"message_id":%q,"accepted":true%s}`, identity.ConnectionID, identity.MessageID, grant)
}

func answers(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// TestACLIReportsEachMessageOutsideTheContract runs a CLI against a stub
// that sends or accepts one message outside the contract, and checks that
// the CLI reports that message to its test and nothing else.
func TestACLIReportsEachMessageOutsideTheContract(t *testing.T) {
	values := map[string]json.RawMessage{}
	for _, file := range []string{connectionFile, toolFile, driveFile} {
		for _, f := range readFixtures(t, file) {
			values[f.Name] = f.Value
		}
	}
	valid := func(name string) []byte {
		value, found := values[name]
		if !found {
			t.Fatalf("no fixture %s", name)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, value); err != nil {
			t.Fatal(err)
		}
		return compact.Bytes()
	}
	without := func(name, member string) []byte {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(valid(name), &object); err != nil {
			t.Fatal(err)
		}
		delete(object, member)
		data, _ := json.Marshal(object)
		return data
	}
	padded := func(name string) []byte {
		data := valid(name)
		return []byte(string(data[:len(data)-1]) + `,"padding":"` + strings.Repeat("a", protocol.DefaultMessageJSONBytes) + `"}`)
	}
	ctx := t.Context()
	read := func(cli *CLI) {
		stream, _, err := cli.OpenEvents(ctx, cli.EventsQuery("", nil))
		if err != nil || stream == nil {
			t.Errorf("open events: %v", err)
			return
		}
		defer func() { _ = stream.Close() }()
		for {
			if _, err := stream.Next(); err == io.EOF {
				return
			}
		}
	}
	post := func(path string, body []byte) func(*CLI) {
		return func(cli *CLI) {
			if _, err := cli.Post(ctx, path, body); err != nil {
				t.Error(err)
			}
		}
	}
	request := func(method, target string, header http.Header, body []byte) func(*CLI) {
		return func(cli *CLI) {
			if _, err := cli.Request(ctx, method, target, header, body); err != nil {
				t.Error(err)
			}
		}
	}
	// prepared plays a connection whose ready declared drive mounting and
	// whose binding of drive_team_ai, a drive with a commit, is prepared.
	prepared := func(cli *CLI) {
		read(cli)
		post("/v1/client/ready", valid("ready_tools_and_local_mcp"))(cli)
		post("/v1/client/session-status", valid("prepared_report_minimal"))(cli)
	}
	first := connected + event("msg_prepare", "session.prepare", prepare) + event("msg_changed", "drive.changed", changed)
	upload := http.Header{"Content-Type": {"application/octet-stream"}}

	rows := []struct {
		name string
		// streams is what the stub writes on each event stream in turn;
		// answer answers every other request.
		streams []string
		answer  http.HandlerFunc
		run     func(*CLI)
		// want is part of the one report the row expects, or empty for
		// none.
		want string
	}{
		// The stream rules.
		{name: "the first event is not connected", streams: []string{event("msg_hb", "heartbeat", `{"heartbeat_seq":1}`)}, run: read,
			want: "the first event is heartbeat, not connected"},
		{name: "an event of two data lines", streams: []string{connected + "data: {}\ndata: {}\n\n"}, run: read,
			want: "not one data line"},
		{name: "an event over 64 KiB", streams: []string{connected + "data: " + string(padded("heartbeat_instruction")) + "\n\n"}, run: read,
			want: "over 64 KiB"},
		{name: "a body over 64 KiB", answer: answers(http.StatusRequestEntityTooLarge, `{"code":"request_too_large","message":"too large"}`),
			run: post("/v1/client/ready", padded("ready_no_tools")), want: "over 64 KiB"},
		{name: "a message twice on one stream", run: read,
			streams: []string{connected + event("msg_hb", "heartbeat", `{"heartbeat_seq":1}`) + event("msg_hb", "heartbeat", `{"heartbeat_seq":1}`)},
			want:    "message msg_hb is on the stream twice"},
		{name: "an instruction before a start notice", answer: acknowledge,
			streams: []string{first, resumed + event("msg_close", "session.close", closing)},
			run:     func(cli *CLI) { prepared(cli); read(cli) },
			want:    "session.close comes before the stream's drive.changed of drive drive_team_ai"},
		{name: "the start notices before an instruction", answer: acknowledge,
			streams: []string{first, resumed + event("msg_changed_again", "drive.changed", changed) + event("msg_close", "session.close", closing)},
			run:     func(cli *CLI) { prepared(cli); read(cli) }},
		{name: "no start notice owed once the closed report is sent", answer: acknowledge,
			streams: []string{first, resumed + event("msg_prepare_again", "session.prepare", prepare)},
			run: func(cli *CLI) {
				prepared(cli)
				post("/v1/client/session-status", valid("closed_report"))(cli)
				read(cli)
			}},

		// One message outside each schema file.
		{name: "connection schema: a ready ACK without its grant", answer: answers(http.StatusOK, `{"connection_id":"conn_example","message_id":"ready_1","accepted":true}`),
			run: post("/v1/client/ready", valid("ready_no_tools")), want: "connection-v1.schema.json#/$defs/GrantAcknowledgement"},
		{name: "tool execution schema: a report without its task", answer: answers(http.StatusBadRequest, `{"code":"invalid_message","message":"no task"}`),
			run: post("/v1/client/tool-status", without("running", "task_id")), want: "tool-execution-v1.schema.json#/$defs/ToolStatus"},
		{name: "drive transfer schema: a page whose has_more is a string", answer: answers(http.StatusOK, `{"files":[],"has_more":"no","start_seq":0}`),
			run: request(http.MethodGet, filesPath, nil, nil), want: "drive-transfer-v1.schema.json#/$defs/ListFilesResponse"},

		// A request with its answer.
		{name: "a code under another status", answer: answers(http.StatusConflict, `{"code":"invalid_message","message":"no"}`),
			run: post("/v1/client/ready", valid("ready_no_tools")), want: "answer 409 outside the contract"},
		{name: "an ACK of another message", answer: answers(http.StatusOK, `{"connection_id":"conn_example","message_id":"ready_2","accepted":true,"valid_for_ms":45000}`),
			run: post("/v1/client/ready", valid("ready_no_tools")), want: "acknowledges ready_2 of conn_example"},
		{name: "a credential that may be stored", answer: answers(http.StatusOK, string(valid("credential_response"))),
			run: post("/v1/client/credential", valid("credential_request")), want: "not no-store"},
		{name: "a success status the endpoint does not have", answer: answers(http.StatusCreated, `{}`),
			run: post("/v1/client/ready", valid("ready_no_tools")), want: "answer 201 is no success answer of the endpoint"},
		{name: "start_seq on a later page", answer: answers(http.StatusOK, `{"files":[],"has_more":false,"start_seq":0}`),
			run: request(http.MethodGet, filesPath+"&after_path=docs/b.md", nil, nil), want: "start_seq"},
		{name: "a code the endpoint does not list", answer: answers(http.StatusGone, `{"code":"drive_cursor_expired","message":"list again"}`),
			run: request(http.MethodGet, filesPath, nil, nil), want: "answer 410 outside the contract"},
		{name: "an upload answer for other content", answer: answers(http.StatusOK, `{"content_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size_bytes":12}`),
			run: request(http.MethodPut, contentPath, upload, []byte("hello world\n")), want: "answer 200 outside the contract"},
		{name: "the whole content for a range", answer: answers(http.StatusOK, "hello world\n"),
			run: request(http.MethodGet, contentPath, http.Header{"Range": {"bytes=0-4"}}, nil), want: "answer 200 to a download with a Range"},
		{name: "a rate limit without its wait", answer: answers(http.StatusTooManyRequests, `{"code":"rate_limited","message":"wait"}`),
			run: request(http.MethodPut, contentPath, upload, []byte("hello world\n")), want: "drive-transfer-v1.schema.json#/$defs/RateLimitedResponseHeaders"},

		// The Invalid mark.
		{name: "a request to no endpoint", answer: answers(http.StatusMethodNotAllowed, ``),
			run: request(http.MethodGet, "/v1/client/ready", nil, nil), want: "no endpoint has this method and path"},
		{name: "a marked request outside the contract", answer: answers(http.StatusBadRequest, `{"code":"invalid_message","message":"not JSON"}`),
			run: func(cli *CLI) { post("/v1/client/ready", []byte(`{`))(cli.Invalid()) }},
		{name: "a marked request to no endpoint", answer: answers(http.StatusMethodNotAllowed, `not allowed`),
			run: func(cli *CLI) { request(http.MethodGet, "/v1/client/ready", nil, nil)(cli.Invalid()) }},
		{name: "a marked request within the contract", answer: acknowledge,
			run:  func(cli *CLI) { post("/v1/client/ready", valid("ready_no_tools"))(cli.Invalid()) },
			want: "marked Invalid and is within the contract"},
		{name: "the answer to a marked request", answer: answers(http.StatusBadRequest, `{"code":"stale_connection","message":"no"}`),
			run:  func(cli *CLI) { post("/v1/client/ready", []byte(`{`))(cli.Invalid()) },
			want: "answer 400 outside the contract"},

		// Within the contract.
		{name: "a ready and its ACK", answer: acknowledge, run: post("/v1/client/ready", valid("ready_no_tools"))},
		{name: "a temporary failure with any body", answer: answers(http.StatusServiceUnavailable, `upstream connect error`),
			run: post("/v1/client/ready", valid("ready_no_tools"))},
		{name: "an upload and its answer", answer: answers(http.StatusOK, string(valid("upload_response"))),
			run: request(http.MethodPut, contentPath, upload, []byte("hello world\n"))},
		{name: "the whole content without a range", answer: answers(http.StatusOK, "hello world\n"),
			run: request(http.MethodGet, contentPath, nil, nil)},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			var opened int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == "/v1/client/events" {
					stream := row.streams[opened]
					opened++
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, stream)
					return
				}
				row.answer(w, r)
			}))
			defer server.Close()
			var reported failures
			cli := NewCLI(&reported)
			cli.BaseURL, cli.InstanceID, cli.CredentialID, cli.Key = server.URL, "inst_example", "ccred_example", "key"
			row.run(cli)
			switch {
			case row.want == "" && len(reported.messages) > 0:
				t.Fatalf("reported %q; want nothing", reported.messages)
			case row.want != "" && (len(reported.messages) != 1 || !strings.Contains(reported.messages[0], row.want)):
				t.Fatalf("reported %q; want one report holding %q", reported.messages, row.want)
			}
		})
	}
}
