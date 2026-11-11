package reference

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestBehaviourCases runs every Engine-direction case of
// protocol/behaviour-v1.cases.json against this server, as Engine's tests
// run them against Engine: each request over HTTP as the case writes it,
// each event read from the stream, each state and stimulus brought about by
// the controls. A state, stimulus, step or matcher the runner does not
// implement fails; `go test -v` lists the cases it ran.
func TestBehaviourCases(t *testing.T) {
	file, err := loadCases()
	if err != nil {
		t.Fatal(err)
	}
	for name := range file.States {
		if caseStates[name] == nil {
			t.Errorf("state %s is not implemented", name)
		}
	}
	for name := range file.Stimuli {
		if caseStimuli[name] == nil {
			t.Errorf("stimulus %s is not implemented", name)
		}
	}
	for _, c := range file.Cases {
		// A CLI-direction case runs against `tetral`, in the CLI's own tests.
		if c.Direction != "engine" {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			run := &caseRun{
				t: t, cliVersion: "1.4.0",
				bound:    &caseBindings{contents: file.Contents, symbols: map[string]string{}, values: map[string]any{}},
				keys:     map[string]string{},
				sessions: map[string]string{}, driveSets: map[string][]map[string]any{},
			}
			run.ts = startServer(t, func(config *Config) {
				config.Credentials = nil
				for _, given := range c.Given {
					if version, set := given.With["min_cli_version"].(string); set && given.State == "instance" {
						config.MinCLIVersion, run.cliVersion = version, version
					}
				}
			})
			for _, given := range c.Given {
				run.what = "state " + given.State
				caseStates[given.State](run, given.With)
			}
			for index, step := range c.Steps {
				run.what = fmt.Sprintf("step %d", index+1)
				switch {
				case step.Request != nil:
					run.request(*step.Request, *step.Answer)
				case step.Expect != nil:
					run.expect(*step.Expect)
				default:
					with, err := run.bound.expand(map[string]any(step.With))
					if err != nil {
						run.fatalf("%v", err)
					}
					caseStimuli[step.Stimulus](run, with.(map[string]any))
				}
			}
		})
	}
}

// caseRun is one case's run against a server of its own.
type caseRun struct {
	t     *testing.T
	ts    *testServer
	bound *caseBindings
	// what is the state or step being run, for a failure's message.
	what string
	// instance is the identity of the case's instance and firstCredential
	// that of the credential its requests are sent under by default.
	instance, firstCredential string
	cliVersion                string
	// keys holds each credential's connection key by its identity: the one
	// the runner created it with, or the one its fetch returned.
	keys   map[string]string
	stream *eventStream
	// pending are the events read from the stream while a state or stimulus
	// looked for its own, in the order received.
	pending []map[string]any
	// sessions is each binding symbol's Session symbol and driveSets the
	// drive set last sent for it.
	sessions  map[string]string
	driveSets map[string][]map[string]any
	// made counts the identities the runner made up.
	made int
}

func (run *caseRun) fatalf(format string, args ...any) {
	run.t.Helper()
	run.t.Fatalf("%s: %s", run.what, fmt.Sprintf(format, args...))
}

// symbol returns member name of with, a symbol.
func (run *caseRun) symbol(with map[string]any, name string) string {
	run.t.Helper()
	symbol, isString := with[name].(string)
	if !isString || symbol == "" {
		run.fatalf("%q names no symbol: %v", name, with[name])
	}
	return symbol
}

// identity returns the identity of a symbol that a state or stimulus
// before this one created.
func (run *caseRun) identity(kind, symbol string) string {
	run.t.Helper()
	identity, err := run.bound.lookup(kind, symbol)
	if err != nil {
		run.fatalf("%v", err)
	}
	return identity
}

func (run *caseRun) control(name string, input, result any) {
	run.t.Helper()
	status, answer := run.ts.callStatus(name, input)
	if status != http.StatusOK {
		run.fatalf("control %s: %d %s", name, status, answer)
	}
	if result != nil {
		if err := json.Unmarshal(answer, result); err != nil {
			run.fatalf("control %s: %v", name, err)
		}
	}
}

// issue runs an instruction control for the case's instance and returns the
// instruction's message_id.
func (run *caseRun) issue(control string, payload map[string]any) string {
	run.t.Helper()
	var issued struct {
		MessageID string `json:"message_id"`
	}
	run.control(control, map[string]any{"instance_id": run.instance, "payload": payload}, &issued)
	return issued.MessageID
}

// nextEvent is the next event of the case's stream: one a state or stimulus
// read past first, then the stream's next.
func (run *caseRun) nextEvent() map[string]any {
	run.t.Helper()
	if len(run.pending) > 0 {
		event := run.pending[0]
		run.pending = run.pending[1:]
		return event
	}
	if run.stream == nil {
		run.fatalf("the case has no stream")
	}
	select {
	case event := <-run.stream.events:
		return event
	case <-run.stream.ended:
		// The events written before the stream ended are still to read.
		select {
		case event := <-run.stream.events:
			return event
		default:
			run.fatalf("the stream ended with no further event")
		}
	case <-time.After(waitLimit):
		run.fatalf("no event arrived")
	}
	return nil
}

// take reads the stream up to the event with messageID, which a state or
// stimulus consumes, and keeps the events before it for the case.
func (run *caseRun) take(messageID string) map[string]any {
	run.t.Helper()
	var passed []map[string]any
	for {
		event := run.nextEvent()
		if event["message_id"] == messageID {
			run.pending = append(passed, run.pending...)
			return event
		}
		passed = append(passed, event)
	}
}

// post sends a message of the runner's own to an endpoint and requires its
// ACK.
func (run *caseRun) post(endpoint string, body map[string]any) {
	run.t.Helper()
	answer := run.ts.post(endpoint, run.keys[run.firstCredential], body)
	if answer.status != http.StatusOK {
		run.fatalf("%s: %d %s %v", endpoint, answer.status, answer.body, answer.err)
	}
}

// driveOf returns the identity of a drive symbol, which its first naming in
// a state or stimulus creates.
func (run *caseRun) driveOf(symbol string) string {
	if identity, err := run.bound.lookup("drive", symbol); err == nil {
		return identity
	}
	return run.bound.bind("drive", symbol)
}

// driveSet reads a state's or stimulus's `drives`, [{drive, access}], as the
// drive set of an instruction.
func (run *caseRun) driveSet(with map[string]any) []map[string]any {
	run.t.Helper()
	listed, isArray := with["drives"].([]any)
	if !isArray {
		run.fatalf("drives is no array: %v", with["drives"])
	}
	set := []map[string]any{}
	for _, entry := range listed {
		members, _ := entry.(map[string]any)
		set = append(set, map[string]any{"drive_id": run.driveOf(run.symbol(members, "drive")), "access": members["access"]})
	}
	return set
}

// prepare issues a session.prepare that creates the Session and binding of
// with, and returns its message_id.
func (run *caseRun) prepare(with map[string]any) (messageID, session, binding string) {
	run.t.Helper()
	bindingSymbol := run.symbol(with, "binding")
	session = run.bound.bind("session", run.symbol(with, "session"))
	binding = run.bound.bind("binding", bindingSymbol)
	run.sessions[bindingSymbol] = session
	run.driveSets[bindingSymbol] = run.driveSet(with)
	messageID = run.issue("prepare", map[string]any{
		"session_id": session, "binding_id": binding, "cwd": "/home/user/project",
		"drives": run.driveSets[bindingSymbol], "drive_exclude": []any{}, "repositories": []any{},
	})
	return messageID, session, binding
}

func (run *caseRun) commitAsAnotherWriter(with map[string]any, change map[string]any) {
	run.t.Helper()
	change["path"] = with["path"]
	run.control("drive_commit", map[string]any{"drive_id": run.driveOf(run.symbol(with, "drive")), "changes": []any{change}}, nil)
}

func (run *caseRun) written(with map[string]any) map[string]any {
	run.t.Helper()
	content, err := run.bound.content(run.symbol(with, "content"))
	if err != nil {
		run.fatalf("%v", err)
	}
	return map[string]any{"text": string(content)}
}

var caseStates = map[string]func(*caseRun, map[string]any){
	"instance": func(run *caseRun, with map[string]any) {
		run.instance = run.bound.bind("instance", run.symbol(with, "instance"))
		// A case that never names its credential gives it no symbol.
		run.firstCredential = "ccred_given"
		if _, named := with["credential"]; named {
			run.firstCredential = run.bound.bind("credential", run.symbol(with, "credential"))
		}
		run.keys[run.firstCredential] = "tetral_ck_" + run.firstCredential
		run.control("credential", map[string]any{"action": "add", "credential": Credential{
			InstanceID: run.instance, CredentialID: run.firstCredential, ConnectionKey: run.keys[run.firstCredential],
			ExpiresAt: testStart.Add(time.Hour), State: "active",
		}}, nil)
	},
	"live_connection": func(run *caseRun, with map[string]any) {
		if run.identity("instance", run.symbol(with, "instance")) != run.instance {
			run.fatalf("a case has one instance")
		}
		stream, refused := run.ts.openQuery(run.keys[run.firstCredential], url.Values{
			"protocol_version": {"1"}, "instance_id": {run.instance}, "credential_id": {run.firstCredential},
		}, nil)
		if stream == nil {
			run.fatalf("events: %d %s %v", refused.status, refused.body, refused.err)
		}
		run.stream = stream
		run.bound.connection, _ = run.nextEvent()["connection_id"].(string)
		capabilities := map[string]any{"session_preparation": true, "drives": map[string]any{"mount": with["mount"] == true}}
		if with["tools"] == true {
			capabilities["tool_execution"] = map[string]any{
				"schema_version": 1, "tools": []string{"exec_command"}, "controls": []string{"stdin", "cancel"},
				"sandbox_policy_versions": []int{1}, "network_modes": []string{"disabled"},
				"limits": map[string]any{
					"metadata_json_bytes": 65536, "tool_input_json_bytes": 4194304, "execute_sse_json_bytes": 4259840,
					"stdin_utf8_bytes": 32768, "stdin_sse_json_bytes": 262144, "default_message_json_bytes": 65536,
					"helper_result_json_bytes": 262144, "drive_sync_json_bytes": 65536,
					"ordinary_tool_status_json_bytes": 327680, "drive_sync_report_json_bytes": 131072,
					"view_image_decoded_bytes": 10485760, "media_tool_status_json_bytes": 14680064,
				},
			}
		}
		run.post("ready", map[string]any{
			"protocol_version": 1, "connection_id": run.bound.connection, "message_id": "given_ready",
			"os": "linux", "arch": "x86_64", "cli_version": run.cliVersion, "capabilities": capabilities,
		})
	},
	"prepared_binding": func(run *caseRun, with map[string]any) {
		messageID, session, binding := run.prepare(with)
		run.take(messageID)
		run.post("session-status", map[string]any{
			"protocol_version": 1, "connection_id": run.bound.connection, "message_id": "given_prepared_" + binding,
			"request_message_id": messageID, "session_id": session, "binding_id": binding,
			"status": "prepared", "cwd": "/home/user/project",
		})
	},
	"drive_file": func(run *caseRun, with map[string]any) {
		run.commitAsAnotherWriter(with, run.written(with))
	},
	"drive_deleted_file": func(run *caseRun, with map[string]any) {
		run.commitAsAnotherWriter(with, map[string]any{"text": "deleted later"})
		run.commitAsAnotherWriter(with, map[string]any{"delete": true})
	},
	"drive_limits": func(run *caseRun, with map[string]any) {
		limits := map[string]any{"drive_id": run.driveOf(run.symbol(with, "drive"))}
		for _, name := range []string{"max_bytes", "max_files"} {
			if limit, set := with[name]; set {
				limits[name] = limit
			}
		}
		run.control("drive_limits", limits, nil)
	},
	"upload_budget": func(run *caseRun, with map[string]any) {
		run.control("drive_budget", map[string]any{"bytes_per_minute": with["bytes_per_minute"]}, nil)
	},
}

var caseStimuli = map[string]func(*caseRun, map[string]any){
	"other_writer_commits": func(run *caseRun, with map[string]any) {
		run.commitAsAnotherWriter(with, run.written(with))
	},
	"records_expire": func(run *caseRun, with map[string]any) {
		run.control("drive_expire", map[string]any{
			"drive_id": run.identity("drive", run.symbol(with, "drive")), "before_seq": with["before_seq"],
		}, nil)
	},
	"version_is_removed": func(run *caseRun, with map[string]any) {
		drive := run.identity("drive", run.symbol(with, "drive"))
		seq, err := integer(with["seq"])
		if err != nil {
			run.fatalf("seq: %v", err)
		}
		var state struct {
			Records []struct {
				Seq       int64  `json:"seq"`
				Path      string `json:"path"`
				VersionID string `json:"version_id"`
			} `json:"records"`
		}
		run.control("drive_state", map[string]any{"drive_id": drive}, &state)
		for _, record := range state.Records {
			if record.Seq == seq && record.Path == with["path"] {
				run.control("drive_remove_version", map[string]any{"drive_id": drive, "version_id": record.VersionID}, nil)
				return
			}
		}
		run.fatalf("commit %d of %s made no version of %v", seq, drive, with["path"])
	},
	"engine_prepares": func(run *caseRun, with map[string]any) { run.prepare(with) },
	"engine_closes": func(run *caseRun, with map[string]any) {
		symbol := run.symbol(with, "binding")
		run.issue("close", map[string]any{"session_id": run.sessions[symbol], "binding_id": run.identity("binding", symbol)})
	},
	"engine_changes_drives": func(run *caseRun, with map[string]any) {
		symbol := run.symbol(with, "binding")
		drive := run.driveOf(run.symbol(with, "drive"))
		// The set as it was, with this drive's access changed in its
		// place or the drive taken out.
		var set []map[string]any
		for _, entry := range run.driveSets[symbol] {
			access, stays := with["access"]
			switch {
			case entry["drive_id"] != drive:
				set = append(set, entry)
			case stays:
				set = append(set, map[string]any{"drive_id": drive, "access": access})
			}
		}
		run.driveSets[symbol] = set
		run.issue("drives", map[string]any{
			"session_id": run.sessions[symbol], "binding_id": run.identity("binding", symbol), "drives": set,
		})
	},
	"stream_ends": func(run *caseRun, _ map[string]any) {
		if run.stream == nil {
			run.fatalf("the case has no stream")
		}
		run.stream.cancel()
		run.stream, run.pending = nil, nil
		// The server learns of the closed stream from its own read of it.
		for deadline := time.Now().Add(waitLimit); run.ts.connection(run.bound.connection).StreamOpen; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				run.fatalf("the server still holds the stream open")
			}
		}
	},
	"heartbeat_is_due": func(run *caseRun, _ map[string]any) { run.ts.advance(HeartbeatInterval) },
	"stream_reaches_its_maximum_age": func(run *caseRun, _ map[string]any) {
		// The time passes as it does for a client: a heartbeat interval at a
		// time, each heartbeat answered, so that the lease outlives the
		// stream.
		for beats := 1; ; beats++ {
			run.ts.advance(HeartbeatInterval)
			conn := run.ts.connection(run.bound.connection)
			if !conn.StreamOpen {
				return
			}
			if beats > int(DefaultMaxStreamAge/HeartbeatInterval) {
				run.fatalf("the stream is open past its maximum age")
			}
			var passed []map[string]any
			for {
				event := run.nextEvent()
				if event["type"] == "heartbeat" {
					run.pending = append(passed, run.pending...)
					run.post("heartbeat", map[string]any{
						"protocol_version": 1, "connection_id": run.bound.connection,
						"message_id": fmt.Sprintf("given_beat_%d", beats), "request_message_id": event["message_id"],
						"heartbeat_seq": payload(event)["heartbeat_seq"],
					})
					break
				}
				passed = append(passed, event)
			}
		}
	},
	"lease_ends": func(run *caseRun, _ map[string]any) {
		lease := run.ts.connection(run.bound.connection).LeaseUntil
		run.ts.advance(lease.Sub(run.ts.state().Now) + time.Millisecond)
	},
	"engine_renews_the_credential": func(run *caseRun, with map[string]any) {
		run.control("credential", map[string]any{"action": "add", "renew": true, "credential": Credential{
			InstanceID: run.instance, CredentialID: run.bound.bind("credential", run.symbol(with, "credential")),
			ExpiresAt: testStart.Add(2 * time.Hour), State: "pending",
		}}, nil)
	},
	"credential_revoked": func(run *caseRun, with map[string]any) {
		run.control("credential", map[string]any{"action": "revoke", "credential_id": run.identity("credential", run.symbol(with, "credential"))}, nil)
	},
	"credential_expired": func(run *caseRun, with map[string]any) {
		credential := run.identity("credential", run.symbol(with, "credential"))
		state := run.ts.state()
		for _, found := range state.Credentials {
			if found.CredentialID == credential {
				run.ts.advance(found.ExpiresAt.Sub(state.Now))
				return
			}
		}
		run.fatalf("the server has no credential %s", credential)
	},
	"engine_executes": func(run *caseRun, with map[string]any) {
		symbol := run.symbol(with, "binding")
		run.made++
		run.issue("execute", map[string]any{
			"session_id": run.sessions[symbol], "binding_id": run.identity("binding", symbol),
			"tool_use_event_id": fmt.Sprintf("toolu_given_%d", run.made), "task_id": fmt.Sprintf("task_given_%d", run.made),
			"tool": "exec_command", "input": map[string]any{"cmd": "true"},
			"sandbox_policy": map[string]any{
				"schema_version": 1, "filesystem": map[string]any{"rules": []any{}}, "network": map[string]any{"mode": "disabled"},
			},
		})
	},
}

// request sends a case's request as written and holds its answer to what
// the case requires.
func (run *caseRun) request(request caseRequest, want caseAnswer) {
	run.t.Helper()
	path, err := run.bound.expandString(request.Path)
	if err != nil {
		run.fatalf("%v", err)
	}
	query := url.Values{}
	for name, value := range request.Query {
		if query[name], err = run.expandedList(value); err != nil {
			run.fatalf("%v", err)
		}
	}
	credential := run.firstCredential
	if request.Credential != "" {
		credential = run.identity("credential", request.Credential)
	}
	key, known := run.keys[credential]
	if !known {
		run.fatalf("the key of credential %s is not known", credential)
	}
	header := http.Header{}
	for name, value := range request.Headers {
		expanded, err := run.bound.expandString(value)
		if err != nil {
			run.fatalf("header %s: %v", name, err)
		}
		header.Set(name, expanded)
	}
	var body []byte
	bodies := 0
	if len(request.Body) > 0 {
		bodies++
		value, err := decodeCaseValue(request.Body)
		if err == nil {
			value, err = run.bound.expand(value)
		}
		if err != nil {
			run.fatalf("%v", err)
		}
		if body, err = json.Marshal(value); err != nil {
			run.fatalf("%v", err)
		}
		if header.Get("Content-Type") == "" {
			header.Set("Content-Type", "application/json")
		}
	}
	if request.Content != "" {
		bodies++
		if body, err = run.bound.content(request.Content); err != nil {
			run.fatalf("%v", err)
		}
	}
	if request.DeclaresBytes != nil {
		bodies++
	}
	if bodies > 1 {
		run.fatalf("a request has one of body, content and declares_bytes")
	}
	target := path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	var got reply
	switch {
	case request.Method == http.MethodGet && path == "/v1/client/events":
		got = run.openStream(key, query, header)
	case request.DeclaresBytes != nil:
		got = run.declare(request.Method, target, key, header, *request.DeclaresBytes)
	default:
		got = run.send(request.Method, target, key, header, body)
	}
	if got.err != nil {
		run.fatalf("%s %s: %v", request.Method, target, got.err)
	}
	if got.status != want.Status {
		run.fatalf("%s %s: got %d %s, want %d", request.Method, target, got.status, shownBytes(got.body), want.Status)
	}
	for name, expected := range want.Headers {
		if err := run.bound.matchHeader("header "+name, expected, got.header.Values(name)); err != nil {
			run.fatalf("%s %s: %v", request.Method, target, err)
		}
	}
	if len(want.Body) > 0 && want.Content != nil {
		run.fatalf("an answer has one of body and content")
	}
	if len(want.Body) > 0 {
		expected, err := decodeCaseValue(want.Body)
		if err != nil {
			run.fatalf("%v", err)
		}
		received, err := decodeCaseValue(got.body)
		if err != nil {
			run.fatalf("%s %s: the answer %s is no JSON: %v", request.Method, target, shownBytes(got.body), err)
		}
		if err := run.bound.match("body", expected, received); err != nil {
			run.fatalf("%s %s: %v", request.Method, target, err)
		}
	}
	if want.Content != nil {
		content, err := run.bound.content(want.Content.Of)
		if err != nil {
			run.fatalf("%v", err)
		}
		from, to := 0, len(content)-1
		if want.Content.From != nil {
			from = *want.Content.From
		}
		if want.Content.To != nil {
			to = *want.Content.To
		}
		if !bytes.Equal(got.body, content[from:to+1]) {
			run.fatalf("%s %s: got the bytes %s, want %d to %d of %s", request.Method, target, shownBytes(got.body), from, to, want.Content.Of)
		}
	}
	// The key of a fetched credential is known from its answer alone.
	if path == "/v1/client/credential" && got.status == http.StatusOK {
		var fetched struct {
			CredentialID  string `json:"credential_id"`
			ConnectionKey string `json:"connection_key"`
		}
		if err := json.Unmarshal(got.body, &fetched); err != nil {
			run.fatalf("%v", err)
		}
		run.keys[fetched.CredentialID] = fetched.ConnectionKey
	}
}

// expandedList is a query value with its references replaced.
func (run *caseRun) expandedList(value string) ([]string, error) {
	expanded, err := run.bound.expandString(value)
	return []string{expanded}, err
}

func (run *caseRun) send(method, target, key string, header http.Header, body []byte) reply {
	request, err := http.NewRequest(method, run.ts.api.URL+target, bytes.NewReader(body))
	if err != nil {
		return reply{err: err}
	}
	request.Header = header
	request.Header.Set("Authorization", "Bearer "+key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return reply{err: err}
	}
	defer func() { _ = response.Body.Close() }()
	answer, err := io.ReadAll(response.Body)
	return reply{status: response.StatusCode, header: response.Header, body: answer, err: err}
}

// declare sends a request's head with a Content-Length of declared bytes
// and no byte of a body, and reads the answer that must come before one.
// Go's HTTP server holds a connection whose body it left unread for half a
// second before it closes it, which the test server's close waits for: the
// case that declares a body takes that long.
func (run *caseRun) declare(method, target, key string, header http.Header, declared int64) reply {
	address := run.ts.api.Listener.Addr().String()
	conn, err := net.DialTimeout("tcp", address, waitLimit)
	if err != nil {
		return reply{err: err}
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(waitLimit))
	head := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Length: %d\r\n", method, target, address, key, declared)
	for name, values := range header {
		head += name + ": " + strings.Join(values, ", ") + "\r\n"
	}
	if _, err := io.WriteString(conn, head+"\r\n"); err != nil {
		return reply{err: err}
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return reply{err: err}
	}
	defer func() { _ = response.Body.Close() }()
	answer, err := io.ReadAll(response.Body)
	return reply{status: response.StatusCode, header: response.Header, body: answer, err: err}
}

// openStream sends an events request; one answered 200 becomes the case's
// stream.
func (run *caseRun) openStream(key string, query url.Values, header http.Header) reply {
	stream, answer := run.ts.openQuery(key, query, header)
	if stream != nil {
		if run.stream != nil {
			run.stream.cancel()
		}
		run.stream, run.pending = stream, nil
	}
	return answer
}

// expect holds the next event of the case's stream, or the stream's end, to
// the case. A heartbeat is passed over unless the case expects one.
func (run *caseRun) expect(expect caseExpect) {
	run.t.Helper()
	switch {
	case expect.End && len(expect.Event) == 0 && len(expect.Request) == 0:
		if run.stream == nil {
			run.fatalf("the case has no stream")
		}
		if rest := run.stream.waitEnded(); len(run.pending)+len(rest) != 0 {
			run.fatalf("events before the stream's end: %s", shown(append(run.pending, rest...)))
		}
	case len(expect.Event) > 0 && len(expect.Request) == 0 && !expect.End:
		want, err := decodeCaseValue(expect.Event)
		if err != nil {
			run.fatalf("%v", err)
		}
		members, _ := want.(map[string]any)
		event := run.nextEvent()
		for members["type"] != "heartbeat" && event["type"] == "heartbeat" {
			event = run.nextEvent()
		}
		if err := run.bound.match("event", want, map[string]any(event)); err != nil {
			run.fatalf("%v\nin %s", err, shown(event))
		}
		if event["type"] == "connected" {
			run.bound.connection, _ = event["connection_id"].(string)
		}
	default:
		// A request the CLI sends is a step of a CLI-direction case.
		run.fatalf("an expectation of an Engine-direction case is one event or the stream's end")
	}
}

func shownBytes(data []byte) string {
	if len(data) > 400 {
		return fmt.Sprintf("%q… (%d bytes)", data[:400], len(data))
	}
	return fmt.Sprintf("%q", data)
}
