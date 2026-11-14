// Layer 1: the developer client against in-process stand-ins for the API.
package enginetest

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	drive        = `{"id":"drive_1","skills_path":"skills/","memory":{"path":"memory/","instructions":null},"limits":{"max_bytes":null,"max_files":null},"usage":{"bytes":0,"files":0},"stored_bytes":null,"metadata":{},"created_at":"2026-01-01T00:00:00Z"}`
	refusal      = `{"type":"error","error":{"type":"invalid_request_error","message":"refused"},"request_id":"req_1"}`
	tokenRequest = `{"grant_type":"urn:ietf:params:oauth:grant-type:jwt-bearer","assertion":"a","federation_rule_id":"rule_1","organization_id":"org_1"}`
	token        = `{"access_token":"t","token_type":"Bearer","expires_in":3600}`
	driveChange  = "event: drive.changed\ndata: {\"drive_id\":\"drive_1\",\"seq\":1}\n\n"
)

// stub answers every request with one scripted answer.
func stub(status int, header map[string]string, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for name, value := range header {
			w.Header().Set(name, value)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

var (
	jsonAnswer     = map[string]string{"Content-Type": "application/json", "request-id": "req_1"}
	streamAnswer   = map[string]string{"Content-Type": "text/event-stream", "request-id": "req_1"}
	exchangeAnswer = map[string]string{"Content-Type": "application/json", "request-id": "req_1", "Cache-Control": "no-store"}
)

// with is header with name set to value.
func with(header map[string]string, name, value string) map[string]string {
	changed := map[string]string{name: value}
	for key, held := range header {
		if key != name {
			changed[key] = held
		}
	}
	return changed
}

// A Developer reports each request, answer, header and stream event that
// is outside Engine's OpenAPI file, and nothing that is within it.
func TestADeveloperReportsWhatIsOutsideTheFile(t *testing.T) {
	without := func(text, part string) string { return strings.Replace(text, part, "", 1) }
	rows := []struct {
		name           string
		method, target string
		body           string
		// request is the request's header; nil, a body is sent as JSON.
		request         http.Header
		invalid, stream bool
		status          int
		header          map[string]string
		answer          string
		// want is part of the one report; empty, nothing is reported.
		want string
		// comments is how many comment lines a stream's answer carries.
		comments int
	}{
		{name: "an answer within the file", method: "GET", target: "/v1/drives/drive_1", status: 200, header: jsonAnswer, answer: drive},
		{name: "an undeclared status", method: "GET", target: "/v1/drives/drive_1", status: 418, header: jsonAnswer, answer: refusal,
			want: "answer 418 is not one drives.read declares"},
		{name: "an answer without a required field", method: "GET", target: "/v1/drives/drive_1", status: 200, header: jsonAnswer,
			answer: without(drive, `"skills_path":"skills/",`), want: "missing property 'skills_path'"},
		{name: "an answer with a field the file does not name", method: "GET", target: "/v1/drives/drive_1", status: 200, header: jsonAnswer,
			answer: strings.Replace(drive, `{"id"`, `{"owner":"u","id"`, 1), want: "additional properties 'owner' not allowed"},
		{name: "an answer with a field of the wrong type", method: "GET", target: "/v1/drives/drive_1", status: 200, header: jsonAnswer,
			answer: strings.Replace(drive, `"bytes":0`, `"bytes":"0"`, 1), want: "/usage/bytes"},
		{name: "an answer with a time that is not RFC 3339", method: "GET", target: "/v1/drives/drive_1", status: 200, header: jsonAnswer,
			answer: strings.Replace(drive, `"2026-01-01T00:00:00Z"`, `"2026-01-01 00:00:00+00"`, 1), want: "date-time"},
		{name: "an answer without a declared header", method: "GET", target: "/v1/drives/drive_1", status: 200,
			header: map[string]string{"Content-Type": "application/json"}, answer: drive, want: "lacks the header request-id"},
		{name: "a header value the file does not give", method: "POST", target: "/v1/oauth/token", body: tokenRequest, status: 200,
			header: with(exchangeAnswer, "Cache-Control", "public, max-age=60"), answer: token, want: `has Cache-Control "public, max-age=60", not ^no-store$`},
		{name: "a wait that is no number of seconds", method: "POST", target: "/v1/oauth/token", body: tokenRequest, status: 429,
			header: with(exchangeAnswer, "Retry-After", "soon"), answer: strings.Replace(refusal, "invalid_request_error", "rate_limit_error", 1),
			want: `has Retry-After "soon"`},
		{name: "a retry flag the file does not give", method: "POST", target: "/v1/drives/drive_1/restore", body: `{"as_of":"2026-01-01T00:00:00Z"}`,
			status: 409, header: with(jsonAnswer, "x-should-retry", "true"), answer: refusal,
			want: `has x-should-retry "true"`},
		{name: "an error repeating another request's ID", method: "GET", target: "/v1/drives/drive_1", status: 404, header: jsonAnswer,
			answer: `{"type":"error","error":{"type":"not_found_error","message":"no"},"request_id":"req_2"}`, want: `repeats request "req_2", not its request-id "req_1"`},
		{name: "an error type its status does not declare", method: "GET", target: "/v1/drives/drive_1", status: 404, header: jsonAnswer,
			answer: refusal, want: "error invalid_request_error under 404 is not one drives.read declares"},
		{name: "an answer's metadata with a new key", method: "GET", target: "/v1/drives/drive_1", status: 200, header: jsonAnswer,
			answer: strings.Replace(drive, `"metadata":{}`, `"metadata":{"team":"ai"}`, 1)},

		{name: "a stream event within the file", method: "GET", target: "/v1/drives/drive_1/changes/stream", stream: true, status: 200,
			header: streamAnswer, answer: ": quiet\n\n" + driveChange, comments: 1},
		{name: "an undeclared stream event", method: "GET", target: "/v1/drives/drive_1/changes/stream", stream: true, status: 200,
			header: streamAnswer, answer: "event: drive.removed\ndata: {\"drive_id\":\"drive_1\"}\n\n", want: `stream event "drive.removed" is not one drive_changes.stream declares`},
		{name: "a stream frame of two data lines", method: "GET", target: "/v1/drives/drive_1/changes/stream", stream: true, status: 200,
			header: streamAnswer, answer: "event: drive.changed\ndata: {\"drive_id\":\"drive_1\",\ndata: \"seq\":1}\n\n", want: "a stream frame has 1 event and 2 data lines"},
		{name: "a stream frame of two event lines", method: "GET", target: "/v1/drives/drive_1/changes/stream", stream: true, status: 200,
			header: streamAnswer, answer: "event: drive.removed\n" + driveChange, want: "a stream frame has 2 event and 1 data lines"},
		{name: "a stream line of a field the file does not give an event", method: "GET", target: "/v1/drives/drive_1/changes/stream", stream: true, status: 200,
			header: streamAnswer, answer: "id: 7\n" + driveChange, want: `a stream line "id: 7" is neither event, data nor a comment`},
		{name: "a stream event outside its schema", method: "GET", target: "/v1/drives/drive_1/changes/stream", stream: true, status: 200,
			header: streamAnswer, answer: strings.Replace(driveChange, `"seq":1`, `"seq":1,"path":"a"`, 1), want: "additional properties 'path' not allowed"},

		{name: "a request with an undeclared field", method: "POST", target: "/v1/drives", body: `{"name":"x"}`, status: 400, header: jsonAnswer,
			answer: refusal, want: "request outside the file, answered 400: body: "},
		{name: "the same request, marked", method: "POST", target: "/v1/drives", body: `{"name":"x"}`, invalid: true, status: 400, header: jsonAnswer,
			answer: refusal},
		{name: "a marked request that is not refused", method: "POST", target: "/v1/drives", body: `{"name":"x"}`, invalid: true, status: 201,
			header: jsonAnswer, answer: drive, want: "marked invalid and was answered 201"},
		{name: "an undeclared query name on a closed operation", method: "GET", target: "/v1/drives/drive_1?depth=2", status: 200, header: jsonAnswer,
			answer: drive, want: "query parameter depth is not declared"},
		{name: "an array query parameter sent once", method: "GET", target: "/v1/sessions/ses_1/events/stream?beta=true&event_deltas[]=agent.message",
			stream: true, status: 200, header: streamAnswer, answer: ": quiet\n\n", comments: 1},
		{name: "an array query parameter with a value outside its items", method: "GET",
			target: "/v1/sessions/ses_1/events/stream?beta=true&event_deltas[]=agent.message&event_deltas[]=agent.tool_use", stream: true, status: 200,
			header: streamAnswer, answer: ": quiet\n\n", want: "query parameter event_deltas[]: ", comments: 1},
		{name: "a token exchange without its media type", method: "POST", target: "/v1/oauth/token", body: tokenRequest, request: http.Header{}, status: 400,
			header: exchangeAnswer, answer: refusal, want: `request outside the file, answered 400: the body's type "" is not one of [application/json]`},

		{name: "a marked request to an undeclared path", method: "GET", target: "/v1/batches", invalid: true, status: 404,
			header: map[string]string{"Content-Type": "text/plain"}, answer: "404 page not found"},
		{name: "an undeclared path answered outside the file's rule", method: "GET", target: "/v1/batches", invalid: true, status: 400, header: jsonAnswer,
			answer: refusal, want: "answer 400 to a path the file does not declare; its rule gives 404"},
		{name: "an undeclared path without valid credentials", method: "GET", target: "/v1/batches", invalid: true, status: 401, header: jsonAnswer,
			answer: strings.Replace(refusal, "invalid_request_error", "authentication_error", 1)},
		{name: "a refusal of credentials that is no authentication error", method: "GET", target: "/v1/batches", invalid: true, status: 401,
			header: jsonAnswer, answer: refusal, want: "is no authentication_error"},
		{name: "a surface Engine does not serve", method: "POST", target: "/v1/agents/agent_1/multiagent/runs", invalid: true, status: 400,
			header: jsonAnswer, answer: refusal},
		{name: "a surface Engine does not serve answered as unknown", method: "POST", target: "/v1/messages", invalid: true, status: 404,
			header: map[string]string{"Content-Type": "text/plain"}, answer: "404 page not found", want: "its rule gives 400"},
		{name: "a declared path with another method", method: "PUT", target: "/v1/drives/drive_1", invalid: true, status: 405, header: map[string]string{}},
		{name: "a declared path with another method answered as unknown", method: "PUT", target: "/v1/drives/drive_1", invalid: true, status: 404,
			header: map[string]string{"Content-Type": "text/plain"}, answer: "404 page not found", want: "its rule gives 405"},

		{name: "the edge's answer while a service cannot be reached", method: "GET", target: "/v1/drives/drive_1", status: 503,
			header: map[string]string{"Content-Type": "text/plain"}, answer: "upstream connect error"},
		{name: "an answer of that status that is an operation's", method: "GET", target: "/v1/drives/drive_1", status: 503,
			header: jsonAnswer, answer: refusal, want: "answer 503 is not one drives.read declares"},

		{name: "metadata: a new key with a string", method: "POST", target: "/v1/drives/drive_1", body: `{"metadata":{"team":"ai"}}`, status: 200,
			header: jsonAnswer, answer: drive},
		{name: "metadata: a null in an update", method: "POST", target: "/v1/drives/drive_1", body: `{"metadata":{"team":null}}`, status: 200,
			header: jsonAnswer, answer: drive},
		{name: "metadata: a number as a value", method: "POST", target: "/v1/drives/drive_1", body: `{"metadata":{"team":5}}`, status: 200,
			header: jsonAnswer, answer: drive, want: "/metadata/team"},
		{name: "metadata: an unnamed field beside it", method: "POST", target: "/v1/drives/drive_1", body: `{"metadata":{},"labels":{}}`, status: 200,
			header: jsonAnswer, answer: drive, want: "additional properties 'labels' not allowed"},

		{name: "free JSON: a tool call's input holding an array of objects", method: "GET", target: "/v1/sessions/ses_1/events?beta=true", status: 200,
			header: jsonAnswer, answer: `{"data":[{"type":"agent.tool_use","id":"evt_1","name":"bash","processed_at":"2026-01-01T00:00:00Z","evaluated_permission":"allow",` +
				`"input":{"steps":[{"run":"ls"},{"run":["a",1,null]}]}}],"next_page":null}`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			reported := &failures{}
			developer := NewDeveloper(reported)
			developer.HTTP = InProcess(stub(row.status, row.header, row.answer))
			if row.invalid {
				developer = developer.Invalid()
			}
			header := row.request
			if header == nil && row.body != "" {
				header = http.Header{"Content-Type": {"application/json"}}
			}
			if row.stream {
				stream, _, err := developer.Stream(context.Background(), row.method, row.target, header)
				if err != nil {
					t.Fatal(err)
				}
				for err == nil {
					_, err = stream.Next()
				}
				if stream.Comments() != row.comments {
					t.Errorf("the stream counted %d comment lines; want %d", stream.Comments(), row.comments)
				}
			} else if _, err := developer.Do(context.Background(), row.method, row.target, header, []byte(row.body)); err != nil {
				t.Fatal(err)
			}
			switch {
			case row.want == "" && len(reported.messages) > 0:
				t.Errorf("reported %q; want nothing", reported.messages)
			case row.want != "" && (len(reported.messages) != 1 || !strings.Contains(reported.messages[0], row.want)):
				t.Errorf("reported %q; want one report holding %q", reported.messages, row.want)
			}
		})
	}
}

// The requests a test builds itself are checked as Do's are: through a
// router called in process, through an HTTP client's transport and as bytes
// the test wrote to a socket itself.
func TestAHandlerAndATransportCheckAsDoDoes(t *testing.T) {
	undeclared := http.NewServeMux()
	undeclared.Handle("/v1/drives/drive_1", stub(200, jsonAnswer, strings.Replace(drive, `{"id"`, `{"owner":"u","id"`, 1)))
	undeclared.Handle("/v1/drives/drive_1/changes/stream", stub(200, streamAnswer, strings.Replace(driveChange, `"seq":1`, `"seq":1,"path":"a"`, 1)))
	server := httptest.NewServer(undeclared)
	defer server.Close()
	entries := map[string]func(d *Developer, target string, invalid bool){
		"handler": func(d *Developer, target string, invalid bool) {
			request := httptest.NewRequest("GET", target, nil)
			if invalid {
				request = InvalidRequest(request)
			}
			d.Handler(undeclared).ServeHTTP(httptest.NewRecorder(), request)
		},
		"transport": func(d *Developer, target string, invalid bool) {
			request, err := http.NewRequest("GET", server.URL+target, nil)
			if err != nil {
				t.Fatal(err)
			}
			if invalid {
				request = InvalidRequest(request)
			}
			response, err := (&http.Client{Transport: d.Transport(http.DefaultTransport)}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		},
		"exchange": func(d *Developer, target string, invalid bool) {
			request, response := rawExchange(t, server, "GET "+target+" HTTP/1.1\r\nHost: engine.test\r\nConnection: close\r\n\r\n")
			if invalid {
				request = InvalidRequest(request)
			}
			d.Exchange(request, response)
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		},
	}
	for name, send := range entries {
		t.Run(name, func(t *testing.T) {
			reported := &failures{}
			send(NewDeveloper(reported), "/v1/drives/drive_1", false)
			if len(reported.messages) != 1 || !strings.Contains(reported.messages[0], "additional properties 'owner' not allowed") {
				t.Errorf("an answer with an undeclared field: reported %q", reported.messages)
			}
			reported = &failures{}
			send(NewDeveloper(reported), "/v1/drives/drive_1/changes/stream", false)
			if len(reported.messages) != 1 || !strings.Contains(reported.messages[0], "additional properties 'path' not allowed") {
				t.Errorf("a stream event with an undeclared field: reported %q", reported.messages)
			}
			reported = &failures{}
			send(NewDeveloper(reported), "/v1/drives/drive_1?depth=2", true)
			if len(reported.messages) != 2 || !strings.Contains(reported.messages[0], "marked invalid and was answered 200") {
				t.Errorf("a marked request that is not refused: reported %q; want that and the answer's report", reported.messages)
			}
		})
	}
}

// A streamed request's body reaches the server while the test still
// produces it, and its answer is held as any other.
func TestATransportPassesAStreamedBodyOnAsItComes(t *testing.T) {
	first := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if _, err := io.ReadFull(request.Body, make([]byte, 1)); err == nil {
			close(first)
		}
		_, _ = io.Copy(io.Discard, request.Body)
		stub(200, jsonAnswer, strings.Replace(drive, `{"id"`, `{"owner":"u","id"`, 1)).ServeHTTP(w, request)
	}))
	defer server.Close()
	reader, writer := io.Pipe()
	go func() {
		_, _ = writer.Write([]byte("{"))
		<-first
		_, _ = writer.Write([]byte("}"))
		_ = writer.Close()
	}()
	request, err := http.NewRequest("POST", server.URL+"/v1/drives/drive_1", reader)
	if err != nil {
		t.Fatal(err)
	}
	reported := &failures{}
	response, err := (&http.Client{Transport: NewDeveloper(reported).Transport(http.DefaultTransport)}).Do(StreamedRequest(request))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if len(reported.messages) != 1 || !strings.Contains(reported.messages[0], "additional properties 'owner' not allowed") {
		t.Errorf("reported %q; want the answer's undeclared field alone", reported.messages)
	}
}

// An exchange whose answer the test never reads holds its request alone.
func TestAnExchangeWithoutItsAnswerHoldsTheRequestAlone(t *testing.T) {
	for _, row := range []struct {
		name, target string
		invalid      bool
		want         string
	}{
		{name: "a request within the file", target: "/v1/drives/drive_1"},
		{name: "a request outside it", target: "/v1/drives/drive_1?depth=2", want: "request outside the file: query parameter depth is not declared"},
		{name: "the same request, marked", target: "/v1/drives/drive_1?depth=2", invalid: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			request, err := http.ReadRequest(bufio.NewReader(strings.NewReader("GET " + row.target + " HTTP/1.1\r\nHost: engine.test\r\n\r\n")))
			if err != nil {
				t.Fatal(err)
			}
			if row.invalid {
				request = InvalidRequest(request)
			}
			reported := &failures{}
			NewDeveloper(reported).Exchange(request, nil)
			switch {
			case row.want == "" && len(reported.messages) > 0:
				t.Errorf("reported %q; want nothing", reported.messages)
			case row.want != "" && (len(reported.messages) != 1 || !strings.Contains(reported.messages[0], row.want)):
				t.Errorf("reported %q; want one report holding %q", reported.messages, row.want)
			}
		})
	}
}

// rawExchange writes raw to server's socket and returns the request those
// bytes make and the answer read back.
func rawExchange(t *testing.T, server *httptest.Server, raw string) (*http.Request, *http.Response) {
	t.Helper()
	request, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, err := connection.Write([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), request)
	if err != nil {
		t.Fatal(err)
	}
	return request, response
}
