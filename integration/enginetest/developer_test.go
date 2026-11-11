package enginetest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	drive       = `{"id":"drive_1","skills_path":"skills/","memory":{"path":"memory/","instructions":null},"limits":{"max_bytes":null,"max_files":null},"usage":{"bytes":0,"files":0},"stored_bytes":null,"metadata":{},"created_at":"2026-01-01T00:00:00Z"}`
	refusal     = `{"type":"error","error":{"type":"invalid_request_error","message":"refused"},"request_id":"req_1"}`
	driveChange = "event: drive.changed\ndata: {\"drive_id\":\"drive_1\",\"seq\":1}\n\n"
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
	jsonAnswer   = map[string]string{"Content-Type": "application/json", "request-id": "req_1"}
	streamAnswer = map[string]string{"Content-Type": "text/event-stream", "request-id": "req_1"}
)

// A Developer reports each request, answer, header and stream event that
// is outside Engine's OpenAPI file, and nothing that is within it.
func TestADeveloperReportsWhatIsOutsideTheFile(t *testing.T) {
	without := func(text, part string) string { return strings.Replace(text, part, "", 1) }
	rows := []struct {
		name            string
		method, target  string
		body            string
		invalid, stream bool
		status          int
		header          map[string]string
		answer          string
		// want is part of the one report; empty, nothing is reported.
		want string
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
		{name: "an answer without a declared header", method: "GET", target: "/v1/drives/drive_1", status: 200,
			header: map[string]string{"Content-Type": "application/json"}, answer: drive, want: "lacks the header request-id"},
		{name: "an error type its status does not declare", method: "GET", target: "/v1/drives/drive_1", status: 404, header: jsonAnswer,
			answer: refusal, want: "error invalid_request_error under 404 is not one drives.read declares"},
		{name: "an answer's metadata with a new key", method: "GET", target: "/v1/drives/drive_1", status: 200, header: jsonAnswer,
			answer: strings.Replace(drive, `"metadata":{}`, `"metadata":{"team":"ai"}`, 1)},

		{name: "a stream event within the file", method: "GET", target: "/v1/drives/drive_1/changes/stream", stream: true, status: 200,
			header: streamAnswer, answer: ": quiet\n\n" + driveChange},
		{name: "an undeclared stream event", method: "GET", target: "/v1/drives/drive_1/changes/stream", stream: true, status: 200,
			header: streamAnswer, answer: "event: drive.removed\ndata: {\"drive_id\":\"drive_1\"}\n\n", want: `stream event "drive.removed" is not one drive_changes.stream declares`},
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
		{name: "a marked request to an undeclared path", method: "GET", target: "/v1/batches", invalid: true, status: 400, header: jsonAnswer, answer: refusal},
		{name: "an undeclared path answered outside the file's rule", method: "GET", target: "/v1/batches", invalid: true, status: 410, header: jsonAnswer,
			answer: refusal, want: "answer 410 to a path the file does not declare"},

		{name: "metadata: a new key with a string", method: "POST", target: "/v1/drives/drive_1", body: `{"metadata":{"team":"ai"}}`, status: 200,
			header: jsonAnswer, answer: drive},
		{name: "metadata: a null in an update", method: "POST", target: "/v1/drives/drive_1", body: `{"metadata":{"team":null}}`, status: 200,
			header: jsonAnswer, answer: drive},
		{name: "metadata: a number as a value", method: "POST", target: "/v1/drives/drive_1", body: `{"metadata":{"team":5}}`, status: 200,
			header: jsonAnswer, answer: drive, want: "/metadata/team"},
		{name: "metadata: an unnamed field beside it", method: "POST", target: "/v1/drives/drive_1", body: `{"metadata":{},"labels":{}}`, status: 200,
			header: jsonAnswer, answer: drive, want: "additional properties 'labels' not allowed"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			reported := &failures{}
			developer := NewDeveloper(reported)
			developer.HTTP = InProcess(stub(row.status, row.header, row.answer))
			if row.invalid {
				developer = developer.Invalid()
			}
			var header http.Header
			if row.body != "" {
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
// router called in process and through an HTTP client's transport.
func TestAHandlerAndATransportCheckAsDoDoes(t *testing.T) {
	undeclared := stub(200, jsonAnswer, strings.Replace(drive, `{"id"`, `{"owner":"u","id"`, 1))
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
			send(NewDeveloper(reported), "/v1/drives/drive_1?depth=2", true)
			if len(reported.messages) != 2 || !strings.Contains(reported.messages[0], "marked invalid and was answered 200") {
				t.Errorf("a marked request that is not refused: reported %q; want that and the answer's report", reported.messages)
			}
		})
	}
}
