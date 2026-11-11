package enginetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	engineapi "github.com/tetral-ai/tetral/api"
)

const apiFile = "file:///api/" + engineapi.File

// apiSpec is Engine's OpenAPI file, read once.
type apiSpec struct {
	operations []*apiOperation
	compiler   *jsonschema.Compiler
	mu         sync.Mutex
	schemas    map[string]*jsonschema.Schema
	// undeclared holds the statuses of a /v1 path the file does not
	// declare.
	undeclared []string
}

type apiOperation struct {
	id, method, path string
	segments         []string
	// pointer is the operation's JSON pointer in the file.
	pointer    string
	parameters []apiParameter
	closed     bool
	body       map[string]bool
	required   bool
	responses  map[string]apiResponse
}

type apiParameter struct {
	name, in string
	required bool
	schema   bool
}

type apiResponse struct {
	// content maps a media type to whether it has a schema; events is a
	// stream's event names.
	content    map[string]bool
	events     map[string]bool
	headers    []string
	errorTypes []string
}

var loadAPI = sync.OnceValues(func() (*apiSpec, error) {
	data, err := engineapi.Files.ReadFile(engineapi.File)
	if err != nil {
		return nil, err
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	var file struct {
		Undeclared struct {
			Statuses []string `json:"statuses"`
		} `json:"x-undeclared-paths"`
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
			Closed      bool   `json:"x-closed-query"`
			Parameters  []struct {
				Name     string          `json:"name"`
				In       string          `json:"in"`
				Required bool            `json:"required"`
				Schema   json.RawMessage `json:"schema"`
			} `json:"parameters"`
			RequestBody struct {
				Required bool                       `json:"required"`
				Content  map[string]json.RawMessage `json:"content"`
			} `json:"requestBody"`
			Responses map[string]struct {
				Headers map[string]json.RawMessage `json:"headers"`
				Content map[string]struct {
					Schema json.RawMessage            `json:"schema"`
					Events map[string]json.RawMessage `json:"x-events"`
				} `json:"content"`
				ErrorTypes []string `json:"x-error-types"`
			} `json:"responses"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, err
	}
	spec := &apiSpec{compiler: jsonschema.NewCompiler(), schemas: map[string]*jsonschema.Schema{}, undeclared: file.Undeclared.Statuses}
	spec.compiler.DefaultDraft(jsonschema.Draft2020)
	spec.compiler.UseRegexpEngine(compileECMAScript)
	if err := spec.compiler.AddResource(apiFile, document); err != nil {
		return nil, err
	}
	for path, methods := range file.Paths {
		for method, declared := range methods {
			operation := &apiOperation{id: declared.OperationID, method: strings.ToUpper(method), path: path, segments: strings.Split(path, "/"),
				pointer: "/paths/" + escapePointer(path) + "/" + method, closed: declared.Closed, body: map[string]bool{},
				required: declared.RequestBody.Required, responses: map[string]apiResponse{}}
			for _, parameter := range declared.Parameters {
				operation.parameters = append(operation.parameters, apiParameter{name: parameter.Name, in: parameter.In, required: parameter.Required, schema: parameter.Schema != nil})
			}
			for media := range declared.RequestBody.Content {
				operation.body[media] = true
			}
			for status, response := range declared.Responses {
				answer := apiResponse{content: map[string]bool{}, errorTypes: response.ErrorTypes}
				for media, content := range response.Content {
					answer.content[media] = content.Schema != nil
					if content.Events != nil {
						answer.events = map[string]bool{}
						for name := range content.Events {
							answer.events[name] = true
						}
					}
				}
				for name := range response.Headers {
					answer.headers = append(answer.headers, name)
				}
				operation.responses[status] = answer
			}
			spec.operations = append(spec.operations, operation)
		}
	}
	return spec, nil
})

func escapePointer(token string) string {
	return strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}

// operation returns the operation of method and path, with its path
// parameters; a literal segment wins over a parameter.
func (s *apiSpec) operation(method, path string) (*apiOperation, map[string]string) {
	segments := strings.Split(path, "/")
	var found *apiOperation
	var values map[string]string
	literals := -1
	for _, operation := range s.operations {
		if operation.method != method || len(operation.segments) != len(segments) {
			continue
		}
		matched, count := map[string]string{}, 0
		for i, segment := range operation.segments {
			switch {
			case strings.HasPrefix(segment, "{") && segments[i] != "":
				matched[strings.Trim(segment, "{}")] = segments[i]
			case segment == segments[i]:
				count++
			default:
				matched = nil
			}
			if matched == nil {
				break
			}
		}
		if matched != nil && count > literals {
			found, values, literals = operation, matched, count
		}
	}
	return found, values
}

// validate holds value to the schema at pointer of the file.
func (s *apiSpec) validate(pointer string, value any) error {
	s.mu.Lock()
	schema, found := s.schemas[pointer]
	if !found {
		compiled, err := s.compiler.Compile(apiFile + "#" + pointer)
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("the file's schema at %s: %w", pointer, err)
		}
		schema, s.schemas[pointer] = compiled, compiled
	}
	s.mu.Unlock()
	if err := schema.Validate(value); err != nil {
		text := err.Error()
		if _, causes, cut := strings.Cut(text, "\n"); cut {
			text = strings.TrimSpace(causes)
		}
		return errors.New(text)
	}
	return nil
}

// Developer is a test's client of Engine's public API, a developer's or a
// Controller's. It sends exactly the request a test gives it, with no retry
// and no repair, and holds the request, the answer with its headers and
// every stream event to Engine's OpenAPI file: a request outside it fails
// the test as the test's defect, an answer or event outside it as Engine's.
// A request made invalid on purpose, or sent to a path the file does not
// declare, is marked, through Invalid or InvalidRequest: only its answer is
// checked, and it must be a refusal.
//
// A test sends through Do and Stream, or keeps the requests it builds and
// sends them through Handler (a router called in process) or Transport (an
// HTTP client's); the three check alike.
type Developer struct {
	BaseURL string
	// HTTP sends the requests of Do and Stream.
	HTTP *http.Client
	// Authorize adds the credentials of a request of Do and Stream.
	Authorize func(*http.Request) error

	t       T
	invalid bool
}

// NewDeveloper returns a client that reports to t.
func NewDeveloper(t T) *Developer { return &Developer{t: t, HTTP: http.DefaultClient} }

// Invalid returns the client for one request of Do or Stream that is
// outside the file on purpose.
func (d *Developer) Invalid() *Developer {
	marked := *d
	marked.invalid = true
	return &marked
}

type invalidRequest struct{}

// InvalidRequest marks a request sent through Handler or Transport as
// outside the file on purpose.
func InvalidRequest(request *http.Request) *http.Request {
	return request.WithContext(context.WithValue(request.Context(), invalidRequest{}, true))
}

// InvalidRequests returns router with every request sent through it marked
// as InvalidRequest marks one.
func InvalidRequests(router http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		router.ServeHTTP(w, InvalidRequest(request))
	})
}

// InProcess returns an HTTP client that serves each request from handler
// without a connection; the whole answer is written before it returns.
func InProcess(handler http.Handler) *http.Client {
	return &http.Client{Transport: roundTrip(func(request *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Result(), nil
	})}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func (d *Developer) report(format string, args ...any) {
	d.t.Helper()
	d.t.Errorf("enginetest: "+format, args...)
}

// Do sends one request and returns its whole answer.
func (d *Developer) Do(ctx context.Context, method, target string, header http.Header, body []byte) (Response, error) {
	d.t.Helper()
	response, err := d.send(ctx, method, target, header, body)
	if err != nil {
		return Response{}, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	return Response{Status: response.StatusCode, Header: response.Header, Body: data}, err
}

// DeveloperEvent is one event of a stream: its name and its data.
type DeveloperEvent struct {
	Name string
	Data []byte
}

// DeveloperStream is an open event stream of the public API.
type DeveloperStream struct {
	body   io.ReadCloser
	frames frames
	events []DeveloperEvent
}

// Stream sends one request whose success answer is an event stream. A 200
// answer returns the open stream; any other answer returns its Response.
func (d *Developer) Stream(ctx context.Context, method, target string, header http.Header) (*DeveloperStream, Response, error) {
	d.t.Helper()
	response, err := d.send(ctx, method, target, header, nil)
	if err != nil {
		return nil, Response{}, err
	}
	if response.StatusCode != http.StatusOK {
		defer func() { _ = response.Body.Close() }()
		data, err := io.ReadAll(response.Body)
		return nil, Response{Status: response.StatusCode, Header: response.Header, Body: data}, err
	}
	return &DeveloperStream{body: response.Body}, Response{Status: response.StatusCode, Header: response.Header}, nil
}

// Close closes the stream.
func (s *DeveloperStream) Close() error { return s.body.Close() }

// Next returns the stream's next event.
func (s *DeveloperStream) Next() (DeveloperEvent, error) {
	buffer := make([]byte, 4096)
	for len(s.events) == 0 {
		n, err := s.body.Read(buffer)
		s.events = s.frames.feed(buffer[:n], nil)
		if err != nil && len(s.events) == 0 {
			return DeveloperEvent{}, err
		}
	}
	event := s.events[0]
	s.events = s.events[1:]
	return event, nil
}

func (d *Developer) send(ctx context.Context, method, target string, header http.Header, body []byte) (*http.Response, error) {
	d.t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, d.BaseURL+target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for name, values := range header {
		request.Header[name] = values
	}
	if d.invalid {
		request = InvalidRequest(request)
	}
	if d.Authorize != nil {
		if err := d.Authorize(request); err != nil {
			return nil, err
		}
	}
	client := *d.HTTP
	if client.Transport == nil {
		client.Transport = http.DefaultTransport
	}
	client.Transport = d.Transport(client.Transport)
	return client.Do(request)
}

// Transport returns base with every request, answer and stream event held
// to the file.
func (d *Developer) Transport(base http.RoundTripper) http.RoundTripper {
	return roundTrip(func(request *http.Request) (*http.Response, error) {
		d.t.Helper()
		body, err := requestBody(request)
		if err != nil {
			return nil, err
		}
		sent := d.sent(request, body)
		what, operation := sent.what, sent.operation
		forward := request.Clone(request.Context())
		forward.Body = io.NopCloser(bytes.NewReader(body))
		response, err := base.RoundTrip(forward)
		if err != nil || !sent.public {
			return response, err
		}
		sent.answered(response.StatusCode)
		if stream := d.stream(what, operation, response.StatusCode, response.Header); stream != nil {
			response.Body = &checkedBody{ReadCloser: response.Body, stream: stream}
			return response, nil
		}
		if isEventStream(response.Header) {
			d.report("%s: answer %d is an event stream the file does not declare", what, response.StatusCode)
			return response, nil
		}
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		d.answered(what, operation, response.StatusCode, response.Header, data)
		response.Body = io.NopCloser(bytes.NewReader(data))
		return response, err
	})
}

// Handler returns handler, a router a test calls in process, with every
// request, answer and stream event held to the file.
func (d *Developer) Handler(handler http.Handler) http.Handler {
	return checkedHandler{d: d, handler: handler}
}

type checkedHandler struct {
	d       *Developer
	handler http.Handler
}

func (h checkedHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	d := h.d
	d.t.Helper()
	body, err := requestBody(request)
	if err != nil {
		d.report("%s %s: the request's body: %v", request.Method, request.URL.RequestURI(), err)
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	checked := &checkedWriter{ResponseWriter: w, d: d, sent: d.sent(request, body)}
	h.handler.ServeHTTP(checked, request)
	checked.WriteHeader(http.StatusOK)
	if checked.stream == nil && checked.sent.public {
		d.answered(checked.sent.what, checked.sent.operation, checked.status, checked.header, checked.body.Bytes())
	}
}

func requestBody(request *http.Request) ([]byte, error) {
	if request.Body == nil {
		return nil, nil
	}
	defer func() { _ = request.Body.Close() }()
	return io.ReadAll(request.Body)
}

func isEventStream(header http.Header) bool {
	media, _, _ := mime.ParseMediaType(header.Get("Content-Type"))
	return media == "text/event-stream"
}

// checkedWriter holds what a handler writes to the file: a stream's events
// as they are written, any other answer once the handler returns.
type checkedWriter struct {
	http.ResponseWriter
	d      *Developer
	sent   developerRequest
	status int
	header http.Header
	body   bytes.Buffer
	stream *checkedStream
}

func (w *checkedWriter) WriteHeader(status int) {
	w.d.t.Helper()
	if w.status == 0 && status >= http.StatusOK {
		w.status, w.header = status, w.ResponseWriter.Header().Clone()
		if !w.sent.public {
			w.ResponseWriter.WriteHeader(status)
			return
		}
		w.sent.answered(status)
		if w.stream = w.d.stream(w.sent.what, w.sent.operation, status, w.header); w.stream == nil && isEventStream(w.header) {
			w.d.report("%s: answer %d is an event stream the file does not declare", w.sent.what, status)
		}
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *checkedWriter) Write(data []byte) (int, error) {
	w.d.t.Helper()
	w.WriteHeader(http.StatusOK)
	if w.stream != nil {
		w.stream.written(data)
	} else {
		w.body.Write(data)
	}
	return w.ResponseWriter.Write(data)
}

func (w *checkedWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap lets http.ResponseController reach the writer's deadlines.
func (w *checkedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type checkedBody struct {
	io.ReadCloser
	stream *checkedStream
}

func (b *checkedBody) Read(buffer []byte) (int, error) {
	n, err := b.ReadCloser.Read(buffer)
	b.stream.written(buffer[:n])
	return n, err
}

// frames splits the bytes of an event stream into its events. A frame of
// comment lines alone carries no event.
type frames struct {
	line  []byte
	event DeveloperEvent
	data  []string
	open  bool
}

// feed returns the events data completes; unknown is told each line that
// is no field of an event stream.
func (f *frames) feed(data []byte, unknown func(line string)) []DeveloperEvent {
	var events []DeveloperEvent
	for _, b := range data {
		if b != '\n' {
			f.line = append(f.line, b)
			continue
		}
		line := strings.TrimSuffix(string(f.line), "\r")
		f.line = f.line[:0]
		if line == "" {
			if f.open {
				f.event.Data = []byte(strings.Join(f.data, "\n"))
				events = append(events, f.event)
			}
			f.event, f.data, f.open = DeveloperEvent{}, nil, false
			continue
		}
		name, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch name {
		case "":
		case "event":
			f.event.Name, f.open = value, true
		case "data":
			f.data, f.open = append(f.data, value), true
		case "id", "retry":
		default:
			if unknown != nil {
				unknown(line)
			}
		}
	}
	return events
}

// checkedStream holds the events of one stream answer to its operation.
type checkedStream struct {
	d         *Developer
	what      string
	operation *apiOperation
	frames    frames
}

// stream returns the check of a 200 answer that is one of the operation's
// streams, after checking its headers; nil for any other answer.
func (d *Developer) stream(what string, operation *apiOperation, status int, header http.Header) *checkedStream {
	d.t.Helper()
	if operation == nil || status != http.StatusOK || operation.responses["200"].events == nil {
		return nil
	}
	d.headers(what, operation, status, header)
	if !isEventStream(header) {
		d.report("%s: a stream's answer is %q, not text/event-stream", what, header.Get("Content-Type"))
	}
	record(operation, "200", "", "")
	return &checkedStream{d: d, what: what, operation: operation}
}

func (s *checkedStream) written(data []byte) {
	s.d.t.Helper()
	events := s.frames.feed(data, func(line string) {
		s.d.report("%s: a stream line %q is no field of an event stream", s.what, abbreviate([]byte(line)))
	})
	for _, event := range events {
		if !s.operation.responses["200"].events[event.Name] {
			s.d.report("%s: stream event %q is not one %s declares", s.what, event.Name, s.operation.id)
			continue
		}
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(event.Data))
		if err != nil {
			s.d.report("%s: stream event %s holds no JSON: %s", s.what, event.Name, abbreviate(event.Data))
			continue
		}
		pointer := s.operation.pointer + "/responses/200/content/text~1event-stream/x-events/" + escapePointer(event.Name)
		if err := loadedAPI().validate(pointer, value); err != nil {
			s.d.report("%s: stream event %s of %s is outside the file: %v: %s", s.what, event.Name, s.operation.id, err, abbreviate(event.Data))
		}
		record(s.operation, "", "", event.Name)
	}
}

func loadedAPI() *apiSpec {
	spec, err := loadAPI()
	if err != nil {
		panic(fmt.Sprintf("enginetest: Engine's OpenAPI file: %v", err))
	}
	return spec
}

// developerRequest is a request with what of it is outside the file.
type developerRequest struct {
	d         *Developer
	what      string
	operation *apiOperation
	invalid   bool
	outside   []string
	// public is false for a path outside /v1, such as a health probe, and
	// for the machine's routes under /v1/client, whose contract is
	// protocol/: the file says nothing of them and nothing is checked.
	public bool
}

// sent holds a request to the file: its name in reports, its operation
// (nil for a path the file does not declare) and what of it is outside.
func (d *Developer) sent(request *http.Request, body []byte) developerRequest {
	sent := developerRequest{d: d, what: request.Method + " " + request.URL.RequestURI() + sentAt(), invalid: request.Context().Value(invalidRequest{}) != nil}
	sent.public = strings.HasPrefix(request.URL.Path, "/v1/") && !strings.HasPrefix(request.URL.Path, "/v1/client/")
	if !sent.public {
		return sent
	}
	spec := loadedAPI()
	var pathValues map[string]string
	if sent.operation, pathValues = spec.operation(request.Method, request.URL.EscapedPath()); sent.operation == nil {
		sent.outside = []string{"the file declares no " + request.Method + " " + request.URL.Path}
	} else {
		sent.outside = requestOutside(spec, sent.operation, pathValues, request, body)
	}
	return sent
}

// sentAt names the line of the test a request was sent from, which a
// report's own position does not when a helper or the test's middleware
// stands between.
func sentAt() string {
	callers := make([]uintptr, 64)
	frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers)])
	at := ""
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.File, "_test.go") {
			at = fmt.Sprintf(" (%s:%d)", filepath.Base(frame.File), frame.Line)
		}
		if !more {
			return at
		}
	}
}

// answered reports the request once its answer's status is known: one
// outside the file and not marked, or marked and not refused.
func (s developerRequest) answered(status int) {
	s.d.t.Helper()
	switch {
	case !s.invalid && len(s.outside) > 0:
		s.d.report("%s: request outside the file, answered %d: %s", s.what, status, strings.Join(s.outside, "; "))
	case s.invalid && status < http.StatusBadRequest:
		s.d.report("%s: request is marked invalid and was answered %d", s.what, status)
	}
}

// requestOutside returns what of a request is outside its operation.
func requestOutside(spec *apiSpec, operation *apiOperation, pathValues map[string]string, request *http.Request, body []byte) []string {
	var outside []string
	query := request.URL.Query()
	declared := map[string]bool{}
	for i, parameter := range operation.parameters {
		var value string
		var present bool
		switch parameter.in {
		case "path":
			value, present = pathValues[parameter.name]
		case "query":
			declared[parameter.name] = true
			value, present = query.Get(parameter.name), query.Has(parameter.name)
		case "header":
			value, present = request.Header.Get(parameter.name), request.Header.Get(parameter.name) != ""
		}
		switch {
		case !present && parameter.required:
			outside = append(outside, fmt.Sprintf("%s parameter %s is missing", parameter.in, parameter.name))
		case present && parameter.schema:
			// A parameter is text; a schema of a number or a boolean
			// reads its text, any other schema the text itself.
			pointer := fmt.Sprintf("%s/parameters/%d/schema", operation.pointer, i)
			if err := spec.validate(pointer, parameterValue(value)); err != nil && spec.validate(pointer, value) != nil {
				outside = append(outside, fmt.Sprintf("%s parameter %s: %v", parameter.in, parameter.name, err))
			}
		}
	}
	if operation.closed {
		for name := range query {
			if !declared[name] {
				outside = append(outside, "query parameter "+name+" is not declared")
			}
		}
	}
	media, parameters, _ := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if media == "" && operation.body["application/json"] {
		// Engine reads a JSON body whatever its Content-Type.
		media = "application/json"
	}
	switch {
	case len(body) == 0 && operation.required:
		outside = append(outside, "the body is missing")
	case len(body) == 0:
	case len(operation.body) == 0:
		outside = append(outside, "the operation takes no body")
	case !operation.body[media]:
		outside = append(outside, fmt.Sprintf("the body's type %q is not one of %v", media, slices.Sorted(maps.Keys(operation.body))))
	case media == "application/json":
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(body))
		if err != nil {
			return append(outside, "the body is no JSON")
		}
		if err := spec.validate(operation.pointer+"/requestBody/content/application~1json/schema", value); err != nil {
			outside = append(outside, "body: "+err.Error())
		}
	case media == "multipart/form-data":
		fields, err := formFields(body, parameters["boundary"])
		if err != nil {
			return append(outside, "the body is no form: "+err.Error())
		}
		if err := spec.validate(operation.pointer+"/requestBody/content/multipart~1form-data/schema", fields); err != nil {
			outside = append(outside, "form: "+err.Error())
		}
	}
	return outside
}

// parameterValue reads a parameter's text as the JSON value it spells, the
// text itself when it spells none.
func parameterValue(text string) any {
	value, err := jsonschema.UnmarshalJSON(strings.NewReader(text))
	if err != nil {
		return text
	}
	switch value.(type) {
	case json.Number, bool:
		return value
	}
	return text
}

// formFields is a form's fields as an object: a file field is its bytes'
// text; a field sent twice, or named with a trailing [], is an array.
func formFields(body []byte, boundary string) (map[string]any, error) {
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	fields := map[string]any{}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			return fields, nil
		}
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(part)
		if err != nil {
			return nil, err
		}
		name := part.FormName()
		switch held := fields[name].(type) {
		case nil:
			if strings.HasSuffix(name, "[]") {
				fields[name] = []any{string(data)}
			} else {
				fields[name] = string(data)
			}
		case []any:
			fields[name] = append(held, string(data))
		default:
			fields[name] = []any{held, string(data)}
		}
	}
}

// headers reports each header the answer's status declares and the answer
// lacks.
func (d *Developer) headers(what string, operation *apiOperation, status int, header http.Header) {
	d.t.Helper()
	for _, name := range operation.responses[fmt.Sprint(status)].headers {
		if header.Get(name) == "" {
			d.report("%s: answer %d of %s lacks the header %s", what, status, operation.id, name)
		}
	}
}

// answered holds an answer that is no stream to its operation, or to the
// file's rule for a path it does not declare.
func (d *Developer) answered(what string, operation *apiOperation, code int, header http.Header, body []byte) {
	d.t.Helper()
	status, spec := fmt.Sprint(code), loadedAPI()
	if operation == nil {
		if !slices.Contains(spec.undeclared, status) {
			d.report("%s: answer %d to a path the file does not declare; it states %v", what, code, spec.undeclared)
		}
		return
	}
	answer, declared := operation.responses[status]
	if !declared {
		d.report("%s: answer %d is not one %s declares: %s", what, code, operation.id, abbreviate(body))
		return
	}
	d.headers(what, operation, code, header)
	media, _, _ := mime.ParseMediaType(header.Get("Content-Type"))
	hasSchema, known := answer.content[media]
	if _, wildcard := answer.content["*/*"]; wildcard {
		known = true
	}
	errorType := ""
	switch {
	case len(answer.content) == 0:
		if len(body) > 0 {
			d.report("%s: answer %d of %s has a body where the file has none: %s", what, code, operation.id, abbreviate(body))
		}
	case !known:
		d.report("%s: answer %d of %s is %q, not one of the file's types", what, code, operation.id, media)
	case media == "application/json" && hasSchema:
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(body))
		if err != nil {
			d.report("%s: answer %d of %s holds no JSON: %s", what, code, operation.id, abbreviate(body))
			return
		}
		pointer := operation.pointer + "/responses/" + status + "/content/application~1json/schema"
		if err := spec.validate(pointer, value); err != nil {
			d.report("%s: answer %d of %s is outside the file: %v: %s", what, code, operation.id, err, abbreviate(body))
		}
		if answer.errorTypes != nil {
			var failure struct {
				Error struct {
					Type string `json:"type"`
				} `json:"error"`
			}
			_ = json.Unmarshal(body, &failure)
			if errorType = failure.Error.Type; !slices.Contains(answer.errorTypes, errorType) {
				d.report("%s: error %s under %d is not one %s declares (%v)", what, errorType, code, operation.id, answer.errorTypes)
			}
		}
	}
	record(operation, status, errorType, "")
}

var coverage struct {
	sync.Mutex
	file *os.File
	seen map[string]bool
}

// record appends what was checked to the coverage file, each line once per
// process.
func record(operation *apiOperation, status, errorType, event string) {
	path := os.Getenv(engineapi.CoverageFileEnv)
	if path == "" || operation == nil {
		return
	}
	line, _ := json.Marshal(engineapi.Checked{Operation: operation.id, Status: status, Error: errorType, Event: event})
	coverage.Lock()
	defer coverage.Unlock()
	if coverage.seen[string(line)] {
		return
	}
	if coverage.file == nil {
		// The test runner names the record file.
		//nolint:gosec
		file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return
		}
		coverage.file, coverage.seen = file, map[string]bool{}
	}
	coverage.seen[string(line)] = true
	_, _ = coverage.file.Write(append(line, '\n'))
}

// Target is path with query, when it has any.
func Target(path string, query url.Values) string {
	if len(query) == 0 {
		return path
	}
	return path + "?" + query.Encode()
}
