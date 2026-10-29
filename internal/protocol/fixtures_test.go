package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// fixture is one entry of a protocol fixture file.
type fixture struct {
	Name       string          `json:"name"`
	Schema     string          `json:"schema"`
	Valid      bool            `json:"valid"`
	Value      json.RawMessage `json:"value"`
	HTTPStatus int             `json:"http_status"`
	// Endpoint and Status are the endpoint and HTTP status of an error
	// body; Endpoint and Error name the answer a rejected request gets.
	Endpoint DriveTransferEndpoint `json:"endpoint"`
	Status   int                   `json:"status"`
	Error    *ClientError          `json:"error"`
	// Pair names the fixtures of the request a response answers.
	Pair map[string]string `json:"pair"`
	// Expect names the rules an invalid drive transfer fixture breaks, a
	// relation beyond the schema as semantic:<name>.
	Expect []string `json:"expect"`
}

// codec decodes one schema definition's fixtures through its exported
// decoder and encodes the result back to the fixture's form. decode takes the
// fixture for the context its decoder needs, such as an error body's HTTP
// status, and the JSON to decode.
type codec struct {
	decode func(f fixture, raw []byte) (any, error)
	encode func(any) []byte
}

func jsonCodec[T encodable](decode func([]byte) (T, error)) codec {
	return codec{
		decode: func(_ fixture, raw []byte) (any, error) { return decode(raw) },
		encode: func(v any) []byte { return Encode(v.(T)) },
	}
}

// parameterCodec decodes a query, path parameter or header fixture, an
// object of strings, through its exported decoder, and encodes the result
// back to that object. A fixture member that is not a string has no
// parameter form, so the definition's decoder reads the fixture's JSON
// instead.
func parameterCodec[T any](decode func(map[string]string) (T, error), definition decodeFunc[T], encode func(T) map[string][]string) codec {
	return codec{
		decode: func(_ fixture, raw []byte) (any, error) {
			var params map[string]string
			if json.Unmarshal(raw, &params) != nil {
				return decodeChecked(raw, definition, nil)
			}
			return decode(params)
		},
		encode: func(v any) []byte {
			params := make(map[string]string)
			for name, values := range encode(v.(T)) {
				params[name] = values[0]
			}
			data, _ := json.Marshal(params)
			return data
		},
	}
}

// values adapts a decoder of url.Values or http.Header to a fixture's
// object of strings.
func values[P ~map[string][]string, T any](decode func(P) (T, error)) func(map[string]string) (T, error) {
	return func(params map[string]string) (T, error) { return decode(singleValues(params)) }
}

// uploadHeadersAndLength decodes upload headers and then applies the length
// rules, as the hub does, so that a fixture answered by a length rule gets
// that answer.
func uploadHeadersAndLength(header http.Header) (UploadContentRequestHeaders, error) {
	h, err := DecodeUploadContentRequestHeaders(header)
	if err != nil {
		return h, err
	}
	return h, CheckUploadLength(h)
}

var toolExecutionCodecs = map[string]codec{
	"Downstream":          jsonCodec(DecodeToolDownstream),
	"ToolStatus":          jsonCodec(DecodeToolStatus),
	"DriveSyncReport":     jsonCodec(DecodeDriveSyncReport),
	"Acknowledgement":     jsonCodec(DecodeAcknowledgement),
	"ProtocolErrorReport": jsonCodec(DecodeProtocolErrorReport),
	"ReadyExtension":      jsonCodec(DecodeReadyExtension),
	"SandboxPolicy":       jsonCodec(DecodeSandboxPolicy),
}

var connectionCodecs = map[string]codec{
	"EventsQuery":          parameterCodec(values(DecodeEventsQuery), eventsQuery, EventsQuery.Query),
	"Downstream":           jsonCodec(DecodeDownstream),
	"Ready":                jsonCodec(DecodeReady),
	"Heartbeat":            jsonCodec(DecodeHeartbeat),
	"Acknowledgement":      toolExecutionCodecs["Acknowledgement"],
	"GrantAcknowledgement": jsonCodec(DecodeGrantAcknowledgement),
	"SessionStatus":        jsonCodec(DecodeSessionStatus),
	"McpTools":             jsonCodec(DecodeMcpTools),
	"CredentialRequest":    jsonCodec(DecodeCredentialRequest),
	"CredentialResponse":   jsonCodec(DecodeCredentialResponse),
	"GitTicketRequest":     jsonCodec(DecodeGitTicketRequest),
	"GitTicketResponse":    jsonCodec(DecodeGitTicketResponse),
	"ErrorBody": {
		decode: func(f fixture, raw []byte) (any, error) { return DecodeErrorBody(f.HTTPStatus, raw) },
		encode: func(v any) []byte { return Encode(v.(ErrorBody)) },
	},
	"ToolStatus":          toolExecutionCodecs["ToolStatus"],
	"DriveSyncReport":     toolExecutionCodecs["DriveSyncReport"],
	"ProtocolErrorReport": toolExecutionCodecs["ProtocolErrorReport"],
}

// driveTransferCodecs decode the drive transfer fixtures; a response's
// decoder takes the decoded fixtures that its fixture's pair names as the
// request it answers.
func driveTransferCodecs(fixtures []fixture) map[string]codec {
	byName := make(map[string]fixture, len(fixtures))
	for _, f := range fixtures {
		byName[f.Name] = f
	}
	codecs := map[string]codec{
		"ListFilesQuery":   parameterCodec(values(DecodeListFilesQuery), listFilesQuery, ListFilesQuery.Query),
		"ListChangesQuery": parameterCodec(values(DecodeListChangesQuery), listChangesQuery, ListChangesQuery.Query),
		"CommonQuery":      parameterCodec(values(DecodeCommonQuery), commonQuery, CommonQuery.Query),
		"DrivePathParams": parameterCodec(DecodeDrivePathParams, drivePathParams, func(p DrivePathParams) map[string][]string {
			return singleValues(p.Params())
		}),
		"ContentPathParams": parameterCodec(DecodeContentPathParams, contentPathParams, func(p ContentPathParams) map[string][]string {
			return singleValues(p.Params())
		}),
		"UploadContentRequestHeaders":   parameterCodec(values(uploadHeadersAndLength), uploadContentRequestHeaders, UploadContentRequestHeaders.Header),
		"DownloadContentRequestHeaders": parameterCodec(values(DecodeDownloadContentRequestHeaders), downloadContentRequestHeaders, DownloadContentRequestHeaders.Header),
		"RateLimitedResponseHeaders":    parameterCodec(values(DecodeRateLimitedResponseHeaders), rateLimitedResponseHeaders, RateLimitedResponseHeaders.Header),
		"DriveChanged":                  jsonCodec(DecodeDriveChanged),
		"CommitRequest":                 jsonCodec(DecodeCommitRequest),
		"Error": {
			decode: func(f fixture, raw []byte) (any, error) { return DecodeDriveTransferError(f.Endpoint, f.Status, raw) },
			encode: func(v any) []byte { return Encode(v.(ErrorBody)) },
		},
	}
	// paired decodes the fixture that f's pair names under key.
	paired := func(f fixture, key string) (any, bool) {
		name, ok := f.Pair[key]
		if !ok {
			return nil, false
		}
		request := byName[name]
		v, err := codecs[request.Schema].decode(request, request.Value)
		if err != nil {
			panic("paired fixture " + name + " does not decode: " + err.Error())
		}
		return v, true
	}
	codecs["PartialContentResponseHeaders"] = codec{
		decode: func(f fixture, raw []byte) (any, error) {
			var ctx PartialContentResponseContext
			if h, ok := paired(f, "request_headers"); ok {
				ctx.RequestHeaders = Some(h.(DownloadContentRequestHeaders))
			}
			return parameterCodec(values(func(h http.Header) (PartialContentResponseHeaders, error) {
				return DecodePartialContentResponseHeaders(h, ctx)
			}), partialContentResponseHeaders, PartialContentResponseHeaders.Header).decode(f, raw)
		},
		encode: parameterCodec[PartialContentResponseHeaders](nil, nil, PartialContentResponseHeaders.Header).encode,
	}
	codecs["ListFilesResponse"] = codec{
		decode: func(f fixture, raw []byte) (any, error) {
			var ctx ListFilesResponseContext
			if q, ok := paired(f, "query"); ok {
				ctx.Query = Some(q.(ListFilesQuery))
			}
			return DecodeListFilesResponse(raw, ctx)
		},
		encode: func(v any) []byte { return Encode(v.(ListFilesResponse)) },
	}
	codecs["ListChangesResponse"] = codec{
		decode: func(f fixture, raw []byte) (any, error) {
			var ctx ListChangesResponseContext
			if q, ok := paired(f, "query"); ok {
				ctx.Query = Some(q.(ListChangesQuery))
			}
			return DecodeListChangesResponse(raw, ctx)
		},
		encode: func(v any) []byte { return Encode(v.(ListChangesResponse)) },
	}
	codecs["UploadContentResponse"] = codec{
		decode: func(f fixture, raw []byte) (any, error) {
			var ctx UploadContentResponseContext
			if p, ok := paired(f, "path_params"); ok {
				ctx.PathParams = Some(p.(ContentPathParams))
			}
			if h, ok := paired(f, "request_headers"); ok {
				ctx.RequestHeaders = Some(h.(UploadContentRequestHeaders))
			}
			return DecodeUploadContentResponse(raw, ctx)
		},
		encode: func(v any) []byte { return Encode(v.(UploadContentResponse)) },
	}
	codecs["CommitResponse"] = codec{
		decode: func(f fixture, raw []byte) (any, error) {
			var ctx CommitResponseContext
			if r, ok := paired(f, "request"); ok {
				ctx.Request = Some(r.(CommitRequest))
			}
			return DecodeCommitResponse(raw, ctx)
		},
		encode: func(v any) []byte { return Encode(v.(CommitResponse)) },
	}
	return codecs
}

func readFixtures(t *testing.T, name string) []fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", name))
	if err != nil {
		t.Fatal(err)
	}
	// json.Unmarshal reads the file as one whole document, so a truncated
	// file or data after the document fails.
	var fixtures []fixture
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

func TestFixturesDecodeAndReencode(t *testing.T) {
	files := []struct {
		name string
		// selects names the checks below that at least one of the file's
		// fixtures reaches, so that a selector that matches nothing fails.
		selects []string
		codecs  func([]fixture) map[string]codec
	}{
		{"tool-execution-v1.fixtures.json", nil, func([]fixture) map[string]codec { return toolExecutionCodecs }},
		{"connection-v1.fixtures.json", []string{"http_status"}, func([]fixture) map[string]codec { return connectionCodecs }},
		{"drive-transfer-v1.fixtures.json", []string{"relation only", "endpoint and status", "endpoint and error", "pair"}, driveTransferCodecs},
	}
	for _, file := range files {
		t.Run(file.name, func(t *testing.T) {
			fixtures := readFixtures(t, file.name)
			counts := census(fixtures)
			for _, selector := range file.selects {
				if counts[selector] == 0 {
					t.Fatalf("no fixture is selected by %q", selector)
				}
			}
			codecs := file.codecs(fixtures)
			for _, f := range fixtures {
				t.Run(f.Name, func(t *testing.T) {
					runFixture(t, codecs, f)
				})
			}
		})
	}
}

func census(fixtures []fixture) map[string]int {
	counts := map[string]int{}
	for _, f := range fixtures {
		if f.HTTPStatus != 0 {
			counts["http_status"]++
		}
		if f.Endpoint != "" && f.Status != 0 {
			counts["endpoint and status"]++
		}
		if f.Endpoint != "" && f.Error != nil {
			counts["endpoint and error"]++
		}
		if len(f.Pair) > 0 {
			counts["pair"]++
		}
		if !f.Valid && len(f.Expect) > 0 && !slices.ContainsFunc(f.Expect, func(rule string) bool { return !strings.HasPrefix(rule, "semantic:") }) {
			counts["relation only"]++
		}
	}
	return counts
}

// unknownMembers lists, for each fixture that has them, the members its
// schema allows without defining them. A decoder that skips any other member
// has dropped a field, and the encoding differs from the fixture.
var unknownMembers = map[string][]string{
	"connected_with_unknown_optional_field": {"/payload/hub", "/trace"},
	"ready_with_unknown_optional_field":     {"/capabilities/gpu", "/hostname"},
}

func runFixture(t *testing.T, codecs map[string]codec, f fixture) {
	t.Helper()
	c, ok := codecs[f.Schema]
	if !ok {
		t.Fatalf("no decoder for %s", f.Schema)
	}
	got, err := c.decode(f, f.Value)
	if !f.Valid {
		var decodeErr *DecodeError
		if !errors.As(err, &decodeErr) {
			t.Fatalf("invalid fixture decoded: %v, %v", got, err)
		}
		if f.Error != nil && (decodeErr.Status != f.Error.Status || decodeErr.Code != f.Error.Code) {
			t.Fatalf("answered %d %s, want %d %s", decodeErr.Status, decodeErr.Code, f.Error.Status, f.Error.Code)
		}
		return
	}
	if err != nil {
		t.Fatalf("valid fixture rejected: %v", err)
	}
	assertReencodes(t, c, f, got)
	if f.HTTPStatus != 0 || f.Endpoint != "" {
		assertRejectedUnderAnotherStatus(t, c, f)
	}
}

// assertRejectedUnderAnotherStatus decodes a valid error body again with a
// status its table does not pair with its code: 400, or 409 for a 400 body.
func assertRejectedUnderAnotherStatus(t *testing.T, c codec, f fixture) {
	t.Helper()
	another := 400
	if f.HTTPStatus == 400 || f.Status == 400 {
		another = 409
	}
	other := f
	if f.HTTPStatus != 0 {
		other.HTTPStatus = another
	} else {
		other.Status = another
	}
	_, err := c.decode(other, f.Value)
	var decodeErr *DecodeError
	if !errors.As(err, &decodeErr) || decodeErr.Status != 400 || decodeErr.Code != ErrorInvalidMessage {
		t.Fatalf("decoded under HTTP %d: %v", another, err)
	}
}

// assertReencodes checks that the encoding of a decoded fixture decodes to an
// equal value and, read by encoding/json, equals the fixture once the members
// its schema allows without defining are removed from it.
func assertReencodes(t *testing.T, c codec, f fixture, got any) {
	t.Helper()
	encoded := c.encode(got)
	again, err := c.decode(f, encoded)
	if err != nil {
		t.Fatalf("encoding %s does not decode: %v", encoded, err)
	}
	if !reflect.DeepEqual(got, again) {
		t.Fatalf("encoding decodes to %#v, want %#v", again, got)
	}
	want := jsonValue(t, f.Value)
	for _, ptr := range unknownMembers[f.Name] {
		removeMember(t, want, ptr)
	}
	if path, ok := sameJSON(want, jsonValue(t, encoded), ""); !ok {
		t.Fatalf("encoding differs at %q:\n got %s\nwant %s", path, encoded, f.Value)
	}
}

// jsonValue reads data with encoding/json, keeping each number's text.
func jsonValue(t *testing.T, data []byte) any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func removeMember(t *testing.T, v any, ptr string) {
	t.Helper()
	tokens := strings.Split(ptr, "/")[1:]
	for i, token := range tokens {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch node := v.(type) {
		case map[string]any:
			child, ok := node[token]
			if !ok {
				t.Fatalf("unknown member %q is not in the fixture", ptr)
			}
			if i == len(tokens)-1 {
				delete(node, token)
				return
			}
			v = child
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index >= len(node) {
				t.Fatalf("unknown member %q is not in the fixture", ptr)
			}
			v = node[index]
		default:
			t.Fatalf("unknown member %q is not in the fixture", ptr)
		}
	}
}

// sameJSON compares two values read by jsonValue and returns the pointer of
// the first difference.
func sameJSON(a, b any, path string) (string, bool) {
	switch a := a.(type) {
	case map[string]any:
		b, ok := b.(map[string]any)
		if !ok || len(a) != len(b) {
			return path, false
		}
		for _, key := range slices.Sorted(maps.Keys(a)) {
			other, ok := b[key]
			if !ok {
				return pointer(path, key), false
			}
			if p, ok := sameJSON(a[key], other, pointer(path, key)); !ok {
				return p, false
			}
		}
		return path, true
	case []any:
		b, ok := b.([]any)
		if !ok || len(a) != len(b) {
			return path, false
		}
		for i := range a {
			if p, ok := sameJSON(a[i], b[i], pointer(path, strconv.Itoa(i))); !ok {
				return p, false
			}
		}
		return path, true
	default:
		return path, reflect.DeepEqual(a, b)
	}
}

func TestClientErrorsPairEachSchemaErrorCodeWithItsStatus(t *testing.T) {
	var connection struct {
		Defs struct {
			ErrorCode struct {
				Enum []string `json:"enum"`
			}
		} `json:"$defs"`
	}
	readSchema(t, "connection-v1.schema.json", &connection)
	codes := connection.Defs.ErrorCode.Enum
	slices.Sort(codes)
	if got := errorCodeNames(); !slices.Equal(got, codes) {
		t.Fatalf("ClientErrors has codes %q, the schema %q", got, codes)
	}
	// The schema names the codes; the client connection error table pairs
	// each with its status.
	table := []string{
		"400 invalid_message", "400 content_hash_mismatch", "401 credential_invalid", "401 credential_expired",
		"401 credential_revoked", "403 instance_unavailable", "403 cli_upgrade_required", "403 drive_access_denied",
		"404 not_found", "404 content_not_found", "409 connection_conflict", "409 previous_connection_active",
		"409 stale_connection", "409 stale_binding", "409 content_missing", "409 message_conflict",
		"409 unknown_instruction", "410 drive_cursor_expired", "413 request_too_large", "413 file_too_large",
		"416 range_not_satisfiable", "429 rate_limited",
	}
	got := make([]string, 0, len(ClientErrors))
	for code, status := range ClientErrors {
		got = append(got, strconv.Itoa(status)+" "+string(code))
	}
	slices.Sort(got)
	slices.Sort(table)
	if !slices.Equal(got, table) {
		t.Fatalf("ClientErrors pairs %q, the error table %q", got, table)
	}
}

func readSchema(t *testing.T, name string, schema any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, schema); err != nil {
		t.Fatal(err)
	}
}

// TestDriveTransferEndpointsMatchTheSchemaTable compares DriveTransferEndpoints
// with the drive transfer schema's endpoint table, pair by pair, and each pair
// with ClientErrors.
func TestDriveTransferEndpointsMatchTheSchemaTable(t *testing.T) {
	var drive struct {
		Defs struct {
			DriveTransferEndpoints struct {
				Properties map[DriveTransferEndpoint]struct {
					Const struct {
						Errors []string `json:"errors"`
					} `json:"const"`
				} `json:"properties"`
			}
		} `json:"$defs"`
	}
	readSchema(t, "drive-transfer-v1.schema.json", &drive)
	endpoints := drive.Defs.DriveTransferEndpoints.Properties
	if len(endpoints) != len(DriveTransferEndpoints) {
		t.Fatalf("DriveTransferEndpoints has %d endpoints, the schema %d", len(DriveTransferEndpoints), len(endpoints))
	}
	for endpoint, answers := range DriveTransferEndpoints {
		var got []string
		for _, answer := range answers {
			if ClientErrors[answer.Code] != answer.Status {
				t.Errorf("%s: %d %s is not a pair of ClientErrors", endpoint, answer.Status, answer.Code)
			}
			got = append(got, strconv.Itoa(answer.Status)+" "+string(answer.Code))
		}
		want := endpoints[endpoint].Const.Errors
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: DriveTransferEndpoints lists %q, the schema %q", endpoint, got, want)
		}
	}
}

// TestDriveTransferLimitsMatchTheSchema compares each limit the decoders apply
// with the drive transfer schema's DriveTransferLimits.
func TestDriveTransferLimitsMatchTheSchema(t *testing.T) {
	var drive struct {
		Defs struct {
			DriveTransferLimits struct {
				Properties map[string]struct {
					Const *int64 `json:"const"`
				} `json:"properties"`
			}
		} `json:"$defs"`
	}
	readSchema(t, "drive-transfer-v1.schema.json", &drive)
	limits := drive.Defs.DriveTransferLimits.Properties
	for name, got := range map[string]int64{
		"list_page_max_items":              ListPageMaxItems,
		"list_page_max_json_bytes":         ListPageMaxJSONBytes,
		"commit_max_changes":               CommitMaxChanges,
		"commit_request_max_json_bytes":    CommitRequestMaxJSONBytes,
		"commit_response_max_json_bytes":   CommitResponseMaxJSONBytes,
		"rule_max_utf8_bytes":              RuleMaxUTF8Bytes,
		"file_max_bytes":                   FileMaxBytes,
		"path_max_utf8_bytes":              drivePathMaxBytes,
		"path_segment_max_utf8_bytes":      driveSegmentMaxSize,
		"drive_changed_sse_max_json_bytes": DriveChangedSSEMaxJSONBytes,
		"retry_after_max_seconds":          retryAfterMaxSeconds,
	} {
		if want := limits[name].Const; want == nil || *want != got {
			t.Errorf("%s is %d in the decoders and %v in the schema", name, got, want)
		}
	}
}
