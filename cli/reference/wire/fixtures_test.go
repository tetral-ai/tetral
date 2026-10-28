package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// fixtureFiles are the fixture files with their fixture, valid and invalid
// counts.
var fixtureFiles = []struct {
	name                  string
	total, valid, invalid int
}{
	{"tool-execution-v1", 61, 29, 32},
	{"connection-v1", 239, 61, 178},
	{"drive-transfer-v1", 126, 49, 77},
}

// unknownMembers are the members of valid fixtures that no type knows, which
// decoding drops.
var unknownMembers = map[string][]string{
	"connected_with_unknown_optional_field": {"/payload/hub", "/trace"},
	"ready_with_unknown_optional_field":     {"/hostname", "/capabilities/gpu"},
}

type fixture struct {
	Name     string            `json:"name"`
	Schema   string            `json:"schema"`
	Valid    bool              `json:"valid"`
	Value    json.RawMessage   `json:"value"`
	Pair     map[string]string `json:"pair"`
	Endpoint string            `json:"endpoint"`
	Status   int               `json:"status"`
	HTTP     int               `json:"http_status"`
}

func readProtocolFile(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "protocol", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// compact is a fixture value as the bytes a peer would send.
func compact(t *testing.T, value json.RawMessage) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, value); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func roundTrip[M any, P interface {
	*M
	Message
}](input []byte) ([]byte, error) {
	message, err := Decode[M, P](input)
	if err != nil {
		return nil, err
	}
	return Encode(message)
}

func answerTrip[R any, M any, P interface {
	*M
	Answer[R]
}](input []byte, request R) ([]byte, error) {
	message, err := DecodeAnswer[R, M, P](input, request)
	if err != nil {
		return nil, err
	}
	return Encode(message)
}

// paired decodes the fixture that the fixture's pair names for role.
func paired[M any, P interface {
	*M
	Message
}](t *testing.T, item fixture, role string, byName map[string]fixture) (M, bool) {
	t.Helper()
	var message M
	name, ok := item.Pair[role]
	if !ok {
		return message, false
	}
	message, err := Decode[M, P](compact(t, byName[name].Value))
	if err != nil {
		t.Fatalf("%s: paired %s %s: %v", item.Name, role, name, err)
	}
	return message, true
}

// decodeFixture decodes a fixture as the type of its schema, with the
// context the fixture gives, and returns the type's encoding.
func decodeFixture(t *testing.T, file string, item fixture, byName map[string]fixture) ([]byte, error) {
	t.Helper()
	input := compact(t, item.Value)
	switch {
	case file == "tool-execution-v1" && item.Schema == "Downstream":
		return roundTrip[ToolDownstream](input)
	case item.Schema == "ReadyExtension":
		return roundTrip[ReadyExtension](input)
	case item.Schema == "SandboxPolicy":
		return roundTrip[SandboxPolicy](input)
	case item.Schema == "ToolStatus":
		return roundTrip[ToolStatus](input)
	case item.Schema == "DriveSyncReport":
		return roundTrip[DriveSyncReport](input)
	case item.Schema == "Acknowledgement":
		return roundTrip[Acknowledgement](input)
	case item.Schema == "ProtocolErrorReport":
		return roundTrip[ProtocolErrorReport](input)
	case item.Schema == "EventsQuery":
		return roundTrip[EventsQuery](input)
	case item.Schema == "Downstream":
		return roundTrip[Downstream](input)
	case item.Schema == "Ready":
		return roundTrip[Ready](input)
	case item.Schema == "Heartbeat":
		return roundTrip[Heartbeat](input)
	case item.Schema == "GrantAcknowledgement":
		return roundTrip[GrantAcknowledgement](input)
	case item.Schema == "SessionStatus":
		return roundTrip[SessionStatus](input)
	case item.Schema == "McpTools":
		return roundTrip[MCPTools](input)
	case item.Schema == "CredentialRequest":
		return roundTrip[CredentialRequest](input)
	case item.Schema == "CredentialResponse":
		return roundTrip[CredentialResponse](input)
	case item.Schema == "GitTicketRequest":
		return roundTrip[GitTicketRequest](input)
	case item.Schema == "GitTicketResponse":
		return roundTrip[GitTicketResponse](input)
	case item.Schema == "ErrorBody":
		body, err := DecodeClientError(item.HTTP, input)
		if err != nil {
			return nil, err
		}
		return Encode(body)
	case item.Schema == "ListFilesQuery":
		return roundTrip[ListFilesQuery](input)
	case item.Schema == "ListChangesQuery":
		return roundTrip[ListChangesQuery](input)
	case item.Schema == "CommonQuery":
		return roundTrip[CommonQuery](input)
	case item.Schema == "DrivePathParams":
		return roundTrip[DrivePathParams](input)
	case item.Schema == "ContentPathParams":
		return roundTrip[ContentPathParams](input)
	case item.Schema == "UploadContentRequestHeaders":
		return roundTrip[UploadContentRequestHeaders](input)
	case item.Schema == "DownloadContentRequestHeaders":
		return roundTrip[DownloadContentRequestHeaders](input)
	case item.Schema == "PartialContentResponseHeaders":
		if request, ok := paired[DownloadContentRequestHeaders](t, item, "request_headers", byName); ok {
			return answerTrip[DownloadContentRequestHeaders, PartialContentResponseHeaders](input, request)
		}
		return roundTrip[PartialContentResponseHeaders](input)
	case item.Schema == "RateLimitedResponseHeaders":
		return roundTrip[RateLimitedResponseHeaders](input)
	case item.Schema == "ListFilesResponse":
		if query, ok := paired[ListFilesQuery](t, item, "query", byName); ok {
			return answerTrip[ListFilesQuery, ListFilesResponse](input, query)
		}
		return roundTrip[ListFilesResponse](input)
	case item.Schema == "ListChangesResponse":
		if query, ok := paired[ListChangesQuery](t, item, "query", byName); ok {
			return answerTrip[ListChangesQuery, ListChangesResponse](input, query)
		}
		return roundTrip[ListChangesResponse](input)
	case item.Schema == "DriveChanged":
		return roundTrip[DriveChanged](input)
	case item.Schema == "UploadContentResponse":
		path, hasPath := paired[ContentPathParams](t, item, "path_params", byName)
		headers, hasHeaders := paired[UploadContentRequestHeaders](t, item, "request_headers", byName)
		if hasPath != hasHeaders {
			t.Fatalf("%s: an upload pairs with its path and its headers", item.Name)
		}
		if hasPath {
			return answerTrip[UploadRequest, UploadContentResponse](input, UploadRequest{Path: path, Headers: headers})
		}
		return roundTrip[UploadContentResponse](input)
	case item.Schema == "CommitRequest":
		return roundTrip[CommitRequest](input)
	case item.Schema == "CommitResponse":
		if request, ok := paired[CommitRequest](t, item, "request", byName); ok {
			return answerTrip[CommitRequest, CommitResponse](input, request)
		}
		return roundTrip[CommitResponse](input)
	case item.Schema == "Error":
		body, err := DecodeDriveTransferError(DriveTransferEndpoint(item.Endpoint), item.Status, input)
		if err != nil {
			return nil, err
		}
		return Encode(body)
	}
	t.Fatalf("%s: no decoder for %s %s", item.Name, file, item.Schema)
	return nil, nil
}

// readTree reads JSON as the strict pass does, numbers kept as written.
func readTree(t *testing.T, data []byte) any {
	t.Helper()
	tree, err := readStrict(data)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

// expectedEncoding is a fixture's value without the members decoding drops.
func expectedEncoding(t *testing.T, item fixture) any {
	t.Helper()
	tree := readTree(t, compact(t, item.Value))
	for _, pointer := range unknownMembers[item.Name] {
		parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
		object := tree.(map[string]any)
		for _, part := range parts[:len(parts)-1] {
			object = object[part].(map[string]any)
		}
		if _, ok := object[parts[len(parts)-1]]; !ok {
			t.Fatalf("%s: no member %s", item.Name, pointer)
		}
		delete(object, parts[len(parts)-1])
	}
	return tree
}

func TestEveryProtocolFixtureRoundTripsOrIsRejected(t *testing.T) {
	var total, valid, invalid int
	for _, file := range fixtureFiles {
		var fixtures []fixture
		if err := json.Unmarshal(readProtocolFile(t, file.name+".fixtures.json"), &fixtures); err != nil {
			t.Fatal(err)
		}
		byName := map[string]fixture{}
		for _, item := range fixtures {
			byName[item.Name] = item
		}
		var seenValid, seenInvalid int
		for _, item := range fixtures {
			encoded, err := decodeFixture(t, file.name, item, byName)
			if item.Valid {
				seenValid++
				if err != nil {
					t.Errorf("%s: %v", item.Name, err)
					continue
				}
				if got, want := readTree(t, encoded), expectedEncoding(t, item); !reflect.DeepEqual(got, want) {
					t.Errorf("%s: re-encoded as %s", item.Name, encoded)
				}
			} else {
				seenInvalid++
				if err == nil {
					t.Errorf("%s: invalid fixture accepted", item.Name)
				}
			}
		}
		if got := [3]int{len(fixtures), seenValid, seenInvalid}; got != [3]int{file.total, file.valid, file.invalid} {
			t.Errorf("%s: %v fixtures, valid and invalid", file.name, got)
		}
		total, valid, invalid = total+len(fixtures), valid+seenValid, invalid+seenInvalid
	}
	if got := [3]int{total, valid, invalid}; got != [3]int{426, 139, 287} {
		t.Errorf("%v fixtures, valid and invalid", got)
	}
}

func TestDriveTransferEndpointTableMatchesTheSchema(t *testing.T) {
	var schema struct {
		Defs struct {
			Endpoints struct {
				Properties map[string]struct {
					Const struct {
						Method string   `json:"method"`
						Path   string   `json:"path"`
						Errors []string `json:"errors"`
					} `json:"const"`
				} `json:"properties"`
			} `json:"DriveTransferEndpoints"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(readProtocolFile(t, "drive-transfer-v1.schema.json"), &schema); err != nil {
		t.Fatal(err)
	}
	rows := schema.Defs.Endpoints.Properties
	for _, endpoint := range DriveTransferEndpoints {
		row, spec := rows[string(endpoint)].Const, endpoint.Spec()
		var errors []string
		for _, code := range spec.Errors {
			errors = append(errors, fmt.Sprintf("%d %s", code.Status(), code))
		}
		slices.Sort(errors)
		listed := slices.Sorted(slices.Values(row.Errors))
		if row.Method != spec.Method || row.Path != spec.Path || !slices.Equal(errors, listed) {
			t.Errorf("%s: %s %s %v, schema %s %s %v", endpoint, spec.Method, spec.Path, errors, row.Method, row.Path, listed)
		}
	}
	if len(rows) != len(DriveTransferEndpoints) {
		t.Errorf("the schema lists %d endpoints", len(rows))
	}
}
