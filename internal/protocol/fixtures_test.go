package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
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
	Name   string          `json:"name"`
	Schema string          `json:"schema"`
	Valid  bool            `json:"valid"`
	Value  json.RawMessage `json:"value"`
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

var toolExecutionCodecs = map[string]codec{
	"Downstream":          jsonCodec(DecodeToolDownstream),
	"ToolStatus":          jsonCodec(DecodeToolStatus),
	"DriveSyncReport":     jsonCodec(DecodeDriveSyncReport),
	"Acknowledgement":     jsonCodec(DecodeAcknowledgement),
	"ProtocolErrorReport": jsonCodec(DecodeProtocolErrorReport),
	"ReadyExtension":      jsonCodec(DecodeReadyExtension),
	"SandboxPolicy":       jsonCodec(DecodeSandboxPolicy),
}

func readFixtures(t *testing.T, name string) []fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", name))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []fixture
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

func TestFixturesDecodeAndReencode(t *testing.T) {
	files := []struct {
		name string
		// census is the file's count of fixtures and of those each check
		// below selects, so that a truncated file or a selector that
		// matches nothing fails.
		census map[string]int
		codecs func([]fixture) map[string]codec
	}{
		{"tool-execution-v1.fixtures.json", map[string]int{"fixtures": 61, "valid": 29}, func([]fixture) map[string]codec { return toolExecutionCodecs }},
	}
	for _, file := range files {
		t.Run(file.name, func(t *testing.T) {
			fixtures := readFixtures(t, file.name)
			if got := census(fixtures); !maps.Equal(got, file.census) {
				t.Fatalf("census = %v; want %v", got, file.census)
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
	counts := map[string]int{"fixtures": len(fixtures)}
	for _, f := range fixtures {
		if f.Valid {
			counts["valid"]++
		}
	}
	return counts
}

// unknownMembers lists, for each fixture that has them, the members its
// schema allows without defining them. A decoder that skips any other member
// has dropped a field, and the encoding differs from the fixture.
var unknownMembers = map[string][]string{}

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
		return
	}
	if err != nil {
		t.Fatalf("valid fixture rejected: %v", err)
	}
	assertReencodes(t, c, f, got)
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
