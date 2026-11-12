// Layer 1: Engine's OpenAPI file read against the operation table.
package api_test

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/api"
	"github.com/tetral-ai/tetral/internal/auth"
)

func readFile(t *testing.T) map[string]any {
	t.Helper()
	data, err := api.Files.ReadFile(api.File)
	if err != nil {
		t.Fatal(err)
	}
	var file map[string]any
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	return file
}

// The file's operations are the operation table's and the token exchange,
// which is public without a row in that table.
func TestTheFileDeclaresEveryPublicOperationAndNoOther(t *testing.T) {
	want := map[string]string{"POST /v1/oauth/token": "oauth.token"}
	for _, route := range auth.OperationRoutes() {
		want[route.Method+" "+route.Pattern] = string(route.Operation)
	}
	got := map[string]string{}
	for path, methods := range readFile(t)["paths"].(map[string]any) {
		for method, operation := range methods.(map[string]any) {
			got[strings.ToUpper(method)+" "+path], _ = operation.(map[string]any)["operationId"].(string)
		}
	}
	for _, route := range slices.Sorted(maps.Keys(want)) {
		if id, declared := got[route]; !declared {
			t.Errorf("the file lacks %s (%s)", route, want[route])
		} else if id != want[route] {
			t.Errorf("%s is %q in the file; the operation is %q", route, id, want[route])
		}
	}
	for _, route := range slices.Sorted(maps.Keys(got)) {
		if _, public := want[route]; !public {
			t.Errorf("the file declares %s, which is no public operation", route)
		}
	}
}

// defined reports whether ref, a JSON pointer into the file, names a value.
func defined(file map[string]any, ref string) bool {
	pointer, local := strings.CutPrefix(ref, "#/")
	var at any = file
	for _, token := range strings.Split(pointer, "/") {
		object, isObject := at.(map[string]any)
		if at = object[strings.NewReplacer("~1", "/", "~0", "~").Replace(token)]; !isObject || at == nil {
			return false
		}
	}
	return local
}

// Every object with fixed fields is closed, every reference resolves, every
// answer declares request-id, every error answer names its error types and
// every stream its events.
func TestTheFileKeepsItsForm(t *testing.T) {
	file := readFile(t)
	var walk func(at string, node any)
	walk = func(at string, node any) {
		switch v := node.(type) {
		case map[string]any:
			if properties, fixed := v["properties"].(map[string]any); fixed {
				if v["additionalProperties"] != false {
					t.Errorf("%s has fixed fields and is not closed", at)
				}
				for name, member := range properties {
					walk(at+"/properties/"+name, member)
				}
			}
			if ref, isRef := v["$ref"].(string); isRef {
				if !defined(file, ref) {
					t.Errorf("%s refers to %s, which the file does not define", at, ref)
				}
			}
			for name, member := range v {
				if name != "properties" {
					walk(at+"/"+name, member)
				}
			}
		case []any:
			for _, member := range v {
				walk(at, member)
			}
		}
	}
	walk("", file)

	for path, methods := range file["paths"].(map[string]any) {
		for method, operation := range methods.(map[string]any) {
			for status, response := range operation.(map[string]any)["responses"].(map[string]any) {
				answer := response.(map[string]any)
				_, typed := answer["x-error-types"].([]any)
				if isError := status >= "400"; isError != typed {
					t.Errorf("%s %s: answer %s: only an error answer, and every one, names its error types", method, path, status)
				}
				if headers, _ := answer["headers"].(map[string]any); headers["request-id"] == nil {
					t.Errorf("%s %s: answer %s does not declare request-id", method, path, status)
				}
				content, _ := answer["content"].(map[string]any)
				if stream, isStream := content["text/event-stream"].(map[string]any); isStream {
					if events, _ := stream["x-events"].(map[string]any); len(events) == 0 {
						t.Errorf("%s %s: the stream names no event", method, path)
					}
				}
			}
		}
	}
}
