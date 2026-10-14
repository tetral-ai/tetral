package memory_test

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/memory"
)

func TestSDKCompatibilityMemoryDepthZeroUsesUnlimitedPath(t *testing.T) {
	for _, raw := range []string{"", "depth=0"} {
		values, err := url.ParseQuery(raw)
		if err != nil {
			t.Fatalf("ParseQuery %q: %v", raw, err)
		}
		options, err := memory.DecodeListMemoriesOptions(values)
		if err != nil {
			t.Fatalf("ParseListMemoriesOptions %q: %v", raw, err)
		}
		if options.DepthSet || options.Depth != 0 {
			t.Fatalf("options for %q = %+v; want unbounded DepthSet=false", raw, options)
		}
	}

	for _, raw := range []string{"depth=-1", "depth=1.5"} {
		values, _ := url.ParseQuery(raw)
		if _, err := memory.DecodeListMemoriesOptions(values); err == nil {
			t.Fatalf("ParseListMemoriesOptions %q succeeded; want validation error", raw)
		}
	}
}

func TestSDKCompatibilityMemoryStoreRejectsNullNameAndClearsMetadata(t *testing.T) {
	if _, err := memory.DecodeUpdateStoreRequest([]byte(`{"name":null}`)); err == nil || err.Error() != "name must be a string" {
		t.Fatalf("name:null error = %v; want name must be a string", err)
	}
	patch, err := memory.DecodeUpdateStoreRequest([]byte(`{"metadata":null}`))
	if err != nil {
		t.Fatalf("DecodeUpdateStoreRequest metadata:null: %v", err)
	}
	next, err := patch.Materialize(memory.Store{
		Name:     "before",
		Metadata: map[string]string{"team": "runtime"},
	})
	if err != nil {
		t.Fatalf("Materialize metadata:null: %v", err)
	}
	if next.Name != "before" || len(next.Metadata) != 0 {
		t.Fatalf("store = %+v; want unchanged name and cleared metadata", next)
	}
	body, err := json.Marshal(next)
	if err != nil {
		t.Fatalf("Marshal store: %v", err)
	}
	if !strings.Contains(string(body), `"name":"before"`) {
		t.Fatalf("public name = %s; want unchanged string", body)
	}
	body, err = json.Marshal(memory.Store{})
	if err != nil || !strings.Contains(string(body), `"name":""`) {
		t.Fatalf("empty stored name must project a string: %s, %v", body, err)
	}
}

func TestTCompatMEM19CreateNullRejectedAndEmptyContentSucceeds(t *testing.T) {
	_, err := memory.DecodeCreateMemoryRequest([]byte(`{"path":"/null.md","content":null}`))
	if err == nil || err.Error() != "content must be a string" {
		t.Fatalf("content:null error = %v; want content must be a string", err)
	}
	empty, err := memory.DecodeCreateMemoryRequest([]byte(`{"path":"/empty.md","content":""}`))
	if err != nil {
		t.Fatalf("empty content decode: %v", err)
	}
	if !empty.ContentSet || empty.Content != "" {
		t.Fatalf("empty request = %+v; want admitted empty content", empty)
	}
}

func TestTCompatMEM20UpdateNullsRejectAndOmissionLeavesContentAndPathUnchanged(t *testing.T) {
	for field, body := range map[string]string{
		"content": `{"content":null}`,
		"path":    `{"path":null}`,
	} {
		_, err := memory.DecodeUpdateMemoryRequest([]byte(body))
		want := field + " must be a string"
		if err == nil || err.Error() != want {
			t.Fatalf("%s:null error = %v; want %s", field, err, want)
		}
	}

	request, err := memory.DecodeUpdateMemoryRequest([]byte(`{}`))
	if err != nil {
		t.Fatalf("DecodeUpdateMemoryRequest omitted content/path: %v", err)
	}
	if request.Path != nil || request.ContentSet {
		t.Fatalf("decoded omitted request = %+v; want path/content absent", request)
	}
}
