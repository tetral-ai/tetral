package memory_test

import (
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/memory"
)

func TestDecodeCreateStoreRequestValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "missing name", body: `{}`},
		{name: "blank name", body: `{"name":"   "}`},
		{name: "long name", body: `{"name":"` + strings.Repeat("a", 256) + `"}`},
		{name: "long description", body: `{"name":"ok","description":"` + strings.Repeat("d", 1025) + `"}`},
		{name: "too many metadata keys", body: metadataBody(17, 1, 1)},
		{name: "long metadata key", body: `{"name":"ok","metadata":{"` + strings.Repeat("k", 65) + `":"v"}}`},
		{name: "long metadata value", body: `{"name":"ok","metadata":{"k":"` + strings.Repeat("v", 513) + `"}}`},
		{name: "non string metadata value", body: `{"name":"ok","metadata":{"k":1}}`},
		{name: "metadata null", body: `{"name":"ok","metadata":null}`},
		{name: "display name", body: `{"display_name":"bad"}`},
		{name: "server id", body: `{"id":"memstore_bad","name":"ok"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := memory.DecodeCreateStoreRequest([]byte(tc.body))
			assertMemoryValidationError(t, err)
		})
	}
}

func TestDecodeCreateStoreRequestTrimsAndAcceptsBoundaryValues(t *testing.T) {
	req, err := memory.DecodeCreateStoreRequest([]byte(`{"name":"  ` + strings.Repeat("n", 255) + `  ","description":"ok","metadata":{"team":"infra"}}`))
	if err != nil {
		t.Fatalf("DecodeCreateStoreRequest: %v", err)
	}
	if req.Name != strings.Repeat("n", 255) {
		t.Errorf("Name length = %d; want 255 trimmed runes", len([]rune(req.Name)))
	}
	if req.Metadata["team"] != "infra" {
		t.Errorf("Metadata = %v", req.Metadata)
	}
}

func TestDecodeUpdateStoreRequestValidationAndMaterialization(t *testing.T) {
	current := memory.Store{
		Name:        "old",
		Description: "old description",
		Metadata:    map[string]string{"delete": "yes", "keep": "yes"},
	}
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "empty", body: `{}`},
		{name: "server id", body: `{"id":"memstore_bad"}`},
		{name: "display name", body: `{"display_name":"bad"}`},
		{name: "unknown", body: `{"unknown":"bad"}`},
		{name: "blank name", body: `{"name":"   "}`},
		{name: "long name", body: `{"name":"` + strings.Repeat("a", 256) + `"}`},
		{name: "long description", body: `{"description":"` + strings.Repeat("d", 1025) + `"}`},
		{name: "too many metadata keys", body: metadataBodyWithoutName(17, 1, 1)},
		{name: "long metadata key", body: `{"metadata":{"` + strings.Repeat("k", 65) + `":"v"}}`},
		{name: "long metadata value", body: `{"metadata":{"k":"` + strings.Repeat("v", 513) + `"}}`},
		{name: "non string metadata value", body: `{"metadata":{"k":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch, err := memory.DecodeUpdateStoreRequest([]byte(tc.body))
			if err == nil {
				_, err = patch.Materialize(current)
			}
			assertMemoryValidationError(t, err)
		})
	}

	patch, err := memory.DecodeUpdateStoreRequest([]byte(`{"description":null,"metadata":{"delete":null,"added":"yes"},"name":" new "}`))
	if err != nil {
		t.Fatalf("DecodeUpdateStoreRequest: %v", err)
	}
	updated, err := patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if updated.Name != "new" || updated.Description != "" || updated.Metadata["keep"] != "yes" || updated.Metadata["added"] != "yes" {
		t.Fatalf("updated = %+v; want patched name/description/metadata", updated)
	}
	if _, ok := updated.Metadata["delete"]; ok {
		t.Fatalf("metadata null value must delete key: %+v", updated)
	}

	patch, err = memory.DecodeUpdateStoreRequest([]byte(`{"name":"renamed"}`))
	if err != nil {
		t.Fatalf("DecodeUpdateStoreRequest name only: %v", err)
	}
	updated, err = patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize name only: %v", err)
	}
	if updated.Description != current.Description || updated.Metadata["keep"] != "yes" {
		t.Fatalf("omitted fields not preserved: %+v", updated)
	}
}

func TestDecodeCreateMemoryRequestRejectsNullContentAndAcceptsEmptyContent(t *testing.T) {
	_, err := memory.DecodeCreateMemoryRequest([]byte(`{"path":"/null.md","content":null}`))
	assertMemoryValidationError(t, err)

	request, err := memory.DecodeCreateMemoryRequest([]byte(`{"path":"/empty.md","content":""}`))
	if err != nil {
		t.Fatalf("DecodeCreateMemoryRequest empty content: %v", err)
	}
	if request.Path != "/empty.md" || request.Content != "" || !request.ContentSet {
		t.Fatalf("request = %+v; want empty content preserved and marked set", request)
	}
}

func TestDecodeListStoresOptionsLimitValidation(t *testing.T) {
	for _, raw := range []string{"limit=0", "limit=-1", "limit=abc"} {
		values, err := url.ParseQuery(raw)
		if err != nil {
			t.Fatalf("ParseQuery: %v", err)
		}
		if _, err := memory.DecodeListStoresOptions(values); err == nil {
			t.Fatalf("DecodeListStoresOptions(%s) succeeded; want error", raw)
		}
	}
	values, err := url.ParseQuery("limit=101&include_archived=true&created_at%5Bgte%5D=2026-01-01T00%3A00%3A00Z")
	if err != nil {
		t.Fatalf("ParseQuery valid: %v", err)
	}
	options, err := memory.DecodeListStoresOptions(values)
	if err != nil {
		t.Fatalf("DecodeListStoresOptions valid: %v", err)
	}
	if options.Limit != 101 || !options.LimitSet || !options.IncludeArchived || options.CreatedAtGTE == "" {
		t.Fatalf("options = %+v", options)
	}
}

func metadataBody(keys int, keyLength int, valueLength int) string {
	return metadataBodyWithPrefix(`{"name":"ok","metadata":{`, keys, keyLength, valueLength)
}

func metadataBodyWithoutName(keys int, keyLength int, valueLength int) string {
	return metadataBodyWithPrefix(`{"metadata":{`, keys, keyLength, valueLength)
}

func metadataBodyWithPrefix(prefix string, keys int, keyLength int, valueLength int) string {
	var b strings.Builder
	b.WriteString(prefix)
	for i := 0; i < keys; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"`)
		b.WriteString(strings.Repeat("k", keyLength))
		b.WriteString(string(rune('a' + i)))
		b.WriteString(`":"`)
		b.WriteString(strings.Repeat("v", valueLength))
		b.WriteString(`"`)
	}
	b.WriteString(`}}`)
	return b.String()
}

func assertMemoryValidationError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected validation error")
	}
	var validation *memory.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected *memory.ValidationError, got %T (%v)", err, err)
	}
}
