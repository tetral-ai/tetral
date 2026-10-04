package auth

import "testing"

func TestDecodeStrictJSONRejectsCaseEquivalentSecurityFields(t *testing.T) {
	type selectors struct {
		WorkspaceID string `json:"workspace_id"`
		Kind        string `json:"kind"`
	}
	for _, input := range []string{
		`{"workspace_id":"a","WORKSPACE_ID":"b"}`,
		`{"workspace_id":"a","workspace_\u0069d":"b"}`,
		`{"kind":"human","\u212AIND":"service"}`,
		`{"kind":"human","Kind":"service"}`,
	} {
		var decoded selectors
		if err := DecodeStrictJSON([]byte(input), &decoded); err == nil {
			t.Fatalf("ambiguous security fields accepted: %s", input)
		}
	}
	var decoded selectors
	if err := DecodeStrictJSON([]byte(`{"workspace_id":"a","kind":"human"}`), &decoded); err != nil || decoded.WorkspaceID != "a" {
		t.Fatalf("valid selectors rejected: %v", err)
	}
	// Unicode simple-fold cycles must match encoding/json rather than only ASCII.
	if foldJSONField("S") != foldJSONField("\u017f") || foldJSONField("Σ") != foldJSONField("ς") || foldJSONField("K") != foldJSONField("K") {
		t.Fatal("Unicode folding differs from struct decoder")
	}
}
