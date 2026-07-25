package webconnector

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestClassifyURLAllowsPublicHTTPAndHTTPSAndRejectsNonPublicTargets(t *testing.T) {
	t.Parallel()
	var tests []struct {
		Name   string `json:"name"`
		Raw    string `json:"raw"`
		Allow  bool   `json:"allow"`
		Reason string `json:"reason"`
	}
	raw, err := os.ReadFile(filepath.Join("testdata", "url-classifier.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &tests); err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := ClassifyURL(tt.Raw)
			if got.Allowed != tt.Allow || got.Reason != tt.Reason {
				t.Fatalf("ClassifyURL(%q) = %+v; want allowed=%v reason=%q", tt.Raw, got, tt.Allow, tt.Reason)
			}
		})
	}
}
