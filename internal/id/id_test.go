package id_test

import (
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/id"
)

func TestNewGeneratesUniqueIDs(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for range 1000 {
		generated := id.New("req_")
		if seen[generated] {
			t.Fatalf("duplicate ID generated: %s", generated)
		}
		seen[generated] = true
	}
}

func TestNewPreservesPrefix(t *testing.T) {
	prefixes := []string{"req_", "sesn_", "sesrsc_"}
	for _, prefix := range prefixes {
		generated := id.New(prefix)
		if !strings.HasPrefix(generated, prefix) {
			t.Errorf("expected prefix %q, got %q", prefix, generated)
		}
	}
}

func TestNewSuffixIsNotEmpty(t *testing.T) {
	prefix := "req_"
	generated := id.New(prefix)
	suffix := strings.TrimPrefix(generated, prefix)
	if suffix == "" {
		t.Error("suffix is empty")
	}
}
