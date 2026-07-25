package cli

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestParseCaptureArgsAcceptsOldAndBoundedForms(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantEntries   int
		wantNameBytes int64
	}{
		{
			name:          "old form applies defaults",
			args:          []string{"__capture", "--path", "/mnt/session/outputs", "--max-bytes", "1024"},
			wantEntries:   protocol.CaptureDefaultMaxEntries,
			wantNameBytes: protocol.CaptureDefaultMaxNameBytes,
		},
		{
			name:          "bounded form carries directory ceilings",
			args:          []string{"__capture", "--path", "/mnt/session/outputs", "--max-bytes", "1024", "--max-entries", "7", "--max-name-bytes", "4096"},
			wantEntries:   7,
			wantNameBytes: 4096,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, maxBytes, maxEntries, maxNameBytes, ok := parseCaptureArgs(test.args)
			if !ok || path != "/mnt/session/outputs" || maxBytes != 1024 || maxEntries != test.wantEntries || maxNameBytes != test.wantNameBytes {
				t.Fatalf("parseCaptureArgs() = %q, %d, %d, %d, %v", path, maxBytes, maxEntries, maxNameBytes, ok)
			}
		})
	}
}

func TestParseCaptureArgsRejectsMalformedBounds(t *testing.T) {
	if _, _, _, _, ok := parseCaptureArgs([]string{"__capture", "--path", "/mnt/session/outputs", "--max-bytes", "1", "--max-entries", "-1", "--max-name-bytes", "4096"}); ok {
		t.Fatal("parseCaptureArgs accepted a negative entry bound")
	}
}
