package memory_test

import (
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/memory"
)

func TestMemoryPathRejectsInvalidShapes(t *testing.T) {
	longPath := "/" + strings.Repeat("a", 1024)
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "relative", path: "notes/a.md"},
		{name: "root only", path: "/"},
		{name: "empty segment", path: "/notes//a.md"},
		{name: "dot segment", path: "/notes/./a.md"},
		{name: "dot dot segment", path: "/notes/../a.md"},
		{name: "control char", path: "/notes/\x00.md"},
		{name: "format char", path: "/notes/\u200d.md"},
		{name: "non nfc", path: "/notes/e\u0301.md"},
		{name: "over bytes", path: longPath},
		{name: "mount root", path: "/mnt/memory"},
		{name: "runtime projection path", path: "/mnt/memory/store/a.md"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := memory.ValidatePath(tc.path); err == nil {
				t.Fatalf("ValidatePath(%q) succeeded; want error", tc.path)
			}
		})
	}
}

func TestMemoryPathAcceptsValidFilePath(t *testing.T) {
	if err := memory.ValidatePath("/notes/é.md"); err != nil {
		t.Fatalf("ValidatePath valid NFC path: %v", err)
	}
}
