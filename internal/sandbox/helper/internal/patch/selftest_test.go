package patch

import (
	"os"
	"testing"
)

func TestSelfTestExercisesInMemoryDryRun(t *testing.T) {
	tempRoot := t.TempDir()
	t.Setenv("TMPDIR", tempRoot)
	if err := SelfTest(); err != nil {
		t.Fatalf("SelfTest: %v", err)
	}
	entries, err := os.ReadDir(tempRoot)
	if err != nil {
		t.Fatalf("read temp root: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("SelfTest wrote temp files: %v", entries)
	}
}
