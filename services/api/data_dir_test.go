package tetralapi

import (
	"os"
	"path/filepath"
	"testing"
)

// TestEnsureDataDirRejectsGroupOrOtherAccessibleDir proves EnsureDataDir refuses
// a pre-existing data directory whose mode grants group/other access, and accepts
// an owner-only (0700) directory. The data directory holds service-owned staging
// state, so loosened permissions must fail before serving.
func TestEnsureDataDirRejectsGroupOrOtherAccessibleDir(t *testing.T) {
	t.Run("rejects 0755 pre-existing dir", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		// Chmod overrides umask so the loosened bits are actually set on disk.
		// The loosened mode is the condition under test: EnsureDataDir must reject it.
		if err := os.Chmod(dir, 0755); err != nil { //nolint:gosec // G302: deliberately group/other accessible to drive the rejection path under test.
			t.Fatalf("chmod 0755: %v", err)
		}
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if info.Mode().Perm()&0077 == 0 {
			t.Fatalf("test setup did not produce a group/other accessible dir; mode = %04o", info.Mode().Perm())
		}

		if err := EnsureDataDir(dir); err == nil {
			t.Fatal("EnsureDataDir accepted a group/other accessible data directory")
		}
	})

	t.Run("accepts 0700 pre-existing dir", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Chmod(dir, 0700); err != nil { //nolint:gosec // G302: owner-only mode forced past umask to assert the accept path.
			t.Fatalf("chmod 0700: %v", err)
		}
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if info.Mode().Perm()&0077 != 0 {
			t.Fatalf("test setup did not produce an owner-only dir; mode = %04o", info.Mode().Perm())
		}

		if err := EnsureDataDir(dir); err != nil {
			t.Fatalf("EnsureDataDir rejected an owner-only data directory: %v", err)
		}
	})
}

// TestEnsureDataDirCreatesMissingLeafOwnerOnly (P5c) proves EnsureDataDir creates a
// NON-EXISTENT leaf under a GROUP-WRITABLE parent and that the leaf it creates always
// passes its OWN subsequent &0077==0 mode gate — regardless of parent mode or umask.
// This mirrors the round-02 emptyDir mount, whose parent is group-writable: if the
// creation mode were loosened (or umask leaked group/other bits onto the new dir),
// EnsureDataDir would create a directory its own next line rejects, a self-inflicted
// startup failure. So the leaf must be created owner-only and then accepted.
func TestEnsureDataDirCreatesMissingLeafOwnerOnly(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "parent")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatalf("mkdir parent: %v", err)
	}
	// Force the parent group/other-writable past umask so the leaf is created beneath
	// a permissive parent, the condition under which a loosened creation mode would leak.
	if err := os.Chmod(parent, 0775); err != nil { //nolint:gosec // G302: group-writable parent is the condition under test, mirroring the emptyDir mount.
		t.Fatalf("chmod parent 0775: %v", err)
	}
	parentInfo, err := os.Stat(parent)
	if err != nil {
		t.Fatalf("stat parent: %v", err)
	}
	if parentInfo.Mode().Perm()&0077 == 0 {
		t.Fatalf("test setup did not produce a group-writable parent; mode = %04o", parentInfo.Mode().Perm())
	}

	leaf := filepath.Join(parent, "data")
	if _, err := os.Stat(leaf); !os.IsNotExist(err) {
		t.Fatalf("test setup expects a non-existent leaf; stat err = %v", err)
	}

	if err := EnsureDataDir(leaf); err != nil {
		t.Fatalf("EnsureDataDir failed to create a missing leaf under a group-writable parent: %v", err)
	}

	leafInfo, err := os.Stat(leaf)
	if err != nil {
		t.Fatalf("stat created leaf: %v", err)
	}
	if !leafInfo.IsDir() {
		t.Fatalf("EnsureDataDir did not create a directory at %s", leaf)
	}
	if leafInfo.Mode().Perm()&0077 != 0 {
		t.Fatalf("EnsureDataDir created a group/other accessible leaf; mode = %04o (the created dir would fail its own &0077==0 gate)", leafInfo.Mode().Perm())
	}
}
