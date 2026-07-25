package capture

import (
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestCaptureClassifiesFIFOWithoutBlocking(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)
	sourcePath := filepath.Join(root, "idle.fifo")
	if err := unix.Mkfifo(sourcePath, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	result := make(chan protocol.CaptureEnvelope, 1)
	go func() {
		result <- runCapture(sourcePath, 1024, system)
	}()

	select {
	case envelope := <-result:
		assertSkippedCapture(t, envelope, sourcePath, "fifo", "non_regular", 1, 0)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("FIFO capture blocked; classification must not acquire read access")
	}
}

func TestCaptureClassifiesTrailingSymlinkWithoutFollowingIt(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)
	targetName := "target.txt"
	targetPath := filepath.Join(root, targetName)
	if err := os.WriteFile(targetPath, []byte("body"), 0o600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	linkPath := filepath.Join(root, "link.txt")
	if err := os.Symlink(targetName, linkPath); err != nil {
		t.Fatalf("create trailing symlink: %v", err)
	}

	assertSkippedCapture(t, runCapture(linkPath, 1024, system), linkPath, "symlink", "non_regular", 1, int64(len(targetName)))
}

func TestCaptureClassifiesDisqualifiedEntriesAsSuccess(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)

	directoryPath := filepath.Join(root, "reports")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatalf("mkdir directory fixture: %v", err)
	}
	directory := runCapture(directoryPath, 1024, system)
	if directory.Status != protocol.ToolStatusSuccess || directory.Result == nil {
		t.Fatalf("directory envelope = %+v; want success", directory)
	}
	if directory.Result.SourcePath != directoryPath || directory.Result.Kind != "directory" || directory.Result.Skipped || directory.Result.SkipReason != "" || directory.Result.DataBase64 != "" {
		t.Fatalf("directory result = %+v; want recursion signal without bytes", directory.Result)
	}

	targetPath := filepath.Join(root, "target.txt")
	if err := os.WriteFile(targetPath, []byte("body"), 0o600); err != nil {
		t.Fatalf("write hardlink target: %v", err)
	}
	hardlinkPath := filepath.Join(root, "hardlink.txt")
	if err := os.Link(targetPath, hardlinkPath); err != nil {
		t.Fatalf("create hardlink: %v", err)
	}
	assertSkippedCapture(t, runCapture(hardlinkPath, 1024, system), hardlinkPath, "regular", "multiple_links", 2, 4)

	largePath := filepath.Join(root, "large.bin")
	if err := os.WriteFile(largePath, []byte("large"), 0o600); err != nil {
		t.Fatalf("write oversized fixture: %v", err)
	}
	assertSkippedCapture(t, runCapture(largePath, 4, system), largePath, "regular", "file_too_large", 1, 5)
}

func TestCaptureEnumeratesDirectoryOnClassifiedInodeWithDeterministicBounds(t *testing.T) {
	root := t.TempDir()
	directoryPath := filepath.Join(root, "reports")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatalf("mkdir directory fixture: %v", err)
	}
	for _, name := range []string{"c.txt", "a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(directoryPath, name), []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	full := runCaptureWithBounds(directoryPath, 1024, 10, 1024, newTestCaptureSystem(root))
	if full.Status != protocol.ToolStatusSuccess || full.Result == nil {
		t.Fatalf("full directory envelope = %+v; want success", full)
	}
	if got := full.Result.Entries; !slices.Equal(got, []string{"a.txt", "b.txt", "c.txt"}) || full.Result.EntriesTruncated {
		t.Fatalf("full directory entries/truncated = %q/%v; want sorted complete entries", got, full.Result.EntriesTruncated)
	}

	countBound := runCaptureWithBounds(directoryPath, 1024, 2, 1024, newTestCaptureSystem(root))
	if countBound.Result == nil || len(countBound.Result.Entries) != 2 || !slices.IsSorted(countBound.Result.Entries) || !countBound.Result.EntriesTruncated {
		t.Fatalf("count-bounded directory = %+v; want two sorted entries and truncation", countBound.Result)
	}

	byteBound := runCaptureWithBounds(directoryPath, 1024, 10, int64(len("a.txt")+len("b.txt")), newTestCaptureSystem(root))
	if byteBound.Result == nil || len(byteBound.Result.Entries) != 2 || !slices.IsSorted(byteBound.Result.Entries) || !byteBound.Result.EntriesTruncated {
		t.Fatalf("byte-bounded directory = %+v; want bounded sorted entries and truncation", byteBound.Result)
	}

	zeroBound := runCaptureWithBounds(directoryPath, 1024, 0, 1024, newTestCaptureSystem(root))
	if zeroBound.Result == nil || len(zeroBound.Result.Entries) != 0 || !zeroBound.Result.EntriesTruncated {
		t.Fatalf("zero-bounded directory = %+v; want no entries and truncation", zeroBound.Result)
	}
}

func TestCaptureDirectoryEnumerationStaysOnClassifiedInodeAcrossPathSwap(t *testing.T) {
	root := t.TempDir()
	directoryPath := filepath.Join(root, "reports")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatalf("mkdir directory fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(directoryPath, "real.txt"), []byte("real"), 0o600); err != nil {
		t.Fatalf("write real child: %v", err)
	}
	attackerPath := filepath.Join(root, "attacker")
	if err := os.Mkdir(attackerPath, 0o700); err != nil {
		t.Fatalf("mkdir attacker fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(attackerPath, "attack.txt"), []byte("attack"), 0o600); err != nil {
		t.Fatalf("write attacker child: %v", err)
	}

	system := newTestCaptureSystem(root)
	reopenDirectory := system.reopenDirectory
	system.reopenDirectory = func(classifiedFD int) (int, error) {
		if err := os.Rename(directoryPath, directoryPath+".classified"); err != nil {
			return -1, err
		}
		if err := os.Symlink(attackerPath, directoryPath); err != nil {
			return -1, err
		}
		return reopenDirectory(classifiedFD)
	}

	envelope := runCaptureWithBounds(directoryPath, 1024, 10, 1024, system)
	if envelope.Result == nil || !slices.Equal(envelope.Result.Entries, []string{"real.txt"}) {
		t.Fatalf("swapped directory result = %+v; want classified inode only", envelope.Result)
	}
}

func TestCaptureDirectoryRejectsMismatchedReopenedInode(t *testing.T) {
	root := t.TempDir()
	directoryPath := filepath.Join(root, "reports")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatalf("mkdir classified directory: %v", err)
	}
	otherPath := filepath.Join(root, "other")
	if err := os.Mkdir(otherPath, 0o700); err != nil {
		t.Fatalf("mkdir mismatched directory: %v", err)
	}
	system := newTestCaptureSystem(root)
	system.reopenDirectory = func(int) (int, error) {
		return unix.Open(otherPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	}

	assertSkippedCapture(t, runCaptureWithBounds(directoryPath, 1024, 10, 1024, system), directoryPath, "directory", "unreadable", 2, 0)
}

func TestReadCaptureDirectoryStopsInsideTheReadLoopAtCountBound(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open directory: %v", err)
	}
	defer func() { _ = unix.Close(fd) }()
	readCalls := 0
	readDirent := func(fd int, buffer []byte) (int, error) {
		readCalls++
		if readCalls > 1 {
			t.Fatal("directory reader was called after the count bound was reached")
		}
		return unix.ReadDirent(fd, buffer)
	}

	entries, truncated, _, err := readCaptureDirectory(fd, 1, 1024, readDirent)
	if err != nil {
		t.Fatalf("read bounded directory: %v", err)
	}
	if len(entries) != 1 || !truncated || readCalls != 1 {
		t.Fatalf("bounded directory entries/truncated/readCalls = %q/%v/%d; want one/true/1", entries, truncated, readCalls)
	}
}

func TestCaptureDirectoryOmitsUnrepresentableNamesAndReportsUnreadability(t *testing.T) {
	root := t.TempDir()
	directoryPath := filepath.Join(root, "reports")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatalf("mkdir directory fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(directoryPath, "good.txt"), []byte("good"), 0o600); err != nil {
		t.Fatalf("write good child: %v", err)
	}
	badName := string([]byte{'b', 'a', 'd', 0xff})
	if err := os.WriteFile(filepath.Join(directoryPath, badName), []byte("bad"), 0o600); err != nil {
		t.Fatalf("write invalid UTF-8 child: %v", err)
	}

	envelope := runCaptureWithBounds(directoryPath, 1024, 10, 1024, newTestCaptureSystem(root))
	if envelope.Result == nil || !slices.Equal(envelope.Result.Entries, []string{"good.txt"}) || envelope.Result.UnrepresentableNames != 1 {
		t.Fatalf("unrepresentable-name result = %+v; want good sibling and count one", envelope.Result)
	}

	system := newTestCaptureSystem(root)
	system.readDirectory = func(int, []byte) (int, error) { return 0, unix.EIO }
	unreadable := runCaptureWithBounds(directoryPath, 1024, 10, 1024, system)
	assertSkippedCapture(t, unreadable, directoryPath, "directory", "unreadable", 2, 0)
}

func TestCaptureZeroByteBudgetClassifiesWithoutReadAccess(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)
	sourcePath := filepath.Join(root, "empty.txt")
	if err := os.WriteFile(sourcePath, nil, 0o600); err != nil {
		t.Fatalf("write empty fixture: %v", err)
	}
	reopened := false
	system.reopen = func(int) (int, error) {
		reopened = true
		return -1, unix.EIO
	}

	assertSkippedCapture(t, runCapture(sourcePath, 0, system), sourcePath, "regular", "file_too_large", 1, 0)
	if reopened {
		t.Fatal("zero capture budget acquired read access")
	}
}

func TestCaptureReadsAndHashesThroughSameInodeProcFD(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)
	sourcePath := filepath.Join(root, "result.txt")
	if err := os.WriteFile(sourcePath, []byte("captured body"), 0o600); err != nil {
		t.Fatalf("write output fixture: %v", err)
	}
	reopen := system.reopen
	system.reopen = func(classifiedFD int) (int, error) {
		originalPath := filepath.Join(root, "original.txt")
		if err := os.Rename(sourcePath, originalPath); err != nil {
			return -1, err
		}
		if err := os.WriteFile(sourcePath, []byte("replacement"), 0o600); err != nil {
			return -1, err
		}
		return reopen(classifiedFD)
	}

	envelope := runCapture(sourcePath, 1024, system)
	if envelope.Status != protocol.ToolStatusSuccess || envelope.Result == nil {
		t.Fatalf("capture envelope = %+v; want success", envelope)
	}
	result := envelope.Result
	if result.Kind != "regular" || result.LinkCount != 1 || result.SizeBytes != int64(len("captured body")) || result.SHA256 != "343e5564b69e7e1d964f81995f66c0836156ac283ea7601921051d299145dcf3" || result.DataBase64 != base64.StdEncoding.EncodeToString([]byte("captured body")) {
		t.Fatalf("capture result = %+v; want facts and bytes from classified inode", result)
	}
}

func TestCaptureRejectsUnsafePathResolution(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o755); err != nil {
		t.Fatalf("mkdir real directory: %v", err)
	}
	targetPath := filepath.Join(realDirectory, "target.txt")
	if err := os.WriteFile(targetPath, []byte("body"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(realDirectory, filepath.Join(root, "linked")); err != nil {
		t.Fatalf("symlink directory: %v", err)
	}

	assertCaptureFailure(t, runCapture(filepath.Join(root, "linked", "target.txt"), 1024, system), "unsafe_path")
}

func TestCaptureDetectsChangedFileWithRealRead(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)
	sourcePath := filepath.Join(root, "changing.txt")
	if err := os.WriteFile(sourcePath, []byte("before"), 0o600); err != nil {
		t.Fatalf("write changing fixture: %v", err)
	}
	readAll := system.readAll
	system.readAll = func(reader io.Reader) ([]byte, error) {
		if err := os.Truncate(sourcePath, 0); err != nil {
			return nil, err
		}
		return readAll(reader)
	}

	assertCaptureFailure(t, runCapture(sourcePath, 1024, system), "changed_during_capture")
}

func TestCaptureDetectsSameSizeRewriteAndHardlinkDuringRead(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(string) error
	}{
		{
			name: "same-size rewrite",
			mutate: func(sourcePath string) error {
				return os.WriteFile(sourcePath, []byte("after!"), 0o600)
			},
		},
		{
			name: "hardlink added",
			mutate: func(sourcePath string) error {
				return os.Link(sourcePath, sourcePath+".linked")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			system := newTestCaptureSystem(root)
			sourcePath := filepath.Join(root, "changing.txt")
			if err := os.WriteFile(sourcePath, []byte("before"), 0o600); err != nil {
				t.Fatalf("write changing fixture: %v", err)
			}
			oldTime := time.Unix(1, 0)
			if err := os.Chtimes(sourcePath, oldTime, oldTime); err != nil {
				t.Fatalf("age changing fixture: %v", err)
			}
			readAll := system.readAll
			system.readAll = func(reader io.Reader) ([]byte, error) {
				if err := test.mutate(sourcePath); err != nil {
					return nil, err
				}
				return readAll(reader)
			}

			assertCaptureFailure(t, runCapture(sourcePath, 1024, system), "changed_during_capture")
		})
	}
}

func TestCaptureFailsWhenProcFDReopenIsUnavailableWithoutPathFallback(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)
	sourcePath := filepath.Join(root, "result.txt")
	if err := os.WriteFile(sourcePath, []byte("body"), 0o600); err != nil {
		t.Fatalf("write output fixture: %v", err)
	}
	system.reopen = func(int) (int, error) { return -1, unix.ENOENT }

	assertCaptureFailure(t, runCapture(sourcePath, 1024, system), "capture_failed")
}

func TestCaptureFailureVocabulary(t *testing.T) {
	tests := []struct {
		name     string
		wantKind string
		prepare  func(*testing.T) (string, int64, captureSystem)
	}{
		{
			name:     "invalid input",
			wantKind: "invalid_input",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				root := t.TempDir()
				return filepath.Join(root, "file"), -1, newTestCaptureSystem(root)
			},
		},
		{
			name:     "path escape",
			wantKind: "path_escape",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				root := t.TempDir()
				return filepath.Join(root, "..", "file"), 1, newTestCaptureSystem(root)
			},
		},
		{
			name:     "root unavailable",
			wantKind: "root_unavailable",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				root := filepath.Join(t.TempDir(), "missing")
				return filepath.Join(root, "file"), 1, newTestCaptureSystem(root)
			},
		},
		{
			name:     "entry not found",
			wantKind: "not_found",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				root := t.TempDir()
				return filepath.Join(root, "missing"), 1, newTestCaptureSystem(root)
			},
		},
		{
			name:     "entry permission denied",
			wantKind: "permission_denied",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				root := t.TempDir()
				system := newTestCaptureSystem(root)
				system.openEntry = func(int, string, *unix.OpenHow) (int, error) { return -1, unix.EACCES }
				return filepath.Join(root, "file"), 1, system
			},
		},
		{
			name:     "entry open fault",
			wantKind: "capture_failed",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				root := t.TempDir()
				system := newTestCaptureSystem(root)
				system.openEntry = func(int, string, *unix.OpenHow) (int, error) { return -1, unix.EIO }
				return filepath.Join(root, "file"), 1, system
			},
		},
		{
			name:     "classify fstat fault",
			wantKind: "capture_failed",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				root, sourcePath, system := regularCaptureFixture(t)
				system.fstat = func(int, *unix.Stat_t) error { return unix.EIO }
				return filepath.Join(root, filepath.Base(sourcePath)), 16, system
			},
		},
		{
			name:     "descriptor wrap fault",
			wantKind: "capture_failed",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				_, sourcePath, system := regularCaptureFixture(t)
				system.newFile = func(uintptr, string) *os.File { return nil }
				return sourcePath, 16, system
			},
		},
		{
			name:     "read fstat fault",
			wantKind: "capture_failed",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				_, sourcePath, system := regularCaptureFixture(t)
				fstat := system.fstat
				calls := 0
				system.fstat = func(fd int, stat *unix.Stat_t) error {
					calls++
					if calls == 2 {
						return unix.EIO
					}
					return fstat(fd, stat)
				}
				return sourcePath, 16, system
			},
		},
		{
			name:     "inode mismatch",
			wantKind: "changed_during_capture",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				root, sourcePath, system := regularCaptureFixture(t)
				otherPath := filepath.Join(root, "other.txt")
				if err := os.WriteFile(otherPath, []byte("other"), 0o600); err != nil {
					t.Fatalf("write other fixture: %v", err)
				}
				system.reopen = func(int) (int, error) { return unix.Open(otherPath, unix.O_RDONLY|unix.O_CLOEXEC, 0) }
				return sourcePath, 16, system
			},
		},
		{
			name:     "read fault",
			wantKind: "capture_failed",
			prepare: func(t *testing.T) (string, int64, captureSystem) {
				_, sourcePath, system := regularCaptureFixture(t)
				system.readAll = func(io.Reader) ([]byte, error) { return nil, unix.EIO }
				return sourcePath, 16, system
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sourcePath, maxBytes, system := test.prepare(t)
			assertCaptureFailure(t, runCapture(sourcePath, maxBytes, system), test.wantKind)
		})
	}
}

func TestCaptureRequiresRootIdentity(t *testing.T) {
	root := t.TempDir()
	system := newTestCaptureSystem(root)
	system.currentEUID = func() int { return 1000 }

	assertCaptureFailure(t, runCapture(filepath.Join(root, "file"), 1, system), "permission_denied")
}

func newTestCaptureSystem(root string) captureSystem {
	system := newCaptureSystem(root)
	system.currentEUID = func() int { return 0 }
	return system
}

func regularCaptureFixture(t *testing.T) (string, string, captureSystem) {
	t.Helper()
	root := t.TempDir()
	sourcePath := filepath.Join(root, "file.txt")
	if err := os.WriteFile(sourcePath, []byte("body"), 0o600); err != nil {
		t.Fatalf("write regular fixture: %v", err)
	}
	return root, sourcePath, newTestCaptureSystem(root)
}

func assertSkippedCapture(t *testing.T, envelope protocol.CaptureEnvelope, sourcePath string, kind string, reason string, linkCount uint64, sizeBytes int64) {
	t.Helper()
	if envelope.Status != protocol.ToolStatusSuccess || envelope.Result == nil || envelope.Error != nil {
		t.Fatalf("capture envelope = %+v; want skipped success", envelope)
	}
	result := envelope.Result
	if result.SourcePath != sourcePath || result.Kind != kind || result.LinkCount != linkCount || result.SizeBytes != sizeBytes || !result.Skipped || result.SkipReason != reason || result.DataBase64 != "" {
		t.Fatalf("capture result = %+v; want skipped %s", result, reason)
	}
}

func assertCaptureFailure(t *testing.T, envelope protocol.CaptureEnvelope, wantKind string) {
	t.Helper()
	if envelope.Status != protocol.ToolStatusError || envelope.Error == nil || envelope.Result != nil {
		t.Fatalf("capture envelope = %+v; want %s failure", envelope, wantKind)
	}
	if envelope.Error.Kind != wantKind {
		t.Fatalf("capture error kind = %q; want %q", envelope.Error.Kind, wantKind)
	}
}
