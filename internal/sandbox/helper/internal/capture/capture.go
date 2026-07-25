package capture

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"golang.org/x/sys/unix"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	OutputRoot = "/mnt/session/outputs"

	errorKindInvalidInput         = "invalid_input"
	errorKindPathEscape           = "path_escape"
	errorKindPermissionDenied     = "permission_denied"
	errorKindRootUnavailable      = "root_unavailable"
	errorKindNotFound             = "not_found"
	errorKindUnsafePath           = "unsafe_path"
	errorKindCaptureFailed        = "capture_failed"
	errorKindChangedDuringCapture = "changed_during_capture"
)

type captureSystem struct {
	outputRoot      string
	currentEUID     func() int
	openRoot        func(string, int, uint32) (int, error)
	openEntry       func(int, string, *unix.OpenHow) (int, error)
	fstat           func(int, *unix.Stat_t) error
	reopen          func(int) (int, error)
	reopenDirectory func(int) (int, error)
	readDirectory   func(int, []byte) (int, error)
	newFile         func(uintptr, string) *os.File
	readAll         func(io.Reader) ([]byte, error)
}

func newCaptureSystem(outputRoot string) captureSystem {
	return captureSystem{
		outputRoot:      outputRoot,
		currentEUID:     os.Geteuid,
		openRoot:        unix.Open,
		openEntry:       unix.Openat2,
		fstat:           unix.Fstat,
		reopen:          reopenCaptureDescriptor,
		reopenDirectory: reopenCaptureDirectoryDescriptor,
		readDirectory:   unix.ReadDirent,
		newFile:         os.NewFile,
		readAll:         io.ReadAll,
	}
}

func RunWithBounds(sourcePath string, maxBytes int64, maxEntries int, maxNameBytes int64) protocol.CaptureEnvelope {
	return runCaptureWithBounds(sourcePath, maxBytes, maxEntries, maxNameBytes, newCaptureSystem(OutputRoot))
}

func runCapture(sourcePath string, maxBytes int64, system captureSystem) protocol.CaptureEnvelope {
	return runCaptureWithBounds(sourcePath, maxBytes, protocol.CaptureDefaultMaxEntries, protocol.CaptureDefaultMaxNameBytes, system)
}

func runCaptureWithBounds(sourcePath string, maxBytes int64, maxEntries int, maxNameBytes int64, system captureSystem) protocol.CaptureEnvelope {
	if system.currentEUID() != 0 {
		return failure(errorKindPermissionDenied, "capture mode requires root")
	}
	if maxBytes < 0 || maxEntries < 0 || maxNameBytes < 0 {
		return failure(errorKindInvalidInput, "capture bounds are invalid")
	}
	relativePath, ok := captureRelativePath(sourcePath, system.outputRoot)
	if !ok {
		return failure(errorKindPathEscape, "capture path escapes output root")
	}

	rootFD, err := system.openRoot(system.outputRoot, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return failure(errorKindRootUnavailable, "output root could not be opened")
	}
	defer func() { _ = unix.Close(rootFD) }()

	classifiedFD, err := system.openEntry(rootFD, relativePath, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return entryOpenFailure(err)
	}
	defer func() { _ = unix.Close(classifiedFD) }()

	var classifiedStat unix.Stat_t
	if err := system.fstat(classifiedFD, &classifiedStat); err != nil {
		return failure(errorKindCaptureFailed, "output descriptor could not be inspected")
	}
	kind := captureKind(classifiedStat.Mode)
	if kind == "directory" {
		result := protocol.CaptureResult{
			SourcePath: sourcePath,
			Kind:       kind,
			LinkCount:  uint64(classifiedStat.Nlink),
			Entries:    make([]string, 0),
		}
		directoryFD, err := system.reopenDirectory(classifiedFD)
		if err != nil {
			return skipped(result, "unreadable")
		}
		defer func() { _ = unix.Close(directoryFD) }()
		var directoryStat unix.Stat_t
		if err := system.fstat(directoryFD, &directoryStat); err != nil ||
			!sameCaptureInode(classifiedStat, directoryStat) ||
			captureKind(directoryStat.Mode) != "directory" {
			return skipped(result, "unreadable")
		}
		entries, truncated, unrepresentable, err := readCaptureDirectory(directoryFD, maxEntries, maxNameBytes, system.readDirectory)
		if err != nil {
			return skipped(result, "unreadable")
		}
		result.Entries = entries
		result.EntriesTruncated = truncated
		result.UnrepresentableNames = unrepresentable
		return success(result)
	}

	result := protocol.CaptureResult{
		SourcePath: sourcePath,
		Kind:       kind,
		LinkCount:  uint64(classifiedStat.Nlink),
		SizeBytes:  classifiedStat.Size,
	}
	if kind != "regular" {
		return skipped(result, "non_regular")
	}
	if classifiedStat.Nlink != 1 {
		return skipped(result, "multiple_links")
	}
	if maxBytes == 0 || classifiedStat.Size < 0 || classifiedStat.Size > maxBytes {
		return skipped(result, "file_too_large")
	}

	readFD, err := system.reopen(classifiedFD)
	if err != nil {
		return failure(errorKindCaptureFailed, "output descriptor could not be reopened for reading")
	}
	file := system.newFile(uintptr(readFD), sourcePath)
	if file == nil {
		_ = unix.Close(readFD)
		return failure(errorKindCaptureFailed, "output descriptor could not be opened")
	}
	defer func() { _ = file.Close() }()

	var readStat unix.Stat_t
	if err := system.fstat(readFD, &readStat); err != nil {
		return failure(errorKindCaptureFailed, "read descriptor could not be inspected")
	}
	if !sameCaptureInode(classifiedStat, readStat) || captureKind(readStat.Mode) != "regular" || readStat.Nlink != 1 || readStat.Size != classifiedStat.Size {
		return failure(errorKindChangedDuringCapture, "output file changed during capture")
	}

	body, err := system.readAll(io.LimitReader(file, captureReadLimit(maxBytes)))
	if err != nil {
		return failure(errorKindCaptureFailed, "output file could not be read")
	}
	if int64(len(body)) != classifiedStat.Size {
		return failure(errorKindChangedDuringCapture, "output file changed during capture")
	}
	var finalStat unix.Stat_t
	if err := system.fstat(readFD, &finalStat); err != nil || !sameCaptureFileSnapshot(readStat, finalStat) {
		return failure(errorKindChangedDuringCapture, "output file changed during capture")
	}

	digest := sha256.Sum256(body)
	mimeType := "application/octet-stream"
	if len(body) > 0 {
		mimeType = http.DetectContentType(body[:min(len(body), 512)])
	}
	result.SHA256 = hex.EncodeToString(digest[:])
	result.MIMEType = mimeType
	result.DataBase64 = base64.StdEncoding.EncodeToString(body)
	return success(result)
}

func captureRelativePath(sourcePath string, outputRoot string) (string, bool) {
	if sourcePath == outputRoot {
		return ".", true
	}
	if sourcePath == "" || strings.ContainsRune(sourcePath, 0) || !strings.HasPrefix(sourcePath, outputRoot+"/") || path.Clean(sourcePath) != sourcePath {
		return "", false
	}
	relative := strings.TrimPrefix(sourcePath, outputRoot+"/")
	if relative == "" || relative == "." || relative == ".." || strings.HasPrefix(relative, "../") {
		return "", false
	}
	return relative, true
}

func entryOpenFailure(err error) protocol.CaptureEnvelope {
	switch {
	case errors.Is(err, unix.ENOENT):
		return failure(errorKindNotFound, "output path does not exist")
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		return failure(errorKindPermissionDenied, "output path could not be opened")
	case errors.Is(err, unix.ELOOP), errors.Is(err, unix.EXDEV), errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.EAGAIN):
		return failure(errorKindUnsafePath, "output path could not be opened safely")
	default:
		return failure(errorKindCaptureFailed, "output path could not be opened")
	}
}

func captureKind(mode uint32) string {
	switch mode & unix.S_IFMT {
	case unix.S_IFREG:
		return "regular"
	case unix.S_IFDIR:
		return "directory"
	case unix.S_IFIFO:
		return "fifo"
	case unix.S_IFLNK:
		return "symlink"
	case unix.S_IFSOCK:
		return "socket"
	case unix.S_IFCHR:
		return "character_device"
	case unix.S_IFBLK:
		return "block_device"
	default:
		return "non_regular"
	}
}

func reopenCaptureDescriptor(classifiedFD int) (int, error) {
	return unix.Open(fmt.Sprintf("/proc/self/fd/%d", classifiedFD), unix.O_RDONLY|unix.O_CLOEXEC, 0)
}

func reopenCaptureDirectoryDescriptor(classifiedFD int) (int, error) {
	return unix.Open(fmt.Sprintf("/proc/self/fd/%d", classifiedFD), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
}

func readCaptureDirectory(directoryFD int, maxEntries int, maxNameBytes int64, readDirent func(int, []byte) (int, error)) ([]string, bool, int, error) {
	const directoryBufferBytes = 32 * 1024
	buffer := make([]byte, directoryBufferBytes)
	entries := make([]string, 0, min(maxEntries, 128))
	visited := 0
	var nameBytes int64
	unrepresentable := 0
	for {
		count, err := readDirent(directoryFD, buffer)
		if err != nil {
			return nil, false, 0, err
		}
		if count == 0 {
			sort.Strings(entries)
			return entries, false, unrepresentable, nil
		}
		_, _, names := unix.ParseDirent(buffer[:count], -1, nil)
		for _, name := range names {
			nextNameBytes := nameBytes + int64(len(name))
			if visited >= maxEntries || nextNameBytes > maxNameBytes {
				sort.Strings(entries)
				return entries, true, unrepresentable, nil
			}
			visited++
			nameBytes = nextNameBytes
			if !utf8.ValidString(name) {
				unrepresentable++
				continue
			}
			entries = append(entries, name)
		}
	}
}

func sameCaptureInode(classified unix.Stat_t, read unix.Stat_t) bool {
	return classified.Dev == read.Dev && classified.Ino == read.Ino
}

func sameCaptureFileSnapshot(before unix.Stat_t, after unix.Stat_t) bool {
	return sameCaptureInode(before, after) &&
		captureKind(after.Mode) == "regular" &&
		after.Nlink == 1 &&
		before.Size == after.Size &&
		before.Mtim.Sec == after.Mtim.Sec &&
		before.Mtim.Nsec == after.Mtim.Nsec &&
		before.Ctim.Sec == after.Ctim.Sec &&
		before.Ctim.Nsec == after.Ctim.Nsec
}

func captureReadLimit(maxBytes int64) int64 {
	if maxBytes == math.MaxInt64 {
		return maxBytes
	}
	return maxBytes + 1
}

func skipped(result protocol.CaptureResult, reason string) protocol.CaptureEnvelope {
	result.Skipped = true
	result.SkipReason = reason
	return success(result)
}

func success(result protocol.CaptureResult) protocol.CaptureEnvelope {
	return protocol.CaptureEnvelope{SchemaVersion: protocol.SchemaVersion, Status: protocol.ToolStatusSuccess, Result: &result}
}

func failure(kind string, message string) protocol.CaptureEnvelope {
	return protocol.CaptureEnvelope{SchemaVersion: protocol.SchemaVersion, Status: protocol.ToolStatusError, Error: &protocol.CaptureError{Kind: kind, Message: message}}
}
