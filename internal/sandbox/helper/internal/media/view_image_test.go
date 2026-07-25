package media

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestViewImageAcceptsSupportedMagicBytes(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		mime string
	}{
		{name: "png", body: append([]byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, []byte("png-body")...), mime: "image/png"},
		{name: "jpeg", body: append([]byte{0xff, 0xd8, 0xff}, []byte("jpeg-body")...), mime: "image/jpeg"},
		{name: "gif87a", body: append([]byte("GIF87a"), []byte("gif-body")...), mime: "image/gif"},
		{name: "gif89a", body: append([]byte("GIF89a"), []byte("gif-body")...), mime: "image/gif"},
		{name: "webp", body: append([]byte("RIFFxxxxWEBP"), []byte("webp-body")...), mime: "image/webp"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newImageFixture(t)
			fs.writeBytes("image.dat", tc.body)
			result, toolErr := ViewImage(fs.payload(map[string]any{"path": "image.dat"}))
			if toolErr != nil {
				t.Fatalf("ViewImage error = %+v", toolErr)
			}
			if result.MIME != tc.mime || result.SizeBytes != int64(len(tc.body)) || result.DataBase64 != base64.StdEncoding.EncodeToString(tc.body) {
				t.Fatalf("result = %+v; want mime %s, exact size and base64", result, tc.mime)
			}
		})
	}
}

func TestViewImageSniffsContentInsteadOfExtension(t *testing.T) {
	fs := newImageFixture(t)
	fs.writeBytes("foreign.png", []byte("not actually an image"))
	_, toolErr := ViewImage(fs.payload(map[string]any{"path": "foreign.png"}))
	if toolErr == nil || toolErr.Kind != "unsupported_format" {
		t.Fatalf("ViewImage error = %+v; want unsupported_format", toolErr)
	}
	if !strings.Contains(toolErr.Message, "PNG") || !strings.Contains(toolErr.Message, "JPEG") || !strings.Contains(toolErr.Message, "GIF") || !strings.Contains(toolErr.Message, "WebP") {
		t.Fatalf("unsupported message = %q; want supported format names", toolErr.Message)
	}
}

func TestViewImageEmptyAndOversizedFilesUseContractErrors(t *testing.T) {
	fs := newImageFixture(t)
	fs.writeBytes("empty.gif", nil)
	_, toolErr := ViewImage(fs.payload(map[string]any{"path": "empty.gif"}))
	if toolErr == nil || toolErr.Kind != "unsupported_format" || !strings.Contains(toolErr.Message, "empty file") {
		t.Fatalf("empty error = %+v; want unsupported_format empty file", toolErr)
	}

	oversized := fs.path("huge.bin")
	file, err := os.Create(oversized)
	if err != nil {
		t.Fatalf("create oversized fixture: %v", err)
	}
	if err := file.Truncate(maxImageBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate oversized fixture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close oversized fixture: %v", err)
	}
	_, toolErr = ViewImage(fs.payload(map[string]any{"path": "huge.bin"}))
	if toolErr == nil || toolErr.Kind != "too_large" {
		t.Fatalf("oversized error = %+v; want too_large before format sniffing", toolErr)
	}
}

func TestViewImageRejectsNamedPipeBeforeRead(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("named pipes use Unix mkfifo")
	}
	fs := newImageFixture(t)
	if err := syscall.Mkfifo(fs.path("pipe.png"), 0o600); err != nil {
		t.Fatalf("mkfifo fixture: %v", err)
	}
	done := make(chan *protocol.ToolError, 1)
	go func() {
		_, toolErr := ViewImage(fs.payload(map[string]any{"path": "pipe.png"}))
		done <- toolErr
	}()
	select {
	case toolErr := <-done:
		if toolErr == nil || toolErr.Kind != "unsupported_format" || !strings.Contains(toolErr.Message, "empty file") {
			t.Fatalf("named pipe error = %+v; want size-gate unsupported_format before read", toolErr)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("ViewImage blocked on named pipe open/read")
	}
}

func TestViewImagePathErrors(t *testing.T) {
	fs := newImageFixture(t)
	fs.mkdir("images")
	if _, toolErr := ViewImage(fs.payload(map[string]any{"path": "images"})); toolErr == nil || toolErr.Kind != "is_directory" {
		t.Fatalf("directory error = %+v; want is_directory", toolErr)
	}
	if _, toolErr := ViewImage(fs.payload(map[string]any{"path": "missing.png"})); toolErr == nil || toolErr.Kind != "not_found" {
		t.Fatalf("missing error = %+v; want not_found", toolErr)
	}

	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.png"), []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, 0o600); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.png"), fs.path("escape.png")); err != nil {
		t.Fatalf("symlink fixture: %v", err)
	}
	if _, toolErr := ViewImage(fs.payload(map[string]any{"path": "escape.png"})); toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("symlink escape error = %+v; want path_escape", toolErr)
	}

	if _, toolErr := ViewImage(fs.payload(map[string]any{"path": "/tmp/tetral-runtime/tool-payloads/x/payload.json"})); toolErr == nil || toolErr.Kind != "forbidden_path" {
		t.Fatalf("forbidden path error = %+v; want forbidden_path", toolErr)
	}
	if _, toolErr := ViewImage(fs.payload(map[string]any{"path": "/dev/shm/tetral-runtime/tool-payloads/x/payload.json"})); toolErr == nil || toolErr.Kind != "forbidden_path" {
		t.Fatalf("dev shm forbidden path error = %+v; want forbidden_path", toolErr)
	}
}

type imageFixture struct {
	t         *testing.T
	workspace string
}

func newImageFixture(t *testing.T) imageFixture {
	t.Helper()
	return imageFixture{t: t, workspace: t.TempDir()}
}

func (f imageFixture) payload(input map[string]any) protocol.Payload {
	f.t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		f.t.Fatalf("marshal input: %v", err)
	}
	return protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "view_image",
		ToolUseEventID: "evt_view_image",
		WorkspaceRoot:  f.workspace,
		Roots:          []protocol.Root{{Path: f.workspace, Mode: protocol.RootModeReadWrite}},
		Input:          encoded,
	}
}

func (f imageFixture) writeBytes(name string, body []byte) {
	f.t.Helper()
	path := f.path(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatalf("mkdir fixture: %v", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		f.t.Fatalf("write fixture: %v", err)
	}
}

func (f imageFixture) mkdir(name string) {
	f.t.Helper()
	if err := os.MkdirAll(f.path(name), 0o755); err != nil {
		f.t.Fatalf("mkdir fixture: %v", err)
	}
}

func (f imageFixture) path(name string) string {
	f.t.Helper()
	return filepath.Join(f.workspace, filepath.FromSlash(name))
}
