package media

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

// maxImageBytes (5 MiB) is a size gate applied by stat BEFORE any byte is read:
// a file over the cap is too_large without reading it. Format is decided by
// magic bytes, never by extension (PNG/JPEG/GIF/WebP), so a file with an image
// extension but foreign content is unsupported_format. The resulting base64
// envelope is exempt from the 256 KiB envelope self-cap.
const maxImageBytes = 5 * 1024 * 1024

type ViewImageResult struct {
	MIME       string `json:"mime"`
	SizeBytes  int64  `json:"size_bytes"`
	DataBase64 string `json:"data_base64"`
}

type viewImageInput struct {
	Path string `json:"path"`
}

func ViewImage(payload protocol.Payload) (ViewImageResult, *protocol.ToolError) {
	var input viewImageInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return ViewImageResult{}, invalidInput("input must be an object")
	}
	if input.Path == "" {
		return ViewImageResult{}, invalidInput("path is required")
	}
	resolved, err := (pathsafe.Resolver{WorkspaceRoot: payload.WorkspaceRoot, Roots: payload.Roots}).Resolve(input.Path, false)
	if err != nil {
		return ViewImageResult{}, pathsafe.ToolError(err)
	}
	info, err := os.Stat(resolved.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ViewImageResult{}, &protocol.ToolError{Kind: "not_found", Message: "path does not exist", Path: resolved.DisplayPath}
		}
		return ViewImageResult{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	if info.IsDir() {
		return ViewImageResult{}, &protocol.ToolError{Kind: "is_directory", Message: "path is a directory", Path: resolved.DisplayPath}
	}
	if info.Size() == 0 {
		return ViewImageResult{}, &protocol.ToolError{Kind: "unsupported_format", Message: "empty file", Path: resolved.DisplayPath}
	}
	if info.Size() > maxImageBytes {
		return ViewImageResult{}, &protocol.ToolError{Kind: "too_large", Message: "image exceeds 5 MiB limit", Path: resolved.DisplayPath}
	}
	file, err := os.Open(resolved.Path) //nolint:gosec // path has already been contained by pathsafe.
	if err != nil {
		return ViewImageResult{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	defer func() { _ = file.Close() }()

	header := make([]byte, 16)
	n, err := io.ReadFull(file, header)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return ViewImageResult{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	mime := sniffImageMIME(header[:n])
	if mime == "" {
		return ViewImageResult{}, &protocol.ToolError{
			Kind:    "unsupported_format",
			Message: "unsupported image format; supported formats are PNG, JPEG, GIF, and WebP",
			Path:    resolved.DisplayPath,
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ViewImageResult{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	body, err := io.ReadAll(io.LimitReader(file, info.Size()+1))
	if err != nil {
		return ViewImageResult{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	if int64(len(body)) != info.Size() {
		return ViewImageResult{}, &protocol.ToolError{Kind: "not_found", Message: "path changed while reading", Path: resolved.DisplayPath}
	}
	return ViewImageResult{
		MIME:       mime,
		SizeBytes:  info.Size(),
		DataBase64: base64.StdEncoding.EncodeToString(body),
	}, nil
}

func sniffImageMIME(header []byte) string {
	switch {
	case len(header) >= 8 && bytes.Equal(header[:8], []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}):
		return "image/png"
	case len(header) >= 3 && bytes.Equal(header[:3], []byte{0xff, 0xd8, 0xff}):
		return "image/jpeg"
	case len(header) >= 6 && (bytes.Equal(header[:6], []byte("GIF87a")) || bytes.Equal(header[:6], []byte("GIF89a"))):
		return "image/gif"
	case len(header) >= 12 && bytes.Equal(header[:4], []byte("RIFF")) && bytes.Equal(header[8:12], []byte("WEBP")):
		return "image/webp"
	default:
		return ""
	}
}

func invalidInput(message string) *protocol.ToolError {
	return &protocol.ToolError{Kind: "invalid_input", Message: message}
}
