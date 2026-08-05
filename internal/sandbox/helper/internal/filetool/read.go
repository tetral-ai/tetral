package filetool

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/media"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	// The read window is bounded by three limits at once; capture stops at
	// whichever is hit first, streaming line-by-line from offset so pre-offset
	// bytes are skipped rather than buffered. The line window defaults to and
	// caps at 2000 lines (a larger limit clamps, it does not error); returned
	// content is held to a 200,000-byte budget; and any single line longer than
	// 2000 runes is cut with lineTruncatedSuffix, so one pathological line
	// cannot consume the whole budget.
	//
	// binarySniffBytes is the 4096-byte prefix classified for binary content,
	// checked only AFTER the media magic check fails. A NUL byte, over 30%
	// non-printable bytes, or any invalid UTF-8 in that window makes the file
	// binary_file. Invalid UTF-8 is treated as binary on purpose: a JSON string
	// content field cannot carry arbitrary bytes, and refusing avoids a silent
	// lossy U+FFFD substitution.
	//
	// maxReadMediaBytes (10 MiB) caps the media envelope produced when the file
	// magic-matches PNG/JPEG/GIF/WebP/PDF (checked before the binary sniff).
	// That envelope is transport-only and never model-visible — the runtime
	// converts it to a transient attachment — and it is exempt from text
	// windowing and the Read-specific 200,000-byte encoded-envelope cap. For PDF
	// the cap applies to the
	// TRIMMED page slice, not the source.
	//
	// pdfSourceReadMaxBytes (32 MiB) caps the PDF SOURCE read. It is larger than
	// the media/envelope cap on purpose, so a large document with a small
	// requested slice still works; the trimmed sub-PDF is then held to
	// maxReadMediaBytes. Beyond this, too_large names the source.
	defaultReadLines      = 2000
	maxReadLines          = 2000
	readByteBudget = 200_000
	// maxReadEnvelopeBytes bounds the complete json.Marshal output after string
	// escaping. Runtime decodes this envelope before adding line numbers, then
	// re-encodes the visible provider part under its paired 256 KiB fuse.
	// UPDATE-WITH: services/gateway/packages/protocol/src/bounds.ts
	//   (MaxProviderRequestToolOutputJsonBytes).
	maxReadEnvelopeBytes  = 200_000
	maxReadLineRunes      = 2000
	binarySniffBytes      = 4096
	maxReadMediaBytes     = 10 * 1024 * 1024
	pdfSourceReadMaxBytes = 32 * 1024 * 1024
	lineTruncatedSuffix   = "… [line truncated]"
)

type ReadResult struct {
	Content         string           `json:"content"`
	StartLine       int              `json:"start_line"`
	ReturnedLines   int              `json:"returned_lines"`
	TotalBytesFile  int64            `json:"total_bytes_file"`
	TotalLines      *int             `json:"total_lines"`
	NextOffset      *int             `json:"next_offset"`
	Truncated       bool             `json:"truncated"`
	Empty           bool             `json:"empty"`
	LineTruncations int              `json:"line_truncations"`
	Media           *ReadMediaResult `json:"-"`
}

type ReadMediaResult struct {
	MIME       string `json:"mime"`
	SizeBytes  int64  `json:"size_bytes"`
	DataBase64 string `json:"data_base64"`
	PageRange  string `json:"page_range,omitempty"`
}

func (r ReadResult) MarshalJSON() ([]byte, error) {
	if r.Media != nil {
		return json.Marshal(*r.Media)
	}
	type textReadResult struct {
		Content         string `json:"content"`
		StartLine       int    `json:"start_line"`
		ReturnedLines   int    `json:"returned_lines"`
		TotalBytesFile  int64  `json:"total_bytes_file"`
		TotalLines      *int   `json:"total_lines"`
		NextOffset      *int   `json:"next_offset"`
		Truncated       bool   `json:"truncated"`
		Empty           bool   `json:"empty"`
		LineTruncations int    `json:"line_truncations"`
	}
	return json.Marshal(textReadResult{
		Content:         r.Content,
		StartLine:       r.StartLine,
		ReturnedLines:   r.ReturnedLines,
		TotalBytesFile:  r.TotalBytesFile,
		TotalLines:      r.TotalLines,
		NextOffset:      r.NextOffset,
		Truncated:       r.Truncated,
		Empty:           r.Empty,
		LineTruncations: r.LineTruncations,
	})
}

type readInput struct {
	Path      string `json:"path"`
	Offset    int    `json:"offset"`
	Limit     int    `json:"limit"`
	PageRange string `json:"page_range"`
}

var resolveReadPath = func(payload protocol.Payload, pathValue string) (pathsafe.Resolved, error) {
	return (pathsafe.Resolver{WorkspaceRoot: payload.WorkspaceRoot, Roots: payload.Roots}).Resolve(pathValue, false)
}

type lineWindow struct {
	text          string
	hasLine       bool
	lineTruncated bool
	budgetHit     bool
	eofAfterLine  bool
}

func Read(payload protocol.Payload) (ReadResult, *protocol.ToolError, error) {
	var input readInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return ReadResult{}, invalidInput("input must be an object"), nil
	}
	pathValue := input.Path
	if pathValue == "" {
		return ReadResult{}, invalidInput("path is required"), nil
	}
	offset := input.Offset
	if offset == 0 {
		offset = 1
	}
	if offset < 0 {
		return ReadResult{}, invalidInput("offset must be non-negative"), nil
	}
	limit := input.Limit
	if limit == 0 {
		limit = defaultReadLines
	}
	if limit < 0 {
		return ReadResult{}, invalidInput("limit must be non-negative"), nil
	}
	if limit > maxReadLines {
		limit = maxReadLines
	}
	if payload.Limits.VisibleLines > 0 && payload.Limits.VisibleLines < limit {
		limit = payload.Limits.VisibleLines
	}
	budget := readByteBudget
	if payload.Limits.VisibleBytes > 0 && payload.Limits.VisibleBytes < budget {
		budget = payload.Limits.VisibleBytes
	}
	resolved, err := resolveReadPath(payload, pathValue)
	if err != nil {
		if isBackendReadFailure(err) {
			return ReadResult{}, nil, err
		}
		return ReadResult{}, pathsafe.ToolError(err), nil
	}
	info, err := os.Stat(resolved.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ReadResult{}, &protocol.ToolError{Kind: "not_found", Message: "path does not exist", Path: resolved.DisplayPath}, nil
		}
		if isBackendReadFailure(err) {
			return ReadResult{}, nil, err
		}
		return ReadResult{}, unreadablePathToolError(resolved.DisplayPath), nil
	}
	if info.IsDir() {
		return ReadResult{}, &protocol.ToolError{Kind: "is_directory", Message: "path is a directory", Path: resolved.DisplayPath}, nil
	}
	if !info.Mode().IsRegular() {
		return ReadResult{}, &protocol.ToolError{Kind: "invalid_input", Message: "path is not a regular file", Path: resolved.DisplayPath}, nil
	}
	file, err := os.Open(resolved.Path) //nolint:gosec // Resolved through contract path containment.
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ReadResult{}, &protocol.ToolError{Kind: "not_found", Message: "path does not exist", Path: resolved.DisplayPath}, nil
		}
		if isBackendReadFailure(err) {
			return ReadResult{}, nil, err
		}
		return ReadResult{}, unreadablePathToolError(resolved.DisplayPath), nil
	}
	defer func() { _ = file.Close() }()
	sniffed, err := readSniffBytes(file)
	if err != nil {
		if isBackendReadFailure(err) {
			return ReadResult{}, nil, err
		}
		return ReadResult{}, unreadablePathToolError(resolved.DisplayPath), nil
	}
	if mime := sniffReadMediaMIME(sniffed); mime != "" {
		result, toolErr := readMediaResult(file, info.Size(), mime, resolved.DisplayPath, input.PageRange)
		return result, toolErr, nil
	}
	if sniffBinaryBytes(sniffed) {
		return ReadResult{}, &protocol.ToolError{Kind: "binary_file", Message: "file appears to be binary", Path: resolved.DisplayPath}, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		if isBackendReadFailure(err) {
			return ReadResult{}, nil, err
		}
		return ReadResult{}, unreadablePathToolError(resolved.DisplayPath), nil
	}
	result, err := readWindow(file, offset, limit, budget, info.Size())
	if err != nil {
		if isBackendReadFailure(err) {
			return ReadResult{}, nil, err
		}
		return ReadResult{}, unreadablePathToolError(resolved.DisplayPath), nil
	}
	return result, nil, nil
}

func isBackendReadFailure(err error) bool {
	return errors.Is(err, syscall.EIO)
}

func readWindow(reader io.Reader, offset int, limit int, budget int, totalBytes int64) (ReadResult, error) {
	buffered := bufio.NewReader(reader)
	skipped := 0
	for skipped < offset-1 {
		line, err := readLine(buffered, 0, 0)
		if err != nil {
			return ReadResult{}, err
		}
		if !line.hasLine {
			total := skipped
			return ReadResult{StartLine: offset, TotalBytesFile: totalBytes, TotalLines: &total, Empty: totalBytes == 0}, nil
		}
		skipped++
	}
	content := make([]byte, 0, minInt(budget, 32*1024))
	returned := 0
	lineTruncations := 0
	truncated := false
	eofReached := false
	for returned < limit && len(content) < budget {
		line, err := readLine(buffered, maxReadLineRunes, budget-len(content))
		if err != nil {
			return ReadResult{}, err
		}
		if !line.hasLine {
			eofReached = true
			break
		}
		content = append(content, line.text...)
		returned++
		if line.lineTruncated {
			lineTruncations++
			truncated = true
		}
		if line.budgetHit {
			truncated = true
			break
		}
		if line.eofAfterLine {
			eofReached = true
			break
		}
	}
	if returned == limit && limit > 0 && !eofReached {
		if hasMore(buffered) {
			truncated = true
		} else {
			eofReached = true
		}
	}
	if len(content) >= budget && !eofReached {
		truncated = true
	}
	var totalLines *int
	if eofReached {
		total := skipped + returned
		totalLines = &total
	}
	var nextOffset *int
	if truncated && !eofReached {
		next := offset + returned
		nextOffset = &next
	}
	return ReadResult{
		Content:         string(content),
		StartLine:       offset,
		ReturnedLines:   returned,
		TotalBytesFile:  totalBytes,
		TotalLines:      totalLines,
		NextOffset:      nextOffset,
		Truncated:       truncated,
		Empty:           totalBytes == 0,
		LineTruncations: lineTruncations,
	}, nil
}

func readLine(reader *bufio.Reader, maxRunes int, budget int) (lineWindow, error) {
	var out []byte
	hasLine := false
	runes := 0
	lineTruncated := false
	suffixWritten := false
	for {
		r, size, err := reader.ReadRune()
		if errors.Is(err, io.EOF) {
			if !hasLine {
				return lineWindow{}, nil
			}
			return lineWindow{text: string(out), hasLine: true, lineTruncated: lineTruncated, eofAfterLine: true}, nil
		}
		if err != nil {
			return lineWindow{}, err
		}
		hasLine = true
		if maxRunes == 0 {
			if r == '\n' {
				return lineWindow{hasLine: true}, nil
			}
			continue
		}
		if lineTruncated {
			if r == '\n' {
				if len(out)+size > budget {
					return lineWindow{text: string(out), hasLine: true, lineTruncated: true, budgetHit: true}, nil
				}
				out = append(out, '\n')
				return lineWindow{text: string(out), hasLine: true, lineTruncated: true}, nil
			}
			continue
		}
		if r == '\n' {
			if len(out)+size > budget {
				return lineWindow{text: string(out), hasLine: true, budgetHit: true}, nil
			}
			out = append(out, '\n')
			return lineWindow{text: string(out), hasLine: true}, nil
		}
		if runes >= maxRunes {
			lineTruncated = true
			if !suffixWritten {
				if len(out)+len(lineTruncatedSuffix) > budget {
					return lineWindow{text: string(out), hasLine: true, budgetHit: true}, nil
				}
				out = append(out, lineTruncatedSuffix...)
				suffixWritten = true
			}
			continue
		}
		encoded := make([]byte, utf8.RuneLen(r))
		if len(encoded) == 0 || utf8.EncodeRune(encoded, r) == 0 {
			encoded = []byte(string(r))
		}
		if len(out)+len(encoded) > budget {
			return lineWindow{text: string(out), hasLine: true, budgetHit: true}, nil
		}
		out = append(out, encoded...)
		runes++
	}
}

func FitReadResultForEnvelope(result ReadResult, tool string, startedAt time.Time) ReadResult {
	if result.Media != nil {
		return result
	}
	if readEnvelopeLen(result, tool, startedAt) <= maxReadEnvelopeBytes {
		return result
	}
	result.Truncated = true
	for result.ReturnedLines > 1 {
		trimmed, ok := trimLastReturnedLine(result.Content)
		if !ok {
			break
		}
		result.Content = trimmed
		result.ReturnedLines--
		total := result.StartLine + result.ReturnedLines
		result.NextOffset = &total
		result.TotalLines = nil
		if readEnvelopeLen(result, tool, startedAt) <= maxReadEnvelopeBytes {
			return result
		}
	}
	result.Content = fitSingleLineContent(result, tool, startedAt)
	result.ReturnedLines = minInt(result.ReturnedLines, 1)
	if result.ReturnedLines == 1 {
		next := result.StartLine + 1
		result.NextOffset = &next
	}
	result.TotalLines = nil
	result.LineTruncations++
	return result
}

func readEnvelopeLen(result ReadResult, tool string, startedAt time.Time) int {
	envelope, err := protocol.NewToolResultEnvelope(tool, result, result.Truncated, startedAt)
	if err != nil {
		return maxReadEnvelopeBytes + 1
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return maxReadEnvelopeBytes + 1
	}
	return len(encoded)
}

func trimLastReturnedLine(content string) (string, bool) {
	if content == "" {
		return "", false
	}
	candidate := strings.TrimSuffix(content, "\n")
	index := strings.LastIndex(candidate, "\n")
	if index < 0 {
		return "", false
	}
	return candidate[:index+1], true
}

func fitSingleLineContent(result ReadResult, tool string, startedAt time.Time) string {
	content := []rune(result.Content)
	low, high := 0, len(content)
	best := ""
	for low <= high {
		mid := (low + high) / 2
		candidate := string(content[:mid]) + lineTruncatedSuffix
		probe := result
		probe.Content = candidate
		probe.Truncated = true
		probe.ReturnedLines = minInt(probe.ReturnedLines, 1)
		probe.TotalLines = nil
		if probe.ReturnedLines == 1 {
			next := probe.StartLine + 1
			probe.NextOffset = &next
		}
		if readEnvelopeLen(probe, tool, startedAt) <= maxReadEnvelopeBytes {
			best = candidate
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return best
}

func readMediaResult(file *os.File, size int64, mime string, displayPath string, requestedPageRange string) (ReadResult, *protocol.ToolError) {
	if size == 0 {
		return ReadResult{}, &protocol.ToolError{Kind: "unsupported_format", Message: "empty media file", Path: displayPath}
	}
	if mime == "application/pdf" {
		return readPDFMediaResult(file, size, displayPath, requestedPageRange)
	}
	if size > maxReadMediaBytes {
		return ReadResult{}, &protocol.ToolError{Kind: "too_large", Message: "media attachment exceeds 10 MiB limit", Path: displayPath}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ReadResult{}, unreadablePathToolError(displayPath)
	}
	body, err := io.ReadAll(io.LimitReader(file, size+1))
	if err != nil {
		return ReadResult{}, unreadablePathToolError(displayPath)
	}
	if int64(len(body)) != size {
		return ReadResult{}, &protocol.ToolError{Kind: "not_found", Message: "path changed while reading", Path: displayPath}
	}
	return ReadResult{Media: &ReadMediaResult{
		MIME:       mime,
		SizeBytes:  size,
		DataBase64: base64.StdEncoding.EncodeToString(body),
	}}, nil
}

func readPDFMediaResult(file *os.File, size int64, displayPath string, requestedPageRange string) (ReadResult, *protocol.ToolError) {
	start, end, toolErr := parsePDFPageRange(requestedPageRange)
	if toolErr != nil {
		toolErr.Path = displayPath
		return ReadResult{}, toolErr
	}
	if size > pdfSourceReadMaxBytes {
		return ReadResult{}, &protocol.ToolError{Kind: "too_large", Message: "PDF source exceeds 32 MiB limit", Path: displayPath}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ReadResult{}, unreadablePathToolError(displayPath)
	}
	source, err := io.ReadAll(io.LimitReader(file, size+1))
	if err != nil {
		return ReadResult{}, unreadablePathToolError(displayPath)
	}
	if int64(len(source)) != size {
		return ReadResult{}, &protocol.ToolError{Kind: "not_found", Message: "path changed while reading", Path: displayPath}
	}
	trimmed, actualStart, actualEnd, err := media.ExtractPDF(source, start, end, maxReadMediaBytes)
	if err != nil {
		switch {
		case errors.Is(err, media.ErrPDFOutputTooLarge):
			return ReadResult{}, &protocol.ToolError{Kind: "too_large", Message: "trimmed PDF exceeds 10 MiB limit", Path: displayPath}
		case errors.Is(err, media.ErrPDFPageOutOfRange):
			return ReadResult{}, &protocol.ToolError{Kind: protocol.ErrorKindPDFPageOutOfRange, Message: "PDF page range is beyond the document", Path: displayPath}
		default:
			var pdfErr *media.PDFError
			if errors.As(err, &pdfErr) {
				message := "PDF is corrupt"
				if pdfErr.Kind == media.PDFErrorEncrypted {
					message = "PDF is encrypted or password-protected"
				}
				return ReadResult{}, &protocol.ToolError{Kind: string(pdfErr.Kind), Message: message, Path: displayPath}
			}
			return ReadResult{}, &protocol.ToolError{Kind: protocol.ErrorKindPDFCorrupt, Message: "PDF is corrupt", Path: displayPath}
		}
	}
	actualRange := strconv.Itoa(actualStart)
	if actualEnd != actualStart {
		actualRange += "-" + strconv.Itoa(actualEnd)
	}
	return ReadResult{Media: &ReadMediaResult{
		MIME:       "application/pdf",
		SizeBytes:  int64(len(trimmed)),
		DataBase64: base64.StdEncoding.EncodeToString(trimmed),
		PageRange:  actualRange,
	}}, nil
}

// parsePDFPageRange decides the requested range, and readPDFMediaResult applies
// it, per this matrix: an absent range defaults to 1-5; a malformed range or one
// spanning more than five pages is invalid_input. When extracted, a default or
// partial-overlap range clamps to the document, while an explicit range
// entirely beyond the document is pdf_page_out_of_range. The emitted page_range
// metadata reports the range actually extracted, not the range requested.
func parsePDFPageRange(value string) (int, int, *protocol.ToolError) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 1, 5, nil
	}
	parts := strings.Split(value, "-")
	if len(parts) > 2 {
		return 0, 0, invalidInput("page_range must name one contiguous range of at most five pages")
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil || start < 1 {
		return 0, 0, invalidInput("page_range must name one contiguous range of at most five pages")
	}
	end := start
	if len(parts) == 2 {
		end, err = strconv.Atoi(parts[1])
		if err != nil || end < start {
			return 0, 0, invalidInput("page_range must name one contiguous range of at most five pages")
		}
	}
	if end-start+1 > 5 {
		return 0, 0, invalidInput("page_range must name one contiguous range of at most five pages")
	}
	return start, end, nil
}

func readSniffBytes(file *os.File) ([]byte, error) {
	buffer := make([]byte, binarySniffBytes)
	n, err := file.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buffer[:n], nil
}

func sniffBinaryBytes(buffer []byte) bool {
	if len(buffer) == 0 {
		return false
	}
	if !utf8.Valid(buffer) {
		return true
	}
	nonPrintable := 0
	for _, b := range buffer {
		if b == 0 {
			return true
		}
		if b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		if b < 0x20 || b == 0x7f {
			nonPrintable++
		}
	}
	return nonPrintable*100 > len(buffer)*30
}

func sniffReadMediaMIME(header []byte) string {
	switch {
	case len(header) >= 8 && bytes.Equal(header[:8], []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}):
		return "image/png"
	case len(header) >= 3 && bytes.Equal(header[:3], []byte{0xff, 0xd8, 0xff}):
		return "image/jpeg"
	case len(header) >= 6 && (bytes.Equal(header[:6], []byte("GIF87a")) || bytes.Equal(header[:6], []byte("GIF89a"))):
		return "image/gif"
	case len(header) >= 12 && bytes.Equal(header[:4], []byte("RIFF")) && bytes.Equal(header[8:12], []byte("WEBP")):
		return "image/webp"
	case len(header) >= 5 && bytes.Equal(header[:5], []byte("%PDF-")):
		return "application/pdf"
	default:
		return ""
	}
}

func hasMore(reader *bufio.Reader) bool {
	_, err := reader.Peek(1)
	return err == nil
}

func invalidInput(message string) *protocol.ToolError {
	return &protocol.ToolError{Kind: "invalid_input", Message: message}
}

func unreadablePathToolError(displayPath string) *protocol.ToolError {
	return &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: displayPath}
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
