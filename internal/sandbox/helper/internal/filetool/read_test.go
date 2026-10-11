package filetool

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestReadReturnsBoundedLineWindow(t *testing.T) {
	fs := newReadFixture(t)
	fs.write("notes/a.txt", "one\ntwo\nthree\n")
	result, toolErr, err := Read(fs.payload(map[string]any{
		"path":   "notes/a.txt",
		"offset": 2,
		"limit":  1,
	}))
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr != nil {
		t.Fatalf("Read error = %+v", toolErr)
	}
	if result.Content != "two\n" || result.StartLine != 2 || result.ReturnedLines != 1 || result.NextOffset == nil || *result.NextOffset != 3 || !result.Truncated {
		t.Fatalf("result = %+v; want one-line truncated window", result)
	}
	if result.TotalLines != nil {
		t.Fatalf("total_lines = %v; want null before EOF", *result.TotalLines)
	}
}

func TestReadOffsetBeyondEOFReturnsTotalLines(t *testing.T) {
	fs := newReadFixture(t)
	fs.write("notes/a.txt", "one\ntwo\n")
	result, toolErr, err := Read(fs.payload(map[string]any{
		"path":   "notes/a.txt",
		"offset": 5,
	}))
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr != nil {
		t.Fatalf("Read error = %+v", toolErr)
	}
	if result.Content != "" || result.ReturnedLines != 0 || result.TotalLines == nil || *result.TotalLines != 2 || result.NextOffset != nil {
		t.Fatalf("result = %+v; want empty EOF window with total lines", result)
	}
}

func TestReadExactLimitAtEOFReturnsTotalLines(t *testing.T) {
	fs := newReadFixture(t)
	fs.write("notes/a.txt", "one\ntwo\n")
	result, toolErr, err := Read(fs.payload(map[string]any{
		"path":  "notes/a.txt",
		"limit": 2,
	}))
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr != nil {
		t.Fatalf("Read error = %+v", toolErr)
	}
	if result.Content != "one\ntwo\n" || result.ReturnedLines != 2 || result.TotalLines == nil || *result.TotalLines != 2 || result.NextOffset != nil || result.Truncated {
		t.Fatalf("result = %+v; want exact EOF window with total lines", result)
	}
}

func TestReadRejectsBinaryAndDirectory(t *testing.T) {
	fs := newReadFixture(t)
	fs.writeBytes("binary.dat", []byte{0x00, 0x01, 0x02})
	_, toolErr, err := Read(fs.payload(map[string]any{"path": "binary.dat"}))
	if err != nil {
		t.Fatalf("binary Read internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "binary_file" {
		t.Fatalf("binary Read error = %+v; want binary_file", toolErr)
	}
	_, toolErr, err = Read(fs.payload(map[string]any{"path": "."}))
	if err != nil {
		t.Fatalf("directory Read internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "is_directory" {
		t.Fatalf("directory Read error = %+v; want is_directory", toolErr)
	}
}

func TestReadRejectsMalformedUTF8AsBinary(t *testing.T) {
	fs := newReadFixture(t)
	for _, fixture := range [][]byte{
		{0xff, 'a'},
		{'a', 0xc3, 0x28},
		{0xe2, 0x82},
	} {
		fs.writeBytes("malformed.txt", fixture)
		_, toolErr, err := Read(fs.payload(map[string]any{"path": "malformed.txt"}))
		if err != nil {
			t.Fatalf("malformed UTF-8 Read internal error = %v", err)
		}
		if toolErr == nil || toolErr.Kind != "binary_file" {
			t.Fatalf("malformed UTF-8 %x error = %+v; want binary_file", fixture, toolErr)
		}
	}
}

func TestReadReturnsMediaPayloadForSupportedImages(t *testing.T) {
	fs := newReadFixture(t)
	cases := []struct {
		name string
		path string
		body []byte
		mime string
	}{
		{name: "png", path: "plot.png", body: append([]byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, []byte("png-body")...), mime: "image/png"},
		{name: "jpeg", path: "photo.jpg", body: append([]byte{0xff, 0xd8, 0xff}, []byte("jpeg-body")...), mime: "image/jpeg"},
		{name: "gif87a", path: "anim87.gif", body: append([]byte("GIF87a"), []byte("gif-body")...), mime: "image/gif"},
		{name: "gif89a", path: "anim89.gif", body: append([]byte("GIF89a"), []byte("gif-body")...), mime: "image/gif"},
		{name: "webp", path: "frame.webp", body: append([]byte("RIFFxxxxWEBP"), []byte("webp-body")...), mime: "image/webp"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs.writeBytes(tc.path, tc.body)
			result, toolErr, err := Read(fs.payload(map[string]any{"path": tc.path}))
			if err != nil {
				t.Fatalf("Read media internal error = %v", err)
			}
			if toolErr != nil {
				t.Fatalf("Read media error = %+v", toolErr)
			}
			if result.Media == nil || result.Media.MIME != tc.mime || result.Media.SizeBytes != int64(len(tc.body)) ||
				result.Media.DataBase64 != base64.StdEncoding.EncodeToString(tc.body) {
				t.Fatalf("media result = %+v; want exact %s attachment payload", result.Media, tc.mime)
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("marshal media result: %v", err)
			}
			if strings.Contains(string(encoded), "content") || !strings.Contains(string(encoded), `"data_base64"`) {
				t.Fatalf("media JSON = %s; want media-only transport shape", encoded)
			}
		})
	}
}

func TestReadExtractsPDFPageRanges(t *testing.T) {
	fs := newReadFixture(t)
	fs.writeBytes("report.pdf", minimalTestPDF(3))
	for _, tc := range []struct {
		name      string
		pageRange string
		wantRange string
	}{
		{name: "default clamps", wantRange: "1-3"},
		{name: "partial overlap clamps", pageRange: "2-6", wantRange: "2-3"},
		{name: "single page", pageRange: "2", wantRange: "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := map[string]any{"path": "report.pdf"}
			if tc.pageRange != "" {
				input["page_range"] = tc.pageRange
			}
			result, toolErr, err := Read(fs.payload(input))
			if err != nil || toolErr != nil {
				t.Fatalf("Read PDF err/toolErr = %v / %+v", err, toolErr)
			}
			if result.Media == nil || result.Media.MIME != "application/pdf" || result.Media.PageRange != tc.wantRange {
				t.Fatalf("PDF media = %+v; want actual range %s", result.Media, tc.wantRange)
			}
			trimmed, err := base64.StdEncoding.DecodeString(result.Media.DataBase64)
			if err != nil {
				t.Fatalf("decode trimmed PDF: %v", err)
			}
			count, err := api.PageCount(bytes.NewReader(trimmed), model.NewDefaultConfiguration())
			if err != nil {
				t.Fatalf("parse trimmed PDF: %v", err)
			}
			wantPages := 1
			if tc.wantRange == "1-3" || tc.wantRange == "2-3" {
				wantPages = 3
				if tc.wantRange == "2-3" {
					wantPages = 2
				}
			}
			if count != wantPages {
				t.Fatalf("trimmed page count = %d; want %d", count, wantPages)
			}
		})
	}
}

func TestReadPDFErrorsAreModelVisible(t *testing.T) {
	fs := newReadFixture(t)
	valid := minimalTestPDF(2)
	fs.writeBytes("valid.pdf", valid)
	var encrypted bytes.Buffer
	conf := model.NewAESConfiguration("user", "owner", 256)
	if err := api.Encrypt(bytes.NewReader(valid), &encrypted, conf); err != nil {
		t.Fatalf("encrypt PDF fixture: %v", err)
	}
	fs.writeBytes("encrypted.pdf", encrypted.Bytes())
	fs.writeBytes("corrupt.pdf", []byte("%PDF-1.7\nnot-a-document"))

	for _, tc := range []struct {
		name  string
		path  string
		input map[string]any
		kind  string
	}{
		{name: "out of range", path: "valid.pdf", input: map[string]any{"page_range": "4-5"}, kind: "pdf_page_out_of_range"},
		{name: "corrupt", path: "corrupt.pdf", input: map[string]any{}, kind: "pdf_corrupt"},
		{name: "encrypted", path: "encrypted.pdf", input: map[string]any{}, kind: "pdf_encrypted"},
		{name: "malformed range", path: "valid.pdf", input: map[string]any{"page_range": "2-9"}, kind: "invalid_input"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.input["path"] = tc.path
			_, toolErr, err := Read(fs.payload(tc.input))
			if err != nil {
				t.Fatalf("Read PDF internal error = %v", err)
			}
			if toolErr == nil || toolErr.Kind != tc.kind {
				t.Fatalf("Read PDF tool error = %+v; want %s", toolErr, tc.kind)
			}
		})
	}
}

func TestReadRejectsOversizedPDFSourceBeforeExtraction(t *testing.T) {
	fs := newReadFixture(t)
	path := filepath.Join(fs.workspace, "huge.pdf")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create oversized PDF fixture: %v", err)
	}
	if _, err := file.Write([]byte("%PDF-1.7\n")); err != nil {
		_ = file.Close()
		t.Fatalf("write PDF magic: %v", err)
	}
	if err := file.Truncate(pdfSourceReadMaxBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate PDF fixture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close PDF fixture: %v", err)
	}
	_, toolErr, err := Read(fs.payload(map[string]any{"path": "huge.pdf"}))
	if err != nil {
		t.Fatalf("Read oversized PDF internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "too_large" || !strings.Contains(toolErr.Message, "source") {
		t.Fatalf("oversized PDF error = %+v; want source too_large", toolErr)
	}
}

func minimalTestPDF(pageCount int) []byte {
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
	}
	kids := make([]string, 0, pageCount)
	for page := 0; page < pageCount; page++ {
		kids = append(kids, fmt.Sprintf("%d 0 R", page+3))
	}
	objects = append(objects, fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(kids, " "), pageCount))
	for page := 0; page < pageCount; page++ {
		objects = append(objects, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 72 72] >>")
	}
	var out bytes.Buffer
	out.WriteString("%PDF-1.7\n")
	offsets := make([]int, len(objects)+1)
	for index, object := range objects {
		offsets[index+1] = out.Len()
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", index+1, object)
	}
	xref := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for index := 1; index < len(offsets); index++ {
		fmt.Fprintf(&out, "%010d 00000 n \n", offsets[index])
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return out.Bytes()
}

func TestReadRejectsOversizedMediaBeforeBodyRead(t *testing.T) {
	fs := newReadFixture(t)
	path := filepath.Join(fs.workspace, "huge.jpg")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create oversized media fixture: %v", err)
	}
	if _, err := file.Write([]byte{0xff, 0xd8, 0xff}); err != nil {
		_ = file.Close()
		t.Fatalf("write media magic: %v", err)
	}
	if err := file.Truncate(maxReadMediaBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate media fixture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close media fixture: %v", err)
	}
	_, toolErr, err := Read(fs.payload(map[string]any{"path": "huge.jpg"}))
	if err != nil {
		t.Fatalf("Read oversized media internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "too_large" {
		t.Fatalf("oversized media error = %+v; want too_large", toolErr)
	}
}

func TestReadRejectsFilePathAliasWithoutPath(t *testing.T) {
	fs := newReadFixture(t)
	fs.write("notes/a.txt", "one\n")
	_, toolErr, err := Read(fs.payload(map[string]any{"file_path": "notes/a.txt"}))
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "invalid_input" || toolErr.Message != "path is required" {
		t.Fatalf("Read error = %+v; want missing path invalid_input", toolErr)
	}
}

func TestReadRejectsFIFOWithoutOpening(t *testing.T) {
	fs := newReadFixture(t)
	if err := syscall.Mkfifo(filepath.Join(fs.workspace, "pipe"), 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	_, toolErr, err := Read(fs.payload(map[string]any{"path": "pipe"}))
	if err != nil {
		t.Fatalf("fifo Read internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "invalid_input" {
		t.Fatalf("fifo Read error = %+v; want invalid_input", toolErr)
	}
}

func TestReadTruncatesOverlongLineAndContinuesAfterDrainingRemainder(t *testing.T) {
	fs := newReadFixture(t)
	fs.write("long.txt", strings.Repeat("a", maxReadLineRunes+10)+"\nshort\n")
	result, toolErr, err := Read(fs.payload(map[string]any{"path": "long.txt"}))
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr != nil {
		t.Fatalf("Read error = %+v", toolErr)
	}
	if result.LineTruncations != 1 || !result.Truncated || result.ReturnedLines != 2 || result.TotalLines == nil || *result.TotalLines != 2 || result.NextOffset != nil {
		t.Fatalf("result = %+v; want truncated first line plus following line through EOF", result)
	}
	wantContent := strings.Repeat("a", maxReadLineRunes) + lineTruncatedSuffix + "\nshort\n"
	if result.Content != wantContent {
		t.Fatalf("content = %q; want truncated first line and short second line", result.Content)
	}
}

func TestReadWindowDrainsOverlongSingleLineToEOF(t *testing.T) {
	line := strings.Repeat("a", maxReadLineRunes+10)
	result, err := readWindow(strings.NewReader(line), 1, defaultReadLines, readByteBudget, int64(len(line)))
	if err != nil {
		t.Fatalf("readWindow err = %v", err)
	}
	if result.LineTruncations != 1 || !result.Truncated || result.ReturnedLines != 1 || result.TotalLines == nil || *result.TotalLines != 1 || result.NextOffset != nil {
		t.Fatalf("result = %+v; want drained truncated single-line EOF", result)
	}
}

func TestReadRejectsSymlinkEscape(t *testing.T) {
	fs := newReadFixture(t)
	outside := t.TempDir()
	fs.writeExternalSymlink("escape.txt", outside, "secret.txt", "secret")
	_, toolErr, err := Read(fs.payload(map[string]any{"path": "escape.txt"}))
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("Read error = %+v; want path_escape", toolErr)
	}
}

func TestReadRejectsSymlinkEscapeToMissingOutsideTarget(t *testing.T) {
	fs := newReadFixture(t)
	outside := t.TempDir()
	if err := os.Symlink(filepath.Join(outside, "missing.txt"), filepath.Join(fs.workspace, "escape-missing.txt")); err != nil {
		t.Fatalf("symlink fixture: %v", err)
	}
	_, toolErr, err := Read(fs.payload(map[string]any{"path": "escape-missing.txt"}))
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("Read symlink escape error = %+v; want path_escape", toolErr)
	}
}

func TestReadReportsDanglingSymlinkTargetAsNotFound(t *testing.T) {
	fs := newReadFixture(t)
	if err := os.Symlink(filepath.Join(fs.workspace, "missing.txt"), filepath.Join(fs.workspace, "dangling.txt")); err != nil {
		t.Fatalf("dangling symlink fixture: %v", err)
	}
	_, toolErr, err := Read(fs.payload(map[string]any{"path": "dangling.txt"}))
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr == nil || toolErr.Kind != "not_found" || toolErr.Path != "missing.txt" {
		t.Fatalf("Read dangling symlink error = %+v; want not_found at resolved target", toolErr)
	}
}

func TestReadPropagatesBackendIOFailureFromPathResolution(t *testing.T) {
	oldResolve := resolveReadPath
	resolveReadPath = func(protocol.Payload, string) (pathsafe.Resolved, error) {
		return pathsafe.Resolved{}, &pathsafe.Error{Kind: pathsafe.KindInvalidInput, Message: "input/output error", Err: syscall.EIO}
	}
	t.Cleanup(func() { resolveReadPath = oldResolve })

	fs := newReadFixture(t)
	_, toolErr, err := Read(fs.payload(map[string]any{"path": "mounted.txt"}))
	if toolErr != nil {
		t.Fatalf("Read tool error = %+v; want internal backend I/O failure", toolErr)
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Read err = %v; want EIO propagation", err)
	}
}

func TestReadHonorsLowerPayloadLimits(t *testing.T) {
	fs := newReadFixture(t)
	fs.write("notes/a.txt", "one-very-long\ntwo\nthree\n")
	payload := fs.payload(map[string]any{"path": "notes/a.txt"})
	payload.Limits = protocol.Limits{VisibleBytes: 4, VisibleLines: 2}
	result, toolErr, err := Read(payload)
	if err != nil {
		t.Fatalf("Read internal error = %v", err)
	}
	if toolErr != nil {
		t.Fatalf("Read error = %+v", toolErr)
	}
	if result.Content != "one-" || result.ReturnedLines != 1 || result.NextOffset == nil || *result.NextOffset != 2 || !result.Truncated {
		t.Fatalf("result = %+v; want payload-limited first line with continuation at next line", result)
	}

	payload = fs.payload(map[string]any{"path": "notes/a.txt", "offset": 2})
	next, toolErr, err := Read(payload)
	if err != nil {
		t.Fatalf("Read offset 2 internal error = %v", err)
	}
	if toolErr != nil {
		t.Fatalf("Read offset 2 error = %+v", toolErr)
	}
	if !strings.HasPrefix(next.Content, "two\n") {
		t.Fatalf("offset 2 content = %q; want second line, not remainder of first", next.Content)
	}
}

func TestReadPagesWholeLinesWithinEnvelopeCap(t *testing.T) {
	startedAt := time.Unix(1_700_000_000, 0).UTC()
	widestWallTimeMS := time.Duration(math.MinInt64).Milliseconds()
	threeLinesThenEuros := strings.Repeat(strings.Repeat("x", 39)+"\n", 3) + "€€€\n"
	var mixed, crlf, unterminated strings.Builder
	for line := 1; line <= 20; line++ {
		fmt.Fprintf(&mixed, "%02d €😀 \"q\" \\ \t <a&b> \u2028\n", line)
		fmt.Fprintf(&crlf, "row-%02d\r\n", line)
	}
	for line := 1; line <= 10; line++ {
		fmt.Fprintf(&unterminated, "line %02d text\n", line)
	}
	unterminated.WriteString("tail without newline")
	for _, tc := range []struct {
		name   string
		body   string
		offset int
		budget int
		// window, when set, is handed to the fit instead of readWindow's result.
		window    *ReadResult
		cap       int
		wantLines int
	}{
		{
			name:   "fit never skips a line the window counted without showing",
			body:   threeLinesThenEuros,
			window: &ReadResult{Content: threeLinesThenEuros[:120], StartLine: 1, ReturnedLines: 4, NextOffset: intPointer(5), Truncated: true},
			cap:    400, wantLines: 2,
		},
		{name: "window never counts a budget-hit line it holds no byte of", body: "ab\n€€€\nlast\n", budget: 4, cap: 2_000, wantLines: 1},
		{name: "line_truncations digit rollover stays within the cap", body: strings.Repeat(strings.Repeat("a", maxReadLineRunes+1)+"\n", 9), budget: readByteBudget, cap: 2_000, wantLines: 1},
		{name: "wall_time_ms digit rollover stays within the cap", body: strings.Repeat("a", 1_000) + "\nb\n", budget: readByteBudget, cap: 600, wantLines: 1},
		{name: "multibyte and escape-dense lines", body: mixed.String(), budget: readByteBudget, cap: 1_000, wantLines: 13},
		{name: "CRLF lines from a later start line", body: crlf.String(), offset: 3, budget: readByteBudget, cap: 400, wantLines: 10},
		{name: "empty lines", body: strings.Repeat("\n", 300), budget: readByteBudget, cap: 600, wantLines: 151},
		{name: "last line without newline", body: unterminated.String(), budget: readByteBudget, cap: 450, wantLines: 10},
		{name: "end-of-file window two bytes over the cap keeps a line for the next page", body: unterminated.String() + "\n", budget: readByteBudget, cap: 457, wantLines: 10},
		{name: "window stops before a cut line once it holds a whole line", body: "ab\none-very-long\nz\n", budget: 8, cap: 2_000, wantLines: 1},
		{name: "first line contributing no byte still advances", body: "€x\nb\n", budget: 2, cap: 2_000, wantLines: 1},
		{name: "escape-dense line above the cap keeps its longest prefix", body: strings.Repeat(`"\`, 400) + "\nnext\n", budget: readByteBudget, cap: 600, wantLines: 1},
		{name: "production cap with a window ending before a multibyte rune", body: strings.Repeat(strings.Repeat("€", 90)+"\n", 1_990), budget: readByteBudget, cap: maxReadEnvelopeBytes, wantLines: 734},
	} {
		t.Run(tc.name, func(t *testing.T) {
			offset := max(tc.offset, 1)
			lines := strings.SplitAfter(tc.body, "\n")
			if lines[len(lines)-1] == "" {
				lines = lines[:len(lines)-1]
			}
			raw := ReadResult{}
			if tc.window != nil {
				raw = *tc.window
				raw.TotalBytesFile = int64(len(tc.body))
			} else {
				var err error
				raw, err = readWindow(strings.NewReader(tc.body), offset, maxReadLines, tc.budget, int64(len(tc.body)))
				if err != nil {
					t.Fatalf("readWindow: %v", err)
				}
				if len(raw.Content) > tc.budget {
					t.Fatalf("window content = %d bytes; want <= budget %d", len(raw.Content), tc.budget)
				}
			}
			envelopeLen := func(result ReadResult) int {
				envelope, err := protocol.NewToolResultEnvelope("read", result, result.Truncated, startedAt)
				if err != nil {
					t.Fatalf("build envelope: %v", err)
				}
				envelope.Metrics = &protocol.Metrics{WallTimeMS: widestWallTimeMS}
				encoded, err := json.Marshal(envelope)
				if err != nil {
					t.Fatalf("marshal envelope: %v", err)
				}
				return len(encoded)
			}
			wholeLines := func(k int) ReadResult {
				var content strings.Builder
				for _, line := range lines[offset-1 : offset-1+k] {
					content.WriteString(shownLine(line))
				}
				page := raw
				page.Content = content.String()
				page.ReturnedLines = k
				page.NextOffset = intPointer(offset + k)
				page.TotalLines = nil
				page.Truncated = true
				return page
			}

			page := fitReadResult(raw, "read", startedAt, tc.cap)

			if got := envelopeLen(page); got > tc.cap {
				t.Fatalf("envelope with the widest wall_time_ms = %d bytes; want <= %d", got, tc.cap)
			}
			rawFits := envelopeLen(raw) <= tc.cap
			if rawFits != reflect.DeepEqual(page, raw) {
				t.Fatalf("window fits = %v, page = %s; want the window kept exactly when it fits", rawFits, describePage(page))
			}
			if !rawFits {
				maxLines := min(strings.Count(raw.Content, "\n"), raw.ReturnedLines-1)
				if page.LineTruncations == raw.LineTruncations {
					k := page.ReturnedLines
					if k < 1 || k > maxLines || !reflect.DeepEqual(page, wholeLines(k)) {
						t.Fatalf("page = %s; want the first k whole lines, 1 <= k <= %d", describePage(page), maxLines)
					}
					if k < maxLines && envelopeLen(wholeLines(k+1)) <= tc.cap {
						t.Fatalf("page holds %d lines; %d whole lines also fit", k, k+1)
					}
				} else {
					if maxLines >= 1 && envelopeLen(wholeLines(1)) <= tc.cap {
						t.Fatal("page cut a first line that fits whole")
					}
					if page.LineTruncations != raw.LineTruncations+1 {
						t.Fatalf("line_truncations = %d; want %d", page.LineTruncations, raw.LineTruncations+1)
					}
					first, _, _ := strings.Cut(raw.Content, "\n")
					prefix, cut := strings.CutSuffix(page.Content, lineTruncatedSuffix)
					if !cut || !strings.HasPrefix(first, prefix) {
						t.Fatalf("content = %q; want a prefix of the first line and %q", page.Content, lineTruncatedSuffix)
					}
					if prefix != first {
						next, _ := utf8.DecodeRuneInString(first[len(prefix):])
						longer := page
						longer.Content = prefix + string(next) + lineTruncatedSuffix
						if envelopeLen(longer) <= tc.cap {
							t.Fatalf("cut first line keeps %d bytes; one more rune also fits", len(prefix))
						}
					}
				}
			}

			from := page.StartLine - 1
			whole := wholeLines(page.ReturnedLines).Content
			switch {
			case page.Content == whole && page.NextOffset == nil:
				if page.TotalLines == nil || *page.TotalLines != len(lines) || from+page.ReturnedLines != len(lines) {
					t.Fatalf("end-of-file page = %s; want every line through %d", describePage(page), len(lines))
				}
			case page.Content == whole:
				if *page.NextOffset != page.StartLine+page.ReturnedLines || !page.Truncated || page.TotalLines != nil {
					t.Fatalf("page = %s; want next_offset at the first line not shown", describePage(page))
				}
			default:
				first := shownLine(lines[from])
				prefix := strings.TrimSuffix(page.Content, lineTruncatedSuffix)
				if page.ReturnedLines != 1 || !strings.HasPrefix(first, prefix) || page.NextOffset == nil || *page.NextOffset != page.StartLine+1 || !page.Truncated || page.TotalLines != nil {
					t.Fatalf("page = %s; want whole lines, or one cut first line and next_offset after it", describePage(page))
				}
				if len(first) <= tc.budget && envelopeLen(wholeLines(1)) <= tc.cap {
					t.Fatalf("page cut a first line of %d bytes that fits whole", len(first))
				}
			}
			if page.ReturnedLines != tc.wantLines {
				t.Fatalf("returned_lines = %d; want %d", page.ReturnedLines, tc.wantLines)
			}
		})
	}
}

// shownLine is a source line as a window shows it, cut at maxReadLineRunes.
func shownLine(line string) string {
	text, newline := strings.CutSuffix(line, "\n")
	runes := []rune(text)
	if len(runes) <= maxReadLineRunes {
		return line
	}
	shown := string(runes[:maxReadLineRunes]) + lineTruncatedSuffix
	if newline {
		shown += "\n"
	}
	return shown
}

func intPointer(value int) *int {
	return &value
}

func describePage(page ReadResult) string {
	optional := func(value *int) string {
		if value == nil {
			return "null"
		}
		return strconv.Itoa(*value)
	}
	return fmt.Sprintf("{content %d bytes ending %q, returned_lines %d, next_offset %s, total_lines %s, truncated %v, line_truncations %d}",
		len(page.Content), page.Content[max(len(page.Content)-24, 0):], page.ReturnedLines, optional(page.NextOffset), optional(page.TotalLines), page.Truncated, page.LineTruncations)
}

func TestReadWindowPropagatesUnexpectedIOError(t *testing.T) {
	_, err := readWindow(eioReader{}, 1, 10, 1024, 1024)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("readWindow err = %v; want EIO propagation", err)
	}
}

type eioReader struct{}

func (eioReader) Read([]byte) (int, error) {
	return 0, syscall.EIO
}
