package filetool

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

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

func TestReadWindowAndEnvelopeReserveRuntimeNumberingHeadroom(t *testing.T) {
	body := strings.Repeat(strings.Repeat("x", 149)+"\n", maxReadLines)
	result, err := readWindow(strings.NewReader(body), 1, maxReadLines, readByteBudget, int64(len(body)))
	if err != nil {
		t.Fatalf("readWindow: %v", err)
	}
	if len(result.Content) > 200_000 {
		t.Fatalf("raw read content bytes = %d; want at most 200000", len(result.Content))
	}

	startedAt := time.Unix(1_700_000_000, 0).UTC()
	fitted := FitReadResultForEnvelope(result, "read", startedAt)
	if got := readEnvelopeLen(fitted, "read", startedAt); got > 200_000 {
		t.Fatalf("fitted read envelope bytes = %d; want at most 200000", got)
	}
}

func TestReadEnvelopeFitsEscapeDenseContentAfterJSONEncoding(t *testing.T) {
	startedAt := time.Unix(1_700_000_000, 0).UTC()
	content := strings.Repeat(`"\`, 100_000)
	result := ReadResult{Content: content, StartLine: 1, ReturnedLines: 1, Truncated: false}
	fitted := FitReadResultForEnvelope(result, "read", startedAt)
	encodedBytes := readEnvelopeLen(fitted, "read", startedAt)
	if encodedBytes > maxReadEnvelopeBytes {
		t.Fatalf("escape-dense envelope bytes = %d; want <= %d", encodedBytes, maxReadEnvelopeBytes)
	}
	if encodedBytes < maxReadEnvelopeBytes-1024 {
		t.Fatalf("escape-dense envelope bytes = %d; want a fixture near the %d-byte producer bound", encodedBytes, maxReadEnvelopeBytes)
	}
	if !fitted.Truncated || !strings.Contains(fitted.Content, `"`) || !strings.Contains(fitted.Content, `\`) {
		t.Fatalf("fitted escape-dense result lost contract shape: truncated=%v content-bytes=%d", fitted.Truncated, len(fitted.Content))
	}
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
