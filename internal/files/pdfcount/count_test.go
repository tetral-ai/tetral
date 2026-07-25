package pdfcount_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"

	"github.com/tetral-ai/tetral/internal/files/pdfcount"
)

func TestCountReturnsPageCountWithoutReadingWholeFileIntoMemory(t *testing.T) {
	pdf := minimalPDF(2)
	reader := &recordingReadSeeker{ReadSeeker: bytes.NewReader(pdf)}

	got, err := pdfcount.Count(reader)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 2 {
		t.Fatalf("Count = %d; want 2", got)
	}
	if reader.seekCalls == 0 {
		t.Fatal("Count did not use the io.ReadSeeker contract")
	}
}

func TestCountRejectsUnreadablePDF(t *testing.T) {
	if _, err := pdfcount.Count(bytes.NewReader([]byte("not a pdf"))); err == nil {
		t.Fatal("Count accepted unreadable PDF bytes")
	}
}

func TestCountRejectsEncryptedPDFEvenWhenTheEmptyUserPasswordOpensIt(t *testing.T) {
	config := model.NewDefaultConfiguration()
	config.UserPW = ""
	config.OwnerPW = "owner-password"
	var encrypted bytes.Buffer
	if err := api.Encrypt(bytes.NewReader(minimalPDF(1)), &encrypted, config); err != nil {
		t.Fatalf("encrypt fixture: %v", err)
	}
	if _, err := pdfcount.Count(bytes.NewReader(encrypted.Bytes())); err == nil {
		t.Fatal("Count accepted an encrypted PDF")
	}
}

func TestCountAcceptsReadableZeroPagePDF(t *testing.T) {
	got, err := pdfcount.Count(bytes.NewReader(minimalPDF(0)))
	if err != nil {
		t.Fatalf("Count zero-page PDF: %v", err)
	}
	if got != 0 {
		t.Fatalf("Count zero-page PDF = %d; want 0", got)
	}
}

type recordingReadSeeker struct {
	io.ReadSeeker
	seekCalls int
}

func (r *recordingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	r.seekCalls++
	return r.ReadSeeker.Seek(offset, whence)
}

func minimalPDF(pageCount int) []byte {
	var body strings.Builder
	body.WriteString("%PDF-1.4\n")
	offsets := make([]int, 3+pageCount)
	writeObject := func(number int, value string) {
		offsets[number] = body.Len()
		fmt.Fprintf(&body, "%d 0 obj\n%s\nendobj\n", number, value)
	}
	writeObject(1, "<< /Type /Catalog /Pages 2 0 R >>")
	kids := make([]string, 0, pageCount)
	for index := 0; index < pageCount; index++ {
		kids = append(kids, fmt.Sprintf("%d 0 R", 3+index))
	}
	writeObject(2, fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(kids, " "), pageCount))
	for index := 0; index < pageCount; index++ {
		writeObject(3+index, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>")
	}
	xrefOffset := body.Len()
	fmt.Fprintf(&body, "xref\n0 %d\n", len(offsets))
	body.WriteString("0000000000 65535 f \n")
	for number := 1; number < len(offsets); number++ {
		fmt.Fprintf(&body, "%010d 00000 n \n", offsets[number])
	}
	fmt.Fprintf(&body, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets), xrefOffset)
	return []byte(body.String())
}
