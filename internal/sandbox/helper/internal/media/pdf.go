package media

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
)

var (
	ErrPDFOutputTooLarge = errors.New("trimmed PDF exceeds output limit")
	ErrPDFPageOutOfRange = errors.New("PDF page range is beyond the document")
)

type PDFErrorKind string

const (
	PDFErrorCorrupt   PDFErrorKind = "pdf_corrupt"
	PDFErrorEncrypted PDFErrorKind = "pdf_encrypted"
)

type PDFError struct {
	Kind PDFErrorKind
}

func (e *PDFError) Error() string { return string(e.Kind) }

func ExtractPDF(source []byte, start int, end int, maxOutputBytes int64) ([]byte, int, int, error) {
	conf := model.NewDefaultConfiguration()
	conf.Offline = true
	pageCount, err := api.PageCount(bytes.NewReader(source), conf)
	if err != nil {
		return nil, 0, 0, classifyPDFError(err)
	}
	if pageCount < 1 {
		return nil, 0, 0, &PDFError{Kind: PDFErrorCorrupt}
	}
	if start > pageCount {
		return nil, 0, 0, ErrPDFPageOutOfRange
	}
	if end > pageCount {
		end = pageCount
	}
	writer := &boundedBuffer{remaining: maxOutputBytes}
	selection := []string{fmt.Sprintf("%d-%d", start, end)}
	if start == end {
		selection[0] = fmt.Sprintf("%d", start)
	}
	if err := api.Trim(bytes.NewReader(source), writer, selection, conf); err != nil {
		if errors.Is(err, ErrPDFOutputTooLarge) {
			return nil, 0, 0, ErrPDFOutputTooLarge
		}
		return nil, 0, 0, classifyPDFError(err)
	}
	return writer.Bytes(), start, end, nil
}

func classifyPDFError(err error) error {
	if errors.Is(err, pdfcpu.ErrWrongPassword) {
		return &PDFError{Kind: PDFErrorEncrypted}
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "password") || strings.Contains(message, "encrypted") {
		return &PDFError{Kind: PDFErrorEncrypted}
	}
	return &PDFError{Kind: PDFErrorCorrupt}
}

type boundedBuffer struct {
	bytes.Buffer
	remaining int64
}

func (w *boundedBuffer) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		if w.remaining > 0 {
			n, _ := w.Buffer.Write(p[:w.remaining])
			w.remaining = 0
			return n, ErrPDFOutputTooLarge
		}
		return 0, ErrPDFOutputTooLarge
	}
	n, err := w.Buffer.Write(p)
	w.remaining -= int64(n)
	return n, err
}

var _ io.Writer = (*boundedBuffer)(nil)
