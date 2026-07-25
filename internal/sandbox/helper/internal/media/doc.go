// Package media owns view_image and the pdfcpu-backed PDF page extraction.
//
// OWNS:
//   - view_image: the model-visible image tool.
//   - ExtractPDF: trimming a source PDF to a bounded page slice.
//   - The image size cap and magic-sniff table (view_image.go).
//
// STATE MACHINE: none; each call is a single decode/extract.
//
// INVARIANTS:
//   - Turning a PDF into a trimmed page slice is the helper's job, not the
//     runtime's: Read (filetool) hands the raw source bytes here, and this
//     package returns the already-trimmed sub-PDF. The runtime only wraps the
//     result as an attachment; it never re-derives the page range.
//   - Format is decided by magic bytes, never by file extension.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/media/view_image.go
//   - internal/sandbox/helper/internal/media/pdf.go
//   - internal/sandbox/helper/internal/filetool/read.go (calls ExtractPDF)
package media
