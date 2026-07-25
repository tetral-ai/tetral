// Package pdfcount owns bounded PDF page counting for the Files domain.
package pdfcount

import (
	"errors"
	"io"

	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
)

var ErrUnreadable = errors.New("PDF is not readable")

func Count(source io.ReadSeeker) (int64, error) {
	if source == nil {
		return 0, ErrUnreadable
	}
	config := model.NewDefaultConfiguration()
	config.Offline = true
	info, err := api.PDFInfo(source, "", nil, false, config)
	if err != nil || info.Encrypted || info.PageCount < 0 {
		return 0, ErrUnreadable
	}
	return int64(info.PageCount), nil
}
