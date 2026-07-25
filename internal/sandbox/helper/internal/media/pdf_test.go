package media

import (
	"errors"
	"testing"
)

func TestBoundedPDFWriterStopsAtTrimmedOutputCap(t *testing.T) {
	writer := &boundedBuffer{remaining: 3}
	n, err := writer.Write([]byte("abcde"))
	if n != 3 || !errors.Is(err, ErrPDFOutputTooLarge) || writer.String() != "abc" {
		t.Fatalf("bounded writer = n:%d err:%v body:%q; want capped output", n, err, writer.String())
	}
}
