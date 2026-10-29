package httpapi

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
)

func TestRejectUnstorableJSONText(t *testing.T) {
	for _, test := range []struct {
		name   string
		body   string
		reject bool
	}{
		{"string value", `{"text":"a\u0000b"}`, true},
		{"object key", `{"metadata":{"k\u0000":"v"}}`, true},
		{"nested array element", `{"events":[{"ids":["ok","x\u0000"]}]}`, true},
		{"after a number beyond float64", `{"n":1e999,"text":"a\u0000b"}`, true},
		{"escaped backslash before u0000", `{"text":"a\\u0000b"}`, false},
		{"plain text", `{"text":"hello"}`, false},
		{"malformed before U+0000", `{"text" "a\u0000b"}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := rejectUnstorableJSONText([]byte(test.body))
			if !test.reject {
				if err != nil {
					t.Fatalf("rejectUnstorableJSONText(%s) = %v; want nil", test.body, err)
				}
				return
			}
			var validation *ValidationError
			if !errors.As(err, &validation) || validation.Message != "request body must not contain U+0000" {
				t.Fatalf("rejectUnstorableJSONText(%s) = %v; want U+0000 ValidationError", test.body, err)
			}
		})
	}
}

func TestReadJSONBodyAnswersAnUnreadableBodyAsInvalid(t *testing.T) {
	request := httptest.NewRequest("POST", "/v1/instances", iotest.ErrReader(errors.New("unexpected EOF in chunked body")))
	_, err := ReadJSONBody(httptest.NewRecorder(), request)
	var validation *ValidationError
	if !errors.As(err, &validation) || validation.Message != "request body could not be read" {
		t.Fatalf("ReadJSONBody of an unreadable body = %v; want a ValidationError", err)
	}
	request = httptest.NewRequest("POST", "/v1/instances", strings.NewReader(strings.Repeat(" ", JSONBodyLimit+1)))
	var tooLarge *RequestTooLargeError
	if _, err := ReadJSONBody(httptest.NewRecorder(), request); !errors.As(err, &tooLarge) {
		t.Fatalf("ReadJSONBody of a body over the limit = %v; want a RequestTooLargeError", err)
	}
}
