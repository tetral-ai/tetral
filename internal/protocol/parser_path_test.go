package protocol

import (
	"errors"
	"strings"
	"testing"
)

func nestedReady(depth int) []byte {
	ready := Encode(Ready{ConnectionID: "conn", MessageID: "msg", OS: "linux", Arch: "x86_64", CLIVersion: "1.0.0", Capabilities: Capabilities{Drives: DriveCapability{Mount: true}}})
	return []byte(string(ready[:len(ready)-1]) + `,"unknown":` + strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth) + "}")
}

// The limit's own cases are protocol/nesting.cases.json; this holds the
// pointer of the first container past it.
func TestParserNestingLimitAndErrorPointers(t *testing.T) {
	_, err := DecodeReady(nestedReady(127))
	var failure *DecodeError
	if !errors.As(err, &failure) || failure.Reason != "JSON nested too deeply" || failure.Pointer != "/unknown"+strings.Repeat("/0", 126) {
		t.Fatalf("beyond nesting limit: %v", err)
	}
	for _, test := range []struct{ input, pointer, reason string }{
		{`{"a/b":[0,{"~key":NaN}]}`, "/a~1b/1/~0key", "invalid JSON token"},
		{`{"a/b":[{"~key":0,"\u007ekey":1}]}`, "/a~1b/0/~0key", "duplicate object key"},
		{`{"a/b":[{"~key" 0}]}`, "/a~1b/0/~0key", "expected ':' after an object key"},
		{`{"nested":[{}],"sibling":"\ud800"}`, "/sibling", "lone surrogate escape in a string"},
		{`{"nested":[{}] "sibling":0}`, "", "expected ',' or '}' in an object"},
		{`{"nested":[0 1]}`, "/nested", "expected ',' or ']' in an array"},
	} {
		t.Run(test.reason+test.pointer, func(t *testing.T) {
			_, err := parse([]byte(test.input))
			var failure *DecodeError
			if !errors.As(err, &failure) || failure.Pointer != test.pointer || failure.Reason != test.reason {
				t.Fatalf("parse = %v; want %s at %q", err, test.reason, test.pointer)
			}
		})
	}
}
