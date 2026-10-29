package protocol

import (
	"errors"
	"runtime"
	"strings"
	"testing"
)

func nestedReady(depth int) []byte {
	ready := Encode(Ready{ConnectionID: "conn", MessageID: "msg", OS: "linux", Arch: "x86_64", CLIVersion: "1.0.0", Capabilities: Capabilities{Drives: DriveCapability{Mount: true}}})
	return []byte(string(ready[:len(ready)-1]) + `,"unknown":` + strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth) + "}")
}

func TestParserPathAllocationGrowth(t *testing.T) {
	// Inputs and warm-up precede runtime-wide counters. This test must not
	// run in parallel: the comparison measures bytes, not allocation count.
	small, large := nestedReady(1000), nestedReady(9999)
	for _, input := range [][]byte{small, large} {
		if _, err := DecodeReady(input); err != nil {
			t.Fatal(err)
		}
	}
	allocated := func(input []byte) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		const runs = 3
		for range runs {
			if _, err := DecodeReady(input); err != nil {
				t.Fatal(err)
			}
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / runs
	}
	low, high := allocated(small), allocated(large)
	ratio := float64(high) / float64(low)
	t.Logf("allocated bytes: depth 1000=%d, depth 9999=%d; ratio %.2f", low, high, ratio)
	// A tenfold increase in depth permits ample linear-growth overhead but
	// rejects retaining a complete path string at each recursive level.
	if ratio >= 25 {
		t.Fatalf("path allocation growth %.2f; want below 25", ratio)
	}
}

func TestParserNestingLimitAndErrorPointers(t *testing.T) {
	if _, err := DecodeReady(nestedReady(9999)); err != nil {
		t.Fatalf("at nesting limit: %v", err)
	}
	_, err := DecodeReady(nestedReady(10000))
	var failure *DecodeError
	if !errors.As(err, &failure) || failure.Reason != "JSON nested too deeply" || failure.Pointer != "/unknown"+strings.Repeat("/0", 9999) {
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
