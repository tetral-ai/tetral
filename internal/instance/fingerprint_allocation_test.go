package instance

import (
	"crypto/sha256"
	"runtime"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/protocol"
)

func TestFingerprintPreservesCanonicalBytes(t *testing.T) {
	for _, tc := range []struct{ input, canonical string }{
		{` { "z": 9007199254740993, "\u0061": [1.0, {"y":"\u0041", "x":null}], "a/b":"<>&\u2028" } `, `{"\u0061":[1.0,{"x":null,"y":"\u0041"}],"a/b":"<>&\u2028","z":9007199254740993}`},
		{` [ { "b":false, "a":true }, [], {}, "},[", -0, 1e+2 ] `, `[{"a":true,"b":false},[],{},"},[",-0,1e+2]`},
	} {
		got, ok := Fingerprint([]byte(tc.input))
		want := sha256.Sum256([]byte(tc.canonical))
		if !ok || got != want {
			t.Fatalf("fingerprint %x (valid %v); want hash of %s: %x", got, ok, tc.canonical, want)
		}
	}
	for _, input := range []string{`{"a":1,"\u0061":2}`, `[{"a":1,"a":2}]`} {
		if _, ok := Fingerprint([]byte(input)); ok {
			t.Fatalf("accepted duplicate keys in %s", input)
		}
	}
}

func TestFingerprintNestedObjectAllocationGrowth(t *testing.T) {
	ready := protocol.Encode(protocol.Ready{ConnectionID: "conn", MessageID: "msg", OS: "linux", Arch: "x86_64", CLIVersion: "1.0.0", Capabilities: protocol.Capabilities{Drives: protocol.DriveCapability{Mount: true}}})
	input := func(depth int) []byte {
		body := []byte(string(ready[:len(ready)-1]) + `,"unknown":` + strings.Repeat(`{"a":`, depth) + `0` + strings.Repeat(`}`, depth) + `}`)
		if len(body) > 65536 {
			t.Fatal("fixture exceeds client body limit")
		}
		if _, err := protocol.DecodeReady(body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	small, large := input(1000), input(9999)
	for _, body := range [][]byte{small, large} {
		if _, ok := Fingerprint(body); !ok {
			t.Fatal("invalid warmup fingerprint")
		}
	}
	// Construct and warm both accepted inputs before runtime-wide counters.
	// Do not run this test in parallel: it measures bytes, not allocation count.
	allocated := func(body []byte) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		const runs = 3
		for range runs {
			if _, ok := Fingerprint(body); !ok {
				t.Fatal("invalid fingerprint")
			}
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / runs
	}
	low, high := allocated(small), allocated(large)
	ratio := float64(high) / float64(low)
	t.Logf("fingerprint allocated bytes: depth1000=%d, depth9999=%d; ratio %.2f", low, high, ratio)
	// Tenfold depth growth allows ample linear overhead, but not copying the
	// complete nested value into a new buffer at every ancestor object.
	if ratio >= 25 {
		t.Fatalf("allocation growth %.2f; want below25", ratio)
	}
}
