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

// A tool input is admitted only when the tool.execute that carries it, two
// levels deeper, is within the nesting limit, so every admitted input can be
// sent and read back.
func TestAToolInputIsAdmittedOnlyWithinTheLimitOfItsToolExecute(t *testing.T) {
	input := func(depth int) []byte {
		return []byte(`{"cmd":"ls","x":` + strings.Repeat("[", depth-1) + "0" + strings.Repeat("]", depth-1) + "}")
	}
	compacted, err := CompactToolInput(ToolExecCommand, input(125))
	if err != nil {
		t.Fatalf("an input 125 levels deep: %v", err)
	}
	instruction := ExecuteInstruction{SessionID: "sesn_1", BindingID: "bind_1", ToolUseEventID: "sevt_1", TaskID: "task_1", Tool: ToolExecCommand,
		Input: compacted, SandboxPolicy: RawObject(`{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"disabled"}}`)}
	if _, err := DecodeToolDownstream(Encode(Downstream{ConnectionID: "conn_1", MessageID: "msg_1", Payload: instruction})); err != nil {
		t.Fatalf("the tool.execute of an input 125 levels deep: %v", err)
	}
	if _, err := DecodeExecuteInstruction(Encode(instruction)); err != nil {
		t.Fatalf("the stored instruction of an input 125 levels deep: %v", err)
	}
	var failure *DecodeError
	if _, err := CompactToolInput(ToolExecCommand, input(126)); !errors.As(err, &failure) || failure.Reason != "JSON nested too deeply" {
		t.Fatalf("an input 126 levels deep: %v; want it refused as nested too deeply", err)
	}
}

// The parser keeps one incremental path and renders a pointer only for an
// error: decoding the same values under 120 ancestors with 100-byte keys
// allocates about what it does under one-byte keys. Rendering the pointer at
// each value would allocate the ancestors' keys once per value, about 65
// times more here.
func TestParserPathAllocationDoesNotGrowWithAncestorKeys(t *testing.T) {
	// The inputs and a warm-up come before the process-wide counters; the
	// test must not run in parallel.
	ready := func(key string) []byte {
		values := strings.Repeat("0,", 5000) + "0"
		chain := strings.Repeat(`{"`+key+`":`, 120) + "[" + values + "]" + strings.Repeat("}", 120)
		encoded := Encode(Ready{ConnectionID: "conn", MessageID: "msg", OS: "linux", Arch: "x86_64", CLIVersion: "1.0.0", Capabilities: Capabilities{Drives: DriveCapability{Mount: true}}})
		return []byte(string(encoded[:len(encoded)-1]) + `,"unknown":` + chain + "}")
	}
	short, long := ready("k"), ready(strings.Repeat("k", 100))
	allocated := func(input []byte) uint64 {
		if _, err := DecodeReady(input); err != nil {
			t.Fatal(err)
		}
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
	low, high := allocated(short), allocated(long)
	if ratio := float64(high) / float64(low); ratio >= 2 {
		t.Fatalf("allocated bytes: one-byte keys %d, 100-byte keys %d; ratio %.2f, want below 2", low, high, ratio)
	}
}
