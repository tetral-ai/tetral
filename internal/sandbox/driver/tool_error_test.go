package driver

import (
	"encoding/json"
	"syscall"
	"testing"
)

func TestPayloadFilesystemFailureScope(t *testing.T) {
	const root = "/tmp/tetral-runtime/tool-payloads-stage"
	const destination = root + "/invocation/payload.json"
	const noSpace = ": no space left on device"
	for _, tc := range []struct {
		name, message string
		want          bool
	}{
		{"exact file", "write " + destination + noSpace, true},
		{"staging parent", "mkdir " + root + noSpace, true},
		{"unrelated path", "write /workspace/output" + noSpace, false},
		{"sibling prefix", "mkdir " + root + "-evil" + noSpace, false},
		{"above staging root", "mkdir /tmp/tetral-runtime" + noSpace, false},
		{"wrong operation", "remove " + destination + noSpace, false},
		{"different cause", "write " + destination + ": permission denied", false},
		{"quoted user output", "command said: write " + destination + noSpace, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := payloadPathError(tc.message, "upload_payload", []string{destination}, syscall.ENOSPC); got != tc.want {
				t.Fatalf("classify %q = %t; want %t", tc.message, got, tc.want)
			}
		})
	}
}

func TestBulkUploadFilesystemFailureFormats(t *testing.T) {
	const directory = "/tmp/tetral-runtime/tool-payloads-stage/invocation"
	const destination = directory + "/payload.json"
	const noSpace = ": no space left on device"
	// The four wrappers are emitted by the upstream UploadFiles handler;
	// os.Create's underlying PathError uses "open", not "create".
	for _, tc := range []struct {
		name   string
		errors []string
		want   string
	}{
		{"mkdir", []string{destination + ": mkdir " + directory + ": mkdir " + directory + noSpace}, "mkdir " + directory + noSpace},
		{"create", []string{destination + ": create: open " + destination + noSpace}, "open " + destination + noSpace},
		{"write", []string{destination + ": write: write " + destination + noSpace}, "write " + destination + noSpace},
		{"close", []string{destination + ": close: close " + destination + noSpace}, "close " + destination + noSpace},
		{"other destination", []string{"/workspace/output: write: write " + destination + noSpace}, ""},
		{"multiple errors", []string{destination + ": write: write " + destination + noSpace, "another failure"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{"errors": tc.errors, "files": []any{}})
			if err != nil {
				t.Fatal(err)
			}
			got := bulkUploadPathError(string(body), []string{destination})
			if got != tc.want {
				t.Fatalf("unwrap = %q; want %q", got, tc.want)
			}
			if got != "" && !payloadPathError(got, "upload_payload", []string{destination}, syscall.ENOSPC) {
				t.Fatalf("unwrapped upstream error was not classified: %q", got)
			}
		})
	}
	if got := bulkUploadPathError("not JSON", []string{destination}); got != "" {
		t.Fatalf("malformed response produced a filesystem error: %q", got)
	}
}
