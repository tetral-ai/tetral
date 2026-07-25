package protocol

import (
	"encoding/json"
	"testing"
)

func TestCaptureResultSkipFieldsAreAdditive(t *testing.T) {
	envelope := CaptureEnvelope{
		SchemaVersion: SchemaVersion,
		Status:        ToolStatusSuccess,
		Result: &CaptureResult{
			SourcePath: "/mnt/session/outputs/idle.fifo",
			Kind:       "fifo",
			LinkCount:  1,
			Skipped:    true,
			SkipReason: "non_regular",
		},
	}

	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal skipped capture: %v", err)
	}
	const want = `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/idle.fifo","kind":"fifo","link_count":1,"size_bytes":0,"skipped":true,"skip_reason":"non_regular"}}`
	if string(body) != want {
		t.Fatalf("skipped capture JSON = %s; want %s", body, want)
	}

	var decoded CaptureEnvelope
	if err := json.Unmarshal([]byte(`{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/result.txt","kind":"regular","link_count":1,"size_bytes":4,"data_base64":"Ym9keQ=="}}`), &decoded); err != nil {
		t.Fatalf("decode pre-skip capture shape: %v", err)
	}
	if decoded.Result == nil || decoded.Result.Skipped || decoded.Result.SkipReason != "" {
		t.Fatalf("decoded pre-skip capture = %+v; want additive zero values", decoded)
	}
}

func TestCaptureResultDirectoryEnumerationFieldsAreAdditive(t *testing.T) {
	envelope := CaptureEnvelope{
		SchemaVersion: SchemaVersion,
		Status:        ToolStatusSuccess,
		Result: &CaptureResult{
			SourcePath:           "/mnt/session/outputs/reports",
			Kind:                 "directory",
			LinkCount:            2,
			Entries:              []string{"a.txt", "b.txt"},
			EntriesTruncated:     true,
			UnrepresentableNames: 1,
		},
	}

	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal directory capture: %v", err)
	}
	const want = `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/reports","kind":"directory","link_count":2,"size_bytes":0,"entries":["a.txt","b.txt"],"entries_truncated":true,"unrepresentable_names":1}}`
	if string(body) != want {
		t.Fatalf("directory capture JSON = %s; want %s", body, want)
	}

}
