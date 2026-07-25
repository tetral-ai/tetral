package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestPayloadRoundTripGoldenJSONAndUnknownFieldTolerance(t *testing.T) {
	const golden = `{"schema_version":1,"tool":"read","tool_use_event_id":"evt_payload","workspace_root":"/workspace","roots":[{"path":"/workspace","mode":"read_write"},{"path":"/mnt/session/uploads/file.txt","mode":"read"}],"limits":{"visible_bytes":512,"visible_lines":20},"input":{"path":"README.md"}}`
	payload, err := DecodePayload([]byte(golden))
	if err != nil {
		t.Fatalf("DecodePayload golden: %v", err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal golden payload: %v", err)
	}
	if string(body) != golden {
		t.Fatalf("payload golden round-trip = %s; want %s", body, golden)
	}

	withUnknown := strings.Replace(golden, `"input":`, `"future_field":{"nested":true},"input":`, 1)
	decodedUnknown, err := DecodePayload([]byte(withUnknown))
	if err != nil {
		t.Fatalf("DecodePayload with unknown field: %v", err)
	}
	if decodedUnknown.Tool != "read" || string(decodedUnknown.Input) != `{"path":"README.md"}` {
		t.Fatalf("decoded payload with unknown field = %+v", decodedUnknown)
	}
}

func TestEnvelopeRoundTripGoldenJSON(t *testing.T) {
	truncated := false
	envelope := Envelope{
		SchemaVersion: SchemaVersion,
		Tool:          "read",
		Status:        ToolStatusSuccess,
		Truncated:     &truncated,
		Error:         nil,
		Result:        json.RawMessage(`{"content":"hello","truncated":false}`),
	}
	const golden = `{"schema_version":1,"tool":"read","status":"success","truncated":false,"error":null,"result":{"content":"hello","truncated":false}}`
	body, err := MarshalEnvelope(envelope, false)
	if err != nil {
		t.Fatalf("MarshalEnvelope golden: %v", err)
	}
	if string(body) != golden {
		t.Fatalf("envelope golden = %s; want %s", body, golden)
	}
	var decoded Envelope
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode golden envelope: %v", err)
	}
	if decoded.SchemaVersion != SchemaVersion || decoded.Tool != "read" || decoded.Status != ToolStatusSuccess || decoded.Truncated == nil || *decoded.Truncated {
		t.Fatalf("decoded envelope = %+v", decoded)
	}
}

func TestDecodePayloadRejectsRequiredFieldOmissions(t *testing.T) {
	base := map[string]any{
		"schema_version":    SchemaVersion,
		"tool":              "read",
		"tool_use_event_id": "evt_payload",
		"workspace_root":    "/workspace",
		"roots":             []map[string]string{{"path": "/workspace", "mode": RootModeReadWrite}},
		"limits":            map[string]int{"visible_bytes": 512, "visible_lines": 20},
		"input":             map[string]string{"path": "README.md"},
	}
	for _, field := range []string{"schema_version", "tool", "tool_use_event_id", "workspace_root", "roots", "input"} {
		t.Run(field, func(t *testing.T) {
			payload := make(map[string]any, len(base))
			for key, value := range base {
				payload[key] = value
			}
			delete(payload, field)
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal %s omission: %v", field, err)
			}
			if _, err := DecodePayload(body); err == nil {
				t.Fatalf("DecodePayload without %s succeeded", field)
			}
		})
	}
}

func TestMarshalEnvelopeFitsOversizedResult(t *testing.T) {
	envelope, err := NewToolResultEnvelope("grep", map[string]any{
		"mode": "content",
		"text": strings.Repeat("x", MaxEnvelopeBytes*2),
	}, false, time.Now())
	if err != nil {
		t.Fatalf("NewToolResultEnvelope: %v", err)
	}

	body, err := MarshalEnvelope(envelope, false)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	if len(body) > MaxEnvelopeBytes {
		t.Fatalf("envelope bytes = %d; want <= %d", len(body), MaxEnvelopeBytes)
	}
	var decoded Envelope
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode fitted envelope: %v", err)
	}
	if decoded.Truncated == nil || !*decoded.Truncated {
		t.Fatalf("truncated = %v; want true", decoded.Truncated)
	}
	if strings.Contains(string(body), strings.Repeat("x", 8192)) {
		t.Fatalf("oversized text was not fitted")
	}
}

func TestMarshalEnvelopeBoundsOversizedErrorDetail(t *testing.T) {
	envelope, err := NewToolErrorEnvelope("apply_patch", ToolError{
		Kind:    ErrorKindPatchVerify,
		Message: strings.Repeat("m", maxErrorMessageBytes*2),
		Detail: map[string]any{
			"stderr":  strings.Repeat("x", MaxEnvelopeBytes*2),
			"matches": []any{strings.Repeat("y", MaxEnvelopeBytes), strings.Repeat("z", MaxEnvelopeBytes)},
		},
	}, map[string]any{"truncated": false}, false, time.Now())
	if err != nil {
		t.Fatalf("NewToolErrorEnvelope: %v", err)
	}

	body, err := MarshalEnvelope(envelope, false)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	if len(body) > MaxEnvelopeBytes {
		t.Fatalf("envelope bytes = %d; want <= %d", len(body), MaxEnvelopeBytes)
	}
	var decoded Envelope
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode bounded envelope: %v", err)
	}
	if decoded.Error == nil {
		t.Fatal("decoded error is nil")
	}
	if len([]byte(decoded.Error.Message)) > maxErrorMessageBytes {
		t.Fatalf("error message bytes = %d; want <= %d", len([]byte(decoded.Error.Message)), maxErrorMessageBytes)
	}
	if decoded.Error.Detail["truncated"] != true {
		t.Fatalf("error detail = %#v; want truncated marker", decoded.Error.Detail)
	}
	if strings.Contains(string(body), strings.Repeat("x", 8192)) || strings.Contains(string(body), strings.Repeat("y", 8192)) {
		t.Fatalf("oversized error detail was not fitted")
	}
}

func TestMarshalEnvelopeBoundsMultibyteErrorMessageByUTF8Bytes(t *testing.T) {
	envelope, err := NewToolErrorEnvelope("Read", ToolError{
		Kind:    ErrorKindInvalidInput,
		Message: strings.Repeat("界", 4096),
	}, nil, false, time.Now())
	if err != nil {
		t.Fatalf("NewToolErrorEnvelope: %v", err)
	}
	body, err := MarshalEnvelope(envelope, false)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	var decoded Envelope
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if got := len([]byte(decoded.Error.Message)); got > 4096 {
		t.Fatalf("error message bytes = %d; want <= 4096", got)
	}
	if !utf8.ValidString(decoded.Error.Message) {
		t.Fatal("bounded error message is not valid UTF-8")
	}
}

func TestMarshalEnvelopeAllowsOversizedMediaEnvelope(t *testing.T) {
	envelope, err := NewToolResultEnvelope("view_image", map[string]any{
		"mime":        "image/png",
		"size_bytes":  MaxEnvelopeBytes,
		"data_base64": strings.Repeat("A", MaxEnvelopeBytes+1),
	}, false, time.Now())
	if err != nil {
		t.Fatalf("NewToolResultEnvelope: %v", err)
	}

	body, err := MarshalEnvelope(envelope, true)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	if len(body) <= MaxEnvelopeBytes {
		t.Fatalf("media envelope bytes = %d; want view_image oversize exemption", len(body))
	}
}

func TestAllErrorKindsSerializeInErrorEnvelope(t *testing.T) {
	want := []ErrorKind{
		ErrorKindInvalidInput,
		ErrorKindUnsupportedFormat,
		ErrorKindNotFound,
		ErrorKindIsDirectory,
		ErrorKindNotADirectory,
		ErrorKindPathEscape,
		ErrorKindForbiddenPath,
		ErrorKindTooLarge,
		ErrorKindBinaryFile,
		ErrorKindPDFPageOutOfRange,
		ErrorKindPDFCorrupt,
		ErrorKindPDFEncrypted,
		ErrorKindMatchNotFound,
		ErrorKindMatchAmbiguous,
		ErrorKindPatchParse,
		ErrorKindPatchVerify,
		ErrorKindWriteFailed,
		ErrorKindExecSpawnFailed,
		ErrorKindTimeout,
		ErrorKindTaskNotFound,
		ErrorKindTaskExited,
		ErrorKindTaskLost,
		ErrorKindStdinClosed,
		ErrorKindTaskLimit,
		ErrorKindHelperFailure,
	}
	if len(AllErrorKinds) != len(want) {
		t.Fatalf("AllErrorKinds length = %d; want %d", len(AllErrorKinds), len(want))
	}
	seen := map[ErrorKind]bool{}
	for i, kind := range AllErrorKinds {
		if kind != want[i] {
			t.Fatalf("AllErrorKinds[%d] = %q; want %q", i, kind, want[i])
		}
		if seen[kind] {
			t.Fatalf("AllErrorKinds contains duplicate %q", kind)
		}
		seen[kind] = true
		envelope, err := NewToolErrorEnvelope("read", ToolError{Kind: kind, Message: "contract error kind"}, nil, false, time.Time{})
		if err != nil {
			t.Fatalf("NewToolErrorEnvelope(%q): %v", kind, err)
		}
		body, err := MarshalEnvelope(envelope, false)
		if err != nil {
			t.Fatalf("MarshalEnvelope(%q): %v", kind, err)
		}
		var decoded Envelope
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("decode error envelope %q: %v", kind, err)
		}
		if decoded.Error == nil || decoded.Error.Kind != kind {
			t.Fatalf("decoded error kind = %+v; want %q", decoded.Error, kind)
		}
	}
}
