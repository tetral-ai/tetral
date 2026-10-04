package eventwire

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func previewFixture(t *testing.T) (map[string]any, map[string]json.RawMessage) {
	t.Helper()
	raw, err := os.ReadFile("../../integration/testdata/public-streaming.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Wire struct {
			Identity   map[string]any `json:"identity"`
			MessageID  string         `json:"message_event_id"`
			ThinkingID string         `json:"thinking_event_id"`
		} `json:"wire"`
		Public map[string]json.RawMessage `json:"public"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	fixture.Wire.Identity["event_id"] = fixture.Wire.MessageID
	return fixture.Wire.Identity, fixture.Public
}
func encodeFixtureFrame(t *testing.T, kind string, text string) []byte {
	t.Helper()
	fields, _ := previewFixture(t)
	fields["kind"] = kind
	if kind == "request_open" {
		delete(fields, "event_id")
	} else {
		fields["event_type"] = "agent.message"
		fields["preview_sequence"] = 0
		if kind == "event_delta" {
			fields["preview_sequence"] = 1
			fields["text"] = text
		}
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
func TestPreviewProtocolAndPublicProjectionMatchSharedFixture(t *testing.T) {
	fields, public := previewFixture(t)
	subject := PreviewSubject(fields["workspace_id"].(string), fields["session_id"].(string))
	if subject != "preview.v1.d29ya3NwYWNlIEEvzrI.c2Vzc18wMTIzNDU2Nzg5YWJjZGVm" {
		t.Fatalf("subject=%s", subject)
	}
	for _, kind := range []string{"request_open", "event_start", "event_delta"} {
		t.Run(kind, func(t *testing.T) {
			frame, err := DecodePreviewFrame(subject, encodeFixtureFrame(t, kind, "alpha "))
			if err != nil {
				t.Fatal(err)
			}
			if kind == "request_open" {
				return
			}
			data, err := MarshalPreviewEvent(frame)
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			if json.Unmarshal(data, &got) != nil {
				t.Fatal("invalid public JSON")
			}
			key := "message_start"
			if kind == "event_delta" {
				key = "message_delta"
			}
			if json.Unmarshal(public[key], &want) != nil {
				t.Fatalf("missing fixture %s", key)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("public preview=%s want=%s", data, public[key])
			}
		})
	}
}
func TestPreviewDecoderRejectsUnsafeVocabularyAndUnicode(t *testing.T) {
	fields, _ := previewFixture(t)
	subject := PreviewSubject(fields["workspace_id"].(string), fields["session_id"].(string))
	good := string(encodeFixtureFrame(t, "event_delta", "😀"))
	cases := map[string]string{
		"unknown_field":       strings.TrimSuffix(good, "}") + `,"reasoning":"secret"}`,
		"duplicate_key":       strings.TrimSuffix(good, "}") + `,"kind":"event_delta"}`,
		"missing_sequence":    strings.Replace(good, `"preview_sequence":1,`, "", 1),
		"null_sequence":       strings.Replace(good, `"preview_sequence":1`, `"preview_sequence":null`, 1),
		"unpaired_high":       strings.Replace(good, "😀", `\ud83d`, 1),
		"unpaired_low":        strings.Replace(good, "😀", `\ude00`, 1),
		"interleaved_halves":  strings.Replace(good, "😀", `\ud83d\ud834\ude00\udd1e`, 1),
		"thinking_delta":      strings.Replace(good, "agent.message", "agent.thinking", 1),
		"tool_delta":          strings.Replace(good, "agent.message", "agent.tool_use", 1),
		"fractional_sequence": strings.Replace(good, `"preview_sequence":1`, `"preview_sequence":1.1`, 1),
		"unsafe_sequence":     strings.Replace(good, `"preview_sequence":1`, `"preview_sequence":9007199254740992`, 1),
		"trailing_json":       good + `{}`,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodePreviewFrame(subject, []byte(data)); err == nil {
				t.Fatal("unsafe preview accepted")
			}
		})
	}
	if _, err := DecodePreviewFrame(PreviewSubject("foreign", fields["session_id"].(string)), []byte(good)); err == nil {
		t.Fatal("foreign subject accepted")
	}
	valid := strings.Replace(good, "😀", `\ud83d\ude00`, 1)
	frame, err := DecodePreviewFrame(subject, []byte(valid))
	if err != nil || frame.Text == nil || *frame.Text != "😀" {
		t.Fatalf("valid Unicode pair=%+v err=%v", frame, err)
	}
}
func TestPreviewEncodedFrameByteBound(t *testing.T) {
	fields, _ := previewFixture(t)
	subject := PreviewSubject(fields["workspace_id"].(string), fields["session_id"].(string))
	// Independently pad the known ASCII JSON wire, measuring encoded bytes rather
	// than rune counts or the production encoder's computed budget.
	skeleton := string(encodeFixtureFrame(t, "event_delta", "padding"))
	overhead := len(skeleton) - len("padding")
	for _, size := range []int{262143, 262144, 262145} {
		data := strings.Replace(skeleton, "padding", strings.Repeat("a", size-overhead), 1)
		if len(data) != size {
			t.Fatal("fixture size")
		}
		_, err := DecodePreviewFrame(subject, []byte(data))
		if (err == nil) != (size <= 262144) {
			t.Fatalf("size%d err%v", size, err)
		}
	}
}

func TestPublicPreviewExactEncodingReservationAgainstIndependentJSON(t *testing.T) {
	// Standard-library encoders are an independent oracle for the exact fixed
	// SDK wrapper and Go HTML/control/UTF-8 escaping used in the public contract.
	for _, text := range []string{"alpha βeta omega\n", "😀é𝄞", "\x00\x01\b\f\n\r\t\"\\", "<>&\u2028\u2029", strings.Repeat("<", 40000)} {
		frame := PreviewFrame{Kind: "event_delta", EventType: "agent.message", EventID: "event<&\"\\😀", Text: &text}
		want, err := json.Marshal(map[string]any{"type": "event_delta", "event_id": frame.EventID, "delta": map[string]any{"type": "content_delta", "index": 0, "content": map[string]string{"type": "text", "text": text}}})
		if err != nil {
			t.Fatal(err)
		}
		size, err := PublicPreviewEncodedBytes(frame)
		if err != nil || size != len(want) {
			t.Fatalf("reservation=%d,%v want exact %d", size, err, len(want))
		}
		if allocations := testing.AllocsPerRun(10, func() { _, _ = PublicPreviewEncodedBytes(frame) }); allocations != 0 {
			t.Fatalf("size reservation copied text: %g allocations", allocations)
		}
		got, err := MarshalPreviewEvent(frame)
		if err != nil || string(got) != string(want) || len(got) != cap(got) {
			t.Fatalf("single exact public output differs: length/cap=%d/%d want%d error%v", len(got), cap(got), len(want), err)
		}
	}
	for _, eventType := range []string{"agent.message", "agent.thinking"} {
		frame := PreviewFrame{Kind: "event_start", EventType: eventType, EventID: "evt_β<&"}
		want, err := json.Marshal(struct {
			Type  string `json:"type"`
			Event struct {
				ID   string `json:"id"`
				Type string `json:"type"`
			} `json:"event"`
		}{Type: "event_start", Event: struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}{frame.EventID, eventType}})
		if err != nil {
			t.Fatal(err)
		}
		size, err := PublicPreviewEncodedBytes(frame)
		if err != nil || size != len(want) {
			t.Fatal("start reservation mismatch")
		}
		got, err := MarshalPreviewEvent(frame)
		if err != nil || string(got) != string(want) {
			t.Fatal("start escaping mismatch")
		}
	}
}
