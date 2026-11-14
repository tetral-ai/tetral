// Layer 1: the task notification payloads pinned as vectors.
package runtimecontrol

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/testvector"
)

// The Runtime's runtime-service test reads these vectors as the payloads the
// Job Runner delivers.
func TestCanonicalTaskNotificationPayloadVectors(t *testing.T) {
	cases := []struct {
		name           string
		taskID         string
		terminalStatus string
		storedResult   string
		wantStatus     string
	}{
		{name: "completed-empty", taskID: "task_shape", terminalStatus: "completed", storedResult: `{"status":"completed","exit_code":null,"stdout":{"text":"","truncated":false},"stderr":{"text":"","truncated":false}}`, wantStatus: "completed"},
		{name: "failed-escaped-unicode", taskID: "task_<>& paragraph ", terminalStatus: "failed", storedResult: "{\"status\":\"failed\",\"exit_code\":-1,\"stdout\":{\"text\":\"<line>&\\\\n雪\\u2028paragraph\\u2029\",\"truncated\":false},\"stderr\":{\"text\":\"failed\",\"truncated\":true,\"total_bytes\":24,\"total_lines\":2}}", wantStatus: "failed"},
		{name: "cancelled-safe-maximum", taskID: "task_shape", terminalStatus: "cancelled", storedResult: `{"status":"cancelled","exit_code":9007199254740991,"stdout":{"text":"cancelled","truncated":false,"total_bytes":9},"stderr":{"text":"","truncated":false}}`, wantStatus: "cancelled"},
		{name: "expired-safe-metadata", taskID: "task_shape", terminalStatus: "expired", storedResult: `{"status":"expired","stdout":{"text":"","truncated":false},"stderr":{"text":"expired","truncated":true,"original_bytes":9007199254740991,"original_lines":0}}`, wantStatus: "expired"},
		{name: "unknown-outcome-lowered", taskID: "task_shape", terminalStatus: "unknown_outcome", storedResult: `{"status":"unknown_outcome","stdout":{"text":"","truncated":false},"stderr":{"text":"unknown","truncated":false}}`, wantStatus: "failed"},
		{name: "large-output-fitted", taskID: "task_large_shape", terminalStatus: "completed", storedResult: `{"status":"completed","exit_code":0,"stdout":{"text":"` + strings.Repeat("a", 40*1024) + `","truncated":false},"stderr":{"text":"` + strings.Repeat("b", 40*1024) + `","truncated":false}}`, wantStatus: "completed"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			payload, err := CanonicalTaskNotificationPayloadJSON(testCase.taskID, "sevt_tool_shape", testCase.terminalStatus, testCase.storedResult)
			if err != nil {
				t.Fatalf("build canonical payload: %v", err)
			}
			if len(payload) > RuntimeTaskNotificationPayloadMaxBytes {
				t.Fatalf("payload bytes = %d; want <= %d", len(payload), RuntimeTaskNotificationPayloadMaxBytes)
			}
			var object map[string]any
			if err := json.Unmarshal([]byte(payload), &object); err != nil || object["status"] != testCase.wantStatus {
				t.Fatalf("payload status = %#v (%v); want %s", object["status"], err, testCase.wantStatus)
			}
			vector, err := json.Marshal(map[string]string{"notificationJson": payload})
			if err != nil {
				t.Fatal(err)
			}
			testvector.PinJSON(t, "testdata/task-notifications/"+testCase.name+".json", vector)
		})
	}
}
