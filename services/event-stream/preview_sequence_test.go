package eventstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type admissionReader struct {
	Reader
	descriptor PreviewRequest
	err        error
	reads      int
}

func (r *admissionReader) ReadPreviewRequest(context.Context, workspace.ID, string, string, string, string) (PreviewRequest, error) {
	r.reads++
	return r.descriptor, r.err
}
func previewSequenceFixture(t *testing.T) (*handler, *sseWriter, *httptest.ResponseRecorder, *streamPreviewState, *admissionReader) {
	t.Helper()
	reader := &admissionReader{descriptor: PreviewRequest{StartStreamPosition: 10, RequestKind: "agent_provider_request", ThreadRole: "main", ThreadVisibility: "public", IsPrimaryThread: true}}
	h := &handler{reader: reader, options: newOptions()}
	response := httptest.NewRecorder()
	writer := newSSEWriter(t.Context(), response, http.NewResponseController(response), h.options.streamConfig.WriteTimeout, h.options.previewMetrics)
	t.Cleanup(writer.close)
	state := &streamPreviewState{requests: map[string]*previewRequestState{}, types: map[string]bool{"agent.message": true, "agent.thinking": true}}
	return h, writer, response, state, reader
}
func applyPreview(t *testing.T, h *handler, writer *sseWriter, state *streamPreviewState, frames ...eventwire.PreviewFrame) {
	t.Helper()
	for _, frame := range frames {
		if err := h.previewFrame(t.Context(), writer, ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_preview"}, 10, state, frame); err != nil {
			t.Fatal(err)
		}
	}
}
func observedPreviewText(t *testing.T, response *httptest.ResponseRecorder) (int, string) {
	t.Helper()
	starts := 0
	text := ""
	for _, line := range strings.Split(response.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event struct {
			Type  string `json:"type"`
			Delta struct {
				Content struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"delta"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if event.Type == "event_start" {
			starts++
		}
		if event.Type == "event_delta" {
			text += event.Delta.Content.Text
		}
	}
	return starts, text
}

func TestPreviewWriterDeliversOnlyContinuousPrefixes(t *testing.T) {
	open := fixtureFrame("request_open", "", 0, "")
	start := fixtureFrame("event_start", "evt_message", 0, "")
	first := fixtureFrame("event_delta", "evt_message", 1, "alpha ")
	second := fixtureFrame("event_delta", "evt_message", 2, "βeta ")
	third := fixtureFrame("event_delta", "evt_message", 3, "omega\n")
	cases := []struct {
		name           string
		frames         []eventwire.PreviewFrame
		starts         int
		text           string
		latencySamples uint64
	}{
		{"healthy", []eventwire.PreviewFrame{open, start, first, second, third}, 1, "alpha βeta omega\n", 4},
		{"open_lost", []eventwire.PreviewFrame{start, first, second, third}, 0, "", 0},
		{"start_lost", []eventwire.PreviewFrame{open, first, second, third}, 0, "", 0},
		{"middle_lost", []eventwire.PreviewFrame{open, start, first, third}, 1, "alpha ", 2},
		{"tail_lost", []eventwire.PreviewFrame{open, start, first, second}, 1, "alpha βeta ", 3},
		{"duplicate_start", []eventwire.PreviewFrame{open, start, start, first, second, third}, 1, "", 1},
		{"duplicate_delta", []eventwire.PreviewFrame{open, start, first, first, second, third}, 1, "alpha ", 2},
		{"reordered", []eventwire.PreviewFrame{open, start, second, first, third}, 1, "", 1},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			h, writer, response, state, _ := previewSequenceFixture(t)
			applyPreview(t, h, writer, state, test.frames...)
			starts, text := observedPreviewText(t, response)
			if starts != test.starts || text != test.text {
				t.Fatalf("preview starts=%d text=%q want%d %q", starts, text, test.starts, test.text)
			}
			if got := h.options.previewMetrics.previewLatency.count.Load(); got != test.latencySamples || h.options.previewMetrics.previewLatency.buckets[5].Load() != got {
				t.Fatalf("actual successful preview latency samples=%d want%d", got, test.latencySamples)
			}
			if test.latencySamples > 0 && h.options.previewMetrics.previewLatency.nanos.Load() == 0 {
				t.Fatal("real writer recorded no monotonic elapsed duration")
			}
		})
	}
}

func TestPreviewWriterDatabaseEligibilityAndOpeningMark(t *testing.T) {
	for _, name := range []string{"ended", "child", "private", "compaction", "not_primary", "before_mark", "lookup_missing", "lookup_unavailable"} {
		t.Run(name, func(t *testing.T) {
			h, writer, response, state, reader := previewSequenceFixture(t)
			var logs bytes.Buffer
			h.options.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			switch name {
			case "ended":
				reader.descriptor.Ended = true
			case "child":
				reader.descriptor.ThreadRole = "subagent"
			case "private":
				reader.descriptor.ThreadVisibility = "internal"
			case "compaction":
				reader.descriptor.RequestKind = "compaction_summary"
			case "not_primary":
				reader.descriptor.IsPrimaryThread = false
			case "before_mark":
				state.watermark = 10
			case "lookup_missing":
				reader.err = &httpapi.NotFoundError{Message: "model request start not found"}
			case "lookup_unavailable":
				reader.err = errors.New("database unavailable")
			}
			applyPreview(t, h, writer, state, fixtureFrame("request_open", "", 0, ""), fixtureFrame("event_start", "evt_message", 0, ""), fixtureFrame("event_delta", "evt_message", 1, "secret"))
			if response.Body.Len() != 0 {
				t.Fatal("ineligible request produced previews")
			}
			if reader.reads != 1 {
				t.Fatal("request admission did not consult database")
			}
			// Ineligibility stays silent; only an unavailable admission read is a
			// classified preview stop with formal delivery still active.
			wantStops := 0
			if name == "lookup_unavailable" {
				wantStops = 1
			}
			if got := h.options.previewMetrics.stoppedRequests.Load(); got != uint64(wantStops) {
				t.Fatalf("stopped requests=%d want %d", got, wantStops)
			}
			stops := 0
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				if line == "" {
					continue
				}
				var record map[string]any
				if err := json.Unmarshal([]byte(line), &record); err != nil {
					t.Fatal(err)
				}
				if record["msg"] == "preview_stopped" {
					if record["reason"] != "admission_unavailable" || record["outcome"] != "formal_active" || record["model_request.id"] != "mreq_preview" {
						t.Fatalf("preview stop record=%v", record)
					}
					stops++
				}
			}
			if stops != wantStops {
				t.Fatalf("preview_stopped records=%d want %d", stops, wantStops)
			}
		})
	}
	h, writer, response, state, reader := previewSequenceFixture(t)
	open := fixtureFrame("request_open", "", 0, "")
	applyPreview(t, h, writer, state, open, fixtureFrame("event_start", "evt_message", 0, ""), fixtureFrame("event_delta", "evt_message", 1, "prefix"))
	if reader.reads != 1 {
		t.Fatal("delta consulted database")
	}
	starts, text := observedPreviewText(t, response)
	if starts != 1 || text != "prefix" {
		t.Fatal("healthy control cannot preview")
	}
}

func TestPreviewWriterFormalClosureCannotReopenOrCloseOtherRequest(t *testing.T) {
	h, writer, response, state, _ := previewSequenceFixture(t)
	h.closeFormalPreview(state, StreamChange{Event: Event{ID: "evt_thinking", ThreadID: "thr_main", Type: "agent.thinking"}, ModelRequestID: "mreq_preview", RequestStartEventID: "evt_start", RequestStartStreamPosition: 10, ThreadRole: "main", RequestKind: "agent_provider_request"})
	thinking := fixtureFrame("event_start", "evt_thinking", 0, "")
	thinking.EventType = "agent.thinking"
	applyPreview(t, h, writer, state, fixtureFrame("request_open", "", 0, ""), thinking, fixtureFrame("event_start", "evt_message", 0, ""), fixtureFrame("event_delta", "evt_message", 1, "alpha "))
	h.closeFormalPreview(state, StreamChange{Event: Event{ID: "evt_child_end", Type: "span.model_request_end"}, ModelRequestID: "mreq_child", RequestStartStreamPosition: 999, ThreadRole: "subagent", RequestKind: "agent_provider_request"})
	applyPreview(t, h, writer, state, fixtureFrame("event_delta", "evt_message", 2, "βeta "))
	if state.watermark != 0 {
		t.Fatal("child End advanced primary request watermark")
	}
	h.closeFormalPreview(state, StreamChange{Event: Event{ID: "evt_end", Type: "span.model_request_end"}, ModelRequestID: "mreq_preview", RequestStartStreamPosition: 10, ThreadRole: "main", RequestKind: "agent_provider_request"})
	applyPreview(t, h, writer, state, fixtureFrame("request_open", "", 0, ""), fixtureFrame("event_start", "evt_message", 0, ""), fixtureFrame("event_delta", "evt_message", 3, "omega\n"))
	starts, text := observedPreviewText(t, response)
	if starts != 1 || text != "alpha βeta " || len(state.requests) != 0 || state.watermark != 10 {
		t.Fatalf("closure preview=%d %q state=%+v", starts, text, state)
	}
}

func TestPreviewWriterEventStateBoundAndEndRelease(t *testing.T) {
	h, writer, _, state, _ := previewSequenceFixture(t)
	applyPreview(t, h, writer, state, fixtureFrame("request_open", "", 0, ""))
	for i := 0; i < 4097; i++ {
		frame := fixtureFrame("event_start", fmtEventID(i), 0, "")
		applyPreview(t, h, writer, state, frame)
	}
	request := state.requests["mreq_preview"]
	if len(request.events) != 4096 || !request.stopped {
		t.Fatalf("event state count=%d stopped=%v", len(request.events), request.stopped)
	}
	h.closeFormalPreview(state, StreamChange{Event: Event{Type: "span.model_request_end"}, ModelRequestID: "mreq_preview", RequestStartStreamPosition: 10, ThreadRole: "main", RequestKind: "agent_provider_request"})
	if len(state.requests) != 0 {
		t.Fatal("End retained preview state")
	}
}
func fmtEventID(i int) string { return "evt_" + strconv.Itoa(i) }

func TestStreamQuerySessionOptInAndThreadRejection(t *testing.T) {
	for _, target := range []string{"/?beta=true", "/?beta=true&event_deltas[]=agent.message&event_deltas[]=agent.thinking&event_deltas[]=agent.message"} {
		types, err := parseStreamQuery(httptest.NewRequest(http.MethodGet, target, nil), true)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(target, "event_deltas") && len(types) != 2 {
			t.Fatal("valid duplicate types not deduplicated")
		}
	}
	for _, target := range []string{"/?beta=true&event_deltas[]=agent.tool_use", "/?beta=true&event_deltas[]=", "/?beta=true&event_deltas=agent.message", "/?beta=true&event_deltas[0]=agent.message"} {
		if _, err := parseStreamQuery(httptest.NewRequest(http.MethodGet, target, nil), true); err == nil {
			t.Fatalf("invalid option accepted: %s", target)
		}
	}
	if _, err := parseStreamQuery(httptest.NewRequest(http.MethodGet, "/?beta=true&event_deltas[]=agent.message", nil), false); err == nil {
		t.Fatal("Thread opted into previews")
	}
}

func TestPreviewWriterRejectsEncodingCapacityBeforeAllocation(t *testing.T) {
	h, writer, response, state, _ := previewSequenceFixture(t)
	applyPreview(t, h, writer, state, fixtureFrame("request_open", "", 0, ""), fixtureFrame("event_start", "evt_message", 0, ""))
	response.Body.Reset()
	text := strings.Repeat("<", 100000)
	frame := fixtureFrame("event_delta", "evt_message", 1, text)
	independent, err := json.Marshal(map[string]any{"type": "event_delta", "event_id": "evt_message", "delta": map[string]any{"type": "content_delta", "index": 0, "content": map[string]string{"type": "text", "text": text}}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	writer.reservePreview = func(size int) bool {
		calls++
		if size != len(independent) {
			t.Fatalf("reserved=%d want fully escaped wrapper%d", size, len(independent))
		}
		return false
	}
	request := state.requests[frame.ModelRequestID]
	event := request.events[frame.EventID]
	allocations := testing.AllocsPerRun(10, func() {
		request.stopped = false
		event.next = 1
		if err := h.previewFrame(t.Context(), writer, ReadScope{SessionID: "sesn_preview"}, 10, state, frame); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("rejected encoding allocated an uncharged wrapper: %g allocations", allocations)
	}
	if calls == 0 || response.Body.Len() != 0 || !request.stopped {
		t.Fatal("encoding rejection did not precede publication")
	}
}
