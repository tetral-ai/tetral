package agentruntimebridge

import (
	"encoding/json"
	"os"
	"testing"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestStableRuntimeIDMatchesSharedVectors(t *testing.T) {
	messageID := stableRuntimeID(
		"runtime_message_draft",
		"ws_1",
		"ses_1",
		"thr_1",
		"messages",
		"rin_1",
		"user_message",
		"0",
	)
	if want := "stid_edcc6aa88ddf349e058ade77c0f169d73c6f3c0343d99cef1adc640fd486d82e"; messageID != want {
		t.Fatalf("message stable id = %q; want %q", messageID, want)
	}
	if got, want := stableRuntimeID("runtime_message_part_draft", messageID, "text", "0"),
		"stid_080e946f2be6e403aaaeb2afab207746171d95850660f612af67de33f1ed5828"; got != want {
		t.Fatalf("part stable id = %q; want %q", got, want)
	}
}

func TestCommitInputsDeclarationDigestMatchesSharedVector(t *testing.T) {
	raw, err := os.ReadFile("testdata/runtime_declaration_vectors.json") //nolint:gosec // Repository-owned fixture.
	if err != nil {
		t.Fatalf("read declaration vectors: %v", err)
	}
	var fixture struct {
		CommitInputs struct {
			SessionThreadID string   `json:"session_thread_id"`
			RuntimeInputID  string   `json:"runtime_input_id"`
			EventIDs        []string `json:"event_ids"`
			SequenceFrom    int64    `json:"sequence_from"`
			SequenceTo      int64    `json:"sequence_to"`
			InputKind       string   `json:"input_kind"`
			Draft           struct {
				RuntimeLocalID  string `json:"runtime_local_id"`
				SourceKind      string `json:"source_kind"`
				SourceID        string `json:"source_id"`
				SourceEventID   string `json:"source_event_id"`
				Ordinal         int32  `json:"ordinal"`
				MessageInfoJSON string `json:"message_info_json"`
				Part            struct {
					RuntimeLocalPartID string `json:"runtime_local_part_id"`
					PartKind           string `json:"part_kind"`
					Ordinal            int32  `json:"ordinal"`
					PartJSON           string `json:"part_json"`
				} `json:"part"`
			} `json:"draft"`
			Digest string `json:"digest"`
		} `json:"commit_inputs"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode declaration vectors: %v", err)
	}
	vector := fixture.CommitInputs
	got, err := commitInputsDeclarationDigest(&bridgev1.CommitInputsRequest{
		Scope:          &bridgev1.RuntimeScope{SessionThreadId: vector.SessionThreadID},
		RuntimeInputId: vector.RuntimeInputID,
		EventIds:       vector.EventIDs,
		SequenceFrom:   vector.SequenceFrom,
		SequenceTo:     vector.SequenceTo,
		InputKind:      vector.InputKind,
		Drafts: []*bridgev1.RuntimeMessageDraft{{
			RuntimeLocalId:  vector.Draft.RuntimeLocalID,
			SourceKind:      vector.Draft.SourceKind,
			SourceId:        vector.Draft.SourceID,
			SourceEventId:   vector.Draft.SourceEventID,
			DraftKind:       bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_USER_INPUT,
			Ordinal:         vector.Draft.Ordinal,
			MessageInfoJson: vector.Draft.MessageInfoJSON,
			Parts: []*bridgev1.RuntimePartDraft{{
				RuntimeLocalPartId: vector.Draft.Part.RuntimeLocalPartID,
				PartKind:           vector.Draft.Part.PartKind,
				Ordinal:            vector.Draft.Part.Ordinal,
				PartJson:           vector.Draft.Part.PartJSON,
			}},
		}},
	}, vector.InputKind)
	if err != nil {
		t.Fatalf("digest declaration vector: %v", err)
	}
	if got != vector.Digest {
		t.Fatalf("declaration digest = %q; want %q", got, vector.Digest)
	}
}
