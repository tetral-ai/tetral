package static

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/eventwire"
)

// This is a static cross-language/type guard. The actual HTTP/NATS/SDK parser
// proof belongs to TestPostgreSQLPublicStreamingIdentity, not this compiler run.
func TestPublicPreviewProjectionTracksForkSDK(t *testing.T) {
	engine := finalArchitectureEngineRoot(t)
	sdk := forkSDKRootForStaticTest(t, engine)
	assertFrozenForkSDKTree(t, sdk)
	raw, err := os.ReadFile(filepath.Join(engine, "integration/testdata/public-streaming.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Version int `json:"version"`
		Wire    struct {
			Identity eventwire.PreviewFrame `json:"identity"`
			Message  string                 `json:"message_event_id"`
			Thinking string                 `json:"thinking_event_id"`
		} `json:"wire"`
		Public map[string]json.RawMessage `json:"public"`
	}
	if err = json.Unmarshal(raw, &vectors); err != nil || vectors.Version != 1 {
		t.Fatal("invalid shared public streaming vectors")
	}
	limitsRaw, err := os.ReadFile(filepath.Join(engine, "services/gateway/packages/protocol/src/preview-limits.json"))
	if err != nil {
		t.Fatal(err)
	}
	var limits map[string]int
	if err = json.Unmarshal(limitsRaw, &limits); err != nil {
		t.Fatal(err)
	}
	if eventwire.MaxPreviewFrameBytes != 256*1024 || eventwire.MaxPreviewEventIdentities != 4096 {
		t.Fatal("preview wire policy changed without explicit boundary review")
	}
	if limits["maxFrameBytes"] != eventwire.MaxPreviewFrameBytes || limits["maxEventIdentities"] != eventwire.MaxPreviewEventIdentities {
		t.Fatalf("Go/TS wire bounds differ: %v", limits)
	}
	var projected []string
	for _, name := range []string{"message_start", "thinking_start", "message_delta"} {
		frame := vectors.Wire.Identity
		frame.Kind = "event_start"
		frame.EventType = "agent.message"
		frame.EventID = vectors.Wire.Message
		sequence := int64(0)
		frame.PreviewSequence = &sequence
		if name == "thinking_start" {
			frame.EventType = "agent.thinking"
			frame.EventID = vectors.Wire.Thinking
		}
		if name == "message_delta" {
			frame.Kind = "event_delta"
			sequence = 1
			text := "alpha "
			frame.Text = &text
		}
		encoded, err := eventwire.MarshalPreviewEvent(frame)
		if err != nil {
			t.Fatal(err)
		}
		var actual, expected any
		if json.Unmarshal(encoded, &actual) != nil || json.Unmarshal(vectors.Public[name], &expected) != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("public %s differs from independent whitelist fixture", name)
		}
		projected = append(projected, string(encoded))
	}
	source := fmt.Sprintf(`import type { BetaManagedAgentsStreamSessionEvents, BetaManagedAgentsSessionEvent, EventStreamParams as SessionParams } from %q;
import type { EventStreamParams as ThreadParams } from %q;
type Start = Extract<BetaManagedAgentsStreamSessionEvents, {type:'event_start'}>;
type Delta = Extract<BetaManagedAgentsStreamSessionEvents, {type:'event_delta'}>;
type Assert<T extends true> = T;
type Equal<A,B> = (<T>()=>T extends A?1:2) extends (<T>()=>T extends B?1:2)?true:false;
type StartKeys = Assert<Equal<keyof Start,'type'|'event'>>;
type StartBodyKeys = Assert<Equal<keyof Start['event'],'id'|'type'>>;
type DeltaKeys = Assert<Equal<keyof Delta,'type'|'event_id'|'delta'>>;
type ThreadNoPreview = Assert<Equal<Extract<keyof ThreadParams,'event_deltas'>,never>>;
type HistoryNoPreview = Assert<Equal<Extract<BetaManagedAgentsSessionEvent,{type:'event_start'|'event_delta'}>,never>>;
const previews = [%s] satisfies readonly BetaManagedAgentsStreamSessionEvents[];
const on = {event_deltas:['agent.message','agent.thinking','agent.message']} satisfies SessionParams;
const off = {} satisfies SessionParams;
const thread = {session_id:'session'} satisfies ThreadParams;
void previews; void on; void off; void thread;
`, filepath.ToSlash(filepath.Join(sdk, "src/resources/beta/sessions/events.ts")), filepath.ToSlash(filepath.Join(sdk, "src/resources/beta/sessions/threads/events.ts")), strings.Join(projected, ","))
	runForkSDKFixtureTypecheck(t, engine, "public-preview-fork-sdk.ts", source)
}
