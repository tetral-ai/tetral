/** Fixed external provider output. IDs are allocated by the real Gateway. */
import vectors from "./public-streaming.json";
export type PublicStreamingScenario = "public-text" | "public-load" | "public-cycle" | "public-error" | "public-incomplete" | "public-approval" | "public-unicode-single" | "public-unicode-interleaved" | "public-unicode-healthy";
const event = (value: Record<string, unknown>): string => `event: ${value.type}\ndata: ${JSON.stringify(value)}\n\n`;
const start = (index: number) => event({ type: "content_block_start", index, content_block: { type: "text", text: "" } });
const delta = (index: number, text: string) => event({ type: "content_block_delta", index, delta: { type: "text_delta", text } });
const stop = (index: number) => event({ type: "content_block_stop", index });
export function* publicStreamingScript(scenario: PublicStreamingScenario): Generator<string> {
  yield event({ type: "message_start", message: { id: "msg_public_fixture", type: "message", role: "assistant", model: "claude-opus-4-8", content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 1, output_tokens: 1 } } });
  if (scenario === "public-load" || scenario === "public-cycle") {
    yield start(0);
    for (let index=0; index<32; index++) yield delta(0,scenario === "public-load" ? "x".repeat(96*1024) : "cycle ");
    yield stop(0);
  } else if (scenario.startsWith("public-unicode")) {
    yield start(0);
    if (scenario === "public-unicode-single") {
      for (const text of vectors.unicode.single.fragments) yield delta(0, text);
    } else {
      yield start(1);
      if (scenario === "public-unicode-interleaved") {
        for (const fragment of vectors.unicode.interleaved.fragments) yield delta(fragment.block === "A" ? 0 : 1, fragment.text);
      } else {
        for (const [index, block] of vectors.unicode.healthy.entries()) for (const text of block.fragments) yield delta(index, text);
      }
      yield stop(1);
    }
    yield stop(0);
  } else {
    yield event({ type: "content_block_start", index: 0, content_block: { type: "thinking", thinking: "", signature: "" } });
    yield event({ type: "content_block_delta", index: 0, delta: { type: "thinking_delta", thinking: vectors.content.reasoning_text } });
    yield event({ type: "content_block_delta", index: 0, delta: { type: "signature_delta", signature: vectors.content.reasoning_signature } });
    yield stop(0);
    yield start(1);
    for (const text of vectors.content.first_fragments) yield delta(1, text);
    yield stop(1);
    if (scenario === "public-approval") {
      yield event({ type: "content_block_start", index: 2, content_block: { type: "tool_use", id: "public-write", name: "Write", input: {} } });
      yield event({ type: "content_block_delta", index: 2, delta: { type: "input_json_delta", partial_json: JSON.stringify({ file_path: "/workspace/public-preview.txt", content: vectors.content.tool_input_marker }) } });
      yield stop(2);
    } else {
      yield start(2);
      for (const text of vectors.content.second_fragments) yield delta(2, text);
      yield stop(2);
      yield start(3); // Empty blocks must not invent a durable message.
      yield stop(3);
      if (scenario === "public-error" || scenario === "public-incomplete") {
        yield start(4);
        yield delta(4, vectors.content.incomplete);
        yield "event: fixture_resource_gate\ndata: {}\n\n";
        if (scenario === "public-error") throw new TypeError("controlled public provider failure");
        return;
      }
    }
  }
  yield event({ type: "message_delta", delta: { stop_reason: scenario === "public-approval" ? "tool_use" : "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } });
  yield event({ type: "message_stop" });
}
