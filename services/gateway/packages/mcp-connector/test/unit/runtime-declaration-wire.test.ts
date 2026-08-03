import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { mcpMaterializationDeclarationDigest } from "../../src/bridge-client.js";

interface SharedMcpMaterializationVector {
  readonly session_thread_id: string;
  readonly tool_use_event_id: string;
  readonly normalized_input_hash: string;
  readonly mcp_server_name: string;
  readonly tool_name: string;
  readonly input_json: string;
  readonly result_json: string;
  readonly inline_media: readonly {
    readonly data: string;
    readonly mime: string;
    readonly suggested_filename: string;
  }[];
  readonly canonical_json: string;
  readonly digest: string;
}

describe("MCP Connector shared declaration vector", () => {
  test("mcp_materialization", () => {
    const corpus = JSON.parse(readFileSync(
      resolve(import.meta.dir, "../../../../../bridge/testdata/runtime_declaration_vectors.json"),
      "utf8",
    )) as { readonly mcp_materialization: SharedMcpMaterializationVector };
    const vector = corpus.mcp_materialization;
    expect(createHash("sha256").update(vector.canonical_json, "utf8").digest("hex")).toBe(vector.digest);

    expect(mcpMaterializationDeclarationDigest({
      scope: {
        requestId: "",
        workspaceId: "",
        sessionId: "",
        sessionThreadId: vector.session_thread_id,
        binding: undefined,
      },
      toolUseEventId: vector.tool_use_event_id,
      normalizedInputHash: vector.normalized_input_hash,
      mcpServerName: vector.mcp_server_name,
      toolName: vector.tool_name,
      inputJson: vector.input_json,
      resultJson: vector.result_json,
      inlineMedia: vector.inline_media.map((media) => ({
        data: Uint8Array.from(Buffer.from(media.data, "base64")),
        mime: media.mime,
        suggestedFilename: media.suggested_filename,
      })),
    })).toBe(vector.digest);
  });
});
