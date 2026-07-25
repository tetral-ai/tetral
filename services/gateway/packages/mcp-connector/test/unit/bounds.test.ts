import { describe, expect, test } from "bun:test";
import { McpErrorKind, McpRetryStatus, RunMcpToolStatus } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { validateListMcpToolsResponse, validatePendingRunMcpToolResponse } from "../../src/bounds.js";
import { MCP_BLOB_MAX_BYTES } from "../../src/formatter.js";

describe("MCP connector response bounds", () => {
  test("rejects multiple inline attachments", () => {
    expect(validatePendingRunMcpToolResponse({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText: "two attachments",
      attachments: [attachment(new Uint8Array([1])), attachment(new Uint8Array([2]))],
    })).toEqual({ ok: false, message: "invalid internal request" });
  });

  test("rejects aggregate inline bytes above the 10 MiB cap", () => {
    expect(validatePendingRunMcpToolResponse({
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
      resultText: "oversized attachment",
      attachments: [attachment(new Uint8Array(MCP_BLOB_MAX_BYTES + 1))],
    })).toEqual({ ok: false, message: "invalid internal request" });
  });

  test("accepts internal runtime failures only without retry status", () => {
    const response = {
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "MCP connector failed.",
      attachments: [],
      errorKind: McpErrorKind.MCP_ERROR_KIND_INTERNAL,
    };

    expect(validatePendingRunMcpToolResponse(response)).toEqual({ ok: true });
    expect(validatePendingRunMcpToolResponse({
      ...response,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_EXHAUSTED,
    })).toEqual({ ok: false, message: "invalid internal request" });
  });

  test("accepts only the literal platform-collision omission reason", () => {
    const response = {
      tools: [],
      manifestEtag: "a".repeat(64),
      omittedTools: [{ name: "memory", reason: "builtin_name_collision" }],
    };

    expect(validateListMcpToolsResponse(response)).toEqual({ ok: true });
    expect(validateListMcpToolsResponse({
      ...response,
      omittedTools: [{ name: "memory", reason: "some_other_reason" }],
    })).toEqual({ ok: false, message: "invalid internal request" });
  });
});

function attachment(data: Uint8Array) {
  return { data, mime: "image/png", suggestedFilename: "image.png" };
}
