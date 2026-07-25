import { describe, expect, test } from "bun:test";
import {
  formatMcpToolResult,
  MCP_BLOB_MAX_BYTES,
  MCP_RESULT_TEXT_MAX_BYTES,
  MCP_RESULT_TEXT_MAX_LINES,
  MCP_RESULT_TEXT_TRUNCATED_MARKER,
} from "../../src/formatter.js";

describe("MCP formatter", () => {
  test("formats text content", () => {
    const formatted = formatMcpToolResult({ content: [{ type: "text", text: "hello" }] });

    expect(formatted).toEqual({ resultText: "hello", attachments: [] });
  });

  test("formats image content as inline attachment", () => {
    const formatted = formatMcpToolResult({
      content: [{ type: "image", data: Buffer.from("img").toString("base64"), mimeType: "image/png" }],
    });

    expect(formatted.resultText).toBe("[MCP attachment: mcp-image-1.png]");
    expect(formatted.attachments).toHaveLength(1);
    expect(formatted.attachments[0]).toMatchObject({ mime: "image/png", suggestedFilename: "mcp-image-1.png" });
    expect(Buffer.from(formatted.attachments[0]!.data).toString()).toBe("img");
  });

  test("emits at most one inline attachment and explains later omissions", () => {
    const formatted = formatMcpToolResult({
      content: [
        { type: "image", data: Buffer.from("first").toString("base64"), mimeType: "image/png" },
        { type: "image", data: Buffer.from("second").toString("base64"), mimeType: "image/jpeg" },
        {
          type: "resource",
          resource: {
            uri: "https://example.com/third.pdf",
            blob: Buffer.from("third").toString("base64"),
            mimeType: "application/pdf",
          },
        },
      ],
    });

    expect(formatted.attachments).toHaveLength(1);
    expect(Buffer.from(formatted.attachments[0]!.data).toString()).toBe("first");
    expect(formatted.resultText.match(/attachment_limit/g)).toHaveLength(2);
  });

  test("formats text resources into result text", () => {
    const formatted = formatMcpToolResult({
      content: [{ type: "resource", resource: { uri: "file:///issue.md", text: "issue body" } }],
    });

    expect(formatted).toEqual({ resultText: "issue body", attachments: [] });
  });

  test("formats allowlisted blob resources as inline attachments", () => {
    const formatted = formatMcpToolResult({
      content: [{
        type: "resource",
        resource: { uri: "https://example.com/report.pdf", blob: Buffer.from("pdf").toString("base64"), mimeType: "application/pdf" },
      }],
    });

    expect(formatted.resultText).toBe("[MCP attachment: report.pdf]");
    expect(formatted.attachments).toHaveLength(1);
    expect(formatted.attachments[0]).toMatchObject({ mime: "application/pdf", suggestedFilename: "report.pdf" });
  });

  test("omits unsupported blob resources with a model-visible reason", () => {
    const formatted = formatMcpToolResult({
      content: [{
        type: "resource",
        resource: { uri: "https://example.com/archive.zip", blob: Buffer.from("zip").toString("base64"), mimeType: "application/zip" },
      }],
    });

    expect(formatted.attachments).toHaveLength(0);
    expect(formatted.resultText).toContain("[Binary MCP resource omitted: https://example.com/archive.zip (application/zip, 3)");
    expect(formatted.resultText).toContain("mime_not_allowed");

    const oversized = formatMcpToolResult({
      content: [{
        type: "resource",
        resource: { uri: "https://example.com/large.png", blob: Buffer.alloc(MCP_BLOB_MAX_BYTES + 1).toString("base64"), mimeType: "image/png" },
      }],
    });

    expect(oversized.attachments).toHaveLength(0);
    expect(oversized.resultText).toContain("too_large");
  });

  test("omits unsupported content types", () => {
    const formatted = formatMcpToolResult({ content: [{ type: "audio", data: "..." }] });

    expect(formatted).toEqual({ resultText: "[Unsupported MCP content omitted: audio]", attachments: [] });
  });

  test("uses structuredContent only when text content is absent", () => {
    expect(formatMcpToolResult({ content: [], structuredContent: { b: 2, a: 1 } }).resultText).toBe(`{"a":1,"b":2}`);
    expect(formatMcpToolResult({
      content: [{ type: "text", text: `{"a":1}` }],
      structuredContent: { a: 1 },
    }).resultText).toBe(`{"a":1}`);
  });

  test("truncates text content at the runtime result byte cap with a visible marker", () => {
    const formatted = formatMcpToolResult({ content: [{ type: "text", text: "x".repeat(MCP_RESULT_TEXT_MAX_BYTES + 100) }] });

    expect(Buffer.byteLength(formatted.resultText, "utf8")).toBeLessThanOrEqual(MCP_RESULT_TEXT_MAX_BYTES);
    expect(formatted.resultText).toContain(MCP_RESULT_TEXT_TRUNCATED_MARKER);
  });

  test("truncates text content at the runtime result line cap with a visible marker", () => {
    const formatted = formatMcpToolResult({
      content: [{ type: "text", text: Array.from({ length: MCP_RESULT_TEXT_MAX_LINES + 5 }, (_, index) => `line-${index}`).join("\n") }],
    });

    expect(formatted.resultText).toContain("line-1999");
    expect(formatted.resultText).not.toContain("line-2000");
    expect(formatted.resultText).toContain(MCP_RESULT_TEXT_TRUNCATED_MARKER);
  });
});
