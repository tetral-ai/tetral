/**
 * @packageDocumentation
 *
 * Converts SDK-shaped MCP tool results into bounded model-visible text and at
 * most one in-memory media attachment for the connector's commit path. Text is
 * truncated at byte and line thresholds with a visible marker, accepted media
 * is MIME allowlisted and bounded by decoded size, and unsupported content or
 * excess valid media produces explicit omission text. Disallowed or oversized
 * image blocks fail formatting, while disallowed or oversized blob resources
 * produce omission text. Base64 decoding follows the platform's permissive
 * decoder; a zero-byte decoded attachment is rejected later by response-bound
 * validation. The connector service calls the formatter after an MCP client
 * response; the formatter calls the canonical JSON encoder and platform
 * base64, text, and URL primitives.
 */

import { canonicalJson } from "./idempotency.js";

/** Sets the maximum decoded bytes carried by the formatter's single media attachment. */
export const MCP_BLOB_MAX_BYTES = 10 * 1024 * 1024;
/**
 * Sets the UTF-8 byte threshold at which model-visible MCP result text is
 * truncated. Repairing a sliced partial code point can leave marked output up
 * to two bytes above this threshold. UPDATE-WITH:
 * MaxProviderRequestToolOutputJsonBytes in gateway-protocol bounds; even sixfold
 * JSON escaping of this producer cap must fit that canonical consumer bound.
 */
export const MCP_RESULT_TEXT_MAX_BYTES = 50 * 1024;
/** Sets the maximum number of source lines retained before a truncation-marker line is appended. */
export const MCP_RESULT_TEXT_MAX_LINES = 2000;
/** Marks formatter output whenever byte or line bounds truncate visible text. */
export const MCP_RESULT_TEXT_TRUNCATED_MARKER = "[MCP result truncated: visible output capped at 51200 bytes or 2000 lines.]";
/** Contains the media MIME types that the formatter accepts as inline attachments. */
export const MCP_ATTACHMENT_MIME_ALLOWLIST = new Set([
  "image/png",
  "image/jpeg",
  "image/gif",
  "image/webp",
  "application/pdf",
]);

/**
 * Represents the MCP call-result fields consumed by the connector formatter.
 * The client supplies this shape and the service passes it through unchanged.
 */
export interface McpCallToolResult {
  readonly isError?: boolean | undefined;
  readonly content?: readonly McpContentItem[] | undefined;
  readonly structuredContent?: unknown;
  readonly refreshTriggered?: boolean | undefined;
}

/**
 * Represents known text, image, and resource blocks while retaining unknown MCP
 * content variants so the formatter can emit deterministic omission text.
 */
export type McpContentItem =
  | { readonly type: "text"; readonly text: string }
  | { readonly type: "image"; readonly data: string; readonly mimeType: string }
  | { readonly type: "resource"; readonly resource: McpTextResource | McpBlobResource }
  | { readonly type: string; readonly [key: string]: unknown };

/** Represents a resource whose UTF-8 text is appended to model-visible output. */
export interface McpTextResource {
  readonly uri: string;
  readonly text: string;
  readonly mimeType?: string | undefined;
}

/** Represents a base64 resource considered for the single media attachment. */
export interface McpBlobResource {
  readonly uri: string;
  readonly blob: string;
  readonly mimeType?: string | undefined;
}

/** Contains truncated model-visible text and zero or one decoded media payload. */
export interface FormattedMcpResult {
  readonly resultText: string;
  readonly attachments: readonly McpInlineMedia[];
}

/**
 * Carries decoded media in memory until the connector hands it to the durable
 * result commit path, which replaces bytes with an attachment reference.
 */
export interface McpInlineMedia {
  readonly data: Uint8Array;
  readonly mime: string;
  readonly suggestedFilename: string;
}

/**
 * Formats MCP content in source order, emits placeholders and omission notices,
 * and uses canonical structured content only when no rendered text accumulated.
 *
 * @throws {Error} when an image MIME is disallowed or decoded image bytes exceed
 * the attachment byte cap.
 */
export function formatMcpToolResult(result: McpCallToolResult): FormattedMcpResult {
  const text: string[] = [];
  const attachments: McpInlineMedia[] = [];
  for (const item of result.content ?? []) {
    switch (item.type) {
      case "text": {
        const textItem = item as { readonly type: "text"; readonly text: string };
        if (textItem.text.length > 0) {
          text.push(textItem.text);
        }
        break;
      }
      case "image": {
        const imageItem = item as { readonly type: "image"; readonly data: string; readonly mimeType: string };
        const attachment = inlineAttachment(imageItem.data, imageItem.mimeType, `mcp-image-${attachments.length + 1}`);
        if (attachments.length >= 1 || attachment.data.byteLength > MCP_BLOB_MAX_BYTES - attachmentBytes(attachments)) {
          text.push(binaryOmission(`mcp:image:${attachments.length + 1}`, imageItem.mimeType, attachment.data.byteLength, "attachment_limit"));
          break;
        }
        attachments.push(attachment);
        text.push(`[MCP attachment: ${attachment.suggestedFilename}]`);
        break;
      }
      case "resource": {
        const resourceItem = item as { readonly type: "resource"; readonly resource: McpTextResource | McpBlobResource };
        formatResource(resourceItem.resource, text, attachments);
        break;
      }
      default:
        text.push(`[Unsupported MCP content omitted: ${item.type}]`);
        break;
    }
  }
  if (text.length === 0 && result.structuredContent !== undefined) {
    text.push(canonicalJson(result.structuredContent));
  }
  return {
    resultText: boundResultText(text.join("\n")),
    attachments,
  };
}

function boundResultText(value: string): string {
  let output = value;
  let truncated = false;
  const lines = output.split("\n");
  if (lines.length > MCP_RESULT_TEXT_MAX_LINES) {
    output = lines.slice(0, MCP_RESULT_TEXT_MAX_LINES).join("\n");
    truncated = true;
  }
  const encoded = new TextEncoder().encode(output);
  if (encoded.byteLength > MCP_RESULT_TEXT_MAX_BYTES) {
    output = new TextDecoder().decode(encoded.slice(0, MCP_RESULT_TEXT_MAX_BYTES));
    truncated = true;
  }
  if (!truncated) {
    return output;
  }
  const marker = `\n${MCP_RESULT_TEXT_TRUNCATED_MARKER}`;
  const markerBytes = new TextEncoder().encode(marker);
  const outputBytes = new TextEncoder().encode(output);
  const prefixBudget = Math.max(0, MCP_RESULT_TEXT_MAX_BYTES - markerBytes.byteLength);
  return `${new TextDecoder().decode(outputBytes.slice(0, prefixBudget))}${marker}`;
}

function formatResource(resource: McpTextResource | McpBlobResource, text: string[], attachments: McpInlineMedia[]): void {
  if ("text" in resource) {
    if (resource.text.length > 0) {
      text.push(resource.text);
    }
    return;
  }
  const mime = resource.mimeType ?? "application/octet-stream";
  const bytes = decodeBase64(resource.blob);
  if (!MCP_ATTACHMENT_MIME_ALLOWLIST.has(mime)) {
    text.push(binaryOmission(resource.uri, mime, bytes.byteLength, "mime_not_allowed"));
    return;
  }
  if (bytes.byteLength > MCP_BLOB_MAX_BYTES) {
    text.push(binaryOmission(resource.uri, mime, bytes.byteLength, "too_large"));
    return;
  }
  if (attachments.length >= 1 || bytes.byteLength > MCP_BLOB_MAX_BYTES - attachmentBytes(attachments)) {
    text.push(binaryOmission(resource.uri, mime, bytes.byteLength, "attachment_limit"));
    return;
  }
  const attachment = {
    data: bytes,
    mime,
    suggestedFilename: suggestedResourceFilename(resource.uri, mime, attachments.length + 1),
  };
  attachments.push(attachment);
  text.push(`[MCP attachment: ${attachment.suggestedFilename}]`);
}

function attachmentBytes(attachments: readonly McpInlineMedia[]): number {
  return attachments.reduce((total, attachment) => total + attachment.data.byteLength, 0);
}

function inlineAttachment(base64: string, mime: string, nameBase: string): McpInlineMedia {
  const bytes = decodeBase64(base64);
  if (!MCP_ATTACHMENT_MIME_ALLOWLIST.has(mime)) {
    throw new Error("MCP image mime is not in the attachment allowlist");
  }
  if (bytes.byteLength > MCP_BLOB_MAX_BYTES) {
    throw new Error("MCP image exceeds the attachment byte cap");
  }
  return {
    data: bytes,
    mime,
    suggestedFilename: `${nameBase}${extensionForMime(mime)}`,
  };
}

function decodeBase64(base64: string): Uint8Array {
  return Uint8Array.from(Buffer.from(base64, "base64"));
}

function binaryOmission(uri: string, mime: string, size: number, reason: string): string {
  return `[Binary MCP resource omitted: ${uri} (${mime}, ${size}) \u2014 ${reason}]`;
}

function suggestedResourceFilename(uri: string, mime: string, index: number): string {
  try {
    const parsed = new URL(uri);
    const leaf = parsed.pathname.split("/").filter(Boolean).at(-1);
    if (leaf !== undefined && leaf.length > 0) {
      return leaf;
    }
  } catch {
    // Fall through to a stable synthetic name.
  }
  return `mcp-resource-${index}${extensionForMime(mime)}`;
}

function extensionForMime(mime: string): string {
  switch (mime) {
    case "image/png":
      return ".png";
    case "image/jpeg":
      return ".jpg";
    case "image/gif":
      return ".gif";
    case "image/webp":
      return ".webp";
    case "application/pdf":
      return ".pdf";
    default:
      return ".bin";
  }
}
