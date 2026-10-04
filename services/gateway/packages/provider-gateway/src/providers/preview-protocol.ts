import limits from "@tetral/gateway-protocol/src/preview-limits.json";
import { validProviderEventId, MaxIdBytes } from "@tetral/gateway-protocol/src/bounds.js";

export const PreviewLimits = Object.freeze(limits);
export interface PreviewIdentity {
  readonly workspace_id: string;
  readonly session_id: string;
  readonly thread_id: string;
  readonly model_request_id: string;
  readonly model_request_start_event_id: string;
  readonly request_kind: "agent_provider_request";
}
export type PreviewFrame = PreviewIdentity & { readonly version: 1 } & (
  | { readonly kind: "request_open" }
  | { readonly kind: "event_start"; readonly event_type: "agent.message" | "agent.thinking"; readonly event_id: string; readonly preview_sequence: 0 }
  | { readonly kind: "event_delta"; readonly event_type: "agent.message"; readonly event_id: string; readonly preview_sequence: number; readonly text: string }
);
const encoder = new TextEncoder();
export function isScalarText(text: string): boolean {
  for (let index = 0; index < text.length; index++) {
    const value = text.charCodeAt(index);
    if (value >= 0xd800 && value <= 0xdbff) {
      const low = text.charCodeAt(++index);
      if (!(low >= 0xdc00 && low <= 0xdfff)) return false;
    } else if (value >= 0xdc00 && value <= 0xdfff) return false;
  }
  return true;
}
function validId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= MaxIdBytes && isScalarText(value) && encoder.encode(value).length <= MaxIdBytes && !/[\x00-\x1f\x7f]/.test(value);
}
export function previewSubject(workspace: string, session: string): string {
  if (!validId(workspace) || !validId(session)) throw new Error("invalid preview scope");
  return `preview.v1.${Buffer.from(workspace, "utf8").toString("base64url")}.${Buffer.from(session, "utf8").toString("base64url")}`;
}
/** Strict private schema. Public wrappers are projected separately by Event Stream. */
function validateFrame(frame: PreviewFrame): void {
  if (frame.kind === "event_delta" && (typeof frame.text !== "string" || frame.text.length > PreviewLimits.maxFrameBytes)) throw new Error("preview frame too large");
  const base = ["version", "workspace_id", "session_id", "thread_id", "model_request_id", "model_request_start_event_id", "request_kind", "kind"];
  const event = ["event_type", "event_id", "preview_sequence"];
  const allowed = frame.kind === "request_open" ? base : [...base, ...event, ...(frame.kind === "event_delta" ? ["text"] : [])];
  if (Object.keys(frame).length !== allowed.length || Object.keys(frame).some(key => !allowed.includes(key)) || frame.version !== 1 || frame.request_kind !== "agent_provider_request" ||
    ![frame.workspace_id, frame.session_id, frame.thread_id, frame.model_request_id, frame.model_request_start_event_id].every(validId)) throw new Error("invalid preview identity");
  if (frame.kind !== "request_open") {
    if (!validProviderEventId(frame.event_id) || (frame.event_type !== "agent.message" && frame.event_type !== "agent.thinking") || !Number.isSafeInteger(frame.preview_sequence)) throw new Error("invalid preview event");
    if (frame.kind === "event_start" && frame.preview_sequence !== 0) throw new Error("invalid preview start");
    if (frame.kind === "event_delta" && (frame.event_type !== "agent.message" || frame.preview_sequence <= 0 || typeof frame.text !== "string" || !isScalarText(frame.text) || frame.text.length === 0)) throw new Error("invalid preview delta");
    if (frame.kind !== "event_start" && frame.kind !== "event_delta") throw new Error("invalid preview kind");
  }
}
/** Walk the fixed scalar JSON schema without creating a serialized string. */
function walkJSONString(value: string, byte: (value: number) => void): void {
  byte(34);
  for (let index = 0; index < value.length; index++) {
    let code = value.charCodeAt(index);
    if (code === 34 || code === 92) { byte(92); byte(code); }
    else if (code < 32) {
      byte(92);
      const short = code === 8 ? 98 : code === 9 ? 116 : code === 10 ? 110 : code === 12 ? 102 : code === 13 ? 114 : 0;
      if (short) byte(short);
      else { byte(117); byte(48); byte(48); byte("0123456789abcdef".charCodeAt(code >> 4)); byte("0123456789abcdef".charCodeAt(code & 15)); }
    } else {
      if (code >= 0xd800 && code <= 0xdbff) code = 0x10000 + ((code - 0xd800) << 10) + value.charCodeAt(++index) - 0xdc00;
      if (code < 0x80) byte(code);
      else if (code < 0x800) { byte(0xc0 | (code >> 6)); byte(0x80 | (code & 63)); }
      else if (code < 0x10000) { byte(0xe0 | (code >> 12)); byte(0x80 | ((code >> 6) & 63)); byte(0x80 | (code & 63)); }
      else { byte(0xf0 | (code >> 18)); byte(0x80 | ((code >> 12) & 63)); byte(0x80 | ((code >> 6) & 63)); byte(0x80 | (code & 63)); }
    }
  }
  byte(34);
}
function walkFrame(frame: PreviewFrame, byte: (value: number) => void): void {
  byte(123);
  let first = true;
  for (const [key, value] of Object.entries(frame)) {
    if (!first) byte(44); first = false;
    walkJSONString(key, byte); byte(58);
    if (typeof value === "string") walkJSONString(value, byte);
    else {
      const number = String(value);
      for (let index = 0; index < number.length; index++) byte(number.charCodeAt(index));
    }
  }
  byte(125);
}
/** Exact UTF-8 size before allocating the sole encoded frame buffer. */
export function previewFrameEncodedSize(frame: PreviewFrame): number {
  validateFrame(frame);
  let size = 0;
  walkFrame(frame, () => { if (++size > PreviewLimits.maxFrameBytes) throw new Error("preview frame too large"); });
  return size;
}
export function encodePreviewFrame(frame: PreviewFrame): Uint8Array {
  const data = new Uint8Array(previewFrameEncodedSize(frame));
  let position = 0;
  walkFrame(frame, byte => { data[position++] = byte; });
  return data;
}
