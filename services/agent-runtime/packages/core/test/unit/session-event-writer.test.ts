import { describe, expect, test } from "bun:test";
import type { SessionEventEnvelope, SessionEventWriterAppendResult } from "../../src/contracts/runtime.js";
import { SessionEventWriterRetryPolicy } from "../../src/contracts/runtime.js";
import { createSessionEventWriter } from "../../src/runtime/session-event-writer.js";

const createdAt = "2026-06-14T00:00:00.000Z";
const hostileText = "UNIT5_DUMMY_TOKEN_CANARY authorization: bearer raw-secret raw provider payload marker";

function envelope(writeId = "write-1"): SessionEventEnvelope {
  return {
    workspaceId: "workspace-1",
    sessionId: "session-1",
    sessionThreadId: "thread-1",
    writeId,
    event: { type: "agent.message", content: [{ type: "text", text: "hello" }] },
  };
}

describe("SessionEventWriter boundary", () => {
  test("append request excludes final Bridge id fields and success returns eventId", async () => {
    const requests: unknown[] = [];
    const writer = createSessionEventWriter({
      append: async (request) => {
        requests.push(request);
        return { ok: true, writeId: "write-1", eventId: "bridge-event-1", processedAt: createdAt };
      },
      sleep: async () => true,
    });

    const result = await writer.append(envelope());

    expect(result).toEqual({ ok: true, writeId: "write-1", eventId: "bridge-event-1", processedAt: createdAt });
    expect(requests).toEqual([envelope()]);
    expect(JSON.stringify(requests)).not.toContain("bridge-event-1");
    expect(JSON.stringify(requests)).not.toContain("processedAt");
    expect(JSON.stringify(requests)).not.toContain("processed_at");
  });

  test("retry reuses the same writeId and ack mismatch fails closed", async () => {
    const writeIds: string[] = [];
    const writer = createSessionEventWriter({
      append: async (request): Promise<SessionEventWriterAppendResult> => {
        writeIds.push(request.writeId);
        if (writeIds.length === 1) {
          return {
            ok: false,
            error: { type: "session-event-writer", code: "unavailable", message: "try later", retryable: true, fatal: false, writeId: request.writeId },
          };
        }
        return { ok: true, writeId: "different-write", eventId: "bridge-event-2", processedAt: createdAt };
      },
      sleep: async () => true,
    });

    const result = await writer.append(envelope("stable-write"));

    expect(writeIds).toEqual(["stable-write", "stable-write"]);
    expect(result).toEqual({
      ok: false,
      error: expect.objectContaining({
        type: "session-event-writer",
        code: "ack_mismatch",
        retryable: false,
        fatal: true,
        writeId: "stable-write",
      }),
    });
  });

  test("hung append attempts time out and retry with the stable writeId", async () => {
    const writeIds: string[] = [];
    const sleeps: number[] = [];
    const writer = createSessionEventWriter({
      append: (request) => {
        writeIds.push(request.writeId);
        return new Promise<never>(() => undefined);
      },
      sleep: async (durationMs) => {
        sleeps.push(durationMs);
        return true;
      },
    });

    const result = await writer.append(envelope("timeout-write"));

    expect(writeIds).toEqual(["timeout-write", "timeout-write", "timeout-write"]);
    expect(sleeps).toEqual([3000, 100, 3000, 300, 3000]);
    expect(result).toEqual({
      ok: false,
      error: expect.objectContaining({
        type: "session-event-writer",
        code: "timeout",
        retryable: true,
        fatal: false,
        writeId: "timeout-write",
      }),
    });
  });

  test("invalid envelopes and unsupported terminal payloads fail before transport append", async () => {
    const requests: unknown[] = [];
    const writer = createSessionEventWriter({
      append: async (request) => {
        requests.push(request);
        return { ok: true, writeId: request.writeId, eventId: "bridge-event", processedAt: createdAt };
      },
      sleep: async () => true,
    });

    await expect(writer.append({
      ...envelope("wrong-session"),
      sessionId: "",
    } as unknown as SessionEventEnvelope)).rejects.toThrow();
    await expect(writer.append({
      ...envelope("bad-stop"),
      event: { type: "session.status_idle", stop_reason: { type: "unsupported" } },
    } as unknown as SessionEventEnvelope)).rejects.toThrow();
    await expect(writer.append({
      ...envelope("bad-error"),
      event: {
        type: "session.error",
        error: {
          type: "provider",
          code: "provider_unknown",
          message: "failed",
          retryable: false,
          fatal: true,
          retryStatus: { type: "again" },
        },
      },
    } as unknown as SessionEventEnvelope)).rejects.toThrow();

    expect(requests).toEqual([]);
  });

  test("append bounds hostile text before transport and terminal append failure is not recursive", async () => {
    const requests: unknown[] = [];
    const writer = createSessionEventWriter({
      append: async (request): Promise<SessionEventWriterAppendResult> => {
        requests.push(request);
        return {
          ok: false,
          error: { type: "session-event-writer", code: "unavailable", message: hostileText, retryable: false, fatal: false, writeId: request.writeId },
        };
      },
      sleep: async () => true,
    });

    const result = await writer.append({
      ...envelope("terminal-write"),
      event: {
        type: "session.error",
        error: {
          type: "provider",
          code: "provider_unknown",
          message: hostileText,
          retryable: false,
          fatal: true,
        },
      },
    });

    expect(result).toEqual({
      ok: false,
      error: expect.objectContaining({ type: "session-event-writer", code: "unavailable", writeId: "terminal-write" }),
    });
    expect(requests).toHaveLength(1);
    expect(JSON.stringify(requests)).not.toContain("UNIT5_DUMMY_TOKEN_CANARY");
    expect(JSON.stringify(requests)).not.toContain("authorization: bearer raw-secret");
    expect(JSON.stringify(requests)).not.toContain("raw provider payload marker");
  });

  test("retry constants are fixed at the runtime event-writer boundary", () => {
    expect(SessionEventWriterRetryPolicy).toEqual({ attempts: 3, timeoutPerAttemptMs: 3000, backoffMs: [100, 300] });
  });
});
