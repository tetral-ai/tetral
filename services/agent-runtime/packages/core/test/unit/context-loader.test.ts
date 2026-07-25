import { describe, expect, test } from "bun:test";
import type {
  ContextLoaderError,
  PendingInputResult,
  RuntimeMessageInfo,
  RuntimeMessageStoreOperationControls,
  RuntimeMessageStoreWriteMessageResult,
  RuntimeMessageStoreWritePartResult,
  RuntimePart,
} from "../../src/contracts/runtime.js";
import { RuntimeMessageSchema, RuntimeMessageStore } from "../../src/contracts/runtime.js";
import { ContextLoaderBoundary, ContextLoaderBounds } from "../../src/context/context-loader.js";
import {
  buildContextLoaderTextMessage as textMessage,
  buildContextLoaderAssistantToolMessage as assistantToolMessage,
  buildContextLoaderStructuralAssistantMessage as structuralAssistantMessage,
  buildContextLoaderAbortedVisibleAssistantMessage as abortedVisibleAssistantMessage,
  buildContextLoaderFailedVisibleAssistantMessage as failedVisibleAssistantMessage,
  buildContextLoaderAssistantMessageWithUsage as assistantMessageWithUsage,
  buildContextLoaderStreamingAssistantMessage as streamingAssistantMessage,
} from "./runtime-message-builders.js";

const createdAt = "2026-06-14T00:00:00.000Z";
const hostileText = [
  "UNIT3_DUMMY_TOKEN_CANARY",
  "select secret from context",
  "postgres://user:pass@example.invalid/db",
  "authorization: bearer raw-secret",
  "system prompt raw backend payload marker",
  "raw backend payload marker",
  "raw provider payload marker",
].join(" ");

const forbiddenHostileFragments = [
  "UNIT3_DUMMY_TOKEN_CANARY",
  "select secret from context",
  "postgres://user:pass@example.invalid/db",
  "authorization: bearer raw-secret",
  "system prompt raw backend payload marker",
  "raw backend payload marker",
  "raw provider payload marker",
] as const;

function systemMessage(): unknown {
  return {
    id: "system-1",
    sessionId: "sesn_1",
    role: "system",
    sequence: 0,
    status: "completed",
    createdAt,
    parts: [
      {
        id: "system-1-text",
        sessionId: "sesn_1",
        messageId: "system-1",
        sequence: 0,
        type: "text",
        text: "system prompt",
        truncated: false,
        status: "completed",
        createdAt,
      },
    ],
  };
}

function assistantToolMessageWithUnsafeHeaders(options: { readonly unsafeValue: boolean; readonly preview: string; readonly value?: unknown }): unknown {
  const message = JSON.parse(JSON.stringify(assistantToolMessage("running")));
  message.parts[0].state.input.value = options.unsafeValue
    ? {
        headers: {
          authorization: "Bearer raw-secret",
          "x-api-key": "raw-key",
        },
      }
    : (options.value ?? { q: "safe" });
  message.parts[0].state.input.preview = options.preview;
  return message;
}

function pendingUserMessageWithUnsafeToolHeaders(options: { readonly unsafeValue: boolean; readonly preview: string; readonly value?: unknown }): unknown {
  return RuntimeMessageSchema.parse({
    id: "user-tool-headers",
    sessionId: "sesn_1",
    role: "user",
    origin: "user",
    sequence: 0,
    status: "completed",
    createdAt,
    providerId: "fake",
    modelId: "fake-chat",
    parts: [
      {
        id: "user-tool-headers-part",
        sessionId: "sesn_1",
        messageId: "user-tool-headers",
        sequence: 0,
        type: "tool",
        toolCallId: "tool-call-headers",
        toolName: "lookup",
        state: {
          status: "running",
          input: {
            value: options.unsafeValue
              ? {
                  headers: {
                    authorization: "Bearer raw-secret",
                    "x-api-key": "raw-key",
                  },
                }
              : (options.value ?? { q: "safe" }),
            preview: options.preview,
            truncated: false,
          },
        },
        createdAt,
      },
    ],
  });
}

function controls(): RuntimeMessageStoreOperationControls {
  return {
    signal: new AbortController().signal,
    timeoutMs: 1_000,
    sleep: async () => false,
  };
}

class RecordingStore extends RuntimeMessageStore {
  readonly messageWrites: RuntimeMessageInfo[] = [];
  readonly partWrites: RuntimePart[] = [];

  protected async writeMessageRecord(message: RuntimeMessageInfo): Promise<RuntimeMessageStoreWriteMessageResult> {
    this.messageWrites.push(message);
    return { ok: true, messageId: message.id, operation: "writeMessage" };
  }

  protected async writePartRecord(part: RuntimePart): Promise<RuntimeMessageStoreWritePartResult> {
    this.partWrites.push(part);
    return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
  }
}

function loaderWith(
  buildResult: unknown,
  pendingResult: unknown = { type: "empty" },
  store = new RecordingStore(),
): { readonly loader: ContextLoaderBoundary; readonly store: RecordingStore; readonly calls: { readonly build: string[]; readonly pending: string[] } } {
  const calls = { build: [] as string[], pending: [] as string[] };
  return {
    store,
    calls,
    loader: new ContextLoaderBoundary({
      source: {
        buildContext: async (sessionId) => {
          calls.build.push(sessionId);
          return buildResult;
        },
        loadPendingInput: async (sessionId) => {
          calls.pending.push(sessionId);
          return pendingResult;
        },
      },
      store,
      storeControls: controls,
    }),
  };
}

function expectContextLoaderFailure(error: unknown): asserts error is ContextLoaderError {
  expect(error).toMatchObject({ type: "context-loader" });
  const serialized = JSON.stringify(error);
  for (const fragment of forbiddenHostileFragments) {
    expect(serialized).not.toContain(fragment);
  }
}

describe("ContextLoaderBoundary", () => {
  test("implements contract literal bounds", () => {
    expect(ContextLoaderBounds).toEqual({
      maxLoadedMessages: 500,
      maxPendingMessages: 20,
      maxMessageParts: 2_000,
      maxPartBytes: 256_000,
    });
  });

  test("buildContext accepts empty and multi-message history without merging messages", async () => {
    const empty = loaderWith([]);
    await expect(empty.loader.buildContext("sesn_1")).resolves.toEqual([]);
    expect(empty.calls).toEqual({ build: ["sesn_1"], pending: [] });

    const first = textMessage("user-1", "sesn_1", "user", 0, "One");
    const second = textMessage("user-2", "sesn_1", "user", 1, "Two");
    const multi = loaderWith([first, second]);

    await expect(multi.loader.buildContext("sesn_1")).resolves.toEqual([first, second]);
  });

  test("buildContext accepts completed assistant history with usage token counts", async () => {
    const message = assistantMessageWithUsage();
    const { loader } = loaderWith([message]);

    await expect(loader.buildContext("sesn_1")).resolves.toEqual([message]);
  });

  test("buildContext accepts media-only and text-bearing user projections", async () => {
    const mediaOnly = RuntimeMessageSchema.parse({
      id: "user-media-only",
      sessionId: "sesn_1",
      role: "user",
      origin: "user",
      sequence: 0,
      status: "completed",
      createdAt,
      providerId: "fake",
      modelId: "fake-chat",
      parts: [],
    });
    const textBearing = textMessage("user-text", "sesn_1", "user", 1, "Question");
    const { loader } = loaderWith([mediaOnly, textBearing]);

    await expect(loader.buildContext("sesn_1")).resolves.toEqual([mediaOnly, textBearing]);
  });

  test("loadPendingInput accepts messages and empty Bridge-classified results", async () => {
    const message = textMessage("user-1", "sesn_1", "user", 0, "Question");
    for (const pending of [
      { type: "messages", messages: [message] },
      { type: "empty" },
    ] as const satisfies readonly PendingInputResult[]) {
      const { loader, calls } = loaderWith([], pending);
      await expect(loader.loadPendingInput("sesn_1")).resolves.toEqual(pending);
      expect(calls).toEqual({ build: [], pending: ["sesn_1"] });
    }
  });

  test("fails closed for wrong session, malformed, non-user pending, missing model identity, bounds, and unsafe payloads", async () => {
    const tooManyMessages = Array.from({ length: ContextLoaderBounds.maxLoadedMessages + 1 }, (_, index) =>
      textMessage(`user-${index}`, "sesn_1", "user", index, "Question"),
    );
    const tooManyParts = RuntimeMessageSchema.parse({
      ...textMessage("user-many-parts", "sesn_1", "user", 0, "Question"),
      parts: Array.from({ length: ContextLoaderBounds.maxMessageParts + 1 }, (_, index) => ({
        id: `part-${index}`,
        sessionId: "sesn_1",
        messageId: "user-many-parts",
        sequence: index,
        type: "text",
        text: "x",
        truncated: false,
        status: "completed",
        createdAt,
      })),
    });
    const oversized = textMessage("user-big", "sesn_1", "user", 0, "x".repeat(ContextLoaderBounds.maxPartBytes + 1));
    const unsafeMessage = JSON.parse(JSON.stringify(textMessage("unsafe", "sesn_1", "user", 0, "safe")));
    unsafeMessage.parts[0].text = `${hostileText} sk-secret-token x-api-key: secret`;
    const missingModel = JSON.parse(JSON.stringify(textMessage("missing-model", "sesn_1", "user", 0, "Question")));
    delete missingModel.providerId;
    delete missingModel.modelId;
    const wrongPartOwner = JSON.parse(JSON.stringify(textMessage("wrong-part", "sesn_1", "user", 0, "Question")));
    wrongPartOwner.parts[0].messageId = "other-message";
    const wrongPartSession = JSON.parse(JSON.stringify(textMessage("wrong-part-session", "sesn_1", "user", 0, "Question")));
    wrongPartSession.parts[0].sessionId = "other-session";
    const tooManyPendingMessages = Array.from({ length: 21 }, (_, index) =>
      textMessage(`pending-${index}`, "sesn_1", "user", index, "Question"),
    );
    const manyTotalPartsMessages = Array.from({ length: 401 }, (_, messageIndex) =>
      RuntimeMessageSchema.parse({
        ...textMessage(`part-total-${messageIndex}`, "sesn_1", "user", messageIndex, "x"),
        parts: Array.from({ length: 5 }, (_, partIndex) => ({
          id: `part-total-${messageIndex}-${partIndex}`,
          sessionId: "sesn_1",
          messageId: `part-total-${messageIndex}`,
          sequence: partIndex,
          type: "text",
          text: "x",
          truncated: false,
          status: "completed",
          createdAt,
        })),
      }),
    );
    const cases: readonly { readonly label: string; readonly build?: unknown; readonly pending?: unknown }[] = [
      { label: "wrong session", build: [textMessage("wrong", "other-session", "user", 0, "Question")] },
      { label: "wrong part owner", build: [wrongPartOwner] },
      { label: "wrong part session", build: [wrongPartSession] },
      { label: "malformed", build: [{ id: "not-a-runtime-message" }] },
      { label: "non-user pending", pending: { type: "messages", messages: [textMessage("assistant-1", "sesn_1", "assistant", 0, "Nope")] } },
      { label: "system persistent message", build: [systemMessage()] },
      { label: "missing user model identity", pending: { type: "messages", messages: [missingModel] } },
      { label: "too many messages", build: tooManyMessages },
      { label: "too many pending messages", pending: { type: "messages", messages: tooManyPendingMessages } },
      { label: "too many parts", build: [tooManyParts] },
      { label: "too many total parts", build: manyTotalPartsMessages },
      { label: "oversized", build: [oversized] },
      { label: "unsafe", build: [unsafeMessage] },
      {
        label: "unsafe structured build headers",
        build: [assistantToolMessageWithUnsafeHeaders({ unsafeValue: true, preview: "{\"q\":\"safe\"}" })],
      },
      {
        label: "unsafe structured pending headers",
        pending: {
          type: "messages",
          messages: [pendingUserMessageWithUnsafeToolHeaders({ unsafeValue: true, preview: "{\"q\":\"safe\"}" })],
        },
      },
      {
        label: "unsafe build preview json aliases",
        build: [
          assistantToolMessageWithUnsafeHeaders({
            unsafeValue: false,
            preview: "{\"headers\":{\"x_api_key\":\"raw-key\",\"xApiKey\":\"raw-key\",\"apiKey\":\"raw-key\",\"cookie\":\"sid=raw\",\"token\":\"raw-token\"}}",
          }),
        ],
      },
      {
        label: "unsafe build prefixed preview aliases",
        build: [
          assistantToolMessageWithUnsafeHeaders({
            unsafeValue: false,
            preview: "debug {\"headers\":{\"apiKey\":\"raw-key\",\"x_api_key\":\"raw-key\",\"cookie\":\"sid=raw\"}}",
          }),
        ],
      },
      {
        label: "unsafe build value string aliases",
        build: [
          assistantToolMessageWithUnsafeHeaders({
            unsafeValue: false,
            value: "debug apiKey=raw-key x_api_key=raw-key",
            preview: "{\"q\":\"safe\"}",
          }),
        ],
      },
      {
        label: "unsafe pending preview json aliases",
        pending: {
          type: "messages",
          messages: [
            pendingUserMessageWithUnsafeToolHeaders({
              unsafeValue: false,
              preview: "{\"headers\":{\"x_api_key\":\"raw-key\",\"xApiKey\":\"raw-key\",\"apiKey\":\"raw-key\",\"cookie\":\"sid=raw\",\"token\":\"raw-token\"}}",
            }),
          ],
        },
      },
      {
        label: "unsafe pending prefixed preview aliases",
        pending: {
          type: "messages",
          messages: [
            pendingUserMessageWithUnsafeToolHeaders({
              unsafeValue: false,
              preview: "debug {\"headers\":{\"apiKey\":\"raw-key\",\"x_api_key\":\"raw-key\",\"cookie\":\"sid=raw\"}}",
            }),
          ],
        },
      },
      {
        label: "unsafe pending value string aliases",
        pending: {
          type: "messages",
          messages: [
            pendingUserMessageWithUnsafeToolHeaders({
              unsafeValue: false,
              value: "debug apiKey=raw-key x_api_key=raw-key",
              preview: "{\"q\":\"safe\"}",
            }),
          ],
        },
      },
    ];

    for (const testCase of cases) {
      const { loader } = loaderWith(testCase.build ?? [], testCase.pending ?? { type: "empty" });
      const operation = testCase.pending === undefined ? loader.buildContext("sesn_1") : loader.loadPendingInput("sesn_1");
      try {
        await operation;
        throw new Error(`expected context loader operation to fail: ${testCase.label}`);
      } catch (error) {
        expectContextLoaderFailure(error);
      }
    }
  });

  test("normalizes hostile source failures without leaking raw loader errors", async () => {
    const store = new RecordingStore();
    const loader = new ContextLoaderBoundary({
      source: {
        buildContext: async () => {
          throw new Error(hostileText);
        },
        loadPendingInput: async () => {
          throw new Error(`${hostileText} cookie: secret`);
        },
      },
      store,
      storeControls: controls,
    });

    for (const operation of [loader.buildContext("sesn_1"), loader.loadPendingInput("sesn_1")]) {
      try {
        await operation;
        throw new Error("expected hostile source failure");
      } catch (error) {
        expectContextLoaderFailure(error);
      }
    }
  });

  test("repairs incomplete assistant tool state before returning cold context", async () => {
    const { loader, store } = loaderWith([
      assistantToolMessage("pending"),
      assistantToolMessage("running"),
      structuralAssistantMessage(),
      abortedVisibleAssistantMessage(),
      failedVisibleAssistantMessage(),
    ]);

    const repaired = await loader.buildContext("sesn_1");

    expect(repaired.map((message) => message.id)).toEqual(["assistant-tool-pending", "assistant-tool-running", "assistant-aborted"]);
    expect(repaired[0]?.parts[0]).toMatchObject({ type: "tool", state: { status: "cancelled" } });
    expect(repaired[1]?.parts[0]).toMatchObject({ type: "tool", state: { status: "cancelled" } });
    expect(repaired[2]?.status).toBe("cancelled");
    expect(store.partWrites).toHaveLength(2);
    const pendingRepairedPart = repaired[0]?.parts[0];
    const runningRepairedPart = repaired[1]?.parts[0];
    if (pendingRepairedPart === undefined || runningRepairedPart === undefined) {
      throw new Error("expected repaired tool parts");
    }
    expect(store.partWrites).toEqual([pendingRepairedPart, runningRepairedPart]);
    expect(store.partWrites[0]).toMatchObject({
      id: "assistant-tool-pending-part",
      messageId: "assistant-tool-pending",
      type: "tool",
      state: { status: "cancelled" },
    });
    expect(JSON.stringify(store.partWrites[0])).not.toContain("input");
    expect(store.partWrites[1]).toMatchObject({
      id: "assistant-tool-running-part",
      messageId: "assistant-tool-running",
      type: "tool",
      state: { status: "cancelled", input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false } },
    });
  });

  test("repairs streaming assistant message and active text or reasoning parts before returning cold context", async () => {
    const { loader, store } = loaderWith([streamingAssistantMessage()]);

    const repaired = await loader.buildContext("sesn_1");
    const repairedMessage = repaired[0];
    if (repairedMessage === undefined) {
      throw new Error("expected repaired streaming assistant message");
    }

    expect(repaired).toHaveLength(1);
    expect(repairedMessage).toMatchObject({ id: "assistant-streaming", role: "assistant", status: "cancelled" });
    expect(repairedMessage.parts).toEqual([
      expect.objectContaining({ id: "assistant-streaming-text", type: "text", status: "cancelled", completedAt: createdAt }),
      expect.objectContaining({ id: "assistant-streaming-reasoning", type: "reasoning", status: "cancelled", completedAt: createdAt }),
    ]);
    expect(store.partWrites).toEqual(repairedMessage.parts);
    expect(store.messageWrites).toEqual([
      expect.objectContaining({ id: "assistant-streaming", status: "cancelled", updatedAt: createdAt }),
    ]);
  });
});
