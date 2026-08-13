import {
  ProviderRequestKind,
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "../../src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderRequest,
  RunWebRequest,
} from "../../src/gen/tetral/provider_gateway/v1/provider_gateway.js";

export function validProviderRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  const request: ProviderRequest = {
    requestId: "req_1",
    modelRequestId: "mreq_1",
    requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    parentThreadId: undefined,
    bindingId: "bind_1",
    bindingGeneration: 42,
    runtimeBindingToken: "binding-token",
    model: {
      providerId: "openai",
      modelId: "gpt-5.5",
      variant: "",
    },
    system: [
      {
        kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
        text: "You are concise.",
        cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
      },
    ],
    messages: [
      {
        id: "msg_1",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
        status: "completed",
        origin: "user",
        parts: [
          {
            id: "part_1",
            text: { text: "hello" },
          },
        ],
      },
    ],
    tools: [
      {
        name: "Read",
        description: "Read a file.",
        function: { inputSchemaJson: JSON.stringify({ type: "object" }), outputSchemaJson: undefined },
      },
    ],
    attachments: [
      {
        transient: {
          attachmentRef: "att_1",
          sourceToolUseEventId: "sevt_tool_1",
          sourcePath: "/tmp/image.png",
          pageRange: "",
          detail: "auto",
        },
        fileBacked: undefined,
        mime: "image/png",
        filename: "image.png",
      },
    ],
    limits: {
      maxOutputTokens: 1024,
      timeoutMs: 30_000,
    },
  };
  return { ...request, ...overrides };
}

export function validRunWebRequest(): RunWebRequest {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 42,
    runtimeBindingToken: "binding-token",
    toolUseEventId: "sevt_tool_1",
    input: {
      searchQuery: [{ q: "tetral", domains: [] }],
      open: [],
      find: [],
    },
  };
}
