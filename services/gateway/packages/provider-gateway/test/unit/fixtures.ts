import {
  ProviderRequestKind,
  ProviderContextRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderRequestAttachment,
  ProviderRequest,
  RunWebRequest,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { FetchFunction } from "@ai-sdk/provider-utils";

export function validProviderRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  const request: ProviderRequest = {
    requestId: "req_1",
    outputContractVersion: 2,
    modelRequestStartEventId: "evt_00000000000000000000000000000000",
    threadRole: 1,
    threadVisibility: 1,
    modelRequestId: "mreq_1",
    requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 42,
    runtimeProcessId: "process-test",
    runtimeBindingToken: "binding-token",
    model: {
      providerId: "openai",
      modelId: "gpt-test",
      variant: "",
    },
    system: [
      {
        kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
        text: "You are concise.",
        cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
      },
    ],
    context: [
      {
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
        content: [
          {
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
    attachments: [],
    limits: {
      maxOutputTokens: 1024,
      timeoutMs: 30_000,
    },
  };
  return { ...request, ...overrides };
}

export function validProviderAttachment(overrides: Partial<ProviderRequestAttachment> = {}): ProviderRequestAttachment {
  return {
    transient: {
      attachmentRef: "att_1",
      sourcePath: "/tmp/image.png",
      pageRange: "",
      detail: "auto",
    },
    fileBacked: undefined,
    mime: "image/png",
    filename: "image.png",
    ...overrides,
  };
}

export function validFileBackedProviderAttachment(overrides: Partial<ProviderRequestAttachment> = {}): ProviderRequestAttachment {
  return {
    transient: undefined,
    fileBacked: {
      sourceEventId: "sevt_user_1",
      fileId: "file_1",
    },
    mime: "text/plain",
    filename: "notes.txt",
    ...overrides,
  };
}

export function validRunWebRequest(): RunWebRequest {
  return {
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 42,
    runtimeProcessId: "process-test",
    runtimeBindingToken: "binding-token",
    toolUseEventId: "sevt_tool_1",
    input: {
      searchQuery: [{ q: "tetral", domains: [] }],
      open: [],
      find: [],
    },
  };
}

/**
 * Provider fetch for registries whose cases inject the model stream or stop
 * before provider HTTP. Production injects the command-owned transport; a call
 * here means a case reached the network boundary it does not control.
 */
export const unusedProviderFetch: FetchFunction = Object.assign(
  async (): Promise<Response> => {
    throw new Error("fixture provider fetch is not configured for this case");
  },
  { preconnect: () => {} },
);
