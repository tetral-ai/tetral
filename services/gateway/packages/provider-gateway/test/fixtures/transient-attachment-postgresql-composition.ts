import { Metadata } from "@grpc/grpc-js";
import { ProviderRequestKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { BridgeAPIAttachmentResolver } from "../../src/attachments.js";

const [
  address,
  attachmentRef,
  workspaceId = "default",
  sessionId = "sesn_provider_attachment_composition",
  sessionThreadId = "thr_provider_attachment_composition",
  bindingId = "bind_provider_attachment_composition",
  runtimePodUid = "pod_uid_provider_attachment_composition",
  filename = "gateway_attachment.png",
  sourcePath = "sandbox:gateway_attachment.png",
] = process.argv.slice(2);
if (address === undefined || attachmentRef === undefined) {
  throw new Error("address and attachment ref are required");
}

const request: ProviderRequest = {
  requestId: "req_attachment_composition",
  modelRequestId: "mreq_attachment_composition",
  requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
  workspaceId,
  sessionId,
  sessionThreadId,
  bindingId,
  bindingGeneration: 1,
  runtimeBindingToken: "unused-by-attachment-resolver",
  model: undefined,
  system: [],
  context: [],
  tools: [],
  attachments: [{
    transient: {
      attachmentRef,
      sourcePath,
      pageRange: "",
      detail: "auto",
    },
    fileBacked: undefined,
    mime: "image/png",
    filename,
  }],
  limits: undefined,
  outputSchemaJson: undefined,
};

const resolver = new BridgeAPIAttachmentResolver({
  address,
  tokenPath: "unused",
  metadataFactory: async () => new Metadata(),
});
const result = await resolver.resolve({ request, runtimePodUid });

if (!result.ok) {
  process.stdout.write(JSON.stringify({ ok: false, code: result.error.code }));
} else {
  process.stdout.write(JSON.stringify({
    ok: true,
    attachments: result.attachments.map((attachment) => ({
      attachmentRef: attachment.transient?.attachmentRef,
      bytes: Array.from(attachment.data),
    })),
    rejections: result.rejections.map((rejection) => ({
      attachmentRef: rejection.transientAttachmentRef,
      reason: rejection.reason,
    })),
  }));
}
