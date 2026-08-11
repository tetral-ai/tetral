import { Metadata } from "@grpc/grpc-js";
import { BridgeAPIManifestChangeNotifier } from "../../src/bridge-client.js";

const bridgeAddress = process.argv[2];
if (bridgeAddress === undefined) throw new Error("bridge address is required");

const notifier = new BridgeAPIManifestChangeNotifier({
  address: bridgeAddress,
  tokenPath: "/unused",
  metadataFactory: async () => new Metadata(),
});
const overCap = await notifier.notify({
  workspaceId: "default",
  sessionId: "sesn_mcp_capacity",
  mcpServerName: "github",
  manifestEtag: "etag_over_cap",
});
const transportCapacity = await notifier.notify({
  workspaceId: "default",
  sessionId: "sesn_transport_capacity",
  mcpServerName: "github",
  manifestEtag: "etag_transport_capacity",
});

process.stdout.write(`${JSON.stringify({ overCap, transportCapacity })}\n`);
