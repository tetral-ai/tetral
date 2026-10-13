/** Actual Runtime writer and its production 64 MiB channel, with test-only bearer identity. */
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import type { SessionEventEnvelope } from "@tetral/agent-runtime-core/src/contracts/runtime.js";

const input = JSON.parse(await readFile(process.argv[2]!, "utf8")) as {
  address: string;
  scope: Omit<SessionEventEnvelope, "writeId" | "event">;
  textLength: number;
  expectedCanonicalBytes: number;
  expectedSHA256: string;
  eventId: string;
};
const text = "x".repeat(input.textLength);
const canonical = JSON.stringify(text);
const bytes = Buffer.byteLength(canonical);
const digest = createHash("sha256").update(canonical).digest("hex");
if (bytes !== input.expectedCanonicalBytes || digest !== input.expectedSHA256) {
  throw new Error("independent canonical text oracle differs");
}
const writer = new BridgeAPIEventWriter({
  address: input.address,
  tokenPath: "fixture-token-unused",
  metadataFactory: async () => {
    const metadata = new Metadata();
    metadata.set("authorization", "Bearer content-runtime");
    return metadata;
  },
});
let result: Awaited<ReturnType<typeof writer.append>>;
try {
  result = await writer.append({
    ...input.scope,
    writeId: "complete-text",
    modelRequestId: "content-request",
    preallocatedEventId: input.eventId,
    event: { type: "agent.message", content: [{ type: "text", text }] },
    assistantContextAppend: { parts: [{ type: "text", text, truncated: false }] },
  });
} finally {
  await writer.close();
}
console.log(JSON.stringify({ bytes, digest, result, closed: true }));
