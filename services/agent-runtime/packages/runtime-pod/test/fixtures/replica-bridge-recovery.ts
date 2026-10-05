import { readFile, writeFile } from "node:fs/promises";
import { credentials, Metadata } from "@grpc/grpc-js";
import * as BridgeProtocol from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { BridgeUnaryCalls } from "../../src/bridge-calls.js";
import { parseBridgeMethodPolicies } from "../../src/bridge-policy.js";
const input = JSON.parse(await readFile(process.argv[2]!, "utf8")) as {
  addresses: string[];
  token: string;
  directory: string;
  env: Record<string, string>;
  actions: Array<{
    name: string;
    replica: number;
    method: string;
    request: unknown;
    timeoutMs?: number;
    error?: number;
    waitFor?: string;
  }>;
};
const policies = parseBridgeMethodPolicies(input.env);
if (policies === undefined) throw new Error("invalid method policy");
const owners = input.addresses.map(
  (address) =>
    new BridgeUnaryCalls(
      new AgentRuntimeBridgeServiceClient(address, credentials.createInsecure()),
      policies,
    ),
);
const metadata = new Metadata();
metadata.set("authorization", `Bearer ${input.token}`);
const results: Record<string, unknown> = {};
const completionSamples: Array<Record<string, string | number>> = [];
try {
  for (const action of input.actions) {
    // Controlled test ingress pauses the next actual call until the parent has
    // joined the prior handler and independently checked accepted custody.
    if (action.waitFor !== undefined) {
      while (!(await Bun.file(`${input.directory}/${action.waitFor}`).exists())) {
        await Bun.sleep(10);
      }
    }
    await writeFile(`${input.directory}/${action.name}.started`, "");
    const started = performance.now();
    let outcome = "success";
    try {
      const schema = (
        BridgeProtocol as unknown as Record<string, { fromJSON(value: unknown): unknown }>
      )[action.method[0]!.toUpperCase() + action.method.slice(1) + "Request"];
      if (schema === undefined) throw new Error("unknown request descriptor");
      const response = await owners[action.replica]!.call(
        action.method as keyof typeof policies,
        schema.fromJSON(action.request),
        metadata,
        { deadline: Date.now() + (action.timeoutMs ?? 5000) },
      );
      if (action.error !== undefined) throw new Error(`Expected grpc status ${action.error}`);
      results[action.name] = response;
    } catch (error) {
      if (
        action.error === undefined ||
        typeof error !== "object" ||
        error === null ||
        !("code" in error) ||
        error.code !== action.error
      )
        throw new Error(`Bridge action ${action.name} failed`, { cause: error });
      outcome = error.code === 4 ? "deadline" : error.code === 1 ? "cancel" : "error";
      results[action.name] = { code: error.code };
    }
    const startNS = Math.round(started * 1e6),
      endNS = Math.round(performance.now() * 1e6);
    completionSamples.push({
      cohort:
        action.method === "loadContext"
          ? "runtime-cold-read"
          : action.method === "awaitSandboxExecution"
            ? "runtime-long-wait"
            : "bridge-receipt",
      method: action.method,
      receiver: `bridge_${action.replica}`,
      outcome,
      start_boundary: "actual_unary_call",
      end_boundary: "actual_unary_callback",
      clock_id: `bun:${process.pid}`,
      start_ns: startNS,
      end_ns: endNS,
      duration_ns: endNS - startNS,
    });
    await writeFile(
      `${input.directory}/results.json`,
      JSON.stringify({ ...results, completionSamples }),
    );
  }
} finally {
  await Promise.all(owners.map((owner) => owner.close()));
}
