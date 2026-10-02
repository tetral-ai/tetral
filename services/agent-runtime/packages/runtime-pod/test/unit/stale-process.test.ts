import { expect, test } from "bun:test";
import { Server, ServerCredentials, status } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceService,
  RuntimeProcessPhase,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { AgentRuntimeBridgeServiceServer } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";

test("startup and running stale process rejection exits the actual executable without another registration under every sink", async () => {
  await Promise.all(
    ["startup", "running"].flatMap((phase) =>
      ["normal", "silent", "throw"].map(async (sink) => {
        const registrations: string[] = [];
        const reported: string[] = [];
        let accepting = 0;
        let staleAt = 0;
        const server = new Server();
        const implementation: Pick<
          AgentRuntimeBridgeServiceServer,
          "registerRuntimeProcess" | "reportRuntimeProcess"
        > = {
          registerRuntimeProcess(call, callback) {
            registrations.push(call.request.runtimeProcessId);
            callback(null, {
              runtimeProcessId: call.request.runtimeProcessId,
              registrationOrder: 1,
              registrationReceipt: "controlled-receipt",
            });
          },
          reportRuntimeProcess(call, callback) {
            reported.push(call.request.runtimeProcessId);
            if (call.request.registrationReceipt !== "controlled-receipt")
              throw new Error("wrong registration proof");
            if (
              call.request.phase ===
              RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_ACCEPTING
            )
              accepting++;
            if (phase === "startup" || accepting > 1) {
              if (staleAt === 0) staleAt = Date.now();
              callback({
                code: status.FAILED_PRECONDITION,
                message: "controlled retired process",
              });
            } else {
              callback(null, {
                runtimeProcessId: call.request.runtimeProcessId,
                phase: call.request.phase,
                current: true,
              });
            }
          },
        };
        server.addService(AgentRuntimeBridgeServiceService, implementation);
        const port = await new Promise<number>((resolve, reject) =>
          server.bindAsync(
            "127.0.0.1:0",
            ServerCredentials.createInsecure(),
            (error, port) => (error === null ? resolve(port) : reject(error)),
          ),
        );
        const child = Bun.spawn({
          cmd: [
            process.execPath,
            new URL("../fixtures/stale-process.ts", import.meta.url).pathname,
            `127.0.0.1:${port}`,
            sink,
          ],
          stdout: "pipe",
          stderr: "pipe",
        });
        const stdout = new Response(child.stdout).text();
        const stderr = new Response(child.stderr).text();
        let watchdog: ReturnType<typeof setTimeout> | undefined;
        try {
          const code = await Promise.race([
            child.exited,
            new Promise<never>((_resolve, reject) => {
              watchdog = setTimeout(() => {
                child.kill("SIGKILL");
                reject(new Error("15s stale child watchdog exceeded"));
              }, 15_000);
            }),
          ]);
          const elapsed = Date.now() - staleAt;
          const [out, err] = await Promise.all([stdout, stderr]);
          expect(code, `stdout=${out} stderr=${err}`).toBe(1);
          expect(staleAt).toBeGreaterThan(0);
          expect(elapsed).toBeLessThan(5500);
          expect(registrations).toHaveLength(1);
          expect(new Set(reported)).toEqual(new Set(registrations));
          expect(accepting).toBe(phase === "startup" ? 1 : 2);
          const names = out
            .trim()
            .split("\n")
            .filter(Boolean)
            .map((line) => (JSON.parse(line) as { event: string }).event);
          expect(names.includes("process.ready")).toBe(phase === "running");
          expect(names.indexOf("core.joined")).toBeGreaterThanOrEqual(0);
          expect(names.indexOf("dependency.close")).toBeGreaterThan(
            names.indexOf("core.joined"),
          );
          expect(names.indexOf("core.close")).toBeGreaterThan(
            names.indexOf("dependency.close"),
          );
          if (sink === "normal")
            expect(err).toContain("workload.command_failed");
          else expect(err).toBe("");
          expect(err).not.toContain("controlled retired process");
          expect(err).not.toContain("synthetic diagnostic sink failure");
          console.info(
            "stale_process_exit " +
              JSON.stringify({
                phase,
                sink,
                exit_code: code,
                elapsed_ms: elapsed,
                registration_count: registrations.length,
                joined: true,
              }),
          );
        } finally {
          if (watchdog !== undefined) clearTimeout(watchdog);
          if (child.exitCode === null) child.kill("SIGKILL");
          await child.exited;
          await new Promise<void>((resolve) =>
            server.tryShutdown(() => resolve()),
          );
        }
      }),
    ),
  );
}, 20_000);
