import {Metadata} from "@grpc/grpc-js";
import {validProviderRequest} from "./fixtures.js";
import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { Socket } from "node:net";
import { createProviderGatewayApp } from "../../src/app.js";
import { Writable } from "node:stream";
import { createDiagnosticStreamSink } from "@tetral/ts-observability";
import { runProviderGatewayCommand } from "../../src/command.js";
import { createJsonLogger } from "../../src/logger.js";
import { commandEnv, commandFixture, failureSentinel } from "../fixtures/command-process.js";

describe("ProviderGateway command failure ownership", () => {
  test("actual app worker joins before command SQL closes even after cancellation deadline", async () => {
    const events:string[]=[];
    let release!:()=>void,entered!:()=>void;
    const held=new Promise<void>(resolve=>{release=resolve;});
    const admission=new Promise<void>(resolve=>{entered=resolve;});
    let app:ReturnType<typeof createProviderGatewayApp>|undefined;
    let worker:Promise<unknown>|undefined;
    await withEnv(async()=>{
      process.env.TETRAL_SERVICE_DRAIN_TIMEOUT_MS="200";
      process.env.TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS="1000";
      const command=runProviderGatewayCommand({logger:{info:()=>undefined,error:()=>undefined},
        dependencyBuilder:async input=>{
          const dependencies=await commandFixture("none").options.dependencyBuilder!(input);
          app=createProviderGatewayApp({config:input.config,logger:input.logger,bootstrap:async()=>undefined,tokenReviewClient:{createTokenReview:async()=>{entered();await held;events.push("worker.released");return {authenticated:true,audiences:["tetral-internal-grpc"],username:"system:serviceaccount:tetral-agent-runtime:agent-runtime",podUid:"fixture"};}}});
          return {...dependencies,app,close:async()=>{events.push("database.close");}};
        },registerSignalHandlers:()=>undefined,
        waitForever:async()=>{const metadata=new Metadata();metadata.set("authorization","Bearer fixture");worker=(async()=>{for await(const _event of app!.service.streamProviderRequest(validProviderRequest(),metadata)){}})().catch(error=>error);await Promise.race([admission,worker.then(error=>{throw error;})]);return undefined as never;},
      });
      const outcome=command.catch(error=>error);
      await Promise.race([admission,outcome.then(error=>{throw error;})]);
      await new Promise(resolve=>setTimeout(resolve,1250));
      expect(events).not.toContain("database.close");
      expect(app?.ready()).toEqual({ready:false});
      release();await worker;
      expect(await outcome).toBeInstanceOf(Error);
      expect(events).toEqual(["worker.released","database.close"]);
    });
  });
  test("listener, wait and cleanup failures preserve errors and attempt each later close once", async () => {
    for (const mode of ["listener", "wait", "wait_both_cleanup", "app", "database", "both_cleanup"]) {
      const events: string[] = [], lines: string[] = [];
      const fixture = commandFixture(mode, (event) => events.push(event));
      let releases = 0;
      const base = createJsonLogger({ write: (line) => { lines.push(line); } });
      const logger = { ...base, flush: () => { releases++; base.flush(); } };
      await withEnv(async () => {
        try { await runProviderGatewayCommand({ ...fixture.options, logger, registerSignalHandlers: () => undefined }); throw new Error("expected failure"); }
        catch (error) {
          if (mode !== "database") expect(error).toBe(fixture.failure);
          else expect(error).toBe(fixture.laterFailure);
        }
      });
      expect(events.filter((event) => event === "app.close")).toHaveLength(1);
      expect(events.filter((event) => event === "database.close")).toHaveLength(1);
      expect(events.indexOf("app.close")).toBeLessThan(events.indexOf("database.close"));
      expect(releases).toBe(1);
      const records = lines.map((line) => JSON.parse(line));
      expect(records).toContainEqual(expect.objectContaining({
        event: mode === "listener" || mode.startsWith("wait") ? "workload.command_failed" : "workload.cleanup_failed",
        phase: mode === "listener" || mode.startsWith("wait") ? mode.startsWith("wait") ? "wait" : mode : mode === "database" ? "database" : "app",
        "error.class": mode === "listener" || mode.startsWith("wait") ? "process_error" : "cleanup_error",
      }));
      expect(lines.join("")).not.toContain(failureSentinel);
      expect(lines.join("")).not.toContain("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
    }
  });

  test("repeated shutdown reuses its failed result and still releases later resources", async () => {
    const events: string[] = [];
    let reentered: Promise<void> | undefined;
    const fixture = commandFixture("app", (event) => {
      events.push(event);
      if (event === "app.close") reentered = shutdown!();
    });
    let shutdown: (() => Promise<void>) | undefined;
    await withEnv(async () => {
      await expect(runProviderGatewayCommand({
        ...fixture.options, logger: { info: () => undefined, error: () => { throw new Error(failureSentinel); } },
        registerSignalHandlers: (close) => { shutdown = close; },
        waitForever: async () => {
          const first = shutdown!();
          expect(events).toContain("app.close");
          expect(reentered).toBe(first);
          const second = shutdown!();
          expect(second).toBe(first);
          await first;
          return undefined as never;
        },
      })).rejects.toBe(fixture.failure);
    });
    expect(events.filter((event) => event.endsWith(".close"))).toEqual(["app.close", "database.close"]);
  });

  test("disabled, throwing and real backpressured diagnostics do not extend prompt business cleanup", async () => {
    for (const mode of ["disabled", "throw", "backpressure"]) {
      const events: string[] = [];
      let complete: (() => void) | undefined;
      const stream = new Writable({ highWaterMark: 1, write(_chunk, _encoding, done) { complete = done; } });
      const sink = createDiagnosticStreamSink(stream);
      const logger = mode === "disabled" ? { info: () => undefined, error: () => undefined }
        : mode === "throw" ? { info: () => { throw new Error(failureSentinel); }, error: () => { throw new Error(failureSentinel); }, flush: () => { throw new Error(failureSentinel); } }
          : createJsonLogger({ write: sink.write });
      if (mode === "backpressure") logger.error({ event: "command_backpressure_probe" });
      const fixture = commandFixture("wait", (event) => events.push(event));
      const before = performance.now();
      try {
        await withEnv(async () => { await expect(runProviderGatewayCommand({ ...fixture.options, logger, registerSignalHandlers: () => undefined })).rejects.toBe(fixture.failure); });
        expect(performance.now() - before).toBeLessThan(1_000);
        expect(events.filter((event) => event.endsWith(".close"))).toEqual(["app.close", "database.close"]);
        if (mode === "backpressure") { expect(sink.stats().blocked).toBe(true); expect(sink.stats().dropped).toBeGreaterThan(0); }
      } finally { complete?.(); sink.close(); stream.destroy(); }
    }
  });

  test("actual application HTTP-stop failure still joins gRPC and releases the next command resource", async () => {
    const failure = new Error(failureSentinel), events: string[] = [], lines: string[] = [];
    const originalServe = Bun.serve;
    let closeActualHttp: (() => Promise<void>) | undefined;
    let port: number | undefined;
    Bun.serve = ((...args: Parameters<typeof Bun.serve>) => {
      const server = originalServe(...args);
      closeActualHttp = async () => { await server.stop(true); };
      return new Proxy(server, {
        get(target, key) {
          if (key === "stop") return () => {
            events.push("http.stop");
            void closeActualHttp?.().catch(() => undefined);
            throw failure;
          };
          return Reflect.get(target, key, target);
        }
      });
    }) as typeof Bun.serve;
    try {
      await withEnv(async () => {
        await expect(runProviderGatewayCommand({
          logger: createJsonLogger({ write: (line) => { lines.push(line); } }),
          dependencyBuilder: async (input) => {
            const fixture = commandFixture("none");
            const dependencies = await fixture.options.dependencyBuilder!(input);
            const app = createProviderGatewayApp({ config: input.config, logger: input.logger, tokenReviewClient: dependencies.tokenReviewClient, });
            return {
              ...dependencies,
              close: async () => { events.push("next.close"); },
              app: { ...app, start: async () => { const started = await app.start(); port = started.grpcPort; return started; } },
            };
          },
          registerSignalHandlers: () => undefined,
          waitForever: async () => undefined as never,
        })).rejects.toBe(failure);
      });
      expect(events.filter((event) => event === "http.stop")).toHaveLength(1);
      expect(events.filter((event) => event === "next.close")).toHaveLength(1);

      expect(port).toBeGreaterThan(0);
      await expect(new Promise<void>((resolve, reject) => {
        const socket = new Socket();
        const fail = (error: Error): void => { socket.destroy(); reject(error); };
        socket.setTimeout(250, () => fail(new Error("refused-port probe timed out")));
        socket.once("connect", () => { socket.destroy(); resolve(); });
        socket.once("error", fail);
        socket.connect({ host: "127.0.0.1", port: port! });
      })).rejects.toMatchObject({ code: "ECONNREFUSED" });
      expect(lines.join("")).not.toContain(failureSentinel);
      expect(lines.map((line) => JSON.parse(line))).toContainEqual(expect.objectContaining({ event: "workload.cleanup_failed", phase: "app" }));
    } finally { Bun.serve = originalServe; await closeActualHttp?.(); }
  });

  test("actual application bootstrap failure remains unready when an injected diagnostic callback throws", async () => {
    let app: ReturnType<typeof createProviderGatewayApp> | undefined, nextCloses = 0;
    await withEnv(async () => {
      await expect(runProviderGatewayCommand({
        logger: { info: () => { throw new Error(failureSentinel); }, error: () => { throw new Error(failureSentinel); } },
        dependencyBuilder: async (input) => {
          const dependencies = await commandFixture("none").options.dependencyBuilder!(input);
          app = createProviderGatewayApp({
            config: input.config, logger: input.logger, tokenReviewClient: dependencies.tokenReviewClient,
            bootstrap: async () => { throw new Error(failureSentinel); },
          });
          return {
            ...dependencies, app,
            close: async () => { nextCloses++; },
          };
        }, registerSignalHandlers: () => undefined,
      })).rejects.toThrow("gateway service startup failed");
    });
    expect(app?.ready()).toEqual({ ready: false });
    expect(nextCloses).toBe(1);
  });

  test("the production executable config-failure boundary exits with safe JSON instead of a native stack", () => {
    const path = new URL("../../src/command.ts", import.meta.url).pathname;
    const result = spawnSync(process.execPath, [path], {
      encoding: "utf8", timeout: 10_000,
      env: { ...process.env, ...commandEnv(), TETRAL_LOG_LEVEL: failureSentinel },
    });
    expect(result.error).toBeUndefined();
    expect(result.signal).toBeNull();
    expect(result.status).toBe(1);
    expect(result.stderr).not.toContain(failureSentinel);
    expect(result.stderr).not.toContain("Bun v");
    const records = result.stderr.trim().split("\n").map((line) => JSON.parse(line));
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({ level: "error", kind: "config_error" });
  });

  test("actual Bun entry and both signal exits contain raw failures and report nonzero status", () => {
    const path = new URL("../fixtures/command-process.ts", import.meta.url).pathname;
    for (const mode of ["dependency", "listener", "wait", "both_cleanup", "SIGTERM_failure", "SIGINT_failure", "SIGTERM", "SIGINT"]) {
      const result = spawnSync(process.execPath, [path, mode], { encoding: "utf8", timeout: 10_000 });
      expect(result.error).toBeUndefined();
      expect(result.signal).toBeNull();
      expect(result.status).toBe(mode === "SIGTERM" || mode === "SIGINT" ? 0 : 1);
      expect(result.stderr).not.toContain(failureSentinel);
      expect(result.stderr).not.toContain("PRIVATE_LATER_CLOSE_NON_TOKEN_SENTINEL");
      expect(result.stderr).not.toContain("Bun v");
      for (const line of result.stderr.trim().split("\n").filter(Boolean)) expect(() => JSON.parse(line)).not.toThrow();
      const events = result.stdout.trim().split("\n");
      if (mode !== "dependency") {
        expect(events.filter((event) => event === "app.close")).toHaveLength(1);
        expect(events.filter((event) => event === "database.close")).toHaveLength(1);
      }
      if (mode.endsWith("_failure")) expect(JSON.parse(result.stderr.trim().split("\n").at(-1)!)).toMatchObject({ event: "workload.cleanup_failed", phase: "app", "error.class": "cleanup_error" });
    }
  }, 30_000);
});

async function withEnv(run: () => Promise<void>): Promise<void> {
  const saved = { ...process.env };
  Object.assign(process.env, commandEnv());
  try { await run(); } finally {
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, saved);
  }
}
