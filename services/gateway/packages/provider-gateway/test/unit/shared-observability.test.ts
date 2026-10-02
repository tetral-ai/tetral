import { describe, expect, test } from "bun:test";
import { Writable } from "node:stream";
import { createDiagnosticStreamSink, createTetralJsonLogger, diagnosticDefaults, parseDiagnosticConfig, parseWorkloadResourceConfig, semanticErrorFields } from "@tetral/ts-observability";
import { startupFailureLogRecord } from "../../src/logger.js";

describe("shared process diagnostics", () => {
  test("boot controls have literal defaults and reject malformed or excessive bounds", () => {
    expect(parseDiagnosticConfig({})).toEqual({ level: "info", maxRecordBytes: 16384, summaryIntervalMs: 30000, burst: 1 });
    expect(parseDiagnosticConfig({ TETRAL_LOG_LEVEL: "warn", TETRAL_LOG_MAX_RECORD_BYTES: "4096", TETRAL_LOG_SUMMARY_INTERVAL_MS: "500", TETRAL_LOG_BURST: "3" })).toEqual({ level: "warn", maxRecordBytes: 4096, summaryIntervalMs: 500, burst: 3 });
    for (const env of [{ TETRAL_LOG_LEVEL: "verbose" }, { TETRAL_LOG_MAX_RECORD_BYTES: "1023" }, { TETRAL_LOG_MAX_RECORD_BYTES: "65537" }, { TETRAL_LOG_SUMMARY_INTERVAL_MS: "99" }, { TETRAL_LOG_BURST: "0" }, { TETRAL_LOG_BURST: "9007199254740992" }]) expect(parseDiagnosticConfig(env)).toBeUndefined();
  });
  test("metadata parsing preserves each service bound and explicit required-value semantics", () => {
    expect(parseWorkloadResourceConfig({TETRAL_DEPLOYMENT_ENVIRONMENT: "prod-west", TETRAL_SERVICE_VERSION: "revision-42"},253)).toEqual({deploymentEnvironment: "prod-west",serviceVersion: "revision-42"});
    expect(parseWorkloadResourceConfig({},253)).toBeUndefined();
    expect(parseWorkloadResourceConfig({TETRAL_DEPLOYMENT_ENVIRONMENT: "", TETRAL_SERVICE_VERSION: "v1"},4096)).toBeUndefined();
    expect(parseWorkloadResourceConfig({TETRAL_DEPLOYMENT_ENVIRONMENT: "x".repeat(254), TETRAL_SERVICE_VERSION: "v1"},253)).toBeUndefined();
    expect(parseWorkloadResourceConfig({TETRAL_DEPLOYMENT_ENVIRONMENT: "x".repeat(4096), TETRAL_SERVICE_VERSION: "v1"},4096)?.deploymentEnvironment.length).toBe(4096);
    expect(parseWorkloadResourceConfig({TETRAL_DEPLOYMENT_ENVIRONMENT: "x".repeat(4097), TETRAL_SERVICE_VERSION: "v1"},4096)).toBeUndefined();
  });
  test("debug polling is quiet at Info and severity filtering preserves failed-operation meaning", () => {
    const lines: string[] = [];
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test", instanceId: "process-one" });
    for (let n = 0; n < 1000; n++) logger.debug({ event: "queue.empty", operation: "queue.poll" });
    expect(lines).toHaveLength(0);
    logger.warn({ event: "transport.degraded", operation: "transport.recover", ...semanticErrorFields({ errorClass: "transport", errorCode: "unavailable", messageSafe: "transport temporarily unavailable" }) });
    expect(JSON.parse(lines[0]!)).toMatchObject({ level: "warn", "error.class": "transport", "error.code": "unavailable", "service.instance.id": "process-one" });
    expect(logger.stats().filtered).toBe(1000);
  });
  test("repetition emits initial plus bounded summary, does not key by scope, and recovers", () => {
    const lines: string[] = []; let now = 0;
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test", monotonicClock: () => now, diagnostics: { ...diagnosticDefaults, summaryIntervalMs: 100 } });
    for (let n = 0; n < 1000; n++) logger.error({ event: "dependency.failed", "session.id": `session-${n}`, ...semanticErrorFields({ errorClass: "dependency", errorCode: "unavailable", messageSafe: "dependency unavailable" }) });
    expect(lines).toHaveLength(1); expect(logger.stats().limiterEntries).toBe(1);
    logger.info({ event: "dependency.recovered", "recovery.event": "dependency.failed", outcome: "recovered" });
    expect(lines).toHaveLength(3);
    expect(JSON.parse(lines[1]!)).toMatchObject({ event: "diagnostic.suppressed", "event.original": "dependency.failed", "suppressed.count": 999, "session.id": "session-0" });
    expect(JSON.parse(lines[2]!)).toMatchObject({ event: "dependency.recovered", outcome: "recovered" });
    logger.error({ event: "dependency.failed", ...semanticErrorFields({ errorClass: "dependency", errorCode: "unavailable", messageSafe: "dependency unavailable" }) });
    expect(lines).toHaveLength(4);
    now = 100;
    for (let n = 0; n < 1000; n++) logger.warn({ event: `distinct-${n}`, reason: "failure" });
    expect(logger.stats().limiterEntries).toBeLessThanOrEqual(256);
    logger.flush(); expect(logger.stats().limiterEntries).toBe(0);
  });
  test("expiry, flush and recovery summaries retain classified failures at each severity", async () => {
    for (const path of ["expiry", "flush", "recovery"] as const) {
      for (const severity of ["info", "warn", "error"] as const) {
        const lines: string[] = [];
        let now = 0, observeSummary = (): void => undefined;
        const summarySeen = new Promise<void>((resolve) => { observeSummary = resolve; });
        const logger = createTetralJsonLogger({
          write: (line) => { lines.push(line); if (JSON.parse(line).event === "diagnostic.suppressed") observeSummary(); },
          serviceName: "test", monotonicClock: () => now,
          clock: () => new Date(Date.UTC(2026, 9, 2) + now),
          diagnostics: { ...diagnosticDefaults, summaryIntervalMs: 100 },
        });
        let watchdog: ReturnType<typeof setTimeout> | undefined;
        try {
          const failure = {
            event: "dependency.failed", component: "mcp-connector", phase: "connection\r\nwait\u0000",
            "session.id": "PRIVATE_ID_SENTINEL".repeat(10000),
            ...semanticErrorFields({ errorClass: "transport_error", errorCode: "connection_lost", messageSafe: "MCP connection lost" }),
          };
          logger[severity](failure);
          now = 25;
          logger[severity]({ ...failure, "session.id": "later-session" });
          expect(lines).toHaveLength(1);
          if (path === "expiry") {
            now = 100;
            await Promise.race([summarySeen, new Promise<never>((_resolve, reject) => { watchdog = setTimeout(() => reject(new Error("summary interval never expired")), 2000); })]);
          } else if (path === "flush") {
            logger.flush();
          } else {
            logger.info({ event: "dependency.recovered", "recovery.event": "dependency.failed" });
          }
          const summaries = lines.map((line) => JSON.parse(line)).filter((record) => record.event === "diagnostic.suppressed");
          expect(summaries).toEqual([expect.objectContaining({
            level: severity, "event.original": "dependency.failed", reason: "connection_lost",
            "error.class": "transport_error", "error.code": "connection_lost", "error.message_safe": "MCP connection lost",
            component: "mcp-connector", phase: "connection  wait ", "session.id": "[TRUNCATED]",
            "suppressed.count": 1, first_seen: "2026-10-02T00:00:00.000Z", last_seen: "2026-10-02T00:00:00.025Z",
          })]);
          expect(logger.stats().limiterEntries).toBe(0);
          for (const line of lines) {
            expect(line).not.toContain("PRIVATE_ID_SENTINEL");
            expect(new TextEncoder().encode(line).byteLength).toBeLessThanOrEqual(16384);
          }
          if (path === "recovery") expect(JSON.parse(lines.at(-1)!)).toMatchObject({ event: "dependency.recovered" });
        } finally { if (watchdog !== undefined) clearTimeout(watchdog); logger.close(); }
      }
    }
  });

  test("summary samples remain sanitized and severity alone does not manufacture a failure", () => {
    const lines: string[] = [];
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
    try {
      const failure = {
        event: "safe.failed", component: "Bearer sk-private-component-secret", phase: "PRIVATE_PHASE_SENTINEL".repeat(10000),
        ...semanticErrorFields({ errorClass: "PRIVATE_CLASS_SENTINEL".repeat(10000), errorCode: "safe_code", messageSafe: "Bearer sk-private-message-secret" }),
      };
      logger.error(failure); logger.error(failure);
      logger.warn({ event: "degradation.status", reason: "recovering", phase: "retry", component: "connector" });
      logger.warn({ event: "degradation.status", reason: "recovering", phase: "retry", component: "connector" });
      logger.flush();
      const summaries = lines.map((line) => JSON.parse(line)).filter((record) => record.event === "diagnostic.suppressed");
      expect(summaries[0]).toMatchObject({ "error.class": "[TRUNCATED]", "error.code": "safe_code", "error.message_safe": "[REDACTED]", phase: "[TRUNCATED]", component: "[REDACTED]" });
      expect(summaries[1]).toMatchObject({ level: "warn", phase: "retry", component: "connector" });
      for (const field of ["error.class", "error.code", "error.message_safe"]) expect(summaries[1]).not.toHaveProperty(field);
      expect(lines.join("")).not.toMatch(/PRIVATE_(CLASS|PHASE)_SENTINEL|sk-private/);
    } finally { logger.close(); }
  });

  test("a later marked failure supplies the summary sample without deriving it from escalation", () => {
    const lines: string[] = [];
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
    try {
      logger.warn({ event: "dependency.failed", reason: "unavailable", phase: "waiting", component: "connector" });
      const failure = { event: "dependency.failed", phase: "final", component: "connector", ...semanticErrorFields({ errorClass: "transport_error", errorCode: "unavailable", messageSafe: "transport unavailable" }) };
      logger.error(failure); logger.error(failure); logger.flush();
      expect(JSON.parse(lines.at(-1)!)).toMatchObject({ event: "diagnostic.suppressed", phase: "final", component: "connector", "error.class": "transport_error", "error.code": "unavailable", "error.message_safe": "transport unavailable" });
    } finally { logger.close(); }
  });

  test("content sentinel, CRLF, oversize and structured values cannot expand or inject records", () => {
    const lines: string[] = [];
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test", diagnostics: { ...diagnosticDefaults, maxRecordBytes: 2048 } });
    logger.error(startupFailureLogRecord({kind:"startup_error",message:"PRIVATE_CONTENT_SENTINEL\r\n".repeat(10000)}));
    expect(lines[0]).not.toContain("PRIVATE_CONTENT_SENTINEL");
    lines.length=0;
    logger.info({ event: "safe", operation: "safe", tool_result: "PRIVATE_CONTENT_SENTINEL", message: "first\r\nsecond", note: "x".repeat(100000) });
    expect(lines).toHaveLength(1); expect(lines[0]).not.toContain("PRIVATE_CONTENT_SENTINEL");
    expect(lines[0]!.split("\n")).toHaveLength(2);
    expect(JSON.parse(lines[0]!)).toMatchObject({ message: "first  second", tool_result: "[REDACTED]" });
    expect(new TextEncoder().encode(lines[0]).byteLength).toBeLessThanOrEqual(2048);
    logger.info({ event: "too-large", ...Object.fromEntries(["phase","reason","component","kind","message","operation","request.id","workspace.id","session.id","thread.id","job.id","queue.kind","binding.id","sandbox.id","cleanup.id","provider.request.id","trace.id","span.id","input.kind","status","service.name","service.version","deployment.environment"].map((key) => [key,"x".repeat(1024)])) });
    expect(lines).toHaveLength(1); expect(logger.stats().dropped).toBe(1);
  });
  test("approved existing transport and stream classifications survive the shared boundary", () => {
    const lines: string[] = []; const logger = createTetralJsonLogger({write: (line) => lines.push(line), serviceName: "test"});
    logger.info({event: "provider_request_streamed", "request.outcome": "ok", "transport.outcome": "completed", "terminal.kind": "finish", "stream.open_reasoning_count": 3, "mcp.discovery.pages": 2, "review.id": "review-one", unknown_scalar: "PRIVATE_CONTENT_SENTINEL"});
    expect(JSON.parse(lines[0]!)).toMatchObject({"request.outcome": "ok", "transport.outcome": "completed", "terminal.kind": "finish", "stream.open_reasoning_count": 3, "mcp.discovery.pages": 2, "review.id": "review-one"});
    expect(lines[0]).not.toContain("PRIVATE_CONTENT_SENTINEL"); logger.close();
  });
  test("throwing and disabled sink adapter calls remain fail-open", () => {
    for (const write of [() => { throw new Error("sink failed"); }, () => false]) {
      const logger = createTetralJsonLogger({ write, serviceName: "test" }); let calls = 0;
      const operation = (): string => { calls++; logger.error({ event: "operation.failed" }); return "committed-receipt"; };
      expect(operation()).toBe("committed-receipt"); expect(calls).toBe(1); expect(logger.stats().dropped).toBe(1);
    }
  });

  test("severity escalation is visible within a suppression window", () => {
    const lines: string[] = []; const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
    logger.warn({ event: "dependency.failed", reason: "unavailable" });
    logger.warn({ event: "dependency.failed", reason: "unavailable" });
    logger.error({ event: "dependency.failed", reason: "unavailable" });
    expect(lines).toHaveLength(2); expect(JSON.parse(lines[1]!)).toMatchObject({ level: "error", event: "dependency.failed" });
    logger.flush();
  });
  test("getter and clock failures are contained separately from sink errors", () => {
    const lines: string[] = []; const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
    const record = { event: "safe", get note(): string { throw new Error("serialization getter failure"); } };
    expect(() => logger.info(record)).not.toThrow(); expect(lines).toHaveLength(0); expect(logger.stats().dropped).toBe(1);
    const brokenClock = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test", clock: () => { throw new Error("clock failure"); } });
    expect(() => brokenClock.info({ event: "safe" })).not.toThrow(); expect(lines).toHaveLength(0); expect(brokenClock.stats().dropped).toBe(1);
    logger.close(); brokenClock.close();
  });
  test("invalid dynamic event names and repeated stream owners keep bounded diagnostics", () => {
    const lines: string[] = []; const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
    logger.info({ event: "Bearer sk-private-secret", operation: "safe" });
    expect(lines[0]).not.toContain("sk-private-secret"); expect(JSON.parse(lines[0]!)).toMatchObject({ event: "diagnostic.invalid_event" });
    const stream = new Writable({ write(_chunk,_encoding,done) { done(); } });
    for(let n=0;n<100;n++) { const sink = createDiagnosticStreamSink(stream); sink.close(); }
    expect(stream.listenerCount("error")).toBe(1); expect(stream.listenerCount("drain")).toBe(0);
    stream.destroy(); logger.close();
  });

  test("limiter samples and owned metadata are bounded before retained state", () => {
    const lines: string[] = [];
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test", deploymentEnvironment: "prod\r\nwest\u0000", serviceVersion: "Bearer sk-private-service-secret", instanceId: "PRIVATE_METADATA_SENTINEL".repeat(10000) });
    const oversized = "PRIVATE_CONTENT_SENTINEL".repeat(100000);
    logger.warn({ event: "dependency.failed", "session.id": oversized, reason: "unavailable" });
    logger.warn({ event: "dependency.failed", "session.id": oversized, reason: "unavailable" });
    logger.flush();
    expect(lines).toHaveLength(2);
    for(const line of lines) { expect(line).not.toContain("PRIVATE_CONTENT_SENTINEL"); expect(line).not.toContain("PRIVATE_METADATA_SENTINEL"); expect(line).not.toContain("sk-private-service-secret"); expect(new TextEncoder().encode(line).byteLength).toBeLessThanOrEqual(16384); }
    expect(JSON.parse(lines[1]!)).toMatchObject({ "session.id": "[TRUNCATED]", "deployment.environment": "prod  west ", "service.version": "[REDACTED]", "service.instance.id": "[TRUNCATED]", "suppressed.count": 1 });
  });
  test("production stream adapter bounds real Writable backpressure and async failure", () => {
    let release: (() => void) | undefined;
    const stream = new Writable({ highWaterMark: 8, write(_chunk, _encoding, done) { release = () => done(); } });
    const sink = createDiagnosticStreamSink(stream, 1024);
    try {
      expect(sink.write("first-record\n")).toBe(true);
      for (let n = 0; n < 1000; n++) expect(sink.write("next\n")).toBe(false);
      expect(stream.writableLength).toBeLessThanOrEqual(1024);
      expect(sink.stats()).toMatchObject({ dropped: 1000, blocked: true });
      stream.emit("error", new Error("asynchronous pipe failure"));
      expect(sink.write("after-error\n")).toBe(false); expect(sink.stats().failures).toBe(1);
      sink.close(); expect(sink.write("after-close\n")).toBe(false);
    } finally { release?.(); stream.destroy(); }
  });
});
