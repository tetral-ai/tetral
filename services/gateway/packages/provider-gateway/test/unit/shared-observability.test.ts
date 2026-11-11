import { describe, expect, spyOn, test } from "bun:test";
import { Writable } from "node:stream";
import { causeOf, createDiagnosticStreamSink, createTelemetry, createTetralJsonLogger, diagnosticDefaults, FailureRecordError, noCause, parseDiagnosticConfig, parseDiagnosticConfigResult, parseWorkloadResourceConfig, semanticErrorFields, serviceDeclarations } from "@tetral/ts-observability";
import type { TetralLogRecord } from "@tetral/ts-observability";
import causeShaping from "@tetral/ts-observability/test-support/cause-shaping.json";

const failureFields = { "error.class": "string", "error.code": "string", "error.message_safe": "string", "error.message": "string", "session.id": "string", phase: "string", component: "string", reason: "string" };
// The Go library's failure_test.go declares the same events and codes.
const failureTable = serviceDeclarations({
  service: "failure-test",
  events: {
    "dependency.failed": { level: "ERROR", log_type: "app", audience: "operators", fields: failureFields, errors: {
      unavailable: { class: "dependency", message_safe: "dependency unavailable", cause: true },
      peer_refused: { class: "peer", message_safe: "the peer refused", no_cause: "cli_report" },
    } },
    "dependency.slow": { level: "WARN", log_type: "app", audience: "operators", fields: failureFields, errors: { slow: { class: "dependency", message_safe: "dependency slow", cause: true } } },
    "dependency.recovered": { level: "INFO", log_type: "app", audience: "operators", fields: { "recovery.event": "string" } },
  },
  operations: {}, metrics: {}, spans: {},
});
const refused = causeOf(new Error("dial tcp 10.0.0.7:5432: connection refused"));

/** Writes with a failure-test logger and returns the rule failures of a test run and the records then written, which are production's. */
function failureRecords(write: (logger: ReturnType<typeof createTetralJsonLogger>) => void): { readonly problems: readonly Error[]; readonly records: readonly Record<string, unknown>[] } {
  const lines: string[] = [], problems: Error[] = [];
  const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "failure-test", declarations: failureTable, undeclared: "throw", ruleFailure: (error) => problems.push(error) });
  try { write(logger); } finally { logger.close(); }
  return { problems, records: lines.map((line) => JSON.parse(line) as Record<string, unknown>) };
}

describe("shared process diagnostics", () => {
  test("boot controls have literal defaults and reject malformed or excessive bounds", () => {
    const traceAndSlowQuery = { traceSampleRatio: expect.any(Number), dbSlowQueryMs: expect.any(Number) };
    expect(parseDiagnosticConfig({})).toEqual({ level: "info", maxRecordBytes: 16384, summaryIntervalMs: 30000, burst: 1, ...traceAndSlowQuery });
    expect(parseDiagnosticConfig({ TETRAL_LOG_LEVEL: "warn", TETRAL_LOG_MAX_RECORD_BYTES: "4096", TETRAL_LOG_SUMMARY_INTERVAL_MS: "500", TETRAL_LOG_BURST: "3" })).toEqual({ level: "warn", maxRecordBytes: 4096, summaryIntervalMs: 500, burst: 3, ...traceAndSlowQuery });
    for (const env of [{ TETRAL_LOG_LEVEL: "verbose" }, { TETRAL_LOG_MAX_RECORD_BYTES: "1023" }, { TETRAL_LOG_MAX_RECORD_BYTES: "65537" }, { TETRAL_LOG_SUMMARY_INTERVAL_MS: "99" }, { TETRAL_LOG_BURST: "0" }, { TETRAL_LOG_BURST: "9007199254740992" }]) expect(parseDiagnosticConfig(env)).toBeUndefined();
    expect(parseDiagnosticConfig({ TETRAL_TRACE_SAMPLE_RATIO: "0", TETRAL_DB_SLOW_QUERY_MS: "60001", OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4317" })).toMatchObject({ traceSampleRatio: 0, dbSlowQueryMs: 60001, traceEndpoint: "http://collector:4317" });
    for (const value of ["0", "-1", "2.5", "01"]) expect(parseDiagnosticConfig({ TETRAL_DB_SLOW_QUERY_MS: value })).toBeUndefined();
    for (const [env, problem] of [
      [{ TETRAL_TRACE_SAMPLE_RATIO: "1.5" }, "TETRAL_TRACE_SAMPLE_RATIO must be a decimal from 0 through 1"],
      [{ TETRAL_TRACE_SAMPLE_RATIO: ".5" }, "TETRAL_TRACE_SAMPLE_RATIO must be a decimal from 0 through 1"],
      [{ TETRAL_DB_SLOW_QUERY_MS: "9007199254740992" }, "TETRAL_DB_SLOW_QUERY_MS must be a positive safe integer of milliseconds"],
      [{ OTEL_EXPORTER_OTLP_ENDPOINT: "collector:4317" }, "OTEL_EXPORTER_OTLP_ENDPOINT must be an http or https URL"],
      [{ OTEL_EXPORTER_OTLP_ENDPOINT: "ftp://collector" }, "OTEL_EXPORTER_OTLP_ENDPOINT must be an http or https URL"],
    ] as const) expect(parseDiagnosticConfigResult(env)).toEqual({ ok: false, problem });
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
    logger.warn({ event: "transport.degraded", operation: "transport.recover", ...semanticErrorFields({ errorCode: "unavailable", cause: causeOf(new Error("connection reset by peer")) }) });
    expect(JSON.parse(lines[0]!)).toMatchObject({ level: "WARN", "error.code": "unavailable", "error.message": "connection reset by peer", "service.instance.id": "process-one" });
    expect(logger.stats().filtered).toBe(1000);
  });
  test("repetition emits initial plus bounded summary, does not key by scope, and recovers", () => {
    const lines: string[] = []; let now = 0;
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test", monotonicClock: () => now, diagnostics: { ...diagnosticDefaults, summaryIntervalMs: 100 } });
    for (let n = 0; n < 1000; n++) logger.error({ event: "dependency.failed", "session.id": `session-${n}`, ...semanticErrorFields({ errorCode: "unavailable", cause: refused }) });
    expect(lines).toHaveLength(1); expect(logger.stats().limiterEntries).toBe(1);
    logger.info({ event: "dependency.recovered", "recovery.event": "dependency.failed", outcome: "recovered" });
    expect(lines).toHaveLength(3);
    expect(JSON.parse(lines[1]!)).toMatchObject({ event: "diagnostic.suppressed", "event.original": "dependency.failed", "suppressed.count": 999, "session.id": "session-0" });
    expect(JSON.parse(lines[2]!)).toMatchObject({ event: "dependency.recovered", outcome: "recovered" });
    logger.error({ event: "dependency.failed", ...semanticErrorFields({ errorCode: "unavailable", cause: refused }) });
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
          serviceName: "failure-test", declarations: failureTable, monotonicClock: () => now,
          clock: () => new Date(Date.UTC(2026, 9, 2) + now),
          diagnostics: { ...diagnosticDefaults, summaryIntervalMs: 100 },
        });
        let watchdog: ReturnType<typeof setTimeout> | undefined;
        try {
          const failure = {
            event: "dependency.failed", component: "mcp-connector", phase: "connection\r\nwait\u0000",
            "session.id": "PRIVATE_ID_SENTINEL".repeat(10000),
            ...semanticErrorFields({ errorCode: "unavailable", cause: refused }),
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
            level: severity.toUpperCase(), "event.original": "dependency.failed", reason: "unavailable",
            "error.class": "dependency", "error.code": "unavailable", "error.message_safe": "dependency unavailable",
            component: "mcp-connector", phase: "connection  wait ", "session.id": "[TRUNCATED]",
            "suppressed.count": 1, first_seen: "2026-10-02T00:00:00.000000Z", last_seen: "2026-10-02T00:00:00.025000Z",
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
        ...semanticErrorFields({ errorCode: "safe_code", cause: refused }),
      };
      logger.error(failure); logger.error(failure);
      logger.warn({ event: "degradation.status", reason: "recovering", phase: "retry", component: "connector" });
      logger.warn({ event: "degradation.status", reason: "recovering", phase: "retry", component: "connector" });
      logger.flush();
      const summaries = lines.map((line) => JSON.parse(line)).filter((record) => record.event === "diagnostic.suppressed");
      expect(summaries[0]).toMatchObject({ "error.code": "safe_code", phase: "[TRUNCATED]", component: "[REDACTED]" });
      expect(summaries[1]).toMatchObject({ level: "WARN", phase: "retry", component: "connector" });
      for (const field of ["error.class", "error.code", "error.message_safe"]) expect(summaries[1]).not.toHaveProperty(field);
      expect(lines.join("")).not.toMatch(/PRIVATE_PHASE_SENTINEL|sk-private/);
    } finally { logger.close(); }
  });

  test("a later marked failure supplies the summary sample without deriving it from escalation", () => {
    const lines: string[] = [];
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "failure-test", declarations: failureTable });
    try {
      logger.warn({ event: "dependency.failed", reason: "unavailable", phase: "waiting", component: "connector" });
      const failure = { event: "dependency.failed", phase: "final", component: "connector", ...semanticErrorFields({ errorCode: "unavailable", cause: refused }) };
      logger.error(failure); logger.error(failure); logger.flush();
      expect(JSON.parse(lines.at(-1)!)).toMatchObject({ event: "diagnostic.suppressed", phase: "final", component: "connector", "error.class": "dependency", "error.code": "unavailable", "error.message_safe": "dependency unavailable" });
    } finally { logger.close(); }
  });

  test("a token prefix redacts a value only where a token starts, so identifiers containing one stay readable", () => {
    const lines: string[] = [];
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
    const values: Record<string, boolean> = {
      "sk-private-secret": true, "key sk_live_0123456789": true, "x_sk_live_0123456789": true, "ghp_0123456789abcdef": true,
      "github_pat_0123456789": true, "ant-api03-0123456789": true,
      "task_0123456789abcdef": false, "grant_0123456789abcdef": false, "desk_0123456789abcdef": false,
    };
    for (const [value, redacted] of Object.entries(values)) {
      lines.length = 0;
      logger.info({ event: "safe", operation: "safe", "session.id": value });
      expect(JSON.parse(lines[0]!)["session.id"]).toBe(redacted ? "[REDACTED]" : value);
    }
  });
  test("content sentinel, CRLF, oversize and structured values cannot expand or inject records", () => {
    const lines: string[] = [];
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test", diagnostics: { ...diagnosticDefaults, maxRecordBytes: 2048 } });
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
      const operation = (): string => { calls++; logger.error({ event: "operation.failed", ...semanticErrorFields({ errorCode: "operation_failed", cause: refused }) }); return "committed-receipt"; };
      expect(operation()).toBe("committed-receipt"); expect(calls).toBe(1); expect(logger.stats().dropped).toBe(1);
    }
  });

  test("severity escalation is visible within a suppression window", () => {
    const lines: string[] = []; const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
    logger.warn({ event: "dependency.failed", reason: "unavailable" });
    logger.warn({ event: "dependency.failed", reason: "unavailable" });
    logger.error({ event: "dependency.failed", ...semanticErrorFields({ errorCode: "unavailable", cause: refused }) });
    expect(lines).toHaveLength(2); expect(JSON.parse(lines[1]!)).toMatchObject({ level: "ERROR", event: "dependency.failed" });
    logger.flush();
  });
  test("record time follows the wall clock, the monotonic clock supplying only the sub-millisecond digits", () => {
    const lines: string[] = [];
    const wall = spyOn(Date, "now").mockReturnValue(Date.UTC(2026, 9, 2));
    const monotonic = spyOn(performance, "now").mockReturnValue(5.25);
    const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
    try {
      logger.info({ event: "safe" });
      wall.mockReturnValue(Date.UTC(2026, 9, 2, 1));
      logger.info({ event: "safe" });
      expect(lines.map((line) => JSON.parse(line).time)).toEqual(["2026-10-02T00:00:00.000250Z", "2026-10-02T01:00:00.000250Z"]);
    } finally { wall.mockRestore(); monotonic.mockRestore(); logger.close(); }
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

  // The Go library's TestFailureRecordAgreesWithItsCodeDeclaration holds the same rows.
  test("a failure record agrees with its code's declaration", () => {
    const fields = (errorCode: string, cause: ReturnType<typeof causeOf>): TetralLogRecord => ({ event: "dependency.failed", ...semanticErrorFields({ errorCode, cause }) });
    // The record type refuses the rows a site cannot write; they reach the writer as untyped code would send them.
    const untyped = (record: Record<string, unknown>): TetralLogRecord => record as unknown as TetralLogRecord;
    const rows: readonly { readonly name: string; readonly level: "error" | "warn" | "debug"; readonly record: TetralLogRecord; readonly problem?: string; readonly written?: Record<string, unknown>; readonly absent?: readonly string[] }[] = [
      { name: "declared with a cause, written with one", level: "error", record: fields("unavailable", refused),
        written: { event: "dependency.failed", level: "ERROR", "error.class": "dependency", "error.code": "unavailable", "error.message_safe": "dependency unavailable", "error.message": "dial tcp 10.0.0.7:5432: connection refused" } },
      { name: "declared with a cause, written without one", level: "error", record: fields("unavailable", noCause), problem: `event "dependency.failed", error code "unavailable": the code is declared with a cause and was written without one`,
        written: { event: "dependency.failed", "error.class": "dependency", "error.code": "unavailable" }, absent: ["error.message"] },
      { name: "declared without a cause, written with one", level: "error", record: fields("peer_refused", refused), problem: `event "dependency.failed", error code "peer_refused": the code is declared without a cause and was written with one`,
        written: { event: "dependency.failed", "error.class": "peer", "error.code": "peer_refused" }, absent: ["error.message"] },
      { name: "declared without a cause, written without one", level: "error", record: fields("peer_refused", noCause),
        written: { event: "dependency.failed", "error.class": "peer", "error.code": "peer_refused", "error.message_safe": "the peer refused" }, absent: ["error.message"] },
      { name: "a code the event does not declare", level: "error", record: fields("disk_full", refused), problem: `event "dependency.failed", error code "disk_full": the event does not declare the code for service `,
        written: { event: "diagnostic.invalid_event", level: "WARN", "event.original": "dependency.failed", reason: "undeclared_error_code" }, absent: ["error.code", "error.message"] },
      { name: "the entry's own fields passed by the site", level: "error", record: untyped({ ...fields("unavailable", refused), "error.message_safe": "site text" }), problem: `event "dependency.failed", error code "unavailable": the site passes error.message_safe as a field`,
        written: { event: "dependency.failed", "error.message_safe": "dependency unavailable" } },
      { name: "a record at ERROR written without the entry", level: "error", record: { event: "dependency.failed", phase: "connect" }, problem: `event "dependency.failed": a failure record is written only through semanticErrorFields`,
        written: { event: "diagnostic.invalid_event", level: "WARN", "event.original": "dependency.failed", reason: "failure_outside_entry", phase: "connect" } },
      { name: "a record with an error field written without the entry", level: "warn", record: untyped({ event: "dependency.slow", "error.code": "slow", "error.message": "took 9s" }), problem: `event "dependency.slow", error code "slow": a failure record is written only through semanticErrorFields`,
        written: { event: "diagnostic.invalid_event", level: "WARN", "event.original": "dependency.slow", reason: "failure_outside_entry" }, absent: ["error.code", "error.message"] },
      { name: "a failure through the entry on an event the service does not declare", level: "error", record: untyped({ event: "dependency.unheard_of", ...semanticErrorFields({ errorCode: "unavailable", cause: refused }) }),
        problem: `event "dependency.unheard_of", error code "unavailable": the event is not declared for service `,
        written: { event: "diagnostic.invalid_event", level: "ERROR", "event.original": "dependency.unheard_of" }, absent: ["reason", "error.code", "error.message"] },
      { name: "a record below the level set with an error field written without the entry", level: "debug", record: untyped({ event: "dependency.slow", "error.code": "slow" }),
        problem: `event "dependency.slow", error code "slow": a failure record is written only through semanticErrorFields` },
      { name: "a failure below ERROR written through the entry", level: "warn", record: { event: "dependency.slow", ...semanticErrorFields({ errorCode: "slow", cause: causeOf(new Error("took 9s")) }) },
        written: { event: "dependency.slow", level: "WARN", "error.code": "slow", "error.message": "took 9s" } },
    ];
    for (const row of rows) {
      const { problems, records } = failureRecords((logger) => logger[row.level](row.record));
      if (row.problem === undefined) expect(problems, row.name).toEqual([]);
      else {
        expect(problems, row.name).toHaveLength(1);
        expect(problems[0], row.name).toBeInstanceOf(FailureRecordError);
        expect(problems[0]!.message, row.name).toContain(row.problem);
        // Every problem names its writing site, which is this file.
        expect(problems[0]!.message, row.name).toMatch(/; written at \S+shared-observability\.test\.ts:\d+$/);
      }
      // A row without a record is written below the level set.
      if (row.written === undefined) { expect(records, row.name).toEqual([]); continue; }
      expect(records, row.name).toHaveLength(1);
      const record = records[0]!;
      expect(record, row.name).toMatchObject(row.written);
      // A field name is one key: as a string, its dots would be read as a path.
      for (const field of row.absent ?? []) expect(record, row.name).not.toHaveProperty([field]);
    }
  });

  test("a rule failure reached through an observation or a native caller names this file's line", () => {
    const declarations = serviceDeclarations({
      service: "failure-test",
      events: { "lookup.failed": { level: "ERROR", log_type: "app", audience: "operators", fields: { duration_ms: "integer" }, errors: { operation_failed: { class: "operation_failed", message_safe: "lookup failed", no_cause: "tool_output" } } } },
      operations: { lookup: { long: false } }, metrics: {}, spans: {},
    });
    const problems: Error[] = [];
    const logger = createTetralJsonLogger({ write: () => true, serviceName: "failure-test", declarations, undeclared: "throw", ruleFailure: (error) => problems.push(error) });
    const telemetry = createTelemetry({ serviceName: "failure-test", serviceVersion: "test", deploymentEnvironment: "test", logger, declarations, undeclared: "throw", diagnostics: { traceSampleRatio: 0 } });
    telemetry.observe("lookup").endHandled(new Error("the tool said CANARY"));
    expect(problems.map((problem) => problem.message)).toEqual([
      expect.stringMatching(/^event "lookup\.failed", error code "operation_failed": the code is declared without a cause and was written with one; written at \S+shared-observability\.test\.ts:\d+$/),
    ]);
    // A native array method that calls the logger puts its own frame before this file's.
    const { problems: handed } = failureRecords((logger) => [{ event: "dependency.failed", phase: "connect" } as unknown as TetralLogRecord].forEach(logger.error));
    expect(handed.map((problem) => problem.message)).toEqual([expect.stringMatching(/; written at \S+shared-observability\.test\.ts:\d+$/)]);
  });

  // The Go library's TestFailureRecordShapesItsCause runs the same rows.
  test("the entry shapes a failure's cause", () => {
    expect(causeShaping.rows.length).toBeGreaterThan(0);
    for (const row of causeShaping.rows) {
      const { problems, records } = failureRecords((logger) => logger.error({ event: "dependency.failed", ...semanticErrorFields({ errorCode: "unavailable", cause: causeOf(new Error(row.cause)) }) }));
      expect(problems, row.name).toEqual([]);
      expect(records.map((record) => record["error.message"]), row.name).toEqual([row.want]);
    }
  });
});
