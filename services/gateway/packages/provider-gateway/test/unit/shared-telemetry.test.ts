import { describe, expect, test } from "bun:test";
import { credentials, Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import type { sendUnaryData, ServerUnaryCall, ServiceDefinition } from "@grpc/grpc-js";
import { ProviderGatewayServiceClient, ProviderGatewayServiceService, RunWebRequest, RunWebResponse } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
  activeTraceparent,
  beginServiceColdStart,
  BoundedSpanProcessor,
  continueGrpcTraces,
  completeServiceColdStart,
  createTelemetry,
  createTetralJsonLogger,
  diagnosticDefaults,
  endServiceColdStartPhase,
  failServiceColdStart,
  installTelemetry,
  MetricRegistry,
  parseDeclarations,
  parseDiagnosticConfig,
  PressurePoint,
  Rejection,
  renderMetrics,
  traceparentClientInterceptor,
  UndeclaredNameError,
  causeOf,
  semanticErrorFields,
} from "@tetral/ts-observability";
import type { TelemetryOptions } from "@tetral/ts-observability";
import sharedRules from "@tetral/ts-observability/test-support/go-typescript-rules.json";
import handledFailureRecord from "@tetral/ts-observability/test-support/handled-failure-record.json";
import { recordingTelemetry } from "@tetral/ts-observability/test-support/recording-telemetry";

const declarations = parseDeclarations({
  service: "provider-gateway",
  events: {
    "lookup.read": { level: "INFO", log_type: "app", audience: "operators", fields: {} },
    "lookup.failed": { level: "ERROR", log_type: "app", audience: "operators", fields: {}, errors: {
      not_found: { class: "lookup", message_safe: "item not found", cause: true },
      operation_failed: { class: "operation_failed", message_safe: "operation failed", cause: true },
    } },
  },
  operations: { lookup: { long: false, errors: ["not_found"] }, transfer: { long: true } },
  metrics: {
    provider_admission_rejected_total: { type: "counter", audience: "operators", labels: { cause: ["capacity"] } },
    lookup_wait_seconds: { type: "histogram", audience: "operators", labels: { kind: ["lease"], service: "service names" }, buckets: [1, 2] },
  },
  spans: {},
});

/** The OTLP trace export method, with its messages left as encoded bytes. */
const otlpTraceService = {
  export: {
    path: "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: Buffer) => value,
    requestDeserialize: (bytes: Buffer) => bytes,
    responseSerialize: () => Buffer.alloc(0),
    responseDeserialize: (bytes: Buffer) => bytes,
  },
} as unknown as ServiceDefinition;

/** An in-process OTLP/gRPC trace collector; a request holds each span's trace ID as raw bytes. */
async function otlpReceiver(): Promise<{ readonly endpoint: string; readonly received: (traceId: string) => boolean; readonly close: () => void }> {
  const requests: Buffer[] = [];
  const server = new Server();
  server.addService(otlpTraceService, {
    export: (call: ServerUnaryCall<Buffer, Buffer>, callback: sendUnaryData<Buffer>) => { requests.push(call.request); callback(null, Buffer.alloc(0)); },
  });
  const port = await new Promise<number>((resolve, reject) => server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, bound) => error === null ? resolve(bound) : reject(error)));
  return {
    endpoint: `http://127.0.0.1:${port}`,
    received: (traceId) => requests.some((request) => request.includes(Buffer.from(traceId, "hex"))),
    close: () => server.forceShutdown(),
  };
}

/** The fields a Go record carries with no caller field, outside a span and below ERROR. */
const goRecordFields = ["time", "level", "msg", "event", "operation", "log.type", "service.name", "deployment.environment", "service.version", "service.instance.id", "process.pid"];

describe("shared telemetry", () => {
  test("records carry the Go record's fields, a microsecond UTC time and the trace of the active observation", () => {
    const recording = recordingTelemetry({ serviceName: "provider-gateway", declarations });
    try {
      recording.logger.info({ event: "lookup.read" });
      const observation = recording.telemetry.observe("lookup", { "request.id": "req_1" });
      observation.run(() => recording.logger.error({ event: "lookup.failed", ...semanticErrorFields({ errorCode: "not_found", cause: causeOf(new Error("no row for the key")) }) }));
      observation.end();
      const [outside, inside] = recording.records();
      const span = recording.spans()[0]!.spanContext();
      expect(Object.keys(outside!).sort()).toEqual([...goRecordFields].sort());
      expect(Object.keys(inside!).sort()).toEqual([...goRecordFields, "error.class", "error.code", "error.message_safe", "error.message", "trace_id", "span_id", "request.id"].sort());
      expect(outside).toMatchObject({ level: "INFO", msg: "lookup.read", event: "lookup.read", operation: "lookup", "log.type": "app" });
      expect(outside!.time).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/);
      expect(inside).toMatchObject({ level: "ERROR", trace_id: span.traceId, span_id: span.spanId, "request.id": "req_1", "error.class": "lookup", "error.message": "no row for the key" });
    } finally { recording.restore(); }
  });

  test("a ratio of 0 samples no root span, and records under it still carry a valid trace", () => {
    const recording = recordingTelemetry({ serviceName: "provider-gateway", declarations, traceSampleRatio: 0 });
    try {
      const observation = recording.telemetry.observe("lookup");
      observation.run(() => recording.logger.info({ event: "lookup.read" }));
      observation.end();
      expect(recording.spans()).toEqual([]);
      expect(recording.records()[0]?.trace_id).toMatch(/^(?!0{32})[0-9a-f]{32}$/);
    } finally { recording.restore(); }
  });

  test("OTEL_EXPORTER_OTLP_ENDPOINT naming an OTLP/gRPC receiver gets it an ended span", async () => {
    const receiver = await otlpReceiver();
    try {
      const telemetry = createTelemetry({ serviceName: "provider-gateway", declarations, diagnostics: parseDiagnosticConfig({ OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint, TETRAL_TRACE_SAMPLE_RATIO: "1" }) });
      const observation = telemetry.observe("lookup");
      const traceId = observation.run(() => activeTraceparent())!.split("-")[1]!;
      observation.end();
      await telemetry.shutdown(5_000);
      expect(receiver.received(traceId)).toBe(true);
    } finally { receiver.close(); }
  });

  test("OTLP export resumes after a successful export and an empty timer interval", async () => {
    const receiver = await otlpReceiver();
    const telemetry = createTelemetry({ serviceName: "provider-gateway", diagnostics: { traceEndpoint: receiver.endpoint, traceSampleRatio: 1 } });
    try {
      const before = telemetry.tracer.startSpan("before_idle");
      before.end();
      const deadline = performance.now() + 5_000;
      while (!receiver.received(before.spanContext().traceId) && performance.now() < deadline) {
        await Bun.sleep(10);
      }
      expect(receiver.received(before.spanContext().traceId)).toBe(true);
      // Exercise the production one-second timer with no new spans to export.
      await Bun.sleep(1_200);
      const after = telemetry.tracer.startSpan("after_idle");
      after.end();
      await telemetry.shutdown(5_000);
      expect(after.spanContext().traceId).not.toBe(before.spanContext().traceId);
      expect(receiver.received(after.spanContext().traceId)).toBe(true);
    } finally {
      await telemetry.shutdown(5_000);
      receiver.close();
    }
  }, 15_000);

  test("flush joins an in-flight export even when its batch emptied the queue", async () => {
    let release: (() => void) | undefined;
    let exports = 0;
    const exporter: NonNullable<TelemetryOptions["exporter"]> = {
      export: (spans, callback) => {
        exports++;
        expect(spans).toHaveLength(1);
        release = () => callback({ code: 0 });
      },
      shutdown: async () => undefined,
    };
    const processor = new BoundedSpanProcessor(exporter, { start: false });
    const telemetry = createTelemetry({ serviceName: "provider-gateway", diagnostics: { traceSampleRatio: 1 }, spanProcessors: [processor] });
    try {
      telemetry.tracer.startSpan("held_export").end();
      const first = processor.forceFlush();
      expect(exports).toBe(1);
      expect(processor.queued).toBe(0);
      let flushed = false;
      const second = processor.forceFlush().then(() => { flushed = true; });
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(flushed).toBe(false);
      expect(exports).toBe(1);
      release!();
      await Promise.all([first, second]);
      expect(flushed).toBe(true);
      expect(exports).toBe(1);
    } finally {
      release?.();
      await telemetry.shutdown();
    }
  });

  test("a ratio of 0 exports no new root span and exports the span whose incoming parent is sampled", async () => {
    const receiver = await otlpReceiver();
    const telemetry = createTelemetry({ serviceName: "provider-gateway", declarations, diagnostics: parseDiagnosticConfig({ OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint, TETRAL_TRACE_SAMPLE_RATIO: "0" }) });
    const restore = installTelemetry(telemetry);
    const server = new Server();
    server.addService(ProviderGatewayServiceService, continueGrpcTraces(ProviderGatewayServiceService, {
      streamProviderRequest: (call: { end: () => void }) => call.end(),
      runWeb: (_call: unknown, callback: (error: null, response: RunWebResponse) => void) => callback(null, RunWebResponse.fromPartial({})),
    }));
    const port = await new Promise<number>((resolve, reject) => server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, bound) => error === null ? resolve(bound) : reject(error)));
    const client = new ProviderGatewayServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
    try {
      const root = telemetry.observe("lookup");
      const rootTraceId = root.run(() => activeTraceparent())!.split("-")[1]!;
      root.end();
      const parentTraceId = "4bf92f3577b34da6a3ce929d0e0e4736";
      const metadata = new Metadata();
      metadata.set("traceparent", `00-${parentTraceId}-00f067aa0ba902b7-01`);
      await new Promise<void>((resolve, reject) => client.runWeb(RunWebRequest.fromPartial({ sessionId: "sesn_sampled" }), metadata, (error) => error === null ? resolve() : reject(error)));
      await telemetry.shutdown(5_000);
      expect(receiver.received(parentTraceId)).toBe(true);
      expect(receiver.received(rootTraceId)).toBe(false);
    } finally {
      client.close();
      server.forceShutdown();
      restore();
      receiver.close();
    }
  });

  test("the 1,024-byte string bound counts UTF-8 bytes", () => {
    for (const [value, kept] of [["中".repeat(341), true], ["中".repeat(342), false], ["a".repeat(1024), true]] as const) {
      const lines: string[] = [];
      const logger = createTetralJsonLogger({ write: (line) => lines.push(line), serviceName: "test" });
      logger.info({ event: "lookup.read", phase: value });
      expect(JSON.parse(lines[0]!).phase).toBe(kept ? value : "[TRUNCATED]");
      logger.close();
    }
  });

  test("an observation counts ok, rejected, a declared code or error, and only endHandled writes the failure record", () => {
    const recording = recordingTelemetry({ serviceName: "provider-gateway", declarations });
    try {
      const failureText = "lease lost to another holder";
      const rows = [
        { error: undefined, handled: true, outcome: "ok", record: undefined },
        { error: new Rejection("lookup_admission", "capacity"), handled: true, outcome: "rejected", record: undefined },
        { error: Object.assign(new Error(failureText), { errorCode: "not_found" }), handled: true, outcome: "not_found", record: { "error.class": "lookup", "error.code": "not_found", "error.message_safe": "item not found", "error.message": failureText } },
        { error: Object.assign(new Error(failureText), { errorCode: "undeclared_code" }), handled: true, outcome: "error", record: { "error.class": "operation_failed", "error.code": "operation_failed", "error.message_safe": "operation failed", "error.message": failureText } },
        { error: new Error(failureText), handled: false, outcome: "error", record: undefined },
      ];
      for (const row of rows) {
        const observation = recording.telemetry.observe("lookup");
        if (row.handled) observation.endHandled(row.error); else observation.end(row.error);
      }
      recording.telemetry.observe("transfer").end();
      const text = renderMetrics(recording.telemetry.collect());
      for (const [outcome, count] of [["ok", 1], ["rejected", 1], ["not_found", 1], ["error", 2]] as const) {
        expect(text).toContain(`tetral_operation_duration_seconds_count{service="provider-gateway",operation="lookup",outcome="${outcome}"} ${count}\n`);
      }
      expect(text).toContain('tetral_long_operation_duration_seconds_count{service="provider-gateway",operation="transfer",outcome="ok"} 1\n');
      const failures = recording.records().filter((record) => record.event === "lookup.failed");
      expect(failures).toEqual(rows.flatMap((row) => row.record === undefined ? [] : [expect.objectContaining({ level: "ERROR", ...row.record, duration_ms: expect.any(Number) })]));
      const failedSpans = recording.spans().filter((span) => span.attributes["error.code"] !== undefined);
      expect(failedSpans.map((span) => span.attributes)).toEqual([
        { "error.class": "rejected", "error.code": "rejected" },
        { "error.class": "not_found", "error.code": "not_found" },
        { "error.class": "operation_failed", "error.code": "operation_failed" },
        { "error.class": "operation_failed", "error.code": "operation_failed" },
      ]);
      expect(JSON.stringify(recording.spans().map((span) => [span.attributes, span.events, span.status]))).not.toContain(failureText);
    } finally { recording.restore(); }
  });

  test("a handled failure has the field names the Go library writes for the same failure", () => {
    const recording = recordingTelemetry({ serviceName: "provider-gateway", declarations });
    try {
      recording.telemetry.observe("lookup", { "request.id": "req_1" }).endHandled(Object.assign(new Error("lease lost to another holder"), { errorCode: "not_found" }));
      const records = recording.records();
      expect(records).toHaveLength(1);
      expect(Object.keys(records[0]!).sort()).toEqual(handledFailureRecord.fields);
      expect(records[0]).toMatchObject(handledFailureRecord.values);
    } finally { recording.restore(); }
  });

  test("a failed gRPC call continues the caller's trace under the request's Session and is recorded once, at DEBUG, with one sample per code", async () => {
    const recording = recordingTelemetry({ serviceName: "provider-gateway", declarations });
    const method = "/tetral.provider_gateway.v1.ProviderGatewayService/RunWeb";
    const server = new Server();
    server.addService(ProviderGatewayServiceService, continueGrpcTraces(ProviderGatewayServiceService, {
      streamProviderRequest: (call: { end: () => void }) => call.end(),
      runWeb: (call: { request: { sessionId: string } }, callback: (error: { code?: number; details: string }) => void) =>
        callback(call.request.sessionId === "sesn_coded" ? { code: status.NOT_FOUND, details: "the item is gone" } : { details: "the item is gone" }),
    }));
    const port = await new Promise<number>((resolve, reject) => server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, bound) => error === null ? resolve(bound) : reject(error)));
    const client = new ProviderGatewayServiceClient(`127.0.0.1:${port}`, credentials.createInsecure(), { interceptors: [traceparentClientInterceptor] });
    const call = (sessionId: string) => new Promise<number | undefined>((resolve) => client.runWeb(RunWebRequest.fromPartial({ sessionId }), new Metadata(), (error) => resolve(error?.code)));
    try {
      const observation = recording.telemetry.observe("lookup");
      const code = await observation.run(() => call("sesn_coded"));
      observation.end();
      expect(code).toBe(status.NOT_FOUND);
      expect(await call("sesn_uncoded")).toBe(status.UNKNOWN);
      const caller = recording.spans().find((span) => span.name === "lookup")!;
      const served = recording.spans().filter((span) => span.name === method);
      expect(served[0]?.spanContext().traceId).toBe(caller.spanContext().traceId);
      expect(served.map((span) => span.attributes)).toEqual([
        { "session.id": "sesn_coded", "rpc.grpc.status_name": "NotFound", "error.class": "grpc_error", "error.code": "not_found" },
        { "session.id": "sesn_uncoded", "rpc.grpc.status_name": "Unknown", "error.class": "grpc_error", "error.code": "unknown" },
      ]);
      const records = recording.records().filter((record) => record.operation === "internal.grpc.request");
      expect(records.map((record) => [record.level, record.event, record["session.id"], record["grpc.code"], record["error.code"], record["error.message"]])).toEqual([
        ["DEBUG", "internal.grpc.request", "sesn_coded", "NotFound", "not_found", "rpc error: code = NotFound desc = the item is gone"],
        ["DEBUG", "internal.grpc.request", "sesn_uncoded", "Unknown", "unknown", "rpc error: code = Unknown desc = the item is gone"],
      ]);
      expect(records[0]?.trace_id).toBe(caller.spanContext().traceId);
      expect(recording.records().some((record) => record.level !== "DEBUG" && String(record.event).startsWith("internal.grpc"))).toBe(false);
      const metrics = renderMetrics(recording.telemetry.collect());
      expect(metrics).toContain(`grpc_server_request_duration_seconds_count{service="provider-gateway",method="${method}",code="NotFound"} 1`);
      expect(metrics).toContain(`grpc_server_request_duration_seconds_count{service="provider-gateway",method="${method}",code="Unknown"} 1`);
    } finally {
      client.close();
      server.forceShutdown();
      recording.restore();
    }
  });

  test("a full span queue refuses the 2,049th span without waiting for the exporter", async () => {
    const neverCompletes: NonNullable<TelemetryOptions["exporter"]> = { export: () => undefined, shutdown: async () => undefined };
    const telemetry = createTelemetry({ serviceName: "provider-gateway", exporter: neverCompletes, diagnostics: { traceSampleRatio: 1 } });
    const ending = performance.now();
    for (let i = 0; i < 2049; i++) telemetry.tracer.startSpan("queued").end();
    expect(performance.now() - ending).toBeLessThan(1_000);
    expect(telemetry.droppedSpans).toBe(1);
    expect(renderMetrics(telemetry.collect())).toContain("tetral_diagnostic_spans_dropped_total 1\n");
    const stopping = performance.now();
    await telemetry.shutdown(50);
    expect(performance.now() - stopping).toBeLessThan(1_000);
  });

  test("the service_pod cold start gives a 2 s dependency wait its own phase and a startup failure observes no histogram", () => {
    let now = 0;
    const recording = recordingTelemetry({ serviceName: "provider-gateway", monotonicClock: () => now });
    try {
      beginServiceColdStart(recording.telemetry);
      now = 40; endServiceColdStartPhase("config"); endServiceColdStartPhase("config");
      now = 2_040; endServiceColdStartPhase("dependencies");
      now = 2_100; completeServiceColdStart(); completeServiceColdStart();
      expect(recording.records().filter((record) => record.event === "cold_start.completed")).toEqual([expect.objectContaining({
        "cold_start.kind": "service_pod", "cold_start.start_kind": "new", outcome: "ok", config_ms: 40, dependencies_ms: 2_000, ready_ms: 60,
      })]);
      const phaseRecords = recording.records().filter((record) => record.event === "cold_start.phase_started" || record.event === "cold_start.phase_completed");
      expect(phaseRecords.map((record) => `${String(record.event)} ${String(record["cold_start.phase"])}`)).toEqual([
        "cold_start.phase_started config", "cold_start.phase_completed config",
        "cold_start.phase_started dependencies", "cold_start.phase_completed dependencies",
        "cold_start.phase_started ready", "cold_start.phase_completed ready",
      ]);
      const text = renderMetrics(recording.telemetry.collect());
      expect(text).toContain('cold_start_phase_seconds_sum{service="provider-gateway",kind="service_pod",phase="dependencies"} 2\n');
      expect(text).toContain('cold_start_seconds_sum{service="provider-gateway",kind="service_pod",scope="total"} 2.1\n');
      expect(text).toContain('cold_start_seconds_sum{service="provider-gateway",kind="service_pod",scope="engine"} 2.1\n');
      expect(text).toContain('cold_start_seconds_count{service="provider-gateway",kind="service_pod",scope="engine"} 1\n');
      beginServiceColdStart(recording.telemetry);
      now = 2_500; failServiceColdStart("startup_error"); completeServiceColdStart();
      expect(recording.records().filter((record) => record.event === "cold_start.completed").map((record) => record.outcome)).toEqual(["ok", "startup_error"]);
      expect(renderMetrics(recording.telemetry.collect())).toContain('cold_start_seconds_count{service="provider-gateway",kind="service_pod",scope="total"} 1\n');
    } finally { recording.restore(); }
  });

  test("the registry creates only declared families, with the declared type, labels and buckets", () => {
    const registry = new MetricRegistry(declarations);
    expect(() => registry.counter("lookup_undeclared_total", "Undeclared.", ["cause"])).toThrow(UndeclaredNameError);
    expect(() => registry.gauge("provider_admission_rejected_total", "Wrong type.", ["cause"])).toThrow();
    expect(() => registry.counter("provider_admission_rejected_total", "Wrong label.", ["reason"])).toThrow();
    expect(() => registry.histogram("lookup_wait_seconds", "Missing label.", ["kind"])).toThrow();
    const production = new MetricRegistry(declarations, "record");
    production.counter("lookup_undeclared_total", "Undeclared.", ["cause"]).add(1, "capacity");
    const wait = production.histogram("lookup_wait_seconds", "Declared.", ["service", "kind"]);
    wait.observe(1.5, "provider-gateway", "lease");
    wait.observe(1.5, "provider-gateway", "renew");
    expect(renderMetrics(production.collect())).toBe([
      "# HELP lookup_wait_seconds Declared.",
      "# TYPE lookup_wait_seconds histogram",
      'lookup_wait_seconds_bucket{service="provider-gateway",kind="lease",le="1"} 0',
      'lookup_wait_seconds_bucket{service="provider-gateway",kind="lease",le="2"} 1',
      'lookup_wait_seconds_bucket{service="provider-gateway",kind="lease",le="+Inf"} 1',
      'lookup_wait_seconds_count{service="provider-gateway",kind="lease"} 1',
      'lookup_wait_seconds_sum{service="provider-gateway",kind="lease"} 1.5',
      "",
    ].join("\n"));
  });

  test("a pressure point counts every refusal and records one episode per run of refusals 60 s apart at most", () => {
    let now = 0;
    const recording = recordingTelemetry({ serviceName: "provider-gateway", monotonicClock: () => now });
    try {
      const registry = new MetricRegistry(declarations);
      const point = new PressurePoint(registry, "provider_admission", ["capacity"], { telemetry: recording.telemetry });
      expect(point.reject("capacity")).toBeInstanceOf(Rejection);
      now = 30_000; point.reject("capacity");
      now = 60_000; point.admit();
      now = 90_000; point.admit();
      now = 100_000; point.reject("capacity");
      expect(recording.records().map((record) => [record.event, record["pressure.rejected"]])).toEqual([
        ["pressure.rejecting_started", undefined],
        ["pressure.rejecting_ended", 2],
        ["pressure.rejecting_started", undefined],
      ]);
      expect(renderMetrics(registry.collect())).toContain('provider_admission_rejected_total{cause="capacity"} 3\n');
      expect(() => point.reject("unlisted")).toThrow();
    } finally { recording.restore(); }
  });
});

/**
 * The rows the Go library is tested with too
 * (`TestRulesSharedWithTypeScript`). A row's `want` is what the contract
 * gives, not what the other library does.
 */
describe("rules shared with the Go library", () => {
  type Row = (typeof sharedRules.rows)[number];
  const method = "/tetral.provider_gateway.v1.ProviderGatewayService/RunWeb";

  /** Makes one RunWeb call that fails with `answer`, and returns what the boundary recorded. */
  async function servedCall(metadata: Metadata, answer: { readonly code: number; readonly details: string }) {
    const recording = recordingTelemetry({ serviceName: "provider-gateway", declarations });
    const server = new Server();
    server.addService(ProviderGatewayServiceService, continueGrpcTraces(ProviderGatewayServiceService, {
      streamProviderRequest: (call: { end: () => void }) => call.end(),
      runWeb: (_call: unknown, callback: (error: typeof answer) => void) => callback(answer),
    }));
    const port = await new Promise<number>((resolve, reject) => server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, bound) => error === null ? resolve(bound) : reject(error)));
    const client = new ProviderGatewayServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
    try {
      await new Promise<void>((resolve) => client.runWeb(RunWebRequest.fromPartial({}), metadata, () => resolve()));
      const span = recording.spans().find((each) => each.name === method);
      if (span === undefined) throw new Error(`no server span of ${method}`);
      return {
        span,
        records: recording.records().filter((record) => record.event === "internal.grpc.request"),
        metrics: renderMetrics(recording.telemetry.collect()),
      };
    } finally {
      client.close();
      server.forceShutdown();
      recording.restore();
    }
  }

  const probes: Record<string, (row: Row) => Record<string, unknown> | Promise<Record<string, unknown>>> = {
    debug_failure_rate_limit: () => {
      const lines: string[] = [];
      const table = parseDeclarations({
        service: "failure-test",
        events: { "dependency.probed": { level: "DEBUG", log_type: "app", audience: "operators", fields: {}, errors: {
          unavailable: { class: "dependency", message_safe: "dependency unavailable", cause: true },
        } } },
        operations: {}, metrics: {}, spans: {},
      });
      const logger = createTetralJsonLogger({
        write: (line) => { lines.push(line); return true; }, serviceName: "failure-test", declarations: table,
        monotonicClock: () => 0, diagnostics: { ...diagnosticDefaults, level: "debug" },
      });
      for (let repeat = 0; repeat < 3; repeat++) {
        logger.debug({ event: "dependency.probed", ...semanticErrorFields({ errorCode: "unavailable", cause: causeOf(new Error("connection refused")) }) });
      }
      const suppressed = logger.stats().suppressed;
      logger.flush();
      logger.close();
      const records = lines.map((line) => JSON.parse(line) as Record<string, unknown>);
      const summaries = records.filter((record) => record.event === "diagnostic.suppressed");
      return {
        written: records.filter((record) => record.event === "dependency.probed").length, suppressed,
        summaries: summaries.length, summary_suppressed_count: summaries[0]?.["suppressed.count"],
      };
    },
    // The call goes through a client and a server, as Go's probe does: HTTP/2
    // hands grpc-js the two values joined into one, which is no traceparent.
    repeated_traceparent: async (row) => {
      const sent = row.traceparents ?? [];
      const metadata = new Metadata();
      for (const traceparent of sent) metadata.add("traceparent", traceparent);
      const { span } = await servedCall(metadata, { code: status.UNAVAILABLE, details: "handler failed" });
      return {
        continues_a_sent_trace: sent.some((traceparent) => traceparent.split("-")[1] === span.spanContext().traceId),
        parent_span_id: span.parentSpanContext?.spanId ?? "",
      };
    },
    force_flush: async () => {
      let release: (() => void) | undefined;
      let exported = 0;
      const exporter: NonNullable<TelemetryOptions["exporter"]> = {
        export: (spans, callback) => { exported += spans.length; release = () => callback({ code: 0 }); },
        shutdown: async () => undefined,
      };
      const processor = new BoundedSpanProcessor(exporter, { start: false });
      const telemetry = createTelemetry({ serviceName: "provider-gateway", diagnostics: { traceSampleRatio: 1 }, spanProcessors: [processor] });
      try {
        telemetry.tracer.startSpan("held_export").end();
        let returned = false;
        const flush = processor.forceFlush().then(() => { returned = true; });
        await new Promise<void>((resolve) => setImmediate(resolve));
        const whileHeld = returned;
        release?.();
        await Promise.race([flush, Bun.sleep(5_000)]);
        return { returned_while_held: whileHeld, returned_after_release: returned, spans_exported: exported };
      } finally {
        release?.();
        await telemetry.shutdown();
      }
    },
    unknown_grpc_status: async (row) => {
      if (row.status === undefined) throw new Error("the row names no status");
      const { span, records, metrics } = await servedCall(new Metadata(), { code: row.status, details: "handler failed" });
      return {
        record_error_code: records[0]?.["error.code"],
        span_error_code: span.attributes["error.code"],
        metric_code_label: new RegExp(`grpc_server_request_duration_seconds_count\\{[^}]*method="${method}",code="([^"]*)"\\} 1`).exec(metrics)?.[1],
      };
    },
  };

  for (const row of sharedRules.rows) {
    test(row.rule, async () => {
      const probe = probes[row.rule];
      if (probe === undefined) throw new Error(`rule ${row.rule} has no probe in TypeScript`);
      expect(await probe(row)).toEqual(row.want);
    });
  }
});
