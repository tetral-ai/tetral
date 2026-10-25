import { describe, expect, test } from "bun:test";
import { credentials, Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import type { sendUnaryData, ServerUnaryCall, ServiceDefinition } from "@grpc/grpc-js";
import { ProviderGatewayServiceClient, ProviderGatewayServiceService, RunWebRequest, RunWebResponse } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
  activeTraceparent,
  beginServiceColdStart,
  continueGrpcTraces,
  completeServiceColdStart,
  createTelemetry,
  createTetralJsonLogger,
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
} from "@tetral/ts-observability";
import type { TelemetryOptions } from "@tetral/ts-observability";
import handledFailureRecord from "@tetral/ts-observability/test-support/handled-failure-record.json";
import { recordingTelemetry } from "@tetral/ts-observability/test-support/recording-telemetry";

const declarations = parseDeclarations({
  service: "provider-gateway",
  events: {
    "lookup.read": { level: "INFO", log_type: "app", audience: "operators", fields: {} },
    "lookup.failed": { level: "ERROR", log_type: "app", audience: "operators", fields: {}, errors: { not_found: { class: "lookup", message_safe: "item not found" } } },
  },
  operations: { lookup: { long: false, errors: ["not_found"] }, transfer: { long: true } },
  metrics: {},
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
      observation.run(() => recording.logger.error({ event: "lookup.read" }));
      observation.end();
      const [outside, inside] = recording.records();
      const span = recording.spans()[0]!.spanContext();
      expect(Object.keys(outside!).sort()).toEqual([...goRecordFields].sort());
      expect(Object.keys(inside!).sort()).toEqual([...goRecordFields, "error.class", "error.code", "error.message_safe", "trace_id", "span_id", "request.id"].sort());
      expect(outside).toMatchObject({ level: "INFO", msg: "lookup.read", event: "lookup.read", operation: "lookup", "log.type": "app" });
      expect(outside!.time).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/);
      expect(inside).toMatchObject({ level: "ERROR", trace_id: span.traceId, span_id: span.spanId, "request.id": "req_1", "error.class": "operation_failed" });
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
      const telemetry = createTelemetry({ serviceName: "provider-gateway", declarations, diagnostics: parseDiagnosticConfig({ OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint }) });
      const observation = telemetry.observe("lookup");
      const traceId = observation.run(() => activeTraceparent())!.split("-")[1]!;
      observation.end();
      await telemetry.shutdown(5_000);
      expect(receiver.received(traceId)).toBe(true);
    } finally { receiver.close(); }
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
      const privateText = "PRIVATE_FAILURE_TEXT";
      const rows = [
        { error: undefined, handled: true, outcome: "ok", record: undefined },
        { error: new Rejection("lookup_admission", "capacity"), handled: true, outcome: "rejected", record: undefined },
        { error: Object.assign(new Error(privateText), { errorCode: "not_found" }), handled: true, outcome: "not_found", record: { "error.class": "lookup", "error.code": "not_found", "error.message_safe": "item not found" } },
        { error: Object.assign(new Error(privateText), { errorCode: "undeclared_code" }), handled: true, outcome: "error", record: { "error.class": "operation_failed", "error.code": "operation_failed", "error.message_safe": "operation failed" } },
        { error: new Error(privateText), handled: false, outcome: "error", record: undefined },
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
      expect(JSON.stringify([recording.records(), recording.spans().map((span) => [span.attributes, span.events, span.status])])).not.toContain(privateText);
    } finally { recording.restore(); }
  });

  test("a handled failure has the field names the Go library writes for the same failure", () => {
    const recording = recordingTelemetry({ serviceName: "provider-gateway", declarations });
    try {
      recording.telemetry.observe("lookup", { "request.id": "req_1" }).endHandled(Object.assign(new Error("PRIVATE_FAILURE_TEXT"), { errorCode: "not_found" }));
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
        callback(call.request.sessionId === "sesn_coded" ? { code: status.NOT_FOUND, details: "PRIVATE_FAILURE_TEXT" } : { details: "PRIVATE_FAILURE_TEXT" }),
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
        { "session.id": "sesn_coded", "rpc.grpc.status_name": "NotFound", "error.class": "grpc_error", "error.code": "notfound" },
        { "session.id": "sesn_uncoded", "rpc.grpc.status_name": "Unknown", "error.class": "grpc_error", "error.code": "unknown" },
      ]);
      const records = recording.records().filter((record) => record.operation === "internal.grpc.request");
      expect(records.map((record) => [record.level, record.event, record["session.id"], record["grpc.code"], record["error.code"]])).toEqual([
        ["DEBUG", "internal.grpc.request", "sesn_coded", "NotFound", "notfound"],
        ["DEBUG", "internal.grpc.request", "sesn_uncoded", "Unknown", "unknown"],
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

  test("a pressure point counts every refusal and records one episode per run of refusals 60 s apart at most", () => {
    let now = 0;
    const recording = recordingTelemetry({ serviceName: "provider-gateway", monotonicClock: () => now });
    try {
      const registry = new MetricRegistry();
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
