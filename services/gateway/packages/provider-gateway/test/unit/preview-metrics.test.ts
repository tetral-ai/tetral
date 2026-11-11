import { expect, test } from "bun:test";
import { createJsonLogger } from "../../src/logger.js";
import { PreviewPublisher } from "../../src/providers/preview-publisher.js";
import { PreviewPublisherDefaults } from "../../src/providers/preview-config.js";
import { ObservedPreviewConnection, observed, previewRequest, textId } from "./preview-publisher-fixtures.js";

test("actual publisher instruments distinguish local admission, flush and preview stop with balanced residency", async () => {
  const connection = new ObservedPreviewConnection(); connection.autoFlush = false;
  const publisher = new PreviewPublisher({ connect: async () => connection });
  publisher.start(); await observed(() => publisher.metrics.connected);
  const producer = publisher.createProducer(previewRequest());
  producer.offer({ kind: "text_delta", eventId: textId, providerPartId: "text", delta: "alpha" });
  await observed(() => connection.flushCalls === 1);
  let metric = publisher.metrics.render();
  expect(metric).toContain("providergateway_preview_attempted_total 3\n");
  expect(metric).toContain("providergateway_preview_accepted_total 3\n");
  expect(metric).toContain("providergateway_preview_flushed_total 0\n");
  expect(publisher.metrics.pendingBytes).toBeGreaterThan(0);
  connection.flushing.resolve(); await observed(() => publisher.metrics.pendingFrames === 0);
  metric = publisher.metrics.render();
  expect(metric).toContain("providergateway_preview_flushed_total 3\n");
  expect(metric).toContain("providergateway_preview_pending_bytes 0\n");
  producer.close(); await publisher.close();
  expect(publisher.metrics.connected).toBe(false);
  expect(metric).not.toContain("session"); expect(metric).not.toContain("alpha"); expect(metric).not.toContain(textId);
});
test("healthy preview flood has no per-fragment logs; repeated failures retain first bounded summary and recovery without content", async () => {
  const records: string[] = [];
  const logger = createJsonLogger({ write: line => records.push(line), diagnostics: { level: "info", maxRecordBytes: 4096, summaryIntervalMs: 30000, burst: 1, traceSampleRatio: 1, dbSlowQueryMs: 250 } });
  const connection = new ObservedPreviewConnection();
  const publisher = new PreviewPublisher({ connect: async () => connection, logger, policy: { ...PreviewPublisherDefaults, queueFrames: 2048 } });
  publisher.start(); await observed(() => publisher.metrics.connected);
  const producer = publisher.createProducer(previewRequest());
  for (let index = 0; index < 1000; index++) producer.offer({ kind: "text_delta", eventId: textId, providerPartId: "text", delta: "private non-token sentinel" });
  await observed(() => publisher.metrics.pendingFrames === 0);
  expect(records).toHaveLength(0);
  await connection.close(); await observed(() => !publisher.metrics.connected);
  for (let index = 0; index < 1000; index++) publisher.createProducer(previewRequest()).close();
  logger.flush();
  expect(records.length).toBeLessThan(10);
  expect(records.map(line => JSON.parse(line) as Record<string, unknown>).filter(record => record.event === "preview.disconnected")).toEqual([
    expect.objectContaining({ "error.code": "connection_lost", "error.message": "the preview connection closed" }),
  ]);
  expect(records.some(line => line.includes("suppressed.count"))).toBe(true);
  expect(records.join("")).not.toContain("private non-token sentinel");
  producer.close(); await publisher.close(); logger.close();
});
test("a preview connection that cannot be opened is recorded once with the dial's error as its cause", async () => {
  const records: string[] = [];
  const logger = createJsonLogger({ write: line => records.push(line), diagnostics: { level: "info", maxRecordBytes: 4096, summaryIntervalMs: 30000, burst: 1, traceSampleRatio: 1, dbSlowQueryMs: 250 } });
  const publisher = new PreviewPublisher({ connect: async () => { throw new Error("connect ECONNREFUSED 10.0.0.9:4222"); }, logger, policy: { ...PreviewPublisherDefaults, retryMaxMs: 1 } });
  publisher.start(); await observed(() => publisher.metrics.failures >= 2);
  await publisher.close(); logger.close();
  expect(records.map(line => JSON.parse(line) as Record<string, unknown>)).toEqual([
    expect.objectContaining({ event: "preview.disconnected", level: "WARN", "error.code": "unavailable", "error.message": "no preview connection is open: connect ECONNREFUSED 10.0.0.9:4222" }),
  ]);
});
test("throwing diagnostic sinks cannot reject publication, stop cleanup or add another producer", async () => {
  const connection = new ObservedPreviewConnection();
  const publisher = new PreviewPublisher({ connect: async () => connection, logger: { info: () => { throw new Error("sink failed"); }, error: () => { throw new Error("sink failed"); } } });
  publisher.createProducer(previewRequest()).close();
  publisher.start(); await observed(() => publisher.metrics.connected);
  const producer = publisher.createProducer(previewRequest());
  producer.offer({ kind: "text_delta", eventId: textId, providerPartId: "text", delta: "exact" });
  await observed(() => publisher.metrics.pendingFrames === 0);
  expect(connection.decoded().at(-1)?.text).toBe("exact"); producer.close(); await publisher.close();
  expect(publisher.metrics.pendingBytes).toBe(0);
});
