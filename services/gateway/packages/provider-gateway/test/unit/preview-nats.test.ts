import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createNatsPreviewPublisher } from "../../src/providers/preview-nats.js";
import { parsePreviewNatsConfig } from "../../src/providers/preview-config.js";
import type { ConnectionOptions, Status } from "@nats-io/transport-node";
import { ObservedPreviewConnection, observed } from "./preview-publisher-fixtures.js";

test("native initial credentials enforce raw byte bounds and UTF-8 before any broker dial", async () => {
  const directory = await mkdtemp(join(tmpdir(), "tetral-preview-material-"));
  const userPath = join(directory, "user"), passwordPath = join(directory, "password");
  const config = parsePreviewNatsConfig({ TETRAL_NATS_SERVERS: "nats://127.0.0.1:1", TETRAL_NATS_USER_PATH: userPath, TETRAL_NATS_PASSWORD_PATH: passwordPath })!;
  try {
    await writeFile(passwordPath, "password");
    await writeFile(userPath, "u".repeat(4096));
    const publisher = await createNatsPreviewPublisher(config);
    expect(publisher.metrics.connected).toBe(false); await publisher.close();
    await writeFile(userPath, "u".repeat(4097));
    await expect(createNatsPreviewPublisher(config)).rejects.toThrow("credential file");
    await writeFile(userPath, Buffer.from([0xff]));
    await expect(createNatsPreviewPublisher(config)).rejects.toThrow();
    await rm(userPath);
    await expect(createNatsPreviewPublisher({ ...config, userPath: directory })).rejects.toThrow("credential file");
  } finally { await rm(directory, { recursive: true, force: true }); }
});
test("selected heartbeat controls reach the official native connection options", async () => {
  const directory = await mkdtemp(join(tmpdir(), "tetral-preview-heartbeat-"));
  const userPath = join(directory, "user"), passwordPath = join(directory, "password");
  const connection = new ObservedPreviewConnection();
  let selected: ConnectionOptions | undefined;
  const config = parsePreviewNatsConfig({ TETRAL_NATS_SERVERS: "nats://localhost:4222", TETRAL_NATS_USER_PATH: userPath, TETRAL_NATS_PASSWORD_PATH: passwordPath, TETRAL_NATS_PING_INTERVAL_MS: "125", TETRAL_NATS_MAX_PING_OUT: "3" })!;
  try {
    await writeFile(userPath, "fixture-user"); await writeFile(passwordPath, "fixture-password");
    const publisher = await createNatsPreviewPublisher(config, undefined, { nativeConnect: async options => {
      selected = options;
      return { publish: (subject, bytes) => connection.publish(subject, bytes), flush: () => connection.flush(), close: () => connection.close(), closed: () => connection.closed(), status: async function* (): AsyncIterable<Status> { await connection.ended.promise; } };
    } });
    publisher.start(); await observed(() => publisher.metrics.connected);
    expect(selected?.pingInterval).toBe(125); expect(selected?.maxPingOut).toBe(3);
    expect(selected?.timeout).toBe(1000); expect(selected?.reconnect).toBe(false);
    await publisher.close(); expect(connection.closeCalls).toBe(1);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
