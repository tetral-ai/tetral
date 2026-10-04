import { expect, test } from "bun:test";
import { createServer, type Server, type Socket } from "node:net";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connectOwnedPreviewTransport } from "../../src/providers/preview-transport.js";
import { createNatsPreviewPublisher } from "../../src/providers/preview-nats.js";
import { parsePreviewNatsConfig } from "../../src/providers/preview-config.js";
import { observed } from "./preview-publisher-fixtures.js";
const protectedTLS = { handshakeFirst: true as const, rejectUnauthorized: true };

async function withholdingPeers(count: number) {
  const servers: Server[] = [], sockets = new Set<Socket>(), addresses: string[] = [];
  let accepted = 0, closed = 0, tlsRecords = 0;
  for (let index = 0; index < count; index++) {
    const server = createServer(socket => {
      accepted++; sockets.add(socket);
      socket.on("error", () => undefined);
      socket.once("data", bytes => { if (bytes[0] === 0x16) tlsRecords++; });
      socket.on("close", () => { closed++; sockets.delete(socket); });
      socket.resume();
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    servers.push(server);
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("transport peer address unavailable");
    addresses.push(`nats://localhost:${address.port}`);
  }
  return { addresses, observations: () => ({ accepted, closed, tlsRecords, active: sockets.size }), close: async () => {
    for (const socket of sockets) socket.destroy();
    await Promise.all(servers.map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  } };
}

for (const protectedTransport of [false, true]) {
  test(`one native attempt budget closes ${protectedTransport ? "TLS-handshake" : "INFO"} withholding peers across three seeds`, async () => {
    const peers = await withholdingPeers(3), started = performance.now();
    try {
      await expect(connectOwnedPreviewTransport({ servers: peers.addresses, timeout: 100, noRandomize: true, reconnect: false,
        ...(protectedTransport ? { tls: protectedTLS } : {}) })).rejects.toThrow();
      const joined = performance.now() - started;
      await observed(() => peers.observations().closed === peers.observations().accepted);
      expect(peers.observations().accepted).toBe(1); expect(peers.observations().active).toBe(0);
      if (protectedTransport) expect(peers.observations().tlsRecords).toBe(1);
      expect(joined).toBeLessThan(250);
    } finally { await peers.close(); }
  });
  test(`shutdown signal joins an actual ${protectedTransport ? "TLS-handshake" : "INFO"} withholding socket without waiting its deadline`, async () => {
    const peers = await withholdingPeers(1), controller = new AbortController();
    try {
      const connecting = connectOwnedPreviewTransport({ servers: peers.addresses, timeout: 1000, reconnect: false,
        ...(protectedTransport ? { tls: protectedTLS } : {}) }, controller.signal);
      void connecting.catch(() => undefined);
      await observed(() => peers.observations().accepted === 1 && (!protectedTransport || peers.observations().tlsRecords === 1));
      const started = performance.now(); controller.abort();
      await expect(connecting).rejects.toThrow();
      expect(performance.now() - started).toBeLessThan(250);
      await observed(() => peers.observations().closed === 1);
      expect(peers.observations().active).toBe(0);
    } finally { await peers.close(); }
  });
}
test("overlapping native attempts keep cancellation and socket custody separate", async () => {
  const first = await withholdingPeers(1), second = await withholdingPeers(1);
  const cancelFirst = new AbortController(), cancelSecond = new AbortController();
  try {
    const one = connectOwnedPreviewTransport({ servers: first.addresses, timeout: 1000, reconnect: false }, cancelFirst.signal);
    const two = connectOwnedPreviewTransport({ servers: second.addresses, timeout: 1000, reconnect: false }, cancelSecond.signal);
    void one.catch(() => undefined); void two.catch(() => undefined);
    await observed(() => first.observations().accepted === 1 && second.observations().accepted === 1);
    cancelFirst.abort(); await expect(one).rejects.toThrow(); await observed(() => first.observations().closed === 1);
    expect(first.observations().active).toBe(0); expect(second.observations().active).toBe(1);
    cancelSecond.abort(); await expect(two).rejects.toThrow(); await observed(() => second.observations().closed === 1);
    expect(second.observations().active).toBe(0);
  } finally { cancelFirst.abort(); cancelSecond.abort(); await Promise.all([first.close(), second.close()]); }
});

test("production supervisor retries join each failed socket before the next attempt and shutdown", async () => {
  const peers = await withholdingPeers(3), directory = await mkdtemp(join(tmpdir(), "tetral-preview-retries-"));
  const userPath = join(directory, "user"), passwordPath = join(directory, "password");
  await writeFile(userPath, "fixture-user"); await writeFile(passwordPath, "fixture-password");
  const config = parsePreviewNatsConfig({ TETRAL_NATS_SERVERS: peers.addresses.join(","), TETRAL_NATS_USER_PATH: userPath,
    TETRAL_NATS_PASSWORD_PATH: passwordPath, TETRAL_NATS_CONNECT_TIMEOUT_MS: "100", TETRAL_NATS_RETRY_MAX_MS: "100" })!;
  const publisher = await createNatsPreviewPublisher(config);
  try {
    publisher.start();
    for (let attempt = 1; attempt <= 3; attempt++) {
      await observed(() => publisher.metrics.failures === attempt);
      await observed(() => peers.observations().closed === attempt);
      expect(peers.observations().accepted).toBe(attempt);
      expect(peers.observations().active).toBe(0);
    }
    await publisher.close();
    expect(peers.observations().accepted).toBe(3);
    expect(peers.observations().closed).toBe(3);
    expect(peers.observations().active).toBe(0);
  } finally { await publisher.close(); await peers.close(); await rm(directory, { recursive: true, force: true }); }
});
