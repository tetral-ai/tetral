import { AsyncLocalStorage } from "node:async_hooks";
import { Socket } from "node:net";
import { NatsConnectionImpl, setTransportFactory } from "@nats-io/transport-node";
import type { ConnectionOptions, NatsConnection, Server } from "@nats-io/transport-node";
import { NodeTransport } from "@nats-io/transport-node/lib/node_transport.js";

/** The pinned transport applies timeout per seed and cannot close a pre-INFO
 * socket through close(). This adapter owns sockets and one complete dial
 * budget, while the official client still owns TLS verification and protocol. */
class ConnectAttempt {
  readonly controller = new AbortController();
  readonly sockets = new Set<Socket>();
  private readonly timer: ReturnType<typeof setTimeout>;
  private readonly cancel = () => {
    this.controller.abort();
    for (const socket of this.sockets) socket.destroy();
  };
  constructor(milliseconds: number, private readonly parent?: AbortSignal) {
    this.timer = setTimeout(this.cancel, milliseconds);
    parent?.addEventListener("abort", this.cancel, { once: true });
    if (parent?.aborted) this.cancel();
  }
  assertActive(): void { if (this.controller.signal.aborted) throw new Error("preview connection attempt cancelled"); }
  track(socket: Socket): void { this.sockets.add(socket); }
  private disarm(): void { clearTimeout(this.timer); this.parent?.removeEventListener("abort", this.cancel); }
  transfer(): void { this.disarm(); this.sockets.clear(); }
  async dispose(): Promise<void> {
    this.disarm(); this.cancel();
    await Promise.all([...this.sockets].map(async socket => {
      if (socket.closed) return;
      const closed = new Promise<void>(resolve => socket.once("close", resolve));
      socket.destroy(); await closed;
    }));
    this.sockets.clear();
  }
}
const attempts = new AsyncLocalStorage<ConnectAttempt>();

class OwnedNodeTransport extends NodeTransport {
  private readonly attempt: ConnectAttempt;
  constructor() {
    super();
    const attempt = attempts.getStore();
    if (attempt === undefined) throw new Error("preview transport requires an owned connection attempt");
    this.attempt = attempt;
  }
  override async connect(server: Server, options: ConnectionOptions): Promise<void> {
    this.attempt.assertActive();
    // With resolve:false, this is the original destination hostname. Keep its
    // DNS identity for verification rather than substituting a resolved IP.
    await super.connect({ ...server, tlsName: server.hostname }, options);
  }
  override dial(server: { hostname: string; port: number }): Promise<Socket> {
    this.attempt.assertActive();
    const socket = new Socket({ signal: this.attempt.controller.signal });
    // TLS wrapping can leave the underlying TCP socket without a native error
    // listener while its abort signal remains live. Custody includes that event.
    socket.on("error", () => undefined);
    socket.setNoDelay(true);
    this.socket = socket; this.attempt.track(socket);
    return new Promise<Socket>((resolve, reject) => {
      const error = (cause: Error) => reject(cause);
      const close = () => reject(new Error("preview socket closed during dial"));
      socket.once("error", error); socket.once("close", close);
      socket.connect({ host: server.hostname, port: server.port }, () => {
        socket.removeListener("error", error); socket.removeListener("close", close);
        resolve(socket);
      });
    });
  }
  override async tlsFirst(server: { hostname: string; port: number }) {
    this.socket = await this.dial(server);
    this.attempt.assertActive();
    // Native tlsFirst merges these options into tls.connect. The signal owns
    // its local TLS socket even before the superclass assigns this.socket.
    const tls = { ...this.options.tls, signal: this.attempt.controller.signal };
    this.options.tls = tls;
    const socket = await super.tlsFirst(server);
    this.attempt.track(socket);
    return socket;
  }
  override discard(): void {
    // Preserve socket close listeners used by the owning attempt's join.
    this.done = true; this.socket?.destroy();
  }
  override async close(error?: Error): Promise<void> {
    if (!this.connected) { this.discard(); return; }
    await super.close(error);
  }
}

/** A stable factory obtains ownership from each async connect invocation. It
 * never captures a changing process-global candidate or recovery attempt. */
export async function connectOwnedPreviewTransport(options: ConnectionOptions, signal?: AbortSignal): Promise<NatsConnection> {
  const attempt = new ConnectAttempt(options.timeout ?? 1000, signal);
  setTransportFactory({ factory: () => new OwnedNodeTransport() });
  try {
    const connection = await attempts.run(attempt, () => NatsConnectionImpl.connect({ ...options, resolve: false }));
    try { attempt.assertActive(); }
    catch (error) { await connection.close(); throw error; }
    attempt.transfer(); return connection;
  } catch (error) {
    await attempt.dispose(); throw error;
  }
}
