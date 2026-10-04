/** Test-only gates around the production factory's real native NATS transport.
 * The wrapper captures counts only. It never substitutes a broker or payload. */
import type { PreviewConnection } from "../../services/gateway/packages/provider-gateway/src/providers/preview-publisher.js";

export type PublicPublisherControl = "arm_flush" | "release_flush" | "arm_connect" | "release_connect" | "fail_connect";
interface Gate { promise: Promise<void>; release(): void }
function gate(): Gate {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}
export function createPublicStreamingPublisherControls(holdInitialConnect = false, holdAfterNativeConnect = false) {
  let armedConnect = holdInitialConnect, armedFlush = false;
  let connectGate: Gate | undefined, flushGate: Gate | undefined, failHeldConnect = false;
  let connectStarted = 0, connectHeld = 0, connectCompleted = 0, connectFailed = 0;
  let connectionsCreated = 0, connectionsClosed = 0, closeCalls = 0;
  let flushStarted = 0, flushServerProcessed = 0, flushHeld = 0;
  let publishFrames = 0, publishBytes = 0;
  let shutdownStarted = false;
  const active = new Set<number>();
  const holdConnect = async () => {
    connectHeld++; connectGate = gate();
    try { await connectGate.promise; }
    finally { connectGate = undefined; }
    if (failHeldConnect) { failHeldConnect = false; throw new Error("fixture held connection failure"); }
  };
  const connectionFactory = async (nativeDial: () => Promise<PreviewConnection>): Promise<PreviewConnection> => {
    connectStarted++;
    try {
      const held = armedConnect; armedConnect = false;
      if (held && !holdAfterNativeConnect) await holdConnect();
      const connection = await nativeDial();
      const ordinal = ++connectionsCreated; active.add(ordinal); connectCompleted++;
      const closed = connection.closed().then(value => { if (active.delete(ordinal)) connectionsClosed++; return value; }, error => { if (active.delete(ordinal)) connectionsClosed++; throw error; });
      void closed.catch(() => undefined);
      const wrapped: PreviewConnection = {
        publish: (subject, bytes) => { connection.publish(subject, bytes); publishFrames++; publishBytes += bytes.length; },
        flush: async () => {
          flushStarted++;
          await connection.flush();
          flushServerProcessed++;
          if (armedFlush) {
            armedFlush = false; flushHeld++; flushGate = gate();
            try { await flushGate.promise; }
            finally { flushGate = undefined; }
          }
        },
        close: async () => { closeCalls++; await connection.close(); },
        closed: () => closed,
      };
      if (held && holdAfterNativeConnect) {
        try { await holdConnect(); }
        catch (error) { await wrapped.close(); throw error; }
      }
      return wrapped;
    } catch (error) { connectFailed++; throw error; }
  };
  const observation = () => ({ connectStarted, connectHeld, connectCompleted, connectFailed,
    connectionsCreated, connectionsClosed, connectionsActive: active.size, closeCalls,
    flushStarted, flushServerProcessed, flushHeld, publishFrames, publishBytes,
    connectWaiting: connectGate !== undefined, flushWaiting: flushGate !== undefined, shutdownStarted });
  const control = (operation: PublicPublisherControl) => {
    if (operation === "arm_connect") {
      if (armedConnect || connectGate !== undefined) throw new Error("fixture connection gate already armed");
      armedConnect = true;
    } else if (operation === "arm_flush") {
      if (armedFlush || flushGate !== undefined) throw new Error("fixture flush gate already armed");
      armedFlush = true;
    } else if (operation === "release_flush") {
      if (flushGate === undefined) throw new Error("fixture flush is not held");
      flushGate.release();
    } else if (operation === "release_connect" || operation === "fail_connect") {
      if (connectGate === undefined) throw new Error("fixture connection is not held");
      failHeldConnect = operation === "fail_connect";
      connectGate.release();
    } else throw new Error("unknown fixture publisher control");
    return observation();
  };
  const release = () => { connectGate?.release(); flushGate?.release(); };
  return { connectionFactory, observation, control, release, markShutdown: () => { shutdownStarted = true; } };
}
