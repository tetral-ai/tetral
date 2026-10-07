import { MaxProviderResponseFrameBytes } from "@tetral/gateway-protocol/src/bounds.js";
import { ProviderStreamEvent as ProviderStreamFrame } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
/**
 * @packageDocumentation
 *
 * Adapts the generated Provider Gateway service definition to the in-process
 * service shell. Application composition creates and binds this server; the
 * adapter forwards Runtime metadata and requests to the shell, writes provider
 * events with gRPC backpressure, and propagates stream cancellation through an
 * abort signal. The same server registers the unary Web compatibility method.
 * Status-bearing service failures retain their bounded gRPC code and message,
 * while unknown failures and listener errors surface generic diagnostics.
 */

import {
  Metadata,
  Server,
  ServerCredentials,
  status,
} from "@grpc/grpc-js";
import type {
  sendUnaryData,
  ServerUnaryCall,
  ServerWritableStream,
  ServiceError,
} from "@grpc/grpc-js";
import {
  ProviderGatewayServiceService,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderGatewayServiceServer,
  ProviderRequest,
  ProviderStreamEvent,
  RunWebRequest,
  RunWebResponse,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { grpcServerOptions } from "./bounds.js";
import { GrpcStatusError } from "./errors.js";
import type { ProviderGatewayServiceShell } from "./service.js";

/** Owns the gRPC server together with asynchronous bind and graceful shutdown controls. */
export interface GatewayGrpcServer {
  readonly server: Server;
  readonly bind: (address: string) => Promise<number>;
  readonly shutdown: (deadline?: Date) => Promise<void>;
}

/**
 * Creates an unbound internal Provider Gateway server with process-scoped
 * message and connection bounds.
 *
 * The listener uses insecure transport. Independently, the service shell
 * authenticates workload bearer metadata and verifies Runtime binding tokens
 * before admitting work; those checks do not provide transport security.
 */
export function createGatewayGrpcServer(service: ProviderGatewayServiceShell): GatewayGrpcServer {
  const server = new Server(grpcServerOptions());
  const handlers = new Set<Promise<void>>();
  const implementation: ProviderGatewayServiceServer = {
    streamProviderRequest: (call) => {
      const handler = streamProviderRequest(service, call);
      handlers.add(handler);
      void handler.then(() => handlers.delete(handler), () => handlers.delete(handler));
    },
    runWeb: unaryHandler((request, metadata) => service.runWeb(request, metadata)),
  };
  server.addService(ProviderGatewayServiceService, implementation);
  return {
    server,
    bind: async (address) =>
      await new Promise<number>((resolve, reject) => {
        server.bindAsync(
          address,
          ServerCredentials.createInsecure(),
          (error, port) => {
            if (error !== null) {
              reject(new Error("grpc listener unavailable"));
              return;
            }
            resolve(port);
          },
        );
      }),
    shutdown: async (deadline = new Date(Date.now() + 5000)) => {
      await new Promise<void>((resolve) => {
        const timer = setTimeout(
          () => {
            server.forceShutdown();
            resolve();
          },
          Math.max(0, deadline.getTime() - Date.now()),
        );
        server.tryShutdown(() => {
          clearTimeout(timer);
          resolve();
        });
      });
      await Promise.all(handlers);
    },
  };
}

async function streamProviderRequest(
  service: ProviderGatewayServiceShell,
  call: ServerWritableStream<ProviderRequest, ProviderStreamEvent>,
): Promise<void> {
  const abortController = new AbortController();
  const custody = new Set<Promise<void>>();
  const registerWriteCustody = (joined: Promise<void>): void => {
    custody.add(joined);
    void joined.then(() => custody.delete(joined));
  };
  const abort = (): void => abortController.abort();
  call.once("cancelled", abort);
  call.once("error", abort);
  call.once("close", abort);
  try {
    await writeProviderStreamEvents(call, service.streamProviderRequest(call.request, call.metadata, { abortSignal: abortController.signal }), {registerWriteCustody,deadline:event=>service.providerFrameDeadline(event),onDeadline:event=>{service.recordProviderWriteTimeout(event);abortController.abort(new DOMException("Provider request timed out.","AbortError"));},onWriteStarted:event=>service.recordCompleteFrameWriteStarted(event),onWriteCallback:event=>service.recordCompleteFrameWriteCallback(event),onWriteSettled:(event,outcome)=>service.recordCompleteFrameWrite(event,outcome)});
    if (!call.cancelled) {
      call.end();
    }
  } catch (error) {
    if (!call.cancelled) {
      call.emit("error", toServiceError(error));
    }
  } finally {
    abortController.abort();
    // Publish the terminal status before joining a callback that grpc-js may
    // omit on cancellation. Native Writable close is the alternate witness.
    await Promise.all(custody);
    call.off("cancelled", abort);
    call.off("error", abort);
    call.off("close", abort);
  }
}

/** One application frame remains owned until local write callback and any required drain. */
export async function writeProviderStreamEvents(
  call: ProviderWritableStream,
  events: AsyncIterable<ProviderStreamEvent>,
  options: { readonly registerWriteCustody?: (joined: Promise<void>) => void; readonly onWriteStarted?: (event: ProviderStreamEvent) => void; readonly onWriteCallback?: (event: ProviderStreamEvent) => void; readonly onWriteSettled?: (event: ProviderStreamEvent, outcome: "success" | "error" | "cancelled") => void; readonly deadline?: (event:ProviderStreamEvent)=>number|undefined; readonly onDeadline?: (event:ProviderStreamEvent)=>void; readonly clock?: ProviderWriteClock } = {},
): Promise<void> {
  for await (const event of events) {
    if (call.cancelled) { try { options.onWriteSettled?.(event,"cancelled"); } catch {} return; }
    if (ProviderStreamFrame.encode(event).finish().byteLength > MaxProviderResponseFrameBytes) {
      try { options.onWriteSettled?.(event,"error"); } catch {}
      throw new GrpcStatusError(status.RESOURCE_EXHAUSTED,"gateway response frame exceeds transport bound");
    }
    try { options.onWriteStarted?.(event); } catch { /* Observer cannot change delivery. */ }
    const pending = writeFrame(call,event,options.onWriteCallback,options.onWriteSettled,options.deadline?.(event),()=>{try{options.onDeadline?.(event);}catch{}},options.clock ?? SystemWriteClock);
    try { options.registerWriteCustody?.(pending.custody); } catch { /* Observer cannot alter transport. */ }
    const written = await pending.result;
    if (!written) return;
  }
}

export interface ProviderWritableStream {
  readonly cancelled: boolean;
  readonly write: (event: ProviderStreamEvent, callback: (error?: Error | null) => void) => boolean;
  readonly on: (event: "cancelled" | "drain" | "error" | "close", listener: (...args: any[]) => void) => unknown;
  readonly off: (event: "cancelled" | "drain" | "error" | "close", listener: (...args: any[]) => void) => unknown;
}

/** Outcome stops upstream promptly; custody ends only at callback/drain or close. */
function writeFrame(
  call: ProviderWritableStream,
  event: ProviderStreamEvent,
  onCallback: ((event: ProviderStreamEvent) => void) | undefined,
  onReleased: ((event: ProviderStreamEvent, outcome: "success" | "error" | "cancelled") => void) | undefined,
  deadline: number | undefined,
  onDeadline: () => void,
  clock: ProviderWriteClock,
): { readonly result: Promise<boolean>; readonly custody: Promise<void> } {
  // grpc-js can omit its callback on cancellation or HTTP/2 write failure.
  // Its server cancellation path destroys the Writable and emits close.
  const holder = { event: event as ProviderStreamEvent | undefined, onCallback, onReleased };
  let deadlineObserver: (() => void) | undefined = onDeadline;
  let releaseCustody!: () => void;
  const custody = new Promise<void>(resolve => { releaseCustody = resolve; });
  const result = new Promise<boolean>((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let submitted = false, returned = false, callbackDone = false, needsDrain = false, drained = false, closed = false;
    let outcome: "success" | "error" | "cancelled" | undefined;
    const release = (): void => {
      if (outcome === undefined || holder.event === undefined) return;
      if (submitted && !callbackDone && !closed) return;
      const owned = holder.event;
      const observe = holder.onReleased;
      holder.event = undefined; holder.onCallback = undefined; holder.onReleased = undefined;
      call.off("close", close);
      try { observe?.(owned, outcome); } catch { /* Observability is fail-open. */ }
      releaseCustody();
    };
    const cleanupOutcome = (): void => {
      if (timer !== undefined) clock.clearTimeout(timer);
      deadlineObserver = undefined;
      call.off("cancelled", cancelled); call.off("drain", drain); call.off("error", error);
    };
    const finish = (written: boolean, failure?: unknown): void => {
      if (outcome !== undefined) return;
      outcome = failure !== undefined ? "error" : written ? "success" : "cancelled";
      cleanupOutcome();
      failure === undefined ? resolve(written) : reject(failure);
      release();
    };
    const complete = (): void => { if (returned && callbackDone && (!needsDrain || drained)) finish(true); };
    const cancelled = (): void => finish(false);
    const close = (): void => { closed = true; finish(false); release(); };
    const error = (failure: unknown): void => finish(false, failure ?? new Error("provider transport failed"));
    const drain = (): void => { drained = true; complete(); };
    call.on("cancelled", cancelled); call.on("drain", drain); call.on("error", error); call.on("close", close);
    if (call.cancelled) { cancelled(); return; }
    const timedOut = (): void => { deadlineObserver?.(); error(new GrpcStatusError(status.DEADLINE_EXCEEDED,"gateway provider request deadline exceeded")); };
    if (deadline !== undefined) {
      const remaining = deadline - clock.now();
      if (remaining <= 0) { timedOut(); return; }
      timer = clock.setTimeout(timedOut, remaining);
    }
    try {
      submitted = true;
      needsDrain = !call.write(event, (failure) => {
        if (!callbackDone) {
          callbackDone = true;
          if (holder.event !== undefined) {
            try { holder.onCallback?.(holder.event); } catch { /* Observability is fail-open. */ }
          }
        }
        if (failure != null) error(failure); else complete();
        release();
      });
      returned = true; complete();
    } catch (failure) {
      // A synchronous throw did not hand a frame to the Writable.
      submitted = false; error(failure);
    }
  });
  return { result, custody };
}

function unaryHandler<Request, Response>(
  handler: (request: Request, metadata: Metadata) => Promise<Response>,
): (call: ServerUnaryCall<Request, Response>, callback: sendUnaryData<Response>) => void {
  return (call, callback) => {
    void unary(() => handler(call.request, call.metadata), callback);
  };
}

async function unary<Response>(
  handler: () => Promise<Response>,
  callback: sendUnaryData<Response>,
): Promise<void> {
  try {
    callback(null, await handler());
  } catch (error) {
    callback(toServiceError(error), null);
  }
}

function toServiceError(error: unknown): ServiceError {
  if (error instanceof GrpcStatusError) {
    return serviceError(error.code, error.message);
  }
  return serviceError(status.INTERNAL, "gateway service failed");
}

function serviceError(code: status, message: string): ServiceError {
  const error = new Error(message) as ServiceError;
  error.code = code;
  error.details = message;
  error.metadata = new Metadata();
  return error;
}

/** Same-process absolute deadlines remain armed while a complete frame is writable-owned. */
export interface ProviderWriteClock {
  readonly now:()=>number;
  readonly setTimeout:(callback:()=>void,delayMs:number)=>ReturnType<typeof setTimeout>;
  readonly clearTimeout:(timer:ReturnType<typeof setTimeout>)=>void;
}
const SystemWriteClock:ProviderWriteClock={now:()=>performance.now(),setTimeout:(callback,delay)=>setTimeout(callback,delay),clearTimeout:timer=>clearTimeout(timer)};
