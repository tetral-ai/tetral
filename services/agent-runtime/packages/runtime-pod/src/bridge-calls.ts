/** Owns generated unary handles until their callbacks settle, including cancellation and close. */
import { status } from "@grpc/grpc-js";
import type { CallOptions, ClientUnaryCall, Metadata, ServiceError } from "@grpc/grpc-js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { bridgeMethodDeadline, DefaultBridgeMethodPolicies } from "./bridge-policy.js";
import type { BridgeMethod, BridgeMethodPolicies } from "./bridge-policy.js";

export class BridgeUnaryCalls {
  private readonly calls = new Map<ClientUnaryCall, Promise<void>>();
  private stopping = false;
  private closed: Promise<void> | undefined;
  private phaseDeadline: number | undefined;
  private phaseTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly client: AgentRuntimeBridgeServiceClient,
    readonly policies: BridgeMethodPolicies = DefaultBridgeMethodPolicies,
  ) {}

  async call<Response>(
    method: BridgeMethod,
    request: unknown,
    metadata: Metadata,
    options: { readonly signal?: AbortSignal; readonly deadline?: number } = {},
  ): Promise<Response> {
    if (this.stopping) throw new Error("Bridge client is closing");
    options.signal?.throwIfAborted();
    const remaining =
      this.phaseDeadline === undefined
        ? options.deadline
        : Math.min(this.phaseDeadline, options.deadline ?? Infinity);
    const deadline = bridgeMethodDeadline(this.policies, method, Date.now(), remaining);
    let joined!: () => void;
    const join = new Promise<void>((resolve) => {
      joined = resolve;
    });
    let call: ClientUnaryCall | undefined;
    let settled = false;
    const cancel = (): void => {
      call?.cancel();
    };
    try {
      return await new Promise<Response>((resolve, reject) => {
        const invoke = this.client[method] as unknown as (
          request: unknown,
          metadata: Metadata,
          options: CallOptions,
          callback: (error: ServiceError | null, response: Response) => void,
        ) => ClientUnaryCall;
        call = invoke.call(this.client, request, metadata, { deadline }, (error, response) => {
          settled = true;
          joined();
          if (error !== null) {
            // grpc-js can report its local deadline cancellation before the remote status arrives.
            // Explicit parent/shutdown cancellation keeps its cancellation identity.
            if (
              error.code === status.CANCELLED &&
              !options.signal?.aborted &&
              !this.stopping &&
              Date.now() >= deadline
            ) {
              const expired = new Error("Bridge method deadline exceeded") as ServiceError;
              expired.code = status.DEADLINE_EXCEEDED;
              expired.details = expired.message;
              expired.metadata = error.metadata;
              reject(expired);
            } else reject(error);
          } else resolve(response);
        });
        if (!settled) this.calls.set(call, join);
        options.signal?.addEventListener("abort", cancel, { once: true });
        if (options.signal?.aborted) cancel();
      });
    } finally {
      options.signal?.removeEventListener("abort", cancel);
      if (call !== undefined) this.calls.delete(call);
    }
  }

  setDeadline(deadline: number): void {
    this.phaseDeadline = Math.min(this.phaseDeadline ?? Infinity, deadline);
    if (this.phaseTimer !== undefined) clearTimeout(this.phaseTimer);
    this.phaseTimer = setTimeout(
      () => {
        for (const call of this.calls.keys()) call.cancel();
      },
      Math.max(0, this.phaseDeadline - Date.now()),
    );
  }

  /** Closing admission and cancelling real handles precedes the channel close. */
  close(): Promise<void> {
    if (this.closed !== undefined) return this.closed;
    this.stopping = true;
    if (this.phaseTimer !== undefined) clearTimeout(this.phaseTimer);
    this.closed = (async () => {
      const active = [...this.calls.entries()];
      for (const [call] of active) call.cancel();
      await Promise.all(active.map(([, join]) => join));
      this.client.close();
    })();
    return this.closed;
  }
}

const owners = new WeakMap<AgentRuntimeBridgeServiceClient, BridgeUnaryCalls>();

export function ownBridgeClient(
  client: AgentRuntimeBridgeServiceClient,
  policies: BridgeMethodPolicies = DefaultBridgeMethodPolicies,
): BridgeUnaryCalls {
  const existing = owners.get(client);
  if (existing !== undefined) return existing;
  const owner = new BridgeUnaryCalls(client, policies);
  owners.set(client, owner);
  return owner;
}

export function bridgeUnaryCall<Response>(
  client: AgentRuntimeBridgeServiceClient,
  method: BridgeMethod,
  request: unknown,
  metadata: Metadata,
  options?: { readonly signal?: AbortSignal; readonly deadline?: number },
): Promise<Response> {
  return ownBridgeClient(client).call<Response>(method, request, metadata, options);
}
