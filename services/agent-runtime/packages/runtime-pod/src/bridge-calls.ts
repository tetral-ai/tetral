/** Owns generated unary handles until their callbacks settle, including cancellation and close. */
import { status } from "@grpc/grpc-js";
import type { CallOptions, ClientUnaryCall, Metadata, ServiceError } from "@grpc/grpc-js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { bridgeMethodDeadline, DefaultBridgeMethodPolicies } from "./bridge-policy.js";
import type { BridgeMethod, BridgeMethodPolicies } from "./bridge-policy.js";
import type { RuntimeBridgeDrainPhase } from "./lifecycle-policy.js";
interface OwnedCall {
    readonly join: Promise<void>;
    readonly startedAt: number;
    deadline: number;
    timer?: ReturnType<typeof setTimeout>;
}
export class BridgeUnaryCalls {
    private readonly calls = new Map<ClientUnaryCall, OwnedCall>();
    private stopping = false;
    private closed: Promise<void> | undefined;
    private phaseDeadline: number | undefined;
    private phaseTimer: ReturnType<typeof setTimeout> | undefined;
    private transitionTimer: ReturnType<typeof setTimeout> | undefined;
    private drainPhase: RuntimeBridgeDrainPhase | undefined;
    /** Operation stop differs from an ordinary, rejoinable per-attempt wait expiry. */
    operationStopped(options: {
        readonly signal?: AbortSignal;
        readonly deadline?: number;
    } = {}): boolean {
        return this.stopping || options.signal?.aborted === true ||
            Date.now() >= Math.min(this.phaseDeadline ?? Infinity, options.deadline ?? Infinity);
    }
    constructor(private readonly client: AgentRuntimeBridgeServiceClient, readonly policies: BridgeMethodPolicies = DefaultBridgeMethodPolicies) {
    }
    async call<Response>(method: BridgeMethod, request: unknown, metadata: Metadata, options: {
        readonly signal?: AbortSignal;
        readonly deadline?: number;
    } = {}): Promise<Response> {
        if (this.stopping)
            throw new Error("Bridge client is closing");
        options.signal?.throwIfAborted();
        const remaining = this.phaseDeadline === undefined
            ? options.deadline
            : Math.min(this.phaseDeadline, options.deadline ?? Infinity);
        const now = Date.now();
        const finalAttemptDeadline = this.drainPhase !== undefined && now >= this.drainPhase.currentStepDeadline
            ? now + this.drainPhase.settlementAttemptTimeoutMs : Infinity;
        const deadline = Math.min(bridgeMethodDeadline(this.policies, method, now, remaining), finalAttemptDeadline);
        if (deadline <= Date.now()) {
            // Expired shared authority must not dispatch a fresh business RPC.
            throw Object.assign(new Error("Bridge method deadline exceeded"), {
                code: status.DEADLINE_EXCEEDED,
                details: "Bridge method deadline exceeded",
            });
        }
        let joined!: () => void;
        const join = new Promise<void>((resolve) => {
            joined = resolve;
        });
        const owned: OwnedCall = {
            join, deadline, startedAt: now
        };
        let call: ClientUnaryCall | undefined;
        let settled = false;
        const cancel = (): void => {
            call?.cancel();
        };
        try {
            return await new Promise<Response>((resolve, reject) => {
                const invoke = this.client[method] as unknown as (request: unknown, metadata: Metadata, options: CallOptions, callback: (error: ServiceError | null, response: Response) => void) => ClientUnaryCall;
                call = invoke.call(this.client, request, metadata, {
                    deadline
                }, (error, response) => {
                    settled = true;
                    joined();
                    if (error !== null) {
                        // grpc-js can report its local deadline cancellation before the remote status arrives.
                        // Explicit parent/shutdown cancellation keeps its cancellation identity.
                        if (error.code === status.CANCELLED &&
                            !options.signal?.aborted &&
                            !this.stopping &&
                            Date.now() >= owned.deadline) {
                            const expired = new Error("Bridge method deadline exceeded") as ServiceError;
                            expired.code = status.DEADLINE_EXCEEDED;
                            expired.details = expired.message;
                            expired.metadata = error.metadata;
                            reject(expired);
                        }
                        else
                            reject(error);
                    }
                    else
                        resolve(response);
                });
                if (!settled)
                    this.calls.set(call, owned);
                options.signal?.addEventListener("abort", cancel, {
                    once: true
                });
                if (options.signal?.aborted)
                    cancel();
            });
        }
        finally {
            options.signal?.removeEventListener("abort", cancel);
            if (owned.timer !== undefined)
                clearTimeout(owned.timer);
            if (call !== undefined)
                this.calls.delete(call);
        }
    }
    /** Preserve current-step method budgets, then bound retained and new settlement attempts. */
    beginDrain(phase: RuntimeBridgeDrainPhase): void {
        if (this.stopping)
            return;
        this.drainPhase = this.drainPhase === undefined ? phase : {
            currentStepDeadline: Math.min(this.drainPhase.currentStepDeadline, phase.currentStepDeadline),
            settlementDeadline: Math.min(this.drainPhase.settlementDeadline, phase.settlementDeadline),
            settlementAttemptTimeoutMs: Math.min(this.drainPhase.settlementAttemptTimeoutMs, phase.settlementAttemptTimeoutMs),
        };
        this.setDeadline(this.drainPhase.settlementDeadline);
        if (this.transitionTimer !== undefined)
            clearTimeout(this.transitionTimer);
        const transition = (): void => {
            const phase = this.drainPhase!;
            for (const [call, owned] of this.calls) {
                owned.deadline = Math.min(owned.deadline, Math.max(owned.startedAt, phase.currentStepDeadline) + phase.settlementAttemptTimeoutMs, phase.settlementDeadline);
                if (owned.timer !== undefined)
                    clearTimeout(owned.timer);
                owned.timer = setTimeout(() => call.cancel(), Math.max(0, owned.deadline - Date.now()));
            }
        };
        if (Date.now() >= this.drainPhase.currentStepDeadline)
            transition();
        else
            this.transitionTimer = setTimeout(transition, this.drainPhase.currentStepDeadline - Date.now());
    }
    setDeadline(deadline: number): void {
        this.phaseDeadline = Math.min(this.phaseDeadline ?? Infinity, deadline);
        for (const owned of this.calls.values())
            owned.deadline = Math.min(owned.deadline, this.phaseDeadline);
        if (this.phaseTimer !== undefined)
            clearTimeout(this.phaseTimer);
        this.phaseTimer = setTimeout(() => {
            for (const call of this.calls.keys())
                call.cancel();
        }, Math.max(0, this.phaseDeadline - Date.now()));
    }
    /** Closing admission and cancelling real handles precedes the channel close. */
    close(): Promise<void> {
        if (this.closed !== undefined)
            return this.closed;
        this.stopping = true;
        if (this.phaseTimer !== undefined)
            clearTimeout(this.phaseTimer);
        if (this.transitionTimer !== undefined)
            clearTimeout(this.transitionTimer);
        this.closed = (async () => {
            const active = [...this.calls.entries()];
            for (const [call] of active)
                call.cancel();
            await Promise.all(active.map(([, owned]) => owned.join));
            this.client.close();
        })();
        return this.closed;
    }
}
const owners = new WeakMap<AgentRuntimeBridgeServiceClient, BridgeUnaryCalls>();
export function ownBridgeClient(client: AgentRuntimeBridgeServiceClient, policies: BridgeMethodPolicies = DefaultBridgeMethodPolicies): BridgeUnaryCalls {
    const existing = owners.get(client);
    if (existing !== undefined)
        return existing;
    const owner = new BridgeUnaryCalls(client, policies);
    owners.set(client, owner);
    return owner;
}
export function bridgeUnaryCall<Response>(client: AgentRuntimeBridgeServiceClient, method: BridgeMethod, request: unknown, metadata: Metadata, options?: {
    readonly signal?: AbortSignal;
    readonly deadline?: number;
}): Promise<Response> {
    return ownBridgeClient(client).call<Response>(method, request, metadata, options);
}
