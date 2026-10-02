/** Bridge-backed registration and immutable binding-release receipts for one process boot. */
import { credentials, status } from "@grpc/grpc-js";
import type { Metadata } from "@grpc/grpc-js";
import {
  AgentRuntimeBridgeServiceClient,
  RuntimeProcessPhase,
  RuntimeHandoffDisposition,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  RegisterRuntimeProcessResponse,
  ReportRuntimeProcessResponse,
  ReleaseRuntimeBindingRequest,
  ReleaseRuntimeBindingResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { BridgeUnaryCalls } from "./bridge-calls.js";
import type { BridgeMethodPolicies } from "./bridge-policy.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import type { ServiceAccountTokenConfig } from "./auth.js";

export interface RuntimeProcessPort {
  readonly runtimeProcessId: string;
  register(deadline: number): Promise<void>;
  report(phase: "accepting" | "draining", deadline: number): Promise<void>;
  release(
    request: Omit<ReleaseRuntimeBindingRequest, "runtimeProcessId">,
    deadline: number,
  ): Promise<ReleaseRuntimeBindingResponse>;
  close(): Promise<void>;
}

export class BridgeRuntimeProcess implements RuntimeProcessPort {
  private registration: RegisterRuntimeProcessResponse | undefined;
  private readonly owner: BridgeUnaryCalls;
  private readonly metadata: () => Promise<Metadata>;

  constructor(
    readonly runtimeProcessId: string,
    options: {
      readonly address: string;
      readonly tokenPath: string;
      readonly policies: BridgeMethodPolicies;
      readonly client?: AgentRuntimeBridgeServiceClient;
      readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
    },
  ) {
    this.owner = new BridgeUnaryCalls(
      options.client ??
        new AgentRuntimeBridgeServiceClient(options.address, credentials.createInsecure()),
      options.policies,
    );
    this.metadata = () =>
      (options.metadataFactory ?? buildOutboundBearerMetadata)({ tokenPath: options.tokenPath });
  }

  async register(deadline: number): Promise<void> {
    const registration = await this.owner.call<RegisterRuntimeProcessResponse>(
      "registerRuntimeProcess",
      { runtimeProcessId: this.runtimeProcessId },
      await this.metadata(),
      { deadline },
    );
    if (
      registration.runtimeProcessId !== this.runtimeProcessId ||
      !Number.isSafeInteger(registration.registrationOrder) ||
      registration.registrationOrder < 1 ||
      !registration.registrationReceipt ||
      (this.registration !== undefined &&
        (this.registration.registrationOrder !== registration.registrationOrder ||
          this.registration.registrationReceipt !== registration.registrationReceipt))
    ) {
      throw new Error("Runtime process registration acknowledgement invalid");
    }
    this.registration = registration;
  }

  async report(phase: "accepting" | "draining", deadline: number): Promise<void> {
    if (this.registration === undefined) throw new Error("Runtime process is not registered");
    const expected =
      phase === "accepting"
        ? RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_ACCEPTING
        : RuntimeProcessPhase.RUNTIME_PROCESS_PHASE_DRAINING;
    const response = await this.owner.call<ReportRuntimeProcessResponse>(
      "reportRuntimeProcess",
      {
        runtimeProcessId: this.runtimeProcessId,
        registrationReceipt: this.registration.registrationReceipt,
        phase: expected,
      },
      await this.metadata(),
      { deadline },
    );
    if (
      response.runtimeProcessId !== this.runtimeProcessId ||
      response.phase !== expected ||
      !response.current
    ) {
      throw new Error("Runtime process report acknowledgement invalid");
    }
  }

  async release(
    request: Omit<ReleaseRuntimeBindingRequest, "runtimeProcessId">,
    deadline: number,
  ): Promise<ReleaseRuntimeBindingResponse> {
    const response = await this.owner.call<ReleaseRuntimeBindingResponse>(
      "releaseRuntimeBinding",
      {
        ...request,
        runtimeProcessId: this.runtimeProcessId,
      },
      await this.metadata(),
      { deadline },
    );
    if (
      response.operationId !== request.operationId ||
      !response.handoffId ||
      response.releasedBinding?.bindingId !== request.bindingId ||
      response.releasedBinding.bindingGeneration !== request.bindingGeneration ||
      response.releasedBinding.runtimeProcessId !== this.runtimeProcessId
    ) {
      throw new Error("Runtime binding release acknowledgement invalid");
    }
    let previous = "";
    for (const thread of response.threads) {
      if (
        !thread.sessionThreadId ||
        thread.sessionThreadId <= previous ||
        (thread.disposition !== RuntimeHandoffDisposition.RUNTIME_HANDOFF_DISPOSITION_IDLE &&
          thread.disposition !== RuntimeHandoffDisposition.RUNTIME_HANDOFF_DISPOSITION_RECOVER) ||
        Boolean(thread.queueJobId) !==
          (thread.disposition === RuntimeHandoffDisposition.RUNTIME_HANDOFF_DISPOSITION_RECOVER)
      ) {
        throw new Error("Runtime binding release thread acknowledgement invalid");
      }
      previous = thread.sessionThreadId;
    }
    return response;
  }

  close(): Promise<void> {
    return this.owner.close();
  }
}

/** These three named receipt-bearing operations retry only transport failure, with unchanged identity. */
export async function retryRuntimeProcessOperation(
  operation: () => Promise<void>,
  deadline: number,
): Promise<void> {
  for (;;) {
    try {
      await operation();
      return;
    } catch (error) {
      const code =
        typeof error === "object" && error !== null && "code" in error ? error.code : undefined;
      if (
        (code !== status.UNAVAILABLE && code !== status.DEADLINE_EXCEEDED) ||
        Date.now() + 100 >= deadline
      )
        throw error;
      await new Promise<void>((resolve) =>
        setTimeout(resolve, Math.min(100, deadline - Date.now())),
      );
    }
  }
}
