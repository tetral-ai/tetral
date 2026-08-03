import type {
  RequestExecutionSnapshot,
} from "../session/session-configuration.js";

export type { RequestExecutionSnapshot } from "../session/session-configuration.js";

export type RequestTurnPhase =
  | "assembling"
  | "provider_open"
  | "provider_closed"
  | "effects_draining"
  | "retry_wait"
  | "turn_closed";

/** Explicit request-turn phase and immutable execution-policy snapshot. */
export class RequestTurnState {
  #phase: RequestTurnPhase = "assembling";
  #snapshot: RequestExecutionSnapshot;

  constructor(snapshot: RequestExecutionSnapshot) {
    this.#snapshot = snapshot;
  }

  phase(): RequestTurnPhase {
    return this.#phase;
  }

  snapshot(): RequestExecutionSnapshot {
    return this.#snapshot;
  }

  openProvider(): void {
    this.transition("assembling", "provider_open");
  }

  closeProvider(): void {
    this.transition("provider_open", "provider_closed");
  }

  beginEffectsDrain(): void {
    this.transition("provider_closed", "effects_draining");
  }

  beginRetryWait(): void {
    this.transition("effects_draining", "retry_wait");
  }

  beginRetry(snapshot: RequestExecutionSnapshot): void {
    this.transition("retry_wait", "assembling");
    this.#snapshot = snapshot;
  }

  closeTurn(): void {
    if (this.#phase !== "effects_draining" && this.#phase !== "retry_wait") {
      throw new Error(`invalid request turn phase transition: ${this.#phase} -> turn_closed`);
    }
    this.#phase = "turn_closed";
  }

  private transition(expected: RequestTurnPhase, next: RequestTurnPhase): void {
    if (this.#phase !== expected) {
      throw new Error(`invalid request turn phase transition: ${this.#phase} -> ${next}`);
    }
    this.#phase = next;
  }
}
