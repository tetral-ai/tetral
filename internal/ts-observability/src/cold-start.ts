import type { TetralLogRecord } from "./index.js";
import type { Telemetry, TelemetryOptions } from "./telemetry.js";
import { createTelemetry, currentTelemetry } from "./telemetry.js";

/**
 * One start of a kind of component (a service pod, a Session computer, a thread)
 * whose phases follow one another. Each phase records
 * `cold_start.phase_started` and `cold_start.phase_completed`; the start
 * records one `cold_start.completed` and, when it succeeds, observes
 * `cold_start_phase_seconds` and `cold_start_seconds`, whose `engine` scope
 * leaves out the phases marked external.
 */
export class ColdStart {
  readonly #telemetry: Telemetry;
  readonly #kind: string;
  readonly #startKind: string;
  readonly #phases: readonly string[];
  readonly #external = new Set<string>();
  readonly #durations: number[];
  #phaseStarted: number;
  #current = 0;
  #finished = false;

  constructor(telemetry: Telemetry, started: number, kind: string, startKind: string, phases: readonly string[]) {
    this.#telemetry = telemetry;
    this.#kind = kind;
    this.#startKind = startKind;
    this.#phases = phases;
    this.#durations = phases.map(() => 0);
    this.#phaseStarted = started;
    this.#recordPhaseStarted();
  }

  /** Marks phases spent outside Engine, which the engine scope leaves out. */
  external(...phases: string[]): this {
    for (const phase of phases) this.#external.add(phase);
    return this;
  }

  /** The phase in progress, or undefined once the cold start ended. */
  phase(): string | undefined {
    return this.#finished ? undefined : this.#phases[this.#current];
  }

  /** Completes the current phase and starts the next one; after the last phase it completes the cold start. */
  advance(): void {
    if (this.#finished) return;
    this.#completePhase();
    if (this.#current === this.#phases.length) this.#complete("ok");
    else this.#recordPhaseStarted();
  }

  /** Completes the current phase and every later one at this instant and records the cold start as ok. */
  complete(): void {
    if (this.#finished) return;
    while (this.#current < this.#phases.length) {
      this.#completePhase();
      if (this.#current < this.#phases.length) this.#recordPhaseStarted();
    }
    this.#complete("ok");
  }

  /** Records the cold start as ended by code; it observes no histogram. */
  fail(code: string): void {
    if (this.#finished) return;
    this.#durations[Math.min(this.#current, this.#phases.length - 1)] = this.#telemetry.monotonicClock() - this.#phaseStarted;
    this.#complete(code);
  }

  #completePhase(): void {
    const now = this.#telemetry.monotonicClock();
    const elapsed = Math.max(0, now - this.#phaseStarted);
    const phase = this.#phases[this.#current]!;
    this.#durations[this.#current] = elapsed;
    this.#telemetry.record("info", { event: "cold_start.phase_completed", "cold_start.kind": this.#kind, "cold_start.phase": phase, [`${phase}_ms`]: Math.trunc(elapsed) });
    this.#current++;
    this.#phaseStarted = now;
  }

  #recordPhaseStarted(): void {
    this.#telemetry.record("info", { event: "cold_start.phase_started", "cold_start.kind": this.#kind, "cold_start.phase": this.#phases[this.#current] });
  }

  #complete(outcome: string): void {
    this.#finished = true;
    const record: Record<string, string | number> = { event: "cold_start.completed", "cold_start.kind": this.#kind, "cold_start.start_kind": this.#startKind, outcome };
    let total = 0, engine = 0;
    this.#phases.forEach((phase, i) => {
      const duration = this.#durations[i]!;
      record[`${phase}_ms`] = Math.trunc(duration);
      total += duration;
      if (!this.#external.has(phase)) engine += duration;
    });
    this.#telemetry.record("info", record);
    if (outcome !== "ok") return;
    const service = this.#telemetry.service;
    this.#phases.forEach((phase, i) => this.#telemetry.coldStartPhases.observe(this.#durations[i]! / 1000, service, this.#kind, phase));
    this.#telemetry.coldStarts.observe(total / 1000, service, this.#kind, "total");
    this.#telemetry.coldStarts.observe(engine / 1000, service, this.#kind, "engine");
  }
}

/** Starts a cold start of kind now, in its first phase. */
export function beginColdStart(kind: string, startKind: "new" | "reloaded" | "taken_over", phases: readonly string[], telemetry: Telemetry = currentTelemetry()): ColdStart {
  return new ColdStart(telemetry, telemetry.monotonicClock(), kind, startKind, phases);
}

let processColdStart: ColdStart | undefined;

/**
 * Starts this process's `service_pod` cold start, dated from process start
 * (zero on the monotonic clock), in its `config` phase. The service ends
 * `config` once its configuration is loaded and `dependencies` once every
 * dependency it opens at start is connected; its readiness first passing
 * completes it.
 */
export function beginServiceColdStart(telemetry: Telemetry = currentTelemetry()): ColdStart {
  processColdStart = new ColdStart(telemetry, 0, "service_pod", "new", ["config", "dependencies", "ready"]);
  return processColdStart;
}

/** Ends the process's service_pod phase when it is the phase in progress, so a repeated call changes nothing. */
export function endServiceColdStartPhase(phase: "config" | "dependencies"): void {
  if (processColdStart?.phase() === phase) processColdStart.advance();
}

/** Ends the process's service_pod cold start with a startup failure's error code; after readiness it changes nothing. */
export function failServiceColdStart(code: string): void {
  const coldStart = processColdStart;
  processColdStart = undefined;
  coldStart?.fail(code);
}

/**
 * Records the service_pod cold start of a process whose configuration failed
 * to load: the `config` phase from process start to now, ended by
 * `config_error`. The process's telemetry is built from that configuration, so
 * these records go through logger on a telemetry made for them.
 */
export function failServiceColdStartAtConfig<TRecord extends TetralLogRecord>(serviceName: string, logger: TelemetryOptions<TRecord>["logger"]): void {
  beginServiceColdStart(createTelemetry({ serviceName, logger }));
  failServiceColdStart("config_error");
}

/** Completes the process's service_pod cold start; the first readiness to pass calls it. */
export function completeServiceColdStart(): void {
  const coldStart = processColdStart;
  processColdStart = undefined;
  coldStart?.complete();
}
