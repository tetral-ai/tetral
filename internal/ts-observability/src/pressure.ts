import type { Counter, MetricRegistry } from "./metric-registry.js";
import type { Telemetry } from "./telemetry.js";
import { currentTelemetry } from "./telemetry.js";

/** The quiet time after which a point's rejections for one cause count as a new episode. */
const rejectionEpisodeGapMs = 60_000;

/** The error a pressure point hands its caller when it refuses work; an observation ending with it counts `rejected`, never `error`. */
export class Rejection extends Error {
  constructor(readonly point: string, readonly pressureCause: string) {
    super(`${point} refused work: ${pressureCause}`);
    this.name = "Rejection";
  }
}

/**
 * A place that refuses work. It counts every refusal in
 * `<point>_rejected_total{cause}` and records the refusals of one cause as an
 * episode: `pressure.rejecting_started` at the first refusal after 60 quiet
 * seconds, `pressure.rejecting_ended` with the episode's count at the first
 * admission or refusal 60 seconds or more after its last refusal.
 */
export class PressurePoint {
  readonly #name: string;
  readonly #causes: ReadonlySet<string>;
  readonly #rejected: Counter;
  readonly #telemetry: Telemetry | undefined;
  readonly #clock: (() => number) | undefined;
  readonly #episodes = new Map<string, { last: number; count: number }>();

  /** causes is the point's fixed set; telemetry defaults to the installed one at each record. */
  constructor(registry: MetricRegistry, point: string, causes: readonly string[], options: { readonly telemetry?: Telemetry; readonly clock?: () => number } = {}) {
    this.#name = point;
    this.#causes = new Set(causes);
    this.#rejected = registry.counter(`${point}_rejected_total`, "Work refused at this point, by cause.", ["cause"]);
    this.#telemetry = options.telemetry;
    this.#clock = options.clock;
  }

  /** Counts one refusal and returns the Rejection to hand the caller. */
  reject(cause: string): Rejection {
    if (!this.#causes.has(cause)) throw new Error(`pressure point ${this.#name} has no cause ${cause}`);
    const now = this.#now();
    this.#endQuietEpisodes(now);
    let episode = this.#episodes.get(cause);
    if (episode === undefined) {
      episode = { last: now, count: 0 };
      this.#episodes.set(cause, episode);
      this.#record("pressure.rejecting_started", cause);
    }
    episode.last = now;
    episode.count++;
    this.#rejected.add(1, cause);
    return new Rejection(this.#name, cause);
  }

  /** Marks one admission, which ends the episodes that have been quiet for 60 seconds or more. */
  admit(): void {
    if (this.#episodes.size > 0) this.#endQuietEpisodes(this.#now());
  }

  #now(): number {
    return this.#clock?.() ?? (this.#telemetry ?? currentTelemetry()).monotonicClock();
  }

  #endQuietEpisodes(now: number): void {
    for (const [cause, episode] of this.#episodes) {
      if (now - episode.last < rejectionEpisodeGapMs) continue;
      this.#record("pressure.rejecting_ended", cause, episode.count);
      this.#episodes.delete(cause);
    }
  }

  #record(event: string, cause: string, rejected?: number): void {
    (this.#telemetry ?? currentTelemetry()).record("warn", {
      event, "pressure.point": this.#name, "pressure.cause": cause,
      ...(rejected === undefined ? {} : { "pressure.rejected": rejected }),
    });
  }
}
