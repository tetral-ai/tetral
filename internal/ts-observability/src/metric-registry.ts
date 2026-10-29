import { defaultUndeclaredNamePolicy, lookupMetric, UndeclaredNameError } from "./declarations.js";
import type { Declarations, UndeclaredNamePolicy } from "./declarations.js";

/** The only two bucket sets a duration histogram may use, in seconds. */
export const requestDurationBuckets: readonly number[] = Object.freeze([0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]);
export const longOperationDurationBuckets: readonly number[] = Object.freeze([0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800]);

/** One rendered sample; `family` groups histogram series under one header. */
export interface MetricSample {
  readonly name: string;
  readonly family: string;
  readonly help: string;
  readonly type: "counter" | "gauge" | "histogram";
  readonly labels: readonly (readonly [string, string])[];
  readonly value: number;
}

interface Family {
  readonly name: string;
  readonly help: string;
  readonly type: "counter" | "gauge" | "histogram";
  readonly labels: readonly string[];
  /** Each label's declared values; undefined admits any value of a closed domain fixed in code. */
  readonly allowed: readonly (ReadonlySet<string> | undefined)[];
  readonly buckets: readonly number[];
  readonly series: Map<string, Series>;
}
interface Series { readonly labels: readonly string[]; value: number; count: number; sum: number; readonly buckets: number[] }

/**
 * One service's metric families, each created from its declaration as the
 * README's Declarations section describes. Registering a name twice returns
 * the first family when its label order is the same and throws otherwise: one
 * name has one meaning per process.
 */
export class MetricRegistry {
  readonly #families = new Map<string, Family>();
  readonly #declarations: Declarations | undefined;
  readonly #undeclared: UndeclaredNamePolicy;

  constructor(declarations?: Declarations, undeclared: UndeclaredNamePolicy = defaultUndeclaredNamePolicy()) {
    this.#declarations = declarations;
    this.#undeclared = undeclared;
  }

  counter(name: string, help: string, labels: readonly string[] = []): Counter {
    return new Counter(this.#register(name, help, "counter", labels));
  }
  gauge(name: string, help: string, labels: readonly string[] = []): Gauge {
    return new Gauge(this.#register(name, help, "gauge", labels));
  }
  /** The histogram's buckets are the declared set. */
  histogram(name: string, help: string, labels: readonly string[] = []): Histogram {
    return new Histogram(this.#register(name, help, "histogram", labels));
  }

  /** Every family in registration order, series sorted by their label values. */
  collect(): MetricSample[] {
    const samples: MetricSample[] = [];
    for (const family of this.#families.values()) {
      for (const [, series] of [...family.series].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
        const labels = family.labels.map((label, i) => [label, series.labels[i]!] as const);
        const sample = (suffix: string, sampleLabels: readonly (readonly [string, string])[], value: number): void => {
          samples.push({ name: family.name + suffix, family: family.name, help: family.help, type: family.type, labels: sampleLabels, value });
        };
        if (family.type !== "histogram") { sample("", labels, series.value); continue; }
        family.buckets.forEach((bound, i) => sample("_bucket", [...labels, ["le", String(bound)]], series.buckets[i]!));
        sample("_bucket", [...labels, ["le", "+Inf"]], series.count);
        sample("_count", labels, series.count);
        sample("_sum", labels, series.sum);
      }
    }
    return samples;
  }

  #register(name: string, help: string, type: Family["type"], labels: readonly string[]): Family | undefined {
    const declared = lookupMetric(this.#declarations, name);
    if (declared === undefined) {
      if (this.#undeclared === "throw") throw new UndeclaredNameError("metric family", name);
      return undefined;
    }
    const declaredLabels = Object.keys(declared.labels);
    if (declared.type !== type || declaredLabels.length !== labels.length || labels.some((label) => !Object.hasOwn(declared.labels, label))) {
      if (this.#undeclared === "throw") {
        throw new Error(`metric family ${name} is declared as a ${declared.type} with labels ${declaredLabels.sort().join(",")}, not a ${type} with labels ${labels.join(",")}`);
      }
      return undefined;
    }
    const existing = this.#families.get(name);
    if (existing !== undefined) {
      if (existing.labels.join(",") !== labels.join(",")) throw new Error(`metric family ${name} registered with two label orders`);
      return existing;
    }
    const allowed = labels.map((label) => {
      const values = declared.labels[label]!;
      return typeof values === "string" ? undefined : new Set(values);
    });
    const buckets = type !== "histogram" ? [] : declared.buckets === "request" ? requestDurationBuckets : declared.buckets === "long" ? longOperationDurationBuckets : declared.buckets!;
    const family: Family = { name, help, type, labels: [...labels], allowed, buckets: [...buckets], series: new Map() };
    this.#families.set(name, family);
    return family;
  }
}

/** Returns undefined for a label value its family refuses. */
function seriesFor(family: Family, values: readonly string[]): Series | undefined {
  if (values.length !== family.labels.length) throw new Error(`metric family ${family.name} observed with the wrong label count`);
  if (values.some((value, i) => family.allowed[i]?.has(value) === false)) return undefined;
  const key = values.join("\0");
  let series = family.series.get(key);
  if (series === undefined) {
    series = { labels: [...values], value: 0, count: 0, sum: 0, buckets: family.buckets.map(() => 0) };
    family.series.set(key, series);
  }
  return series;
}

/** Counter, Gauge and Histogram are handles to one family; a handle without one records nothing. */
export class Counter {
  readonly #family: Family | undefined;
  constructor(family: Family | undefined) { this.#family = family; }
  add(delta: number, ...labels: string[]): void {
    if (this.#family === undefined || !(delta >= 0)) return;
    const series = seriesFor(this.#family, labels);
    if (series !== undefined) series.value += delta;
  }
}
export class Gauge {
  readonly #family: Family | undefined;
  constructor(family: Family | undefined) { this.#family = family; }
  set(value: number, ...labels: string[]): void {
    if (this.#family === undefined || !Number.isFinite(value)) return;
    const series = seriesFor(this.#family, labels);
    if (series !== undefined) series.value = value;
  }
}
export class Histogram {
  readonly #family: Family | undefined;
  constructor(family: Family | undefined) { this.#family = family; }
  /** Exact bounds are inclusive; a negative or non-finite sample is dropped rather than shown as zero. */
  observe(value: number, ...labels: string[]): void {
    if (this.#family === undefined || !Number.isFinite(value) || value < 0) return;
    const series = seriesFor(this.#family, labels);
    if (series === undefined) return;
    series.count++;
    series.sum += value;
    this.#family.buckets.forEach((bound, i) => { if (value <= bound) series.buckets[i]!++; });
  }
}

/**
 * Prometheus text for samples of several registries. Samples of one family
 * are grouped under its single HELP/TYPE header, in first-appearance order.
 */
export function renderMetrics(samples: readonly MetricSample[]): string {
  const families = new Map<string, MetricSample[]>();
  for (const sample of samples) {
    const family = families.get(sample.family);
    if (family === undefined) families.set(sample.family, [sample]); else family.push(sample);
  }
  let text = "";
  for (const [name, family] of families) {
    text += `# HELP ${name} ${family[0]!.help}\n# TYPE ${name} ${family[0]!.type}\n`;
    for (const sample of family) {
      const labels = sample.labels.length === 0 ? "" : `{${sample.labels.map(([key, value]) => `${key}="${escapeLabelValue(value)}"`).join(",")}}`;
      text += `${sample.name}${labels} ${Number.isFinite(sample.value) ? sample.value : 0}\n`;
    }
  }
  return text;
}

function escapeLabelValue(value: string): string {
  return value.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll("\"", "\\\"");
}
