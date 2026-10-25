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
  readonly buckets: readonly number[];
  readonly series: Map<string, Series>;
}
interface Series { readonly labels: readonly string[]; value: number; count: number; sum: number; readonly buckets: number[] }

/**
 * Metric families whose label values come from sets fixed in code.
 * Registering a name twice returns the first family when its shape is the
 * same and throws otherwise: one name has one meaning per process.
 */
export class MetricRegistry {
  readonly #families = new Map<string, Family>();

  counter(name: string, help: string, labels: readonly string[] = []): Counter {
    return new Counter(this.#register(name, help, "counter", [], labels));
  }
  gauge(name: string, help: string, labels: readonly string[] = []): Gauge {
    return new Gauge(this.#register(name, help, "gauge", [], labels));
  }
  histogram(name: string, help: string, buckets: readonly number[], labels: readonly string[] = []): Histogram {
    return new Histogram(this.#register(name, help, "histogram", buckets, labels));
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

  #register(name: string, help: string, type: Family["type"], buckets: readonly number[], labels: readonly string[]): Family {
    const existing = this.#families.get(name);
    if (existing !== undefined) {
      if (existing.type !== type || existing.labels.join(",") !== labels.join(",") || existing.buckets.join(",") !== buckets.join(",")) {
        throw new Error(`metric family ${name} registered with two shapes`);
      }
      return existing;
    }
    const family: Family = { name, help, type, labels: [...labels], buckets: [...buckets], series: new Map() };
    this.#families.set(name, family);
    return family;
  }
}

function seriesFor(family: Family, values: readonly string[]): Series {
  if (values.length !== family.labels.length) throw new Error(`metric family ${family.name} observed with the wrong label count`);
  const key = values.join("\0");
  let series = family.series.get(key);
  if (series === undefined) {
    series = { labels: [...values], value: 0, count: 0, sum: 0, buckets: family.buckets.map(() => 0) };
    family.series.set(key, series);
  }
  return series;
}

export class Counter {
  readonly #family: Family;
  constructor(family: Family) { this.#family = family; }
  add(delta: number, ...labels: string[]): void {
    if (!(delta >= 0)) return;
    seriesFor(this.#family, labels).value += delta;
  }
}
export class Gauge {
  readonly #family: Family;
  constructor(family: Family) { this.#family = family; }
  set(value: number, ...labels: string[]): void {
    if (!Number.isFinite(value)) return;
    seriesFor(this.#family, labels).value = value;
  }
}
export class Histogram {
  readonly #family: Family;
  constructor(family: Family) { this.#family = family; }
  /** Exact bounds are inclusive; a negative or non-finite sample is dropped rather than shown as zero. */
  observe(value: number, ...labels: string[]): void {
    if (!Number.isFinite(value) || value < 0) return;
    const series = seriesFor(this.#family, labels);
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
