import libraryTable from "../observability.json";

/**
 * One component's observability table: the events it writes, the operations
 * it observes, its metric families and its span templates. The file is
 * `observability.json` at the root of the component's package; the Go library
 * reads the same format.
 */
export interface Declarations {
  readonly service: string;
  readonly events: Readonly<Record<string, EventDeclaration>>;
  readonly operations: Readonly<Record<string, OperationDeclaration>>;
  readonly metrics: Readonly<Record<string, MetricDeclaration>>;
  readonly spans: Readonly<Record<string, SpanDeclaration>>;
}
export type DeclaredLevel = "DEBUG" | "INFO" | "WARN" | "ERROR";
export type FieldType = "string" | "integer" | "number" | "boolean";
export type Audience = "operators" | "benchmarks" | "developers";
export interface EventDeclaration {
  readonly level: DeclaredLevel;
  readonly log_type: "app" | "audit";
  readonly audience: Audience;
  readonly fields: Readonly<Record<string, FieldType>>;
  /** The fixed safe text a record carries for each error.code. */
  readonly errors?: Readonly<Record<string, { readonly class: string; readonly message_safe: string }>>;
}
export interface OperationDeclaration {
  readonly long: boolean;
  readonly errors?: readonly string[];
}
/** A label lists its allowed values, or names, as a string, a closed domain fixed in code. */
export interface MetricDeclaration {
  readonly type: "counter" | "gauge" | "histogram" | "summary";
  readonly audience: Audience;
  readonly labels: Readonly<Record<string, readonly string[] | string>>;
  readonly buckets?: "request" | "long" | readonly number[];
}
export interface SpanDeclaration {
  readonly attributes: readonly string[];
}

const eventNamePattern = /^[a-z][a-z0-9_.-]{0,127}$/;
const levels = new Set(["DEBUG", "INFO", "WARN", "ERROR"]);
const fieldTypes = new Set(["string", "integer", "number", "boolean"]);
const audiences = new Set(["operators", "benchmarks", "developers"]);
const metricTypes = new Set(["counter", "gauge", "histogram", "summary"]);

/** Reads and validates one observability table; the error names the first invalid entry. */
export function parseDeclarations(value: unknown): Declarations {
  const table = object(value, "observability table", ["service", "events", "operations", "metrics", "spans"]);
  if (typeof table.service !== "string" || table.service === "") throw new Error("observability table has no service");
  const events = object(table.events, "events");
  for (const [name, raw] of Object.entries(events)) {
    const event = object(raw, `event ${name}`, ["level", "log_type", "audience", "fields", "errors"]);
    if (!eventNamePattern.test(name)) throw new Error(`event ${name} is not a valid event name`);
    if (!levels.has(event.level as string)) throw new Error(`event ${name} has an invalid level`);
    if (event.log_type !== "app" && event.log_type !== "audit") throw new Error(`event ${name} has an invalid log_type`);
    if (!audiences.has(event.audience as string)) throw new Error(`event ${name} has an invalid audience`);
    for (const [field, type] of Object.entries(object(event.fields, `event ${name} fields`))) {
      if (!fieldTypes.has(type as string)) throw new Error(`event ${name} field ${field} has an invalid type`);
    }
    if (event.errors !== undefined) {
      for (const [code, rawError] of Object.entries(object(event.errors, `event ${name} errors`))) {
        const declared = object(rawError, `event ${name} error ${code}`, ["class", "message_safe"]);
        if (code === "" || typeof declared.class !== "string" || declared.class === "" || typeof declared.message_safe !== "string" || declared.message_safe === "") {
          throw new Error(`event ${name} has an incomplete error ${code}`);
        }
      }
    }
  }
  for (const [name, raw] of Object.entries(object(table.operations, "operations"))) {
    const operation = object(raw, `operation ${name}`, ["long", "errors"]);
    if (typeof operation.long !== "boolean") throw new Error(`operation ${name} has no long flag`);
    if (operation.errors !== undefined && (!Array.isArray(operation.errors) || operation.errors.some((code) => typeof code !== "string"))) {
      throw new Error(`operation ${name} has invalid errors`);
    }
  }
  for (const [name, raw] of Object.entries(object(table.metrics, "metrics"))) {
    const metric = object(raw, `metric ${name}`, ["type", "audience", "labels", "buckets"]);
    if (!metricTypes.has(metric.type as string)) throw new Error(`metric ${name} has an invalid type`);
    if (!audiences.has(metric.audience as string)) throw new Error(`metric ${name} has an invalid audience`);
    object(metric.labels, `metric ${name} labels`);
    if ((metric.type === "histogram") !== (metric.buckets !== undefined)) throw new Error(`metric ${name}: only a histogram declares buckets`);
  }
  for (const [name, raw] of Object.entries(object(table.spans, "spans"))) {
    const span = object(raw, `span ${name}`, ["attributes"]);
    if (!Array.isArray(span.attributes)) throw new Error(`span ${name} has no attributes`);
  }
  return table as unknown as Declarations;
}

function object(value: unknown, what: string, keys?: readonly string[]): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${what} is not an object`);
  if (keys !== undefined) {
    for (const key of Object.keys(value)) if (!keys.includes(key)) throw new Error(`${what} has unknown key ${key}`);
  }
  return value as Record<string, unknown>;
}

/**
 * The records and metric families this package writes for every service; a
 * service's own table must not repeat them.
 */
export const libraryDeclarations: Declarations = parseDeclarations(libraryTable);

/** Validates a service table and rejects any name it shares with the library table. */
export function serviceDeclarations(value: unknown): Declarations {
  const table = parseDeclarations(value);
  for (const name of Object.keys(table.events)) {
    if (Object.hasOwn(libraryDeclarations.events, name)) throw new Error(`service ${table.service} redeclares library event ${name}`);
  }
  for (const name of Object.keys(table.operations)) {
    if (Object.hasOwn(libraryDeclarations.operations, name)) throw new Error(`service ${table.service} redeclares library operation ${name}`);
  }
  return table;
}

export function lookupEvent(table: Declarations | undefined, event: string): EventDeclaration | undefined {
  if (Object.hasOwn(libraryDeclarations.events, event)) return libraryDeclarations.events[event];
  return table !== undefined && Object.hasOwn(table.events, event) ? table.events[event] : undefined;
}

export function lookupOperation(table: Declarations | undefined, operation: string): OperationDeclaration | undefined {
  if (Object.hasOwn(libraryDeclarations.operations, operation)) return libraryDeclarations.operations[operation];
  return table !== undefined && Object.hasOwn(table.operations, operation) ? table.operations[operation] : undefined;
}

/** How a recorder treats a name its table does not declare. */
export type UndeclaredNamePolicy = "throw" | "record";

/** A test run fails at once; production records diagnostic.invalid_event instead. */
export function defaultUndeclaredNamePolicy(): UndeclaredNamePolicy {
  return process.env.NODE_ENV === "test" ? "throw" : "record";
}

/** Thrown for a name missing from its table; the logger lets it escape its own containment. */
export class UndeclaredNameError extends Error {
  constructor(kind: "event" | "operation", name: string) {
    super(`undeclared ${kind} ${JSON.stringify(name)}; declare it in the service's observability.json`);
    this.name = "UndeclaredNameError";
  }
}
