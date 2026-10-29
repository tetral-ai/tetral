import { context, createContextKey, isSpanContextValid, trace } from "@opentelemetry/api";
import type { Context, SpanContext } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";

/** Scalar values a record field or correlation field may hold. */
export type FieldValue = string | number | boolean | undefined;
export type Fields = Readonly<Record<string, FieldValue>>;

const fieldsKey = createContextKey("tetral.correlation-fields");
let contextManagerInstalled = false;

/**
 * Installs the process's asynchronous context once, so that the active span
 * and correlation fields follow promises and timers. A manager another owner
 * registered first stays in place.
 */
export function ensureContextManager(): void {
  if (contextManagerInstalled) return;
  contextManagerInstalled = true;
  const manager = new AsyncLocalStorageContextManager().enable();
  if (!context.setGlobalContextManager(manager)) manager.disable();
}

/** Runs fn with fields added to the active correlation fields; a repeated name takes the new value. */
export function withFields<T>(fields: Fields, fn: () => T): T {
  ensureContextManager();
  return context.with(contextWithFields(context.active(), fields), fn);
}

export function contextWithFields(parent: Context, fields: Fields): Context {
  const merged: Record<string, FieldValue> = { ...contextFields(parent) };
  for (const [key, value] of Object.entries(fields)) if (value !== undefined) merged[key] = value;
  return parent.setValue(fieldsKey, Object.freeze(merged));
}

/** The correlation fields of ctx, by default the active context. */
export function contextFields(ctx: Context = context.active()): Fields {
  return (ctx.getValue(fieldsKey) as Fields | undefined) ?? {};
}

/** The span context of ctx's span when it has a valid one. */
export function activeSpanContext(ctx: Context = context.active()): SpanContext | undefined {
  const spanContext = trace.getSpanContext(ctx);
  return spanContext !== undefined && isSpanContextValid(spanContext) ? spanContext : undefined;
}

/** Runs fn with ctx active. */
export function runInContext<T>(ctx: Context, fn: () => T): T {
  ensureContextManager();
  return context.with(ctx, fn);
}
