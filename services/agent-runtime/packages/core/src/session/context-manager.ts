/**
 * @packageDocumentation
 * Single committed message owner for one Runtime thread, with an immutable
 * child prefix. ThreadState provides current-request association and historical
 * eligibility; message contents carry no lifecycle fields. Cold preload
 * rebuilds it from durable projections; ThreadLoop mutates it only after the
 * owning write is durably acknowledged. It owns no turn lifecycle decisions,
 * dispatch, Runtime write identities, policy or external I/O.
 */
import type {
	RuntimeContextEntry,
	RuntimeContextPart,
	RuntimeInterruptToolResult,
} from "../contracts/runtime.js";
/** Durable, non-sequenced parent context installed separately from child history. */
export interface ThreadContextPrefix {
	readonly childThreadId: string;
	readonly parentThreadId: string;
	readonly parentBoundaryEventId: string;
	readonly entries: readonly RuntimeContextEntry[];
}
// New writes are projected only after durable ACK; cold hydration may install
// projections already read from durable state. The write discipline lives at
// the ThreadLoop/SessionManager call site:
//
// | hot mutation                       | gated on durable ACK            |
// | ---------------------------------- | ------------------------------- |
// | append model-visible message input | CommitInputs ACK                |
// | append agent event content         | WriteEvent ACK                  |
// | append trailing reasoning         | WriteRequestEnd ACK             |
// | apply compaction (prefix replace)  | compaction event/projection ACK |
//
// ThreadState separately owns the last-request usage hint and route-effective
// limits and lifecycle-derived historical eligibility. Request End changes that
// eligibility. Failed closed content may leave resident history under reducer
// retention; registered work keeps its exact committed owner addressable.
//
// A tool-confirmation commit appends its Bridge-derived user decision before
// waking the approved tool. A task notification uses the background-task settlement
// path and projects a bounded runtime note. A thread context prefix is loaded from the CHILD
// thread's durable context and is never rebuilt from the current parent. The
// generation counter advances on any non-append-only rewrite (compaction or in-place
// update) so the approval-reviewer feed cursor can detect invalidation within one hot
// lifetime. A cold return creates a successor reviewer manager and trunk, which starts
// with a full feed instead of inheriting this generation.
// UPDATE-WITH: services/agent-runtime/packages/core/src/thread-loop/thread-loop.ts,
//              services/agent-runtime/packages/core/src/session/session-manager.ts
/** Mutable hot message list whose generation invalidates reviewer feed cursors on rewrites. */
export class ContextManager {
	readonly sessionId: string;
	#messages = new Map<number, RuntimeContextEntry>();
	#threadContextPrefix: ThreadContextPrefix | undefined;
	#generation = 0;
	constructor(
		sessionId: string,
		initialMessages: readonly RuntimeContextEntry[] = [],
		private readonly currentReference: () => {
			readonly assistantMessageSequence: number;
		} | undefined = () => undefined,
		private readonly historicalEligibility: (message: RuntimeContextEntry) => boolean = () => true,
	) {
		this.sessionId = sessionId;
		this.replaceMessages(initialMessages);
	}

	messages(): readonly RuntimeContextEntry[] {
		return [...this.#messages.values()].sort((a, b) => a.messageSequence - b.messageSequence);
	}

	historyMessages(): readonly RuntimeContextEntry[] {
		return this.messages().filter(this.historicalEligibility);
	}

	threadContextPrefix(): ThreadContextPrefix | undefined {
		return this.#threadContextPrefix;
	}

	installThreadContextPrefix(prefix: ThreadContextPrefix | undefined): void {
		this.#threadContextPrefix = prefix;
	}

	providerEntries(): readonly RuntimeContextEntry[] {
		return [...(this.#threadContextPrefix?.entries ?? []), ...this.historyMessages()];
	}

	providerEntrySegments(): readonly (readonly RuntimeContextEntry[])[] {
		const prefix = this.#threadContextPrefix?.entries ?? [];
		return prefix.length === 0 ? [this.historyMessages()] : [[...prefix], this.historyMessages()];
	}

	entryListSnapshot(): {
		readonly generation: number;
		readonly entries: readonly RuntimeContextEntry[];
	} {
		return { generation: this.#generation, entries: this.historyMessages() };
	}

	replaceMessages(messages: readonly RuntimeContextEntry[]): void {
		if (new Set(messages.map(message => message.messageSequence)).size !== messages.length)
			throw new Error("committed messages must have unique sequences");
		const previous = this.messages();
		const appendOnly = previous.length <= messages.length && previous.every(
			(message, index) => JSON.stringify(message) === JSON.stringify(messages[index]),
		);
		this.#messages = new Map(messages.map(message => [message.messageSequence, message]));
		if (!appendOnly)
			this.#generation++;
	}

	replaceEntriesThroughSequence(boundarySequence: number, messages: readonly RuntimeContextEntry[]): readonly RuntimeContextEntry[] {
		const generation = this.#generation;
		this.replaceMessages([...messages, ...this.messages().filter(message => message.messageSequence > boundarySequence)]);
		if (this.#generation === generation)
			this.#generation++;
		return this.historyMessages();
	}

	appendEntry(message: RuntimeContextEntry): void {
		if (this.#messages.has(message.messageSequence))
			throw new Error("committed message sequence already exists");
		this.#messages.set(message.messageSequence, message);
	}

	entry(sequence: number): RuntimeContextEntry | undefined {
		return this.#messages.get(sequence);
	}

	updateEntry(message: RuntimeContextEntry): void {
		if (!this.#messages.has(message.messageSequence))
			throw new Error("committed message target is missing");
		this.#messages.set(message.messageSequence, message);
		this.#generation++;
	}

	currentAssistantMessage(): RuntimeContextEntry | undefined {
		const reference = this.currentReference();
		return reference === undefined ? undefined : this.entry(reference.assistantMessageSequence);
	}
	/** A receipt may append a new Assistant or update only the exact current owner. */
	installAssistantMessage(message: RuntimeContextEntry): void {
		if (message.contextKind !== "assistant")
			throw new Error("current content must be Assistant");
		const existing = this.entry(message.messageSequence);
		if (existing === undefined) {
			this.appendEntry(message);
			return;
		}
		if (
			existing.contextKind !== "assistant" ||
			this.currentReference()?.assistantMessageSequence !== message.messageSequence
		) {
			throw new Error("Assistant receipt collides with unrelated committed content");
		}
		this.updateEntry(message);
	}

	invalidateHistory(): void {
		this.#generation++;
	}

	appendToolResult(
		messageSequence: number,
		modelToolCallId: string,
		result: Extract<RuntimeContextPart, { readonly type: "tool_result" }>["result"],
	): void {
		const message = this.entry(messageSequence);
		if (message?.contextKind !== "assistant")
			throw new Error("Tool result must name one committed Assistant message");
		const calls = message.parts.filter(part => part.type === "tool_call" && part.modelToolCallId === modelToolCallId);
		const results = message.parts.filter(part => part.type === "tool_result" && part.modelToolCallId === modelToolCallId);
		if (calls.length !== 1 || results.length !== 0)
			throw new Error("Tool result target is ambiguous or already terminal");
		this.updateEntry({ ...message, parts: [...message.parts, { type: "tool_result", modelToolCallId, result }] });
	}

	appendInterruptToolResults(routes: readonly {
		readonly toolUseEventId: string;
		readonly assistantMessageSequence: number;
		readonly modelToolCallId: string;
	}[], results: readonly RuntimeInterruptToolResult[]): void {
		for (const result of results) {
			const route = routes.find(candidate => candidate.toolUseEventId === result.toolUseEventId);
			if (route === undefined)
				throw new Error("interrupt Tool result has no active route");
			this.appendToolResult(route.assistantMessageSequence, route.modelToolCallId, result.result);
		}
	}

	clear(): void {
		this.#messages.clear();
		this.#threadContextPrefix = undefined;
		this.#generation++;
	}
}
