import { diagnosticDefaults } from "@tetral/ts-observability";
import { createJsonLogger } from "../../src/logger.js";
import type { RuntimePodLogger } from "../../src/logger.js";

/**
 * A Runtime logger that appends each record as it is written, at DEBUG and
 * with every repeat, so a test reads what an operator would.
 */
export function writtenLogs(records: Record<string, unknown>[]): RuntimePodLogger {
	return createJsonLogger({
		write: (line) => { records.push(JSON.parse(line) as Record<string, unknown>); },
		diagnostics: { ...diagnosticDefaults, level: "debug", burst: 1_000 },
	});
}
