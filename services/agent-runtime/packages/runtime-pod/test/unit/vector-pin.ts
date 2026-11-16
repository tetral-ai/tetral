import { readFileSync, writeFileSync } from "node:fs";

// The vectors this process pinned; a producer test fails when a row it ran
// declares a vector it did not pin.
export const pinnedVectors = new Set<string>();

// pinVector compares a producer's output with its vector file, or rewrites
// the file when TETRAL_UPDATE_VECTORS=1; CI never sets it.
export function pinVector(path: URL, encoded: string, readers: string): void {
	pinnedVectors.add(path.href);
	if (process.env.TETRAL_UPDATE_VECTORS === "1") {
		if (process.env.CI) throw new Error("TETRAL_UPDATE_VECTORS is set in CI");
		writeFileSync(path, encoded);
		return;
	}
	if (readFileSync(path, "utf8") !== encoded) {
		throw new Error(
			`output differs from vector ${path.pathname}; ${readers} hold the old shape. Rewrite it with TETRAL_UPDATE_VECTORS=1 if the change is intended.\n${encoded}`,
		);
	}
}
