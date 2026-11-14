// Runs the phase observer against descriptor 3, inherited from the test as an
// operator would hand it to the CLI, and prints what it saw as JSON.
import { closeSync, readFileSync } from "node:fs";
import { createPlatformKeyPhaseObserver } from "../../../../scripts/platform-key.js";
import type { PlatformKeyPhase } from "../../../../scripts/platform-key.js";

const env = { TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD: "3" };
const [scenario, path] = process.argv.slice(2);
let report: Record<string, unknown>;
if (scenario === "bounds") {
  const observer = createPlatformKeyPhaseObserver(env);
  for (let index = 0; index < 12; index++) observer?.("cli_enter");
  const afterTwelve = readFileSync(path!, "utf8");
  const failing = createPlatformKeyPhaseObserver(env);
  closeSync(3);
  let threw = false;
  try { failing?.("stdin_begin"); failing?.("query_begin"); } catch { threw = true; }
  report = { observer: observer !== undefined, afterTwelve, failing: failing !== undefined, threw, afterClosure: createPlatformKeyPhaseObserver(env) !== undefined };
} else if (scenario === "observer") {
  report = { observer: createPlatformKeyPhaseObserver(env) !== undefined };
} else if (scenario === "phases") {
  const observer = createPlatformKeyPhaseObserver(env);
  observer?.("secret-sentinel" as PlatformKeyPhase);
  observer?.("cli_enter");
  report = { observer: observer !== undefined };
} else {
  throw new Error(`unknown scenario ${scenario}`);
}
process.stdout.write(JSON.stringify(report));
