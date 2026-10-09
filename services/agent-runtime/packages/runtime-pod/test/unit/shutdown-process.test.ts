import { testExecutableShutdown } from "@tetral/ts-observability/test-support/executable-shutdown";

testExecutableShutdown({
  receiver: "Runtime",
  fixtureUrl: new URL("../fixtures/shutdown-process.ts", import.meta.url),
  // Core quiescence and the admitted producer each join in a cooperative run.
  expectedJoins: 2,
  // The fixture's 2000 ms current step, 2000 ms settlement and 1000 ms local join.
  budgetMs: 5000,
});
