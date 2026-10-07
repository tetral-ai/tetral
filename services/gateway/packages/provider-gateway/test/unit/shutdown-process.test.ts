import { testExecutableShutdown } from "@tetral/ts-observability/test-support/executable-shutdown";

testExecutableShutdown({
  receiver: "Provider",
  fixtureUrl: new URL("../fixtures/shutdown-process.ts", import.meta.url),
  expectedJoins: 1,
  // The fixture's 2000 ms drain plus 3000 ms cancellation join.
  budgetMs: 5000,
});
