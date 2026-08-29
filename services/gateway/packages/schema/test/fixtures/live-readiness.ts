import { verifyPostgreSQLReadiness } from "../../src/verify.js";

const databaseUrl = process.env.TETRAL_TEST_RUNTIME_DATABASE_URL;
if (databaseUrl === undefined || databaseUrl === "") {
	throw new Error("runtime database fixture URL is required");
}

const sql = new Bun.SQL({ url: databaseUrl, max: 1 });
try {
	await verifyPostgreSQLReadiness(sql);
} finally {
	await sql.close({ timeout: 1 });
}
