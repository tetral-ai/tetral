package tetralapi

// startupErrorInventory lists every error-returning AST site in the scanned composition and
// helper functions, keyed by function name, listing the rendered returned-error
// expression and its bucket. The AST anti-omission test in
// startup_error_inventory_test.go enumerates the real error-returns and asserts
// this inventory accounts for each one (as a multiset), that doc-decided sites are
// not re-bucketed, and that any ConfigError-producing return is bucketed C.
//
// Buckets:
//   - bucketConfig ("C"): config-validation leaf -> workload.ConfigError.
//   - bucketDependency ("D"): dependency/construction leaf -> class-only.
//   - bucketPropagate ("propagate"): internal guard or forward of an
//     already-classified callee error.
const (
	bucketConfig     = "C"
	bucketDependency = "D"
	bucketPropagate  = "propagate"
)

// startupInventoryEntry is one (rendered error expression -> bucket) within a
// function. The same expression text may appear more than once in a function
// (e.g. several `err` propagations); count records how many error-returns share
// that exact text so the AST multiset must match exactly.
type startupInventoryEntry struct {
	expression string
	bucket     string
	count      int
}

// startupScannedFunctions is the exact set of composition + package-local helper
// functions the A.1b AST scan walks. Adding a startup composition function
// requires adding it here AND giving its error-returns inventory entries.
var startupScannedFunctions = []string{
	"BuildProductionApplication",
	"PrepareStartupDatabase",
	"BuildRouter",
	"buildOptionalBlobStore",
	"buildSkillHandler",
	"buildFileHandler",
	"loadRuntimeControlConfigFromEnv",
	"loadDefaultEnvironmentArtifactRefFromEnv",
	"loadRequiredPositiveInt",
	"loadPositiveInt",
	"loadPositiveInt64",
	"validatePublicAPIConfig",
	"EnsureDataDir",
	"ValidateVaultKey",
}

var startupErrorInventory = map[string][]startupInventoryEntry{
	"BuildProductionApplication": {
		{expression: "err", bucket: bucketPropagate, count: 4},
	},
	"PrepareStartupDatabase": {
		{expression: "err", bucket: bucketDependency, count: 2},
		{expression: `fmt.Errorf("runtime database client is required")`, bucket: bucketDependency, count: 1},
		{expression: `fmt.Errorf("migration database client is required")`, bucket: bucketDependency, count: 1},
		{expression: `fmt.Errorf("schema migration: %w", err)`, bucket: bucketDependency, count: 1},
		{expression: `fmt.Errorf("close migration database: %w", err)`, bucket: bucketDependency, count: 1},
		{expression: `fmt.Errorf("schema verification: %w", err)`, bucket: bucketDependency, count: 1},
	},
	"BuildRouter": {
		// Seven `err` returns share this text: five forward already-classified
		// callee errors (validatePublicAPIConfig, EnsureDataDir,
		// loadDefaultEnvironmentArtifactRefFromEnv, buildOptionalBlobStore,
		// buildSkillHandler),
		// one is the encryption.NewAES256GCMEncryptor construction leaf (D), and
		// one is the Session-delete transaction's Sandbox release callback.
		// That encryptor leaf is env-unreachable for bad keys — ValidateVaultKey
		// validates hex+length up front — so it owes no A.3/A.4 case (its D label is
		// a type-statement only). The transaction callback is not a startup path.
		// None of the seven produce a ConfigError textually,
		// so the aggregate is labelled propagate for the text-keyed bucket checks.
		{expression: "err", bucket: bucketPropagate, count: 7},
		{expression: `fmt.Errorf("runtime client is required")`, bucket: bucketPropagate, count: 1},
		{expression: `fmt.Errorf("raw database is required")`, bucket: bucketPropagate, count: 1},
		{expression: `fmt.Errorf("internal principal verifier configuration: %w", err)`, bucket: bucketConfig, count: 1},
		{expression: `fmt.Errorf("runtime control configuration: %w", err)`, bucket: bucketConfig, count: 1},
		{expression: `fmt.Errorf("event page token secret: %w", err)`, bucket: bucketPropagate, count: 1},
		{expression: `fmt.Errorf("files: %w", err)`, bucket: bucketPropagate, count: 1},
	},
	"buildOptionalBlobStore": {
		{expression: "asStartupConfigError(err)", bucket: bucketConfig, count: 2},
		{expression: `fmt.Errorf("blob store: %w", err)`, bucket: bucketDependency, count: 1},
	},
	"buildSkillHandler": {
		{expression: `fmt.Errorf("skill upload stage directory: %w", err)`, bucket: bucketDependency, count: 1},
	},
	"buildFileHandler": {
		{expression: `fmt.Errorf("upload stage directory: %w", err)`, bucket: bucketDependency, count: 1},
	},
	"loadRuntimeControlConfigFromEnv": {
		{expression: "err", bucket: bucketConfig, count: 2},
	},
	"loadDefaultEnvironmentArtifactRefFromEnv": {
		{expression: `workload.NewConfigError(envDefaultEnvironmentArtifactRef + " is required")`, bucket: bucketConfig, count: 1},
	},
	"loadRequiredPositiveInt": {
		{expression: `workload.NewConfigError(key + " is required")`, bucket: bucketConfig, count: 1},
		{expression: `workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))`, bucket: bucketConfig, count: 1},
	},
	"loadPositiveInt": {
		{expression: `workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))`, bucket: bucketConfig, count: 1},
	},
	"loadPositiveInt64": {
		{expression: `workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))`, bucket: bucketConfig, count: 1},
	},
	"validatePublicAPIConfig": {
		{expression: "ValidateVaultKey(vaultKey)", bucket: bucketConfig, count: 1},
	},
	"EnsureDataDir": {
		{expression: `fmt.Errorf("create data dir: %w", err)`, bucket: bucketDependency, count: 1},
		{expression: `fmt.Errorf("stat data dir: %w", err)`, bucket: bucketDependency, count: 1},
		{expression: `workload.NewConfigError(fmt.Sprintf("ENGINE_DATA_DIR directory has mode %04o; must not be group/other accessible", info.Mode().Perm()))`, bucket: bucketConfig, count: 1},
	},
	"ValidateVaultKey": {
		{expression: `workload.NewConfigError("ENGINE_VAULT_KEY environment variable is required")`, bucket: bucketConfig, count: 1},
		{expression: `workload.NewConfigError(fmt.Sprintf("ENGINE_VAULT_KEY must be 64 hex characters (32 bytes), got %d characters", len(key)))`, bucket: bucketConfig, count: 1},
		{expression: `workload.NewConfigError("ENGINE_VAULT_KEY must be 64 hexadecimal characters (32 bytes); it contains non-hexadecimal characters")`, bucket: bucketConfig, count: 1},
	},
}

// startupDecidedBuckets hard-codes the doc's decided bucket assignments
// (Criterion A.1's "Decided buckets" + "already-correct Bucket-C producers").
// The AST test fails if the inventory assigns a different bucket to any of these
// (func, expression) pairs, so a real Bucket-C site cannot be demoted to D to
// escape A.3's per-surface proof. Keyed "function|expression".
var startupDecidedBuckets = map[string]string{
	// data-dir
	`EnsureDataDir|fmt.Errorf("create data dir: %w", err)`: bucketDependency,
	`EnsureDataDir|fmt.Errorf("stat data dir: %w", err)`:   bucketDependency,
	`EnsureDataDir|workload.NewConfigError(fmt.Sprintf("ENGINE_DATA_DIR directory has mode %04o; must not be group/other accessible", info.Mode().Perm()))`: bucketConfig,
	// blob (skill + file): validation -> C, construction -> D
	`buildOptionalBlobStore|asStartupConfigError(err)`:         bucketConfig,
	`buildOptionalBlobStore|fmt.Errorf("blob store: %w", err)`: bucketDependency,
	// encryptor / vault key (already-correct C producer via ValidateVaultKey)
	`validatePublicAPIConfig|ValidateVaultKey(vaultKey)`: bucketConfig,
	// session-event caps (already-correct C producers)
	`loadRequiredPositiveInt|workload.NewConfigError(key + " is required")`:                              bucketConfig,
	`loadRequiredPositiveInt|workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))`: bucketConfig,
	`loadPositiveInt|workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))`:         bucketConfig,
	`loadPositiveInt64|workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))`:       bucketConfig,
	// db-open propagation in PrepareStartupDatabase stays Dependency
	`PrepareStartupDatabase|fmt.Errorf("runtime database client is required")`:   bucketDependency,
	`PrepareStartupDatabase|fmt.Errorf("migration database client is required")`: bucketDependency,
	`PrepareStartupDatabase|fmt.Errorf("schema migration: %w", err)`:             bucketDependency,
	`PrepareStartupDatabase|fmt.Errorf("close migration database: %w", err)`:     bucketDependency,
	`PrepareStartupDatabase|fmt.Errorf("schema verification: %w", err)`:          bucketDependency,
}
