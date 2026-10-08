package storage

// Auth owns immutable identity roots and workspace grant/token state. This is
// fresh baseline DDL; existing schemas are never silently rewritten at startup.
const createPostgreSQLAuthFederationRules = `CREATE TABLE IF NOT EXISTS auth_federation_rules (
 id TEXT PRIMARY KEY,
 organization_id TEXT NOT NULL,
 issuer TEXT NOT NULL,
 audience TEXT NOT NULL,
 security_config JSONB NOT NULL CHECK (jsonb_typeof(security_config)='object'),
 enabled BOOLEAN NOT NULL,
 removed BOOLEAN NOT NULL DEFAULT FALSE,
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision > 0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 CHECK(NOT removed OR NOT enabled)
)`

const createPostgreSQLAuthIdentities = `CREATE TABLE IF NOT EXISTS auth_identities (
 id TEXT PRIMARY KEY,
 organization_id TEXT NOT NULL,
 issuer TEXT NOT NULL,
 subject TEXT NOT NULL,
 identity_kind TEXT NOT NULL CHECK(identity_kind IN ('human','service')),
 service_account_id TEXT,
 enabled BOOLEAN NOT NULL,
 removed BOOLEAN NOT NULL DEFAULT FALSE,
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision > 0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(organization_id,issuer,subject),
 CHECK((identity_kind='human' AND service_account_id IS NULL) OR
       (identity_kind='service' AND service_account_id IS NOT NULL AND service_account_id<>'')),
 CHECK(NOT removed OR NOT enabled)
)`

const createPostgreSQLAuthWorkspaceGrants = `CREATE TABLE IF NOT EXISTS auth_workspace_grants (
 id TEXT PRIMARY KEY,
 identity_id TEXT NOT NULL REFERENCES auth_identities(id),
 workspace_id TEXT NOT NULL REFERENCES workspaces(id),
 role TEXT NOT NULL CHECK(role='workspace_full_access'),
 role_version BIGINT NOT NULL CHECK(role_version>0),
 enabled BOOLEAN NOT NULL,
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),
 revoked_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(workspace_id,id),
 CHECK(revoked_at IS NULL OR NOT enabled)
)`

const createPostgreSQLAuthAccessTokens = `CREATE TABLE IF NOT EXISTS auth_access_tokens (
 id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(id),
 token_digest BYTEA NOT NULL UNIQUE CHECK(octet_length(token_digest)=32),
 federation_rule_id TEXT NOT NULL REFERENCES auth_federation_rules(id),
 identity_id TEXT NOT NULL REFERENCES auth_identities(id),
 grant_id TEXT NOT NULL,
 rule_revision BIGINT NOT NULL CHECK(rule_revision>0),
 identity_revision BIGINT NOT NULL CHECK(identity_revision>0),
 grant_revision BIGINT NOT NULL CHECK(grant_revision>0),
 role_version BIGINT NOT NULL CHECK(role_version>0),
 expires_at TIMESTAMPTZ NOT NULL,
 revoked_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(workspace_id,id),
 FOREIGN KEY(workspace_id,grant_id) REFERENCES auth_workspace_grants(workspace_id,id),
 CHECK(expires_at>created_at)
)`

// All SECURITY DEFINER references are schema qualified, the search path is
// fixed, PUBLIC execute is revoked, and only Auth receives these signatures.
// Lookup functions accept an exact credential digest/identity ID, never SQL or
// an arbitrary predicate. They do not acquire credential locks: admission locks
// roots first, then rechecks/locks the credential in its resolved workspace.
const createPostgreSQLAuthLookupKey = `CREATE FUNCTION public.tetral_auth_lookup_key(bytea)
RETURNS SETOF public.api_keys LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 PERFORM pg_catalog.set_config('tetral.auth_lookup','true',true);
 RETURN QUERY SELECT k.* FROM public.api_keys k WHERE k.key_digest=$1 AND k.revoked_at IS NULL;
END $$`
const createPostgreSQLAuthLookupToken = `CREATE FUNCTION public.tetral_auth_lookup_token(bytea)
RETURNS SETOF public.auth_access_tokens LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 PERFORM pg_catalog.set_config('tetral.auth_lookup','true',true);
 RETURN QUERY SELECT t.* FROM public.auth_access_tokens t WHERE t.token_digest=$1
   AND t.revoked_at IS NULL AND t.expires_at>pg_catalog.clock_timestamp();
END $$`
const createPostgreSQLAuthLookupGrants = `CREATE FUNCTION public.tetral_auth_lookup_grants(text,text)
RETURNS SETOF public.auth_workspace_grants LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 PERFORM pg_catalog.set_config('tetral.auth_lookup','true',true);
 RETURN QUERY SELECT g.* FROM public.auth_workspace_grants g WHERE g.identity_id=$1
   AND g.enabled AND g.revoked_at IS NULL AND ($2 IS NULL OR g.workspace_id=$2) ORDER BY g.id LIMIT 2;
END $$`

// Root locks are taken in rule, identity, grant order before a credential lock.
// The function owns the UPDATE privilege required by PostgreSQL row locks;
// serving Auth has only SELECT on policy roots and cannot edit them.
const createPostgreSQLAuthLockAuthority = `CREATE FUNCTION public.tetral_auth_lock_authority(text,text,text,text)
RETURNS TABLE(rule_revision bigint,identity_revision bigint,grant_revision bigint,role_version bigint,identity_kind text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ DECLARE r public.auth_federation_rules%ROWTYPE; i public.auth_identities%ROWTYPE; g public.auth_workspace_grants%ROWTYPE; BEGIN
 PERFORM pg_catalog.set_config('tetral.auth_lookup','true',true);
 SELECT * INTO r FROM public.auth_federation_rules WHERE id=$1 FOR SHARE;
 IF NOT FOUND OR NOT r.enabled OR r.removed THEN RETURN; END IF;
 SELECT * INTO i FROM public.auth_identities WHERE id=$2 FOR SHARE;
 IF NOT FOUND OR NOT i.enabled OR i.removed OR i.organization_id<>r.organization_id OR i.issuer<>r.issuer THEN RETURN; END IF;
 SELECT * INTO g FROM public.auth_workspace_grants WHERE id=$3 AND workspace_id=$4 FOR SHARE;
 IF NOT FOUND OR NOT g.enabled OR g.revoked_at IS NOT NULL OR g.identity_id<>i.id THEN RETURN; END IF;
 rule_revision:=r.revision; identity_revision:=i.revision; grant_revision:=g.revision;
 role_version:=g.role_version; identity_kind:=i.identity_kind;
 RETURN NEXT;
END $$`

// Pruning's cutoff and maximum are enforced here using the database clock.
// Parent token IDs on API keys are audit data without a foreign key; expiry
// cleanup never severs the independently durable identity/grant lineage.
const createPostgreSQLAuthPruneTokens = `CREATE FUNCTION public.tetral_auth_prune_tokens(integer)
RETURNS TABLE(deleted_count integer,expired_backlog bigint,oldest_expiry timestamptz)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ DECLARE cutoff timestamptz; BEGIN
 PERFORM pg_catalog.set_config('tetral.auth_lookup','true',true);
 cutoff:=pg_catalog.clock_timestamp()-interval '24 hours';
 WITH candidates AS (
   SELECT t.id FROM public.auth_access_tokens t WHERE t.expires_at<cutoff
   ORDER BY t.expires_at,t.id LIMIT LEAST(GREATEST(COALESCE($1,1000),0),1000)
   FOR UPDATE SKIP LOCKED
 ), deleted AS (
   DELETE FROM public.auth_access_tokens t USING candidates c WHERE t.id=c.id RETURNING t.id
 ) SELECT count(*)::integer INTO deleted_count FROM deleted;
 SELECT count(*),min(t.expires_at) INTO expired_backlog,oldest_expiry
   FROM public.auth_access_tokens t WHERE t.expires_at<cutoff;
 RETURN NEXT;
END $$`

// A revoked grant ID is terminal. Even a serving role with UPDATE capability
// cannot resurrect an old revision by clearing the tombstone.
const createPostgreSQLAuthGrantTerminal = `CREATE FUNCTION public.tetral_auth_grant_terminal()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog
AS $$ BEGIN
 IF NEW.id IS DISTINCT FROM OLD.id OR NEW.identity_id IS DISTINCT FROM OLD.identity_id
    OR NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
    OR (OLD.revoked_at IS NOT NULL AND (NEW.revoked_at IS DISTINCT FROM OLD.revoked_at OR NEW.enabled)) THEN
  RAISE EXCEPTION 'immutable auth grant lineage' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$`

// Issued key lineage, ceiling and scope cannot be edited after insertion.
const createPostgreSQLAuthKeyLineage = `CREATE FUNCTION public.tetral_auth_key_lineage()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog
AS $$ BEGIN
 IF ROW(NEW.id,NEW.workspace_id,NEW.authority_kind,NEW.federation_rule_id,NEW.identity_id,NEW.grant_id,
        NEW.rule_revision,NEW.identity_revision,NEW.grant_revision,NEW.role_version,NEW.issuance_operations,NEW.parent_credential_id)
 IS DISTINCT FROM ROW(OLD.id,OLD.workspace_id,OLD.authority_kind,OLD.federation_rule_id,OLD.identity_id,OLD.grant_id,
        OLD.rule_revision,OLD.identity_revision,OLD.grant_revision,OLD.role_version,OLD.issuance_operations,OLD.parent_credential_id) THEN
  RAISE EXCEPTION 'immutable api key authority' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$`

// A key's usage generation advances whenever its digest or revocation state
// changes and is otherwise retained, so an UPDATE cannot set it. Asynchronous
// usage samples carry the generation they were admitted under; an earlier
// sample cannot update the same key ID after rotation away and back, or after
// revoke and reactivation. The increment raises bigint out of range rather
// than wrapping.
const createPostgreSQLAuthKeyUsageGeneration = `CREATE FUNCTION public.tetral_auth_key_usage_generation()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog
AS $$ BEGIN
 IF NEW.key_digest IS DISTINCT FROM OLD.key_digest OR NEW.revoked_at IS DISTINCT FROM OLD.revoked_at THEN
  NEW.usage_generation := OLD.usage_generation + 1;
 ELSE
  NEW.usage_generation := OLD.usage_generation;
 END IF;
 RETURN NEW;
END $$`

func postgresqlAuthTableSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"create_auth_federation_rules", createPostgreSQLAuthFederationRules},
		{"create_auth_identities", createPostgreSQLAuthIdentities},
		{"create_auth_workspace_grants", createPostgreSQLAuthWorkspaceGrants},
		{"create_auth_access_tokens", createPostgreSQLAuthAccessTokens},
		{"index_auth_tokens_expiry", `CREATE INDEX idx_auth_access_tokens_expiry ON auth_access_tokens(expires_at,id)`},
		{"index_auth_grants_identity", `CREATE INDEX idx_auth_workspace_grants_identity ON auth_workspace_grants(identity_id,id) WHERE enabled AND revoked_at IS NULL`},
		{"index_auth_grants_workspace", `CREATE UNIQUE INDEX idx_auth_workspace_grants_active_workspace ON auth_workspace_grants(identity_id,workspace_id) WHERE enabled AND revoked_at IS NULL`},
	}
}
func postgresqlAuthFunctionSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"create_auth_key_lineage", createPostgreSQLAuthKeyLineage},
		{"revoke_auth_key_lineage_public", `REVOKE ALL ON FUNCTION public.tetral_auth_key_lineage() FROM PUBLIC`},
		{"trigger_auth_key_lineage", `CREATE TRIGGER api_keys_authority_immutable BEFORE UPDATE ON api_keys FOR EACH ROW EXECUTE FUNCTION public.tetral_auth_key_lineage()`},
		{"create_auth_key_usage_generation", createPostgreSQLAuthKeyUsageGeneration},
		{"revoke_auth_key_usage_generation_public", `REVOKE ALL ON FUNCTION public.tetral_auth_key_usage_generation() FROM PUBLIC`},
		{"trigger_auth_key_usage_generation", `CREATE TRIGGER api_keys_usage_generation BEFORE UPDATE ON api_keys FOR EACH ROW EXECUTE FUNCTION public.tetral_auth_key_usage_generation()`},
		{"create_auth_lookup_key", createPostgreSQLAuthLookupKey},
		{"create_auth_lookup_token", createPostgreSQLAuthLookupToken},
		{"create_auth_lookup_grants", createPostgreSQLAuthLookupGrants},
		{"create_auth_prune_tokens", createPostgreSQLAuthPruneTokens},
		{"create_auth_lock_authority", createPostgreSQLAuthLockAuthority},
		{"revoke_auth_lookup_public", `REVOKE ALL ON FUNCTION public.tetral_auth_lookup_key(bytea), public.tetral_auth_lookup_token(bytea), public.tetral_auth_lookup_grants(text,text), public.tetral_auth_prune_tokens(integer), public.tetral_auth_lock_authority(text,text,text,text) FROM PUBLIC`},
		{"create_auth_grant_terminal", createPostgreSQLAuthGrantTerminal},
		{"revoke_auth_grant_terminal_public", `REVOKE ALL ON FUNCTION public.tetral_auth_grant_terminal() FROM PUBLIC`},
		{"trigger_auth_grant_terminal", `CREATE TRIGGER auth_workspace_grants_terminal BEFORE UPDATE ON auth_workspace_grants FOR EACH ROW EXECUTE FUNCTION public.tetral_auth_grant_terminal()`},
	}
}
