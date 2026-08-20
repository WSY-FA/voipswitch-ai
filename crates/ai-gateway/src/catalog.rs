use crate::config::{
    GatewayConfig, GatewayProfileConfig, GatewayProviderConfig, GatewayProviderKind,
    GatewayProviderParameters, ProviderRuntimeState, ProviderSecretStatus, ProviderUpsertRequest,
};
use ai_protocol::control::AiPipelineType;
use ai_protocol::id::ProviderId;
use ai_provider::{
    LocalHttpAsrProvider, OpenAiCompatibleLlmConfig, OpenAiCompatibleLlmProvider, ProviderRegistry,
};
use anyhow::{Context, Result, bail};
use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use rand::rngs::OsRng;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

pub struct CatalogStore {
    connection: Mutex<Connection>,
    secret_cipher: Option<SecretCipher>,
}

impl CatalogStore {
    pub fn provider_secret(
        &self,
        provider_id: &str,
        kind: GatewayProviderKind,
    ) -> Result<Option<String>> {
        let Some(secret_name) = secret_name(kind) else {
            return Ok(None);
        };
        let connection = self.connection.lock().unwrap();
        let row = connection.query_row("SELECT ciphertext, nonce FROM gateway_provider_secret WHERE provider_id=?1 AND secret_name=?2", params![provider_id, secret_name], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))).optional()?;
        let Some((ciphertext, nonce)) = row else {
            return Ok(None);
        };
        let cipher = self
            .secret_cipher
            .as_ref()
            .context("AI gateway secret master key is not configured")?;
        Ok(Some(cipher.decrypt(
            provider_id,
            secret_name,
            &ciphertext,
            &nonce,
        )?))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayCatalog {
    pub version: u64,
    pub providers: Vec<GatewayProviderConfig>,
    pub profiles: Vec<GatewayProfileConfig>,
}

impl CatalogStore {
    pub fn open(path: &Path, bootstrap: &GatewayConfig) -> Result<Self> {
        let connection = Connection::open(path)
            .with_context(|| format!("open gateway catalog database {}", path.display()))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS gateway_catalog_meta (
               key TEXT PRIMARY KEY,
               value INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS gateway_provider (
               provider_id TEXT PRIMARY KEY,
               kind TEXT NOT NULL,
               enabled INTEGER NOT NULL,
               revision INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS gateway_profile (
               profile_id TEXT PRIMARY KEY,
               profile_version INTEGER NOT NULL,
               enabled INTEGER NOT NULL,
               asr_provider_id TEXT NOT NULL,
               llm_provider_id TEXT NOT NULL,
               capture_complete_ratio_ppm INTEGER NOT NULL,
               capture_process_min_ratio_ppm INTEGER NOT NULL,
               capture_complete_max_gap_ms INTEGER NOT NULL,
               capture_process_max_gap_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS gateway_admin_user (
               username TEXT PRIMARY KEY,
               password_hash TEXT NOT NULL,
               enabled INTEGER NOT NULL,
               created_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS gateway_provider_secret (
               provider_id TEXT NOT NULL,
               secret_name TEXT NOT NULL,
               ciphertext BLOB NOT NULL,
               nonce BLOB NOT NULL,
               key_version INTEGER NOT NULL,
               masked TEXT NOT NULL,
               updated_at_ms INTEGER NOT NULL,
               PRIMARY KEY (provider_id, secret_name),
               FOREIGN KEY (provider_id) REFERENCES gateway_provider(provider_id)
             );",
        )?;
        ensure_column(
            &connection,
            "gateway_provider",
            "display_name",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            &connection,
            "gateway_profile",
            "pipeline_type",
            "TEXT NOT NULL DEFAULT 'post_call_analysis'",
        )?;
        ensure_column(
            &connection,
            "gateway_profile",
            "tts_provider_id",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            &connection,
            "gateway_provider",
            "parameters_json",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        let store = Self {
            connection: Mutex::new(connection),
            secret_cipher: SecretCipher::from_environment()?,
        };
        store.migrate_legacy_mock_catalog()?;
        store.migrate_provider_defaults()?;
        store.seed_if_empty(bootstrap)?;
        Ok(store)
    }

    pub fn load(&self) -> Result<GatewayCatalog> {
        let connection = self.connection.lock().unwrap();
        let version = connection
            .query_row(
                "SELECT value FROM gateway_catalog_meta WHERE key = 'catalog_version'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(1);
        let mut provider_statement = connection.prepare(
            "SELECT provider_id, display_name, kind, enabled, revision, parameters_json
             FROM gateway_provider ORDER BY provider_id",
        )?;
        let providers = provider_statement
            .query_map([], |row| {
                let provider_id: String = row.get(0)?;
                let display_name: String = row.get(1)?;
                let kind_json: String = row.get(2)?;
                let kind: GatewayProviderKind =
                    serde_json::from_str(&kind_json).map_err(to_sql_error)?;
                let parameters_json: String = row.get(5)?;
                let parameters = serde_json::from_str(&parameters_json).map_err(to_sql_error)?;
                let secret = load_secret_status(&connection, &provider_id, kind)
                    .map_err(|error| to_sql_failure(error.to_string()))?;
                let enabled = row.get::<_, i64>(3)? != 0;
                let (runtime_state, runtime_message) =
                    runtime_state(kind, enabled, &secret, &parameters);
                Ok(GatewayProviderConfig {
                    provider_id,
                    display_name,
                    kind,
                    enabled,
                    revision: row.get::<_, i64>(4)? as u64,
                    parameters,
                    secret,
                    runtime_state,
                    runtime_message,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut profile_statement = connection.prepare(
            "SELECT profile_id, profile_version, enabled, pipeline_type,
                    asr_provider_id, llm_provider_id, tts_provider_id,
                    capture_complete_ratio_ppm, capture_process_min_ratio_ppm,
                    capture_complete_max_gap_ms, capture_process_max_gap_ms
             FROM gateway_profile ORDER BY profile_id",
        )?;
        let profiles = profile_statement
            .query_map([], |row| {
                Ok(GatewayProfileConfig {
                    profile_id: row.get(0)?,
                    profile_version: row.get::<_, i64>(1)? as u64,
                    enabled: row.get::<_, i64>(2)? != 0,
                    pipeline_type: parse_pipeline_type(&row.get::<_, String>(3)?)
                        .map_err(to_sql_error)?,
                    asr_provider_id: optional_provider_id(row.get(4)?),
                    llm_provider_id: optional_provider_id(row.get(5)?),
                    tts_provider_id: optional_provider_id(row.get(6)?),
                    capture: crate::config::CaptureThresholds {
                        complete_ratio_ppm: row.get::<_, i64>(7)? as u32,
                        process_min_ratio_ppm: row.get::<_, i64>(8)? as u32,
                        complete_max_gap_ms: row.get::<_, i64>(9)? as u64,
                        process_max_gap_ms: row.get::<_, i64>(10)? as u64,
                    },
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        validate_catalog(version as u64, &providers, &profiles)?;
        Ok(GatewayCatalog {
            version: version as u64,
            providers,
            profiles,
        })
    }

    pub fn upsert_provider(&self, provider: ProviderUpsertRequest) -> Result<GatewayCatalog> {
        provider.validate()?;
        let connection = self.connection.lock().unwrap();
        let transaction = connection.unchecked_transaction()?;
        let existing = transaction
            .query_row(
                "SELECT kind, revision FROM gateway_provider WHERE provider_id = ?1",
                [&provider.provider_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        if let Some((kind_json, revision)) = &existing {
            let existing_kind: GatewayProviderKind = serde_json::from_str(kind_json)?;
            if existing_kind != provider.kind {
                bail!("provider kind cannot be changed");
            }
            let expected = provider
                .expected_revision
                .context("expected_revision is required when updating a provider")?;
            if *revision < 0 || expected != *revision as u64 {
                bail!("provider revision conflict");
            }
        } else if provider.expected_revision.is_some() {
            bail!("expected_revision must be omitted when creating a provider");
        }
        let revision = next_revision(
            &transaction,
            "SELECT revision FROM gateway_provider WHERE provider_id = ?1",
            &provider.provider_id,
        )?;
        transaction.execute(
            "INSERT INTO gateway_provider
             (provider_id, display_name, kind, enabled, revision, parameters_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(provider_id) DO UPDATE SET
               display_name = excluded.display_name,
               enabled = excluded.enabled,
               revision = excluded.revision,
               parameters_json = excluded.parameters_json",
            params![
                provider.provider_id,
                provider.display_name,
                serde_json::to_string(&provider.kind)?,
                if provider.enabled { 1_i64 } else { 0_i64 },
                as_i64(revision)?,
                serde_json::to_string(&provider.parameters)?,
            ],
        )?;
        if let Some(secret) = provider.secret.filter(|value| !value.is_empty()) {
            self.store_secret(&transaction, &provider.provider_id, provider.kind, &secret)?;
        }
        bump_catalog_version(&transaction)?;
        transaction.commit()?;
        drop(connection);
        self.load()
    }

    pub fn provider_secret_available(&self) -> bool {
        self.secret_cipher.is_some()
    }

    pub fn upsert_profile(&self, profile: GatewayProfileConfig) -> Result<GatewayCatalog> {
        profile.validate()?;
        let connection = self.connection.lock().unwrap();
        let transaction = connection.unchecked_transaction()?;
        if let Some(provider_id) = &profile.asr_provider_id {
            validate_profile_provider_reference(&transaction, provider_id, "asr")?;
        }
        if let Some(provider_id) = &profile.llm_provider_id {
            validate_profile_provider_reference(&transaction, provider_id, "llm")?;
        }
        if let Some(provider_id) = &profile.tts_provider_id {
            validate_profile_provider_reference(&transaction, provider_id, "tts")?;
        }
        let current_version = transaction
            .query_row(
                "SELECT profile_version FROM gateway_profile WHERE profile_id = ?1",
                [&profile.profile_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let revision = match current_version {
            Some(value) if value >= 0 => {
                let current = value as u64;
                if profile.profile_version != current {
                    bail!("AI profile revision conflict");
                }
                current.saturating_add(1)
            }
            Some(_) => bail!("AI profile revision must not be negative"),
            None => {
                if profile.profile_version != 1 {
                    bail!("new AI profile profile_version must be 1");
                }
                1
            }
        };
        transaction.execute(
            "INSERT INTO gateway_profile
             (profile_id, profile_version, enabled, pipeline_type,
              asr_provider_id, llm_provider_id, tts_provider_id,
              capture_complete_ratio_ppm, capture_process_min_ratio_ppm,
              capture_complete_max_gap_ms, capture_process_max_gap_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(profile_id) DO UPDATE SET
               profile_version = excluded.profile_version,
               enabled = excluded.enabled,
               pipeline_type = excluded.pipeline_type,
               asr_provider_id = excluded.asr_provider_id,
               llm_provider_id = excluded.llm_provider_id,
               tts_provider_id = excluded.tts_provider_id,
               capture_complete_ratio_ppm = excluded.capture_complete_ratio_ppm,
               capture_process_min_ratio_ppm = excluded.capture_process_min_ratio_ppm,
               capture_complete_max_gap_ms = excluded.capture_complete_max_gap_ms,
               capture_process_max_gap_ms = excluded.capture_process_max_gap_ms",
            params![
                profile.profile_id,
                as_i64(revision)?,
                if profile.enabled { 1_i64 } else { 0_i64 },
                pipeline_type_str(profile.pipeline_type),
                profile.asr_provider_id.unwrap_or_default(),
                profile.llm_provider_id.unwrap_or_default(),
                profile.tts_provider_id.unwrap_or_default(),
                i64::from(profile.capture.complete_ratio_ppm),
                i64::from(profile.capture.process_min_ratio_ppm),
                as_i64(profile.capture.complete_max_gap_ms)?,
                as_i64(profile.capture.process_max_gap_ms)?,
            ],
        )?;
        bump_catalog_version(&transaction)?;
        transaction.commit()?;
        drop(connection);
        self.load()
    }

    pub fn delete_provider(
        &self,
        provider_id: &str,
        expected_revision: u64,
    ) -> Result<GatewayCatalog> {
        let connection = self.connection.lock().unwrap();
        let transaction = connection.unchecked_transaction()?;
        let revision = transaction
            .query_row(
                "SELECT revision FROM gateway_provider WHERE provider_id = ?1",
                [provider_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .context("provider not found")?;
        if revision < 0 || revision as u64 != expected_revision {
            bail!("provider revision conflict");
        }
        let references: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM gateway_profile
             WHERE asr_provider_id = ?1 OR llm_provider_id = ?1 OR tts_provider_id = ?1",
            [provider_id],
            |row| row.get(0),
        )?;
        if references != 0 {
            bail!("provider is referenced by an AI profile");
        }
        transaction.execute(
            "DELETE FROM gateway_provider_secret WHERE provider_id = ?1",
            [provider_id],
        )?;
        transaction.execute(
            "DELETE FROM gateway_provider WHERE provider_id = ?1",
            [provider_id],
        )?;
        bump_catalog_version(&transaction)?;
        transaction.commit()?;
        drop(connection);
        self.load()
    }

    pub fn delete_profile(
        &self,
        profile_id: &str,
        expected_revision: u64,
    ) -> Result<GatewayCatalog> {
        let connection = self.connection.lock().unwrap();
        let transaction = connection.unchecked_transaction()?;
        let revision = transaction
            .query_row(
                "SELECT profile_version FROM gateway_profile WHERE profile_id = ?1",
                [profile_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .context("AI profile not found")?;
        if revision < 0 || revision as u64 != expected_revision {
            bail!("AI profile revision conflict");
        }
        let active_jobs: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM ai_jobs
             WHERE profile_id = ?1
               AND state IN ('capturing', 'queued', 'running_asr', 'running_llm')",
            [profile_id],
            |row| row.get(0),
        )?;
        if active_jobs != 0 {
            bail!("AI profile is referenced by an active job");
        }
        transaction.execute(
            "DELETE FROM gateway_profile WHERE profile_id = ?1",
            [profile_id],
        )?;
        bump_catalog_version(&transaction)?;
        transaction.commit()?;
        drop(connection);
        self.load()
    }

    pub fn bootstrap_admin(&self, password: &str, created_at_ms: u64) -> Result<bool> {
        validate_password(password)?;
        let connection = self.connection.lock().unwrap();
        let existing: i64 =
            connection.query_row("SELECT COUNT(*) FROM gateway_admin_user", [], |row| {
                row.get(0)
            })?;
        if existing != 0 {
            return Ok(false);
        }
        let salt = SaltString::generate(&mut OsRng);
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|error| anyhow::anyhow!("hash gateway admin password: {error}"))?
            .to_string();
        connection.execute(
            "INSERT INTO gateway_admin_user (username, password_hash, enabled, created_at_ms)
             VALUES ('admin', ?1, 1, ?2)",
            params![password_hash, as_i64(created_at_ms)?],
        )?;
        Ok(true)
    }

    pub fn authenticate_admin(&self, username: &str, password: &str) -> Result<bool> {
        if username.trim().is_empty() || password.is_empty() {
            return Ok(false);
        }
        let connection = self.connection.lock().unwrap();
        let record = connection
            .query_row(
                "SELECT password_hash, enabled FROM gateway_admin_user WHERE username = ?1",
                [username],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0)),
            )
            .optional()?;
        let Some((password_hash, enabled)) = record else {
            return Ok(false);
        };
        if !enabled {
            return Ok(false);
        }
        let parsed = match PasswordHash::new(&password_hash) {
            Ok(value) => value,
            Err(_) => return Ok(false),
        };
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    }

    fn seed_if_empty(&self, bootstrap: &GatewayConfig) -> Result<()> {
        let connection = self.connection.lock().unwrap();
        let provider_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM gateway_provider", [], |row| {
                row.get(0)
            })?;
        let profile_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM gateway_profile", [], |row| row.get(0))?;
        if provider_count != 0 || profile_count != 0 {
            return Ok(());
        }
        let transaction = connection.unchecked_transaction()?;
        for provider in &bootstrap.providers {
            transaction.execute(
                "INSERT INTO gateway_provider
                 (provider_id, display_name, kind, enabled, revision, parameters_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    provider.provider_id.as_str(),
                    provider.display_name.as_str(),
                    serde_json::to_string(&provider.kind)?,
                    if provider.enabled { 1_i64 } else { 0_i64 },
                    as_i64(provider.revision)?,
                    serde_json::to_string(&provider.parameters)?,
                ],
            )?;
        }
        for profile in &bootstrap.profiles {
            transaction.execute(
                "INSERT INTO gateway_profile
                 (profile_id, profile_version, enabled, pipeline_type,
                  asr_provider_id, llm_provider_id, tts_provider_id,
                  capture_complete_ratio_ppm, capture_process_min_ratio_ppm,
                  capture_complete_max_gap_ms, capture_process_max_gap_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    profile.profile_id.as_str(),
                    as_i64(profile.profile_version)?,
                    if profile.enabled { 1_i64 } else { 0_i64 },
                    pipeline_type_str(profile.pipeline_type),
                    profile.asr_provider_id.as_deref().unwrap_or(""),
                    profile.llm_provider_id.as_deref().unwrap_or(""),
                    profile.tts_provider_id.as_deref().unwrap_or(""),
                    i64::from(profile.capture.complete_ratio_ppm),
                    i64::from(profile.capture.process_min_ratio_ppm),
                    as_i64(profile.capture.complete_max_gap_ms)?,
                    as_i64(profile.capture.process_max_gap_ms)?,
                ],
            )?;
        }
        transaction.execute(
            "INSERT INTO gateway_catalog_meta (key, value) VALUES ('catalog_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = MAX(value, excluded.value)",
            [as_i64(bootstrap.profile_catalog_version)?],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn migrate_legacy_mock_catalog(&self) -> Result<()> {
        let connection = self.connection.lock().unwrap();
        let transaction = connection.unchecked_transaction()?;
        let mut statement = transaction.prepare(
            "SELECT provider_id FROM gateway_provider
             WHERE kind IN ('\"mock_asr\"', '\"mock_llm\"', 'mock_asr', 'mock_llm')",
        )?;
        let legacy_provider_ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        transaction.execute(
            "DELETE FROM gateway_profile
             WHERE asr_provider_id IN ('mock-asr', 'mock-llm')
                OR llm_provider_id IN ('mock-asr', 'mock-llm')
                OR tts_provider_id IN ('mock-asr', 'mock-llm')",
            [],
        )?;
        for provider_id in &legacy_provider_ids {
            transaction.execute(
                "DELETE FROM gateway_profile
                 WHERE asr_provider_id = ?1 OR llm_provider_id = ?1 OR tts_provider_id = ?1",
                [provider_id],
            )?;
            transaction.execute(
                "DELETE FROM gateway_provider_secret WHERE provider_id = ?1",
                [provider_id],
            )?;
            transaction.execute(
                "DELETE FROM gateway_provider WHERE provider_id = ?1",
                [provider_id],
            )?;
        }
        if !legacy_provider_ids.is_empty() {
            transaction.execute(
                "INSERT INTO gateway_catalog_meta (key, value) VALUES ('catalog_version', 1)
                 ON CONFLICT(key) DO UPDATE SET value = value + 1",
                [],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn migrate_provider_defaults(&self) -> Result<()> {
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT provider_id, kind, display_name, parameters_json FROM gateway_provider",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        for (provider_id, kind_json, display_name, parameters_json) in rows {
            let kind: GatewayProviderKind = serde_json::from_str(&kind_json)?;
            let next_name = if display_name.trim().is_empty() {
                provider_id.clone()
            } else {
                display_name
            };
            let next_parameters = if parameters_json.trim().is_empty() {
                serde_json::to_string(&GatewayProviderParameters::defaults_for(kind))?
            } else {
                parameters_json
            };
            connection.execute(
                "UPDATE gateway_provider SET display_name = ?2, parameters_json = ?3
                 WHERE provider_id = ?1",
                params![provider_id, next_name, next_parameters],
            )?;
        }
        Ok(())
    }

    fn store_secret(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        provider_id: &str,
        kind: GatewayProviderKind,
        plaintext: &str,
    ) -> Result<()> {
        let cipher = self
            .secret_cipher
            .as_ref()
            .context("AI gateway secret master key is not configured")?;
        let secret_name = secret_name(kind).context("provider kind does not accept secrets")?;
        let (ciphertext, nonce) = cipher.encrypt(provider_id, secret_name, plaintext.as_bytes())?;
        transaction.execute(
            "INSERT INTO gateway_provider_secret
             (provider_id, secret_name, ciphertext, nonce, key_version, masked, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6)
             ON CONFLICT(provider_id, secret_name) DO UPDATE SET
               ciphertext = excluded.ciphertext,
               nonce = excluded.nonce,
               key_version = excluded.key_version,
               masked = excluded.masked,
               updated_at_ms = excluded.updated_at_ms",
            params![
                provider_id,
                secret_name,
                ciphertext,
                nonce,
                mask_secret(plaintext),
                as_i64(ai_protocol::time::unix_timestamp_ms())?,
            ],
        )?;
        Ok(())
    }
}

pub fn build_provider_registry(
    store: &CatalogStore,
    catalog: &GatewayCatalog,
) -> Result<ProviderRegistry> {
    let mut registry = ProviderRegistry::default();
    for provider in &catalog.providers {
        if !provider.enabled || provider.parameters.validate().is_err() {
            continue;
        }
        let secret = store.provider_secret(&provider.provider_id, provider.kind)?;
        match (&provider.parameters, provider.kind) {
            (
                GatewayProviderParameters::LocalHttpAsr {
                    base_url,
                    language,
                    request_timeout_seconds,
                },
                GatewayProviderKind::LocalHttpAsr,
            ) => {
                registry
                    .register_asr(std::sync::Arc::new(LocalHttpAsrProvider::new(
                        ProviderId::new(provider.provider_id.clone())?,
                        ai_provider::LocalHttpAsrConfig {
                            base_url: base_url.clone(),
                            language: language.clone(),
                            request_timeout_seconds: *request_timeout_seconds,
                            enabled: true,
                        },
                    )?))
                    .map_err(anyhow::Error::msg)?;
            }
            (
                GatewayProviderParameters::OpenAiCompatibleLlm {
                    base_url,
                    model,
                    structured_output_mode,
                    request_timeout_seconds,
                    max_output_tokens,
                    temperature,
                },
                GatewayProviderKind::OpenAiCompatibleLlm,
            ) => {
                let api_key = secret.context("enabled LLM provider secret is not configured")?;
                let config = OpenAiCompatibleLlmConfig {
                    base_url: base_url.clone(),
                    credential_id: "stored-secret".to_string(),
                    model: model.clone(),
                    structured_output_mode: *structured_output_mode,
                    request_timeout_seconds: *request_timeout_seconds,
                    max_output_tokens: *max_output_tokens,
                    temperature: *temperature,
                    enabled: true,
                };
                registry
                    .register_llm(std::sync::Arc::new(OpenAiCompatibleLlmProvider::new(
                        ProviderId::new(provider.provider_id.clone())?,
                        config,
                        api_key,
                    )?))
                    .map_err(anyhow::Error::msg)?;
            }
            _ => {}
        }
    }
    Ok(registry)
}

fn runtime_state(
    _kind: GatewayProviderKind,
    enabled: bool,
    secret: &ProviderSecretStatus,
    parameters: &GatewayProviderParameters,
) -> (ProviderRuntimeState, Option<String>) {
    if !enabled {
        return (ProviderRuntimeState::Disabled, None);
    }
    if parameters.validate().is_err() {
        return (
            ProviderRuntimeState::Incomplete,
            Some("provider_parameters_invalid".to_string()),
        );
    }
    if secret_name(_kind).is_some() && !secret.configured {
        return (
            ProviderRuntimeState::Incomplete,
            Some("provider_secret_required".to_string()),
        );
    }
    match _kind {
        GatewayProviderKind::LocalHttpAsr | GatewayProviderKind::OpenAiCompatibleLlm => {
            (ProviderRuntimeState::Ready, None)
        }
        GatewayProviderKind::VolcengineAsr => (
            ProviderRuntimeState::AdapterUnavailable,
            Some("provider_adapter_not_implemented".to_string()),
        ),
    }
}

fn secret_name(kind: GatewayProviderKind) -> Option<&'static str> {
    match kind {
        GatewayProviderKind::LocalHttpAsr => None,
        GatewayProviderKind::VolcengineAsr => Some("access_token"),
        GatewayProviderKind::OpenAiCompatibleLlm => Some("api_key"),
    }
}

fn load_secret_status(
    connection: &Connection,
    provider_id: &str,
    kind: GatewayProviderKind,
) -> Result<ProviderSecretStatus> {
    let Some(secret_name) = secret_name(kind) else {
        return Ok(ProviderSecretStatus::default());
    };
    Ok(connection
        .query_row(
            "SELECT masked, updated_at_ms FROM gateway_provider_secret
             WHERE provider_id = ?1 AND secret_name = ?2",
            params![provider_id, secret_name],
            |row| {
                Ok(ProviderSecretStatus {
                    configured: true,
                    masked: Some(row.get(0)?),
                    updated_at_ms: Some(row.get::<_, i64>(1)? as u64),
                })
            },
        )
        .optional()?
        .unwrap_or_default())
}

fn mask_secret(secret: &str) -> String {
    let suffix = secret.chars().rev().take(4).collect::<Vec<_>>();
    format!("****{}", suffix.into_iter().rev().collect::<String>())
}

struct SecretCipher {
    cipher: XChaCha20Poly1305,
}

impl SecretCipher {
    fn from_environment() -> Result<Option<Self>> {
        let bytes = if let Some(path) = std::env::var_os("AI_GATEWAY_SECRET_KEY_FILE") {
            Some(std::fs::read(&path).with_context(|| {
                format!(
                    "read AI gateway secret key file {}",
                    Path::new(&path).display()
                )
            })?)
        } else if let Ok(value) = std::env::var("AI_GATEWAY_MASTER_KEY") {
            Some(value.into_bytes())
        } else {
            None
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        Ok(Some(Self::from_key_bytes(&bytes)?))
    }

    fn from_key_bytes(bytes: &[u8]) -> Result<Self> {
        let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
        if bytes.len() != 32 {
            bail!("AI gateway secret master key must contain exactly 32 bytes");
        }
        Ok(Self {
            cipher: XChaCha20Poly1305::new(Key::from_slice(bytes)),
        })
    }

    fn encrypt(
        &self,
        provider_id: &str,
        secret_name: &str,
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut nonce = [0_u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let associated_data = format!("v1:{provider_id}:{secret_name}");
        let ciphertext = self
            .cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                chacha20poly1305::aead::Payload {
                    msg: plaintext,
                    aad: associated_data.as_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("encrypt provider secret"))?;
        Ok((ciphertext, nonce.to_vec()))
    }

    fn decrypt(
        &self,
        provider_id: &str,
        secret_name: &str,
        ciphertext: &[u8],
        nonce: &[u8],
    ) -> Result<String> {
        let nonce: &[u8; 24] = nonce.try_into().context("invalid secret nonce")?;
        let associated_data = format!("v1:{provider_id}:{secret_name}");
        let plaintext = self
            .cipher
            .decrypt(
                XNonce::from_slice(nonce),
                chacha20poly1305::aead::Payload {
                    msg: ciphertext,
                    aad: associated_data.as_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("decrypt provider secret"))?;
        String::from_utf8(plaintext).context("provider secret is not UTF-8")
    }
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !columns.iter().any(|existing| existing == column) {
        connection.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))?;
    }
    Ok(())
}

fn validate_catalog(
    version: u64,
    providers: &[GatewayProviderConfig],
    profiles: &[GatewayProfileConfig],
) -> Result<()> {
    if version == 0 {
        bail!("catalog version must be greater than zero");
    }
    let mut provider_ids = BTreeSet::new();
    for provider in providers {
        ProviderId::new(provider.provider_id.clone())?;
        if provider.parameters.kind() != provider.kind {
            bail!("provider parameters do not match provider kind");
        }
        if !provider_ids.insert(provider.provider_id.clone()) {
            bail!("duplicate provider {}", provider.provider_id);
        }
    }
    let mut profile_ids = BTreeSet::new();
    for profile in profiles {
        profile.validate()?;
        if !profile_ids.insert(profile.profile_id.clone()) {
            bail!("duplicate profile {}", profile.profile_id);
        }
    }
    Ok(())
}

fn validate_profile_provider_reference(
    transaction: &rusqlite::Transaction<'_>,
    provider_id: &str,
    capability: &str,
) -> Result<()> {
    let kind_json = transaction
        .query_row(
            "SELECT kind FROM gateway_provider WHERE provider_id = ?1",
            [provider_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .with_context(|| format!("provider {provider_id} does not exist"))?;
    let kind: GatewayProviderKind = serde_json::from_str(&kind_json)?;
    if kind.capability() != capability {
        bail!("provider {provider_id} does not provide {capability}");
    }
    Ok(())
}

fn optional_provider_id(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn pipeline_type_str(value: AiPipelineType) -> &'static str {
    match value {
        AiPipelineType::Transcription => "transcription",
        AiPipelineType::PostCallAnalysis => "post_call_analysis",
        AiPipelineType::LlmTask => "llm_task",
        AiPipelineType::VoiceAgent => "voice_agent",
    }
}

fn parse_pipeline_type(value: &str) -> serde_json::Result<AiPipelineType> {
    serde_json::from_str(&format!("\"{value}\""))
}

fn as_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("catalog value exceeds SQLite integer range")
}

fn next_revision(
    transaction: &rusqlite::Transaction<'_>,
    statement: &str,
    id: &str,
) -> Result<u64> {
    let existing = transaction
        .query_row(statement, [id], |row| row.get::<_, i64>(0))
        .optional()?;
    match existing {
        Some(value) if value >= 0 => Ok((value as u64).saturating_add(1)),
        Some(_) => bail!("catalog revision must not be negative"),
        None => Ok(1),
    }
}

fn bump_catalog_version(transaction: &rusqlite::Transaction<'_>) -> Result<()> {
    let version = transaction.query_row(
        "SELECT value FROM gateway_catalog_meta WHERE key = 'catalog_version'",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    if version < 0 {
        bail!("catalog version must not be negative");
    }
    transaction.execute(
        "UPDATE gateway_catalog_meta SET value = ?1 WHERE key = 'catalog_version'",
        [as_i64((version as u64).saturating_add(1))?],
    )?;
    Ok(())
}

fn to_sql_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn to_sql_failure(message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::other(message)),
    )
}

fn validate_password(password: &str) -> Result<()> {
    if !(1..=1024).contains(&password.len()) {
        bail!("gateway admin password must contain 1..=1024 bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(data_dir: &std::path::Path) -> GatewayConfig {
        let mut config = GatewayConfig::with_data_dir(data_dir.to_path_buf());
        config.providers = vec![
            GatewayProviderConfig {
                provider_id: "volcengine-asr".to_string(),
                display_name: "Volcengine ASR".to_string(),
                kind: GatewayProviderKind::VolcengineAsr,
                enabled: false,
                revision: 1,
                parameters: GatewayProviderParameters::defaults_for(
                    GatewayProviderKind::VolcengineAsr,
                ),
                ..GatewayProviderConfig::default()
            },
            GatewayProviderConfig {
                provider_id: "openai-llm".to_string(),
                display_name: "OpenAI Compatible LLM".to_string(),
                kind: GatewayProviderKind::OpenAiCompatibleLlm,
                enabled: false,
                revision: 1,
                parameters: GatewayProviderParameters::defaults_for(
                    GatewayProviderKind::OpenAiCompatibleLlm,
                ),
                ..GatewayProviderConfig::default()
            },
        ];
        if let GatewayProviderParameters::VolcengineAsr {
            app_id,
            resource_id,
            model_or_cluster,
            ..
        } = &mut config.providers[0].parameters
        {
            *app_id = "test-app".to_string();
            *resource_id = "test-resource".to_string();
            *model_or_cluster = "test-model".to_string();
        }
        config.profiles = vec![GatewayProfileConfig::default()];
        config
    }

    #[test]
    fn seeds_bootstrap_catalog_once() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("gateway.db");
        let config = test_config(directory.path());
        let mut store = CatalogStore::open(&path, &config).unwrap();
        store.secret_cipher =
            Some(SecretCipher::from_key_bytes(b"01234567890123456789012345678901").unwrap());
        let catalog = store.load().unwrap();
        assert_eq!(catalog.version, 1);
        assert_eq!(catalog.providers.len(), 2);
        assert_eq!(catalog.profiles.len(), 1);
        assert!(
            build_provider_registry(&store, &catalog)
                .unwrap()
                .asr("mock-asr")
                .is_none()
        );
    }

    #[test]
    fn updates_revisions_and_catalog_version() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("gateway.db");
        let config = test_config(directory.path());
        let _jobs = crate::store::JobStore::open(&path).unwrap();
        let store = CatalogStore::open(&path, &config).unwrap();
        let provider = store
            .load()
            .unwrap()
            .providers
            .into_iter()
            .find(|provider| provider.kind == GatewayProviderKind::VolcengineAsr)
            .unwrap();
        let catalog = store
            .upsert_provider(ProviderUpsertRequest {
                provider_id: provider.provider_id,
                display_name: provider.display_name,
                kind: provider.kind,
                enabled: false,
                expected_revision: Some(provider.revision),
                parameters: provider.parameters,
                secret: None,
            })
            .unwrap();
        assert_eq!(catalog.version, 2);
        assert_eq!(
            catalog
                .providers
                .iter()
                .find(|item| item.provider_id == "volcengine-asr")
                .unwrap()
                .revision,
            2
        );

        let profile = catalog.profiles[0].clone();
        let catalog = store.upsert_profile(profile).unwrap();
        assert_eq!(catalog.version, 3);
        assert_eq!(catalog.profiles[0].profile_version, 2);
    }

    #[test]
    fn rejects_stale_profile_version() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("profile-cas-gateway.db");
        let config = test_config(directory.path());
        let store = CatalogStore::open(&path, &config).unwrap();
        let mut profile = store.load().unwrap().profiles[0].clone();
        profile.profile_version = 2;
        let error = store.upsert_profile(profile).unwrap_err();
        assert!(error.to_string().contains("revision conflict"));
    }

    #[test]
    fn removes_legacy_mock_provider_and_referenced_profile() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("legacy-mock-gateway.db");
        let config = GatewayConfig::with_data_dir(directory.path().to_path_buf());
        let store = CatalogStore::open(&path, &config).unwrap();
        let connection = store.connection.lock().unwrap();
        connection
            .execute(
                "INSERT INTO gateway_provider
                 (provider_id, display_name, kind, enabled, revision, parameters_json)
                 VALUES ('legacy-mock', 'Legacy Mock', '\"mock_asr\"', 1, 1, '{}')",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO gateway_profile
                 (profile_id, profile_version, enabled, pipeline_type,
                  asr_provider_id, llm_provider_id, tts_provider_id,
                  capture_complete_ratio_ppm, capture_process_min_ratio_ppm,
                  capture_complete_max_gap_ms, capture_process_max_gap_ms)
                 VALUES ('legacy-profile', 1, 1, 'post_call_analysis',
                         'legacy-mock', '', '', 995000, 950000, 200, 5000)",
                [],
            )
            .unwrap();
        drop(connection);
        drop(store);

        let store = CatalogStore::open(&path, &config).unwrap();
        let catalog = store.load().unwrap();
        assert!(catalog.providers.is_empty());
        assert!(catalog.profiles.is_empty());
    }

    #[test]
    fn deletes_unreferenced_profile_and_rejects_active_job_reference() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("deletable-gateway.db");
        let config = test_config(directory.path());
        let _jobs = crate::store::JobStore::open(&path).unwrap();
        let store = CatalogStore::open(&path, &config).unwrap();
        let profile = store.load().unwrap().profiles.remove(0);
        let catalog = store
            .delete_profile(&profile.profile_id, profile.profile_version)
            .unwrap();
        assert!(catalog.profiles.is_empty());

        let path = directory.path().join("active-job-gateway.db");
        let _jobs = crate::store::JobStore::open(&path).unwrap();
        let store = CatalogStore::open(&path, &config).unwrap();
        let profile = store.load().unwrap().profiles.remove(0);
        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO ai_jobs
                 (job_id, tenant_id, conversation_id, profile_id, request_json, state,
                  analysis_version, capture_manifest_json, deadline_at_ms, created_at_ms, updated_at_ms)
                 VALUES ('active-job', 'domain-1', 'call-1', ?1, '{}', 'capturing', 1, '{}', 1, 1, 1)",
                [&profile.profile_id],
            )
            .unwrap();
        let error = store
            .delete_profile(&profile.profile_id, profile.profile_version)
            .unwrap_err();
        assert!(error.to_string().contains("referenced by an active job"));
    }

    #[test]
    fn encrypts_real_provider_secret_without_exposing_it_in_catalog() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("gateway.db");
        let config = GatewayConfig::with_data_dir(directory.path().to_path_buf());
        let mut store = CatalogStore::open(&path, &config).unwrap();
        store.secret_cipher =
            Some(SecretCipher::from_key_bytes(b"01234567890123456789012345678901").unwrap());
        let catalog = store
            .upsert_provider(ProviderUpsertRequest {
                provider_id: "llm-production".to_string(),
                display_name: "Production LLM".to_string(),
                kind: GatewayProviderKind::OpenAiCompatibleLlm,
                enabled: true,
                expected_revision: None,
                parameters: GatewayProviderParameters::OpenAiCompatibleLlm {
                    base_url: "https://example.test/v1".to_string(),
                    model: "model-1".to_string(),
                    structured_output_mode: ai_provider::StructuredOutputMode::JsonObject,
                    request_timeout_seconds: 60,
                    max_output_tokens: 2048,
                    temperature: 0.2,
                },
                secret: Some("top-secret-key".to_string()),
            })
            .unwrap();
        let provider = catalog
            .providers
            .iter()
            .find(|provider| provider.provider_id == "llm-production")
            .unwrap();
        assert!(provider.secret.configured);
        assert_eq!(provider.secret.masked.as_deref(), Some("****-key"));
        assert_eq!(provider.runtime_state, ProviderRuntimeState::Ready);
        assert!(
            !serde_json::to_string(provider)
                .unwrap()
                .contains("top-secret-key")
        );
    }

    #[test]
    fn bootstraps_and_verifies_admin_once() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("gateway.db");
        let config = GatewayConfig::with_data_dir(directory.path().to_path_buf());
        let store = CatalogStore::open(&path, &config).unwrap();
        assert!(
            store
                .bootstrap_admin("sufficiently-long-password", 1)
                .unwrap()
        );
        assert!(!store.bootstrap_admin("another-long-password", 2).unwrap());
        assert!(
            store
                .authenticate_admin("admin", "sufficiently-long-password")
                .unwrap()
        );
        assert!(!store.authenticate_admin("admin", "wrong-password").unwrap());
    }
}
