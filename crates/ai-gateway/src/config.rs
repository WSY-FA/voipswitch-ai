use ai_protocol::control::AiPipelineType;
use ai_protocol::id::ProfileId;
use ai_provider::{
    LocalHttpAsrConfig, OpenAiCompatibleLlmConfig, StructuredOutputMode, VolcengineApiVariant,
    VolcengineAsrConfig,
};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GatewayConfig {
    pub data_dir: PathBuf,
    pub worker_count: usize,
    pub worker_queue_capacity: usize,
    pub execution: ExecutionConfig,
    pub capture_defaults: CaptureThresholds,
    pub storage: StorageLimits,
    pub profile_catalog_version: u64,
    pub providers: Vec<GatewayProviderConfig>,
    pub profiles: Vec<GatewayProfileConfig>,
}

impl GatewayConfig {
    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.worker_count == 0 || self.worker_count > 128 {
            bail!("worker_count must be within 1..=128");
        }
        if self.worker_queue_capacity == 0 || self.worker_queue_capacity > 100_000 {
            bail!("worker_queue_capacity must be within 1..=100000");
        }
        self.execution.validate()?;
        self.capture_defaults.validate()?;
        self.storage.validate()?;
        if self.profile_catalog_version == 0 {
            bail!("profile_catalog_version must be greater than zero");
        }
        let mut provider_ids = BTreeSet::new();
        for provider in &self.providers {
            provider.validate()?;
            if !provider_ids.insert(provider.provider_id.clone()) {
                bail!("duplicate provider {}", provider.provider_id);
            }
        }
        let mut profile_ids = BTreeSet::new();
        for profile in &self.profiles {
            profile.validate()?;
            if !profile_ids.insert(profile.profile_id.clone()) {
                bail!("duplicate profile {}", profile.profile_id);
            }
        }
        Ok(())
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            worker_count: 2,
            worker_queue_capacity: 128,
            execution: ExecutionConfig::default(),
            capture_defaults: CaptureThresholds::default(),
            storage: StorageLimits::default(),
            profile_catalog_version: 1,
            providers: Vec::new(),
            profiles: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRuntimeState {
    Disabled,
    Incomplete,
    Ready,
    AdapterUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderSecretStatus {
    pub configured: bool,
    pub masked: Option<String>,
    pub updated_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayProviderParameters {
    LocalHttpAsr {
        base_url: String,
        language: String,
        request_timeout_seconds: u64,
    },
    VolcengineAsr {
        api_variant: VolcengineApiVariant,
        endpoint_override: Option<String>,
        app_id: String,
        resource_id: String,
        model_or_cluster: String,
        language: String,
        request_timeout_seconds: u64,
        max_concurrent_sessions: u32,
        max_session_seconds: u64,
    },
    OpenAiCompatibleLlm {
        base_url: String,
        model: String,
        structured_output_mode: StructuredOutputMode,
        request_timeout_seconds: u64,
        max_output_tokens: u32,
        temperature: f32,
    },
}

impl GatewayProviderParameters {
    pub fn defaults_for(kind: GatewayProviderKind) -> Self {
        match kind {
            GatewayProviderKind::LocalHttpAsr => Self::LocalHttpAsr {
                base_url: "http://127.0.0.1:8000".to_string(),
                language: "zh-CN".to_string(),
                request_timeout_seconds: 60,
            },
            GatewayProviderKind::VolcengineAsr => Self::VolcengineAsr {
                api_variant: VolcengineApiVariant::BigModelStreaming,
                endpoint_override: None,
                app_id: String::new(),
                resource_id: String::new(),
                model_or_cluster: String::new(),
                language: "zh-CN".to_string(),
                request_timeout_seconds: 30,
                max_concurrent_sessions: 10,
                max_session_seconds: 14_400,
            },
            GatewayProviderKind::OpenAiCompatibleLlm => Self::OpenAiCompatibleLlm {
                base_url: String::new(),
                model: String::new(),
                structured_output_mode: StructuredOutputMode::JsonObject,
                request_timeout_seconds: 60,
                max_output_tokens: 2_048,
                temperature: 0.2,
            },
        }
    }

    pub fn kind(&self) -> GatewayProviderKind {
        match self {
            Self::LocalHttpAsr { .. } => GatewayProviderKind::LocalHttpAsr,
            Self::VolcengineAsr { .. } => GatewayProviderKind::VolcengineAsr,
            Self::OpenAiCompatibleLlm { .. } => GatewayProviderKind::OpenAiCompatibleLlm,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::LocalHttpAsr {
                base_url,
                language,
                request_timeout_seconds,
            } => LocalHttpAsrConfig {
                base_url: base_url.clone(),
                language: language.clone(),
                request_timeout_seconds: *request_timeout_seconds,
                enabled: true,
            }
            .validate()
            .map_err(anyhow::Error::msg),
            Self::VolcengineAsr {
                api_variant,
                endpoint_override,
                app_id,
                resource_id,
                model_or_cluster,
                language,
                request_timeout_seconds,
                max_concurrent_sessions,
                max_session_seconds,
            } => VolcengineAsrConfig {
                api_variant: *api_variant,
                endpoint_override: endpoint_override.clone(),
                credential_id: "stored-secret".to_string(),
                app_id: app_id.clone(),
                resource_id: resource_id.clone(),
                model_or_cluster: model_or_cluster.clone(),
                language: language.clone(),
                request_timeout_seconds: *request_timeout_seconds,
                max_concurrent_sessions: *max_concurrent_sessions,
                max_session_seconds: *max_session_seconds,
                enabled: true,
            }
            .validate()
            .map_err(anyhow::Error::msg),
            Self::OpenAiCompatibleLlm {
                base_url,
                model,
                structured_output_mode,
                request_timeout_seconds,
                max_output_tokens,
                temperature,
            } => OpenAiCompatibleLlmConfig {
                base_url: base_url.clone(),
                credential_id: "stored-secret".to_string(),
                model: model.clone(),
                structured_output_mode: *structured_output_mode,
                request_timeout_seconds: *request_timeout_seconds,
                max_output_tokens: *max_output_tokens,
                temperature: *temperature,
                enabled: true,
            }
            .validate()
            .map_err(anyhow::Error::msg),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayProviderKind {
    LocalHttpAsr,
    VolcengineAsr,
    OpenAiCompatibleLlm,
}

impl GatewayProviderKind {
    pub fn capability(self) -> &'static str {
        match self {
            Self::LocalHttpAsr => "asr",
            Self::VolcengineAsr => "asr",
            Self::OpenAiCompatibleLlm => "llm",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GatewayProviderConfig {
    pub provider_id: String,
    pub display_name: String,
    pub kind: GatewayProviderKind,
    pub enabled: bool,
    pub revision: u64,
    pub parameters: GatewayProviderParameters,
    pub secret: ProviderSecretStatus,
    pub runtime_state: ProviderRuntimeState,
    pub runtime_message: Option<String>,
}

impl GatewayProviderConfig {
    pub fn validate(&self) -> Result<()> {
        ai_protocol::id::ProviderId::new(self.provider_id.clone())?;
        if self.display_name.trim().is_empty() || self.display_name.len() > 128 {
            bail!("provider display_name must contain 1..=128 bytes");
        }
        if self.revision == 0 {
            bail!("provider revision must be greater than zero");
        }
        if self.parameters.kind() != self.kind {
            bail!("provider parameters do not match provider kind");
        }
        self.parameters.validate()
    }
}

impl Default for GatewayProviderConfig {
    fn default() -> Self {
        Self {
            provider_id: "volcengine-asr".to_string(),
            display_name: "Volcengine ASR".to_string(),
            kind: GatewayProviderKind::VolcengineAsr,
            enabled: false,
            revision: 1,
            parameters: GatewayProviderParameters::defaults_for(GatewayProviderKind::VolcengineAsr),
            secret: ProviderSecretStatus::default(),
            runtime_state: ProviderRuntimeState::Incomplete,
            runtime_message: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderUpsertRequest {
    pub provider_id: String,
    pub display_name: String,
    pub kind: GatewayProviderKind,
    pub enabled: bool,
    pub expected_revision: Option<u64>,
    pub parameters: GatewayProviderParameters,
    #[serde(default)]
    pub secret: Option<String>,
}

impl ProviderUpsertRequest {
    pub fn validate(&self) -> Result<()> {
        let candidate = GatewayProviderConfig {
            provider_id: self.provider_id.clone(),
            display_name: self.display_name.clone(),
            kind: self.kind,
            enabled: self.enabled,
            revision: self.expected_revision.unwrap_or(1),
            parameters: self.parameters.clone(),
            secret: ProviderSecretStatus::default(),
            runtime_state: ProviderRuntimeState::Incomplete,
            runtime_message: None,
        };
        candidate.validate()?;
        if self
            .secret
            .as_ref()
            .is_some_and(|value| value.len() > 16_384)
        {
            bail!("provider secret must not exceed 16384 bytes");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GatewayProfileConfig {
    pub profile_id: String,
    pub profile_version: u64,
    pub enabled: bool,
    pub pipeline_type: AiPipelineType,
    pub asr_provider_id: Option<String>,
    pub llm_provider_id: Option<String>,
    pub tts_provider_id: Option<String>,
    pub capture: CaptureThresholds,
}

impl GatewayProfileConfig {
    pub fn validate(&self) -> Result<()> {
        ProfileId::new(self.profile_id.clone())?;
        if self.profile_version == 0 {
            bail!("profile_version must be greater than zero");
        }
        ai_protocol::control::AiProfileSnapshot {
            profile_id: ProfileId::new(self.profile_id.clone())?,
            profile_version: self.profile_version,
            pipeline_type: self.pipeline_type,
            asr_provider_id: self.asr_provider_id.clone(),
            llm_provider_id: self.llm_provider_id.clone(),
            tts_provider_id: self.tts_provider_id.clone(),
            capture_complete_ratio: f64::from(self.capture.complete_ratio_ppm) / 1_000_000.0,
            capture_process_min_ratio: f64::from(self.capture.process_min_ratio_ppm) / 1_000_000.0,
            capture_complete_max_gap_ms: self.capture.complete_max_gap_ms,
            capture_process_max_gap_ms: self.capture.process_max_gap_ms,
        }
        .validate()?;
        self.capture.validate()
    }
}

impl Default for GatewayProfileConfig {
    fn default() -> Self {
        Self {
            profile_id: "profile-1".to_string(),
            profile_version: 1,
            enabled: true,
            pipeline_type: AiPipelineType::PostCallAnalysis,
            asr_provider_id: Some("volcengine-asr".to_string()),
            llm_provider_id: Some("openai-llm".to_string()),
            tts_provider_id: None,
            capture: CaptureThresholds::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub asr_max_retries: u32,
    pub llm_max_retries: u32,
    pub retry_initial_delay_ms: u64,
    pub retry_max_delay_ms: u64,
    pub claim_lease_seconds: u64,
    pub claim_renew_interval_seconds: u64,
    pub post_call_job_deadline_seconds: u64,
    pub max_manual_analysis_versions: u32,
}

impl ExecutionConfig {
    fn validate(&self) -> Result<()> {
        if self.asr_max_retries > 10 || self.llm_max_retries > 10 {
            bail!("provider retries must not exceed 10");
        }
        if self.retry_initial_delay_ms == 0 || self.retry_initial_delay_ms > self.retry_max_delay_ms
        {
            bail!("invalid retry delay range");
        }
        if self.claim_lease_seconds < 10
            || self.claim_renew_interval_seconds * 2 >= self.claim_lease_seconds
        {
            bail!("claim renew interval must be less than half the lease");
        }
        if !(60..=86_400).contains(&self.post_call_job_deadline_seconds) {
            bail!("post-call deadline must be within 60..=86400 seconds");
        }
        if self.max_manual_analysis_versions == 0 || self.max_manual_analysis_versions > 100 {
            bail!("max manual analysis versions must be within 1..=100");
        }
        Ok(())
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            asr_max_retries: 2,
            llm_max_retries: 2,
            retry_initial_delay_ms: 1_000,
            retry_max_delay_ms: 10_000,
            claim_lease_seconds: 60,
            claim_renew_interval_seconds: 20,
            post_call_job_deadline_seconds: 14_400,
            max_manual_analysis_versions: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CaptureThresholds {
    pub complete_ratio_ppm: u32,
    pub process_min_ratio_ppm: u32,
    pub complete_max_gap_ms: u64,
    pub process_max_gap_ms: u64,
}

impl CaptureThresholds {
    pub fn validate(&self) -> Result<()> {
        if self.complete_ratio_ppm > 1_000_000
            || self.process_min_ratio_ppm > self.complete_ratio_ppm
        {
            bail!("invalid capture ratio thresholds");
        }
        if self.complete_max_gap_ms > self.process_max_gap_ms {
            bail!("complete gap threshold must not exceed process threshold");
        }
        Ok(())
    }
}

impl Default for CaptureThresholds {
    fn default() -> Self {
        Self {
            complete_ratio_ppm: 995_000,
            process_min_ratio_ppm: 950_000,
            complete_max_gap_ms: 200,
            process_max_gap_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageLimits {
    pub temporary_audio_retention_hours: u64,
    pub persisted_audio_grace_hours: u64,
    pub unacknowledged_result_retention_hours: u64,
    pub disk_warning_percent: u8,
    pub disk_reject_percent: u8,
    pub disk_resume_percent: u8,
    pub disk_min_free_mb: u64,
}

impl StorageLimits {
    fn validate(&self) -> Result<()> {
        if self.temporary_audio_retention_hours == 0
            || self.unacknowledged_result_retention_hours < self.temporary_audio_retention_hours
        {
            bail!("invalid temporary asset retention");
        }
        if !(self.disk_warning_percent < self.disk_resume_percent
            && self.disk_resume_percent < self.disk_reject_percent
            && self.disk_reject_percent < 100)
        {
            bail!("disk watermarks must satisfy warning < resume < reject < 100");
        }
        if self.disk_min_free_mb == 0 {
            bail!("disk_min_free_mb must be greater than zero");
        }
        Ok(())
    }
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            temporary_audio_retention_hours: 72,
            persisted_audio_grace_hours: 1,
            unacknowledged_result_retention_hours: 168,
            disk_warning_percent: 70,
            disk_reject_percent: 85,
            disk_resume_percent: 75,
            disk_min_free_mb: 2_048,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_design() {
        let config = GatewayConfig::default();
        config.validate().unwrap();
        assert_eq!(config.execution.post_call_job_deadline_seconds, 14_400);
        assert_eq!(config.storage.temporary_audio_retention_hours, 72);
        assert_eq!(config.capture_defaults.complete_ratio_ppm, 995_000);
    }
}
