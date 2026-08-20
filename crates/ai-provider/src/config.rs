use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalHttpAsrConfig {
    pub base_url: String,
    pub language: String,
    pub request_timeout_seconds: u64,
    pub enabled: bool,
}

impl LocalHttpAsrConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(self.base_url.starts_with("http://") || self.base_url.starts_with("https://")) {
            return Err("base_url must use http or https".to_string());
        }
        if self.language.trim().is_empty() {
            return Err("language is required".to_string());
        }
        if !(1..=600).contains(&self.request_timeout_seconds) {
            return Err("request_timeout_seconds must be within 1..=600".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuredOutputMode {
    JsonSchema,
    JsonObject,
    PromptOnly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenAiCompatibleLlmConfig {
    pub base_url: String,
    pub credential_id: String,
    pub model: String,
    pub structured_output_mode: StructuredOutputMode,
    pub request_timeout_seconds: u64,
    pub max_output_tokens: u32,
    pub temperature: f32,
    pub enabled: bool,
}

impl OpenAiCompatibleLlmConfig {
    pub fn validate(&self) -> Result<(), String> {
        let base_url = self.base_url.trim_end_matches('/');
        if !base_url.starts_with("https://") {
            return Err("base_url must use https".to_string());
        }
        if base_url.contains('?')
            || base_url.contains('#')
            || base_url.ends_with("/chat/completions")
        {
            return Err(
                "base_url must be an API root without query, fragment, or chat endpoint"
                    .to_string(),
            );
        }
        if self.credential_id.trim().is_empty() || self.model.trim().is_empty() {
            return Err("credential_id and model are required".to_string());
        }
        if !(1..=600).contains(&self.request_timeout_seconds) {
            return Err("request_timeout_seconds must be within 1..=600".to_string());
        }
        if self.max_output_tokens == 0 || self.max_output_tokens > 65_536 {
            return Err("max_output_tokens must be within 1..=65536".to_string());
        }
        if !(0.0..=2.0).contains(&self.temperature) {
            return Err("temperature must be within 0..=2".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolcengineApiVariant {
    BigModelStreaming,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolcengineAsrConfig {
    pub api_variant: VolcengineApiVariant,
    pub endpoint_override: Option<String>,
    pub credential_id: String,
    pub app_id: String,
    pub resource_id: String,
    pub model_or_cluster: String,
    pub language: String,
    pub request_timeout_seconds: u64,
    pub max_concurrent_sessions: u32,
    pub max_session_seconds: u64,
    pub enabled: bool,
}

impl VolcengineAsrConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("credential_id", self.credential_id.as_str()),
            ("app_id", self.app_id.as_str()),
            ("resource_id", self.resource_id.as_str()),
            ("model_or_cluster", self.model_or_cluster.as_str()),
            ("language", self.language.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{name} is required"));
            }
        }
        if let Some(endpoint) = &self.endpoint_override
            && !(endpoint.starts_with("wss://") || endpoint.starts_with("https://"))
        {
            return Err("endpoint_override must use wss or https".to_string());
        }
        if !(1..=600).contains(&self.request_timeout_seconds) {
            return Err("request_timeout_seconds must be within 1..=600".to_string());
        }
        if self.max_concurrent_sessions == 0 || self.max_concurrent_sessions > 100_000 {
            return Err("max_concurrent_sessions must be within 1..=100000".to_string());
        }
        if !(60..=86_400).contains(&self.max_session_seconds) {
            return Err("max_session_seconds must be within 60..=86400".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_full_chat_completions_url() {
        let config = OpenAiCompatibleLlmConfig {
            base_url: "https://example.test/v1/chat/completions".to_string(),
            credential_id: "secret-1".to_string(),
            model: "model-1".to_string(),
            structured_output_mode: StructuredOutputMode::JsonObject,
            request_timeout_seconds: 60,
            max_output_tokens: 2048,
            temperature: 0.2,
            enabled: true,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validates_typed_volcengine_config() {
        let config = VolcengineAsrConfig {
            api_variant: VolcengineApiVariant::BigModelStreaming,
            endpoint_override: None,
            credential_id: "secret-1".to_string(),
            app_id: "app-1".to_string(),
            resource_id: "resource-1".to_string(),
            model_or_cluster: "model-1".to_string(),
            language: "zh-CN".to_string(),
            request_timeout_seconds: 30,
            max_concurrent_sessions: 10,
            max_session_seconds: 14_400,
            enabled: true,
        };
        assert!(config.validate().is_ok());
    }
}
