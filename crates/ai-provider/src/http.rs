use crate::config::{ByteDanceTtsConfig, LocalHttpAsrConfig, LocalHttpTtsConfig};
use crate::{
    AsrAudioInput, AsrOutput, AsrProvider, AsrRequest, LlmOutput, LlmProvider, LlmRequest,
    OpenAiCompatibleLlmConfig, ProviderError, ProviderErrorKind, ProviderId, ProviderResult,
    StructuredOutputMode, TtsOutput, TtsProvider, TtsRequest,
};
use ai_protocol::control::{StructuredCallResult, TranscriptSegment};
use async_trait::async_trait;
use base64::Engine;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;

pub struct OpenAiCompatibleLlmProvider {
    provider_id: ProviderId,
    config: OpenAiCompatibleLlmConfig,
    api_key: String,
    client: reqwest::Client,
}

impl OpenAiCompatibleLlmProvider {
    pub fn new(
        provider_id: ProviderId,
        config: OpenAiCompatibleLlmConfig,
        api_key: String,
    ) -> ProviderResult<Self> {
        config.validate().map_err(invalid_config)?;
        if api_key.trim().is_empty() {
            return Err(invalid_config("api key is required"));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .build()
            .map_err(|error| transport_error(format!("build HTTP client: {error}")))?;
        Ok(Self {
            provider_id,
            config,
            api_key,
            client,
        })
    }
}

#[async_trait]
impl LlmProvider for OpenAiCompatibleLlmProvider {
    fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    async fn summarize(&self, request: LlmRequest) -> ProviderResult<LlmOutput> {
        let transcript = request.transcript.iter().map(|segment| {
            json!({"participant_id": segment.participant_id.as_str(), "start_ms": segment.start_ms,
                   "end_ms": segment.end_ms, "text": segment.text})
        }).collect::<Vec<_>>();
        let action_instruction = if request.allow_actions {
            " Include an optional action object using only play_text, collect_digits, transfer_to_extension, transfer_to_business_target, or end_call; use null when no action is needed. For transfer_to_extension, number must be a JSON string containing only digits, for example {\"type\":\"transfer_to_extension\",\"number\":\"1002\"}."
        } else {
            " Set action to null."
        };
        let system_prompt = if request.allow_actions {
            format!(
                "只返回一个 JSON 对象，字段必须为 schema_version、summary、purpose、outcome、key_points、action_items、tags、action。schema_version 必须为 1。当前是实时语音坐席：summary 必须是可以直接播报给用户的简体中文短句，控制在 200 字以内；purpose、outcome、key_points、action_items、tags 只保留必要内容，数组最多各 3 项。除字段名和 action 结构中的固定枚举值外，不要输出英文、Markdown 或其他文字。参与方只能称为“主叫”或“被叫”。{action_instruction}"
            )
        } else {
            format!(
                "只返回一个 JSON 对象，字段必须为 schema_version、summary、purpose、outcome、key_points、action_items、tags、action。schema_version 必须为 1。除字段名和 action 结构中的固定枚举值外，summary、purpose、outcome、key_points、action_items、tags 的内容全部使用简体中文；参与方只能称为“主叫”或“被叫”，不要使用 caller、callee 或其他英文称呼；不要输出英文翻译，不要添加 Markdown 或其他文字。根据按时间排序的参与方转写内容分析整通电话，不要遗漏任一参与方。{action_instruction}"
            )
        };
        let mut body = json!({
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": serde_json::to_string(&transcript).map_err(|error| invalid_response(error.to_string()))?}
            ],
            "max_tokens": self.config.max_output_tokens,
            "temperature": self.config.temperature,
        });
        if self.config.structured_output_mode != StructuredOutputMode::PromptOnly {
            body["response_format"] = json!({"type": "json_object"});
        }
        let response = self
            .client
            .post(format!(
                "{}/chat/completions",
                self.config.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|error| transport_error(error.to_string()))?;
        let status = response.status();
        let value: Value = response
            .json()
            .await
            .map_err(|error| invalid_response(error.to_string()))?;
        if !status.is_success() {
            return Err(http_status(status.as_u16(), &value));
        }
        let request_id = value.get("id").and_then(Value::as_str).map(str::to_string);
        let content = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|item| item.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_response("missing assistant content".to_string()))?;
        let result = parse_structured_call_result(content)?;
        Ok(LlmOutput {
            request_id,
            result,
            input_tokens: value
                .pointer("/usage/prompt_tokens")
                .and_then(Value::as_u64),
            output_tokens: value
                .pointer("/usage/completion_tokens")
                .and_then(Value::as_u64),
        })
    }
}

fn parse_structured_call_result(content: &str) -> ProviderResult<StructuredCallResult> {
    if let Ok(result) = serde_json::from_str::<StructuredCallResult>(content) {
        return Ok(result);
    }
    let value: Value = serde_json::from_str(content)
        .map_err(|error| invalid_response(format!("structured result JSON: {error}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid_response("structured result must be a JSON object".to_string()))?;
    let strings = |key: &str| {
        object
            .get(key)
            .and_then(Value::as_array)
            .map(|items| items.iter().map(json_value_as_string).collect())
            .unwrap_or_default()
    };
    Ok(StructuredCallResult {
        schema_version: object
            .get("schema_version")
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32,
        summary: object
            .get("summary")
            .map(json_value_as_string)
            .unwrap_or_default(),
        purpose: object
            .get("purpose")
            .map(json_value_as_string)
            .unwrap_or_default(),
        outcome: object
            .get("outcome")
            .map(json_value_as_string)
            .unwrap_or_default(),
        key_points: strings("key_points"),
        action_items: strings("action_items"),
        tags: strings("tags"),
        action: object.get("action").cloned().and_then(parse_agent_action),
    })
}

fn parse_agent_action(mut value: Value) -> Option<ai_protocol::control::AgentAction> {
    if value.get("type").and_then(Value::as_str) == Some("transfer_to_extension")
        && let Some(number) = value.get("number").and_then(Value::as_u64)
        && let Some(object) = value.as_object_mut()
    {
        object.insert("number".to_string(), Value::String(number.to_string()));
    }
    serde_json::from_value(value).ok()
}

fn json_value_as_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| serde_json::to_string(value).unwrap_or_default())
}

#[cfg(test)]
mod structured_result_tests {
    use super::*;

    #[test]
    fn accepts_numeric_extension_number_from_compatible_llm() {
        let result = parse_structured_call_result(
            r#"{"schema_version":1,"summary":"转接中","purpose":"转接","outcome":"已请求","key_points":[],"action_items":[],"tags":[],"action":{"type":"transfer_to_extension","number":1002}}"#,
        )
        .unwrap();
        assert!(matches!(
            result.action,
            Some(ai_protocol::control::AgentAction::TransferToExtension { ref number })
                if number == "1002"
        ));
    }
}

pub struct LocalHttpAsrProvider {
    provider_id: ProviderId,
    config: LocalHttpAsrConfig,
    client: reqwest::Client,
}

pub struct LocalHttpTtsProvider {
    provider_id: ProviderId,
    config: LocalHttpTtsConfig,
    client: reqwest::Client,
}

pub struct ByteDanceTtsProvider {
    provider_id: ProviderId,
    config: ByteDanceTtsConfig,
    token: String,
    client: reqwest::Client,
}

impl ByteDanceTtsProvider {
    pub fn new(
        provider_id: ProviderId,
        config: ByteDanceTtsConfig,
        token: String,
    ) -> ProviderResult<Self> {
        config.validate().map_err(invalid_config)?;
        if token.trim().is_empty() {
            return Err(invalid_config("ByteDance access token is required"));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .build()
            .map_err(|error| transport_error(format!("build HTTP client: {error}")))?;
        Ok(Self {
            provider_id,
            config,
            token,
            client,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ByteDanceTtsResponse {
    data: Option<String>,
    code: Option<i64>,
    message: Option<String>,
}

#[async_trait]
impl TtsProvider for ByteDanceTtsProvider {
    fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    async fn synthesize(&self, request: TtsRequest) -> ProviderResult<TtsOutput> {
        if request.text.trim().is_empty() {
            return Err(invalid_request("TTS text is empty"));
        }
        let body = json!({
            "app": {
                "appid": self.config.app_id,
                "token": self.token,
                "cluster": self.config.cluster,
            },
            "user": {"uid": self.config.uid},
            "audio": {
                "voice_type": if request.voice.trim().is_empty() { &self.config.voice_type } else { &request.voice },
                "encoding": "pcm",
                "rate": self.config.sample_rate,
                "speed_ratio": 1.0,
                "volume_ratio": 1.0,
                "pitch_ratio": 1.0,
                "language": self.config.language,
            },
            "request": {
                "reqid": request.operation_id,
                "text": request.text,
                "text_type": "plain",
                "operation": "query",
                "with_frontend": "1",
                "frontend_type": "unitTson",
                "pure_english_opt": "1",
            },
        });
        let response = self
            .client
            .post(&self.config.endpoint)
            .header("Authorization", format!("Bearer;{}", self.token))
            .json(&body)
            .send()
            .await
            .map_err(|error| transport_error(error.to_string()))?;
        let status = response.status();
        let payload: ByteDanceTtsResponse = response
            .json()
            .await
            .map_err(|error| invalid_response(error.to_string()))?;
        let Some(audio_base64) = payload.data else {
            if status.as_u16() == 401 || payload.code == Some(3001) {
                return Err(ProviderError {
                    kind: ProviderErrorKind::Authentication,
                    code: "tts_authentication_failed",
                    message: "ByteDance TTS authentication failed".to_string(),
                    retry_after_ms: None,
                });
            }
            return Err(http_status(
                status.as_u16(),
                &json!({"code": payload.code, "message": payload.message}),
            ));
        };
        let pcm16_le = base64::engine::general_purpose::STANDARD
            .decode(audio_base64)
            .map_err(|error| invalid_response(format!("invalid ByteDance TTS audio: {error}")))?;
        if pcm16_le.is_empty() || pcm16_le.len() % 2 != 0 {
            return Err(invalid_response(
                "ByteDance TTS audio must be non-empty PCM16LE",
            ));
        }
        Ok(TtsOutput {
            pcm16_le,
            sample_rate: self.config.sample_rate,
        })
    }
}

impl LocalHttpTtsProvider {
    pub fn new(provider_id: ProviderId, config: LocalHttpTtsConfig) -> ProviderResult<Self> {
        config.validate().map_err(invalid_config)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .build()
            .map_err(|error| transport_error(format!("build HTTP client: {error}")))?;
        Ok(Self {
            provider_id,
            config,
            client,
        })
    }
}

#[derive(Debug, Deserialize)]
struct LocalTtsResponse {
    audio_base64: String,
    sample_rate: Option<u32>,
}

#[async_trait]
impl TtsProvider for LocalHttpTtsProvider {
    fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    async fn synthesize(&self, request: TtsRequest) -> ProviderResult<TtsOutput> {
        if request.text.trim().is_empty() {
            return Err(invalid_request("TTS text is empty"));
        }
        let response = self
            .client
            .post(format!(
                "{}/tts",
                self.config.base_url.trim_end_matches('/')
            ))
            .json(&serde_json::json!({"text": request.text, "voice": request.voice}))
            .send()
            .await
            .map_err(|error| transport_error(error.to_string()))?;
        let status = response.status();
        let payload: LocalTtsResponse = response
            .json()
            .await
            .map_err(|error| invalid_response(error.to_string()))?;
        if !status.is_success() {
            return Err(http_status(status.as_u16(), &serde_json::Value::Null));
        }
        let pcm16_le = base64::engine::general_purpose::STANDARD
            .decode(payload.audio_base64)
            .map_err(|error| invalid_response(format!("invalid TTS audio: {error}")))?;
        if pcm16_le.is_empty() || pcm16_le.len() % 2 != 0 {
            return Err(invalid_response("TTS audio must be non-empty PCM16LE"));
        }
        Ok(TtsOutput {
            pcm16_le,
            sample_rate: payload.sample_rate.unwrap_or(self.config.sample_rate),
        })
    }
}

impl LocalHttpAsrProvider {
    pub fn new(provider_id: ProviderId, config: LocalHttpAsrConfig) -> ProviderResult<Self> {
        config.validate().map_err(invalid_config)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .build()
            .map_err(|error| transport_error(format!("build HTTP client: {error}")))?;
        Ok(Self {
            provider_id,
            config,
            client,
        })
    }
}

#[derive(Debug, Deserialize)]
struct LocalAsrResponse {
    text: Option<String>,
    result: Option<Vec<LocalAsrSegment>>,
}

#[derive(Debug, Deserialize)]
struct LocalAsrSegment {
    text: Option<String>,
    timestamp: Option<Vec<Vec<u64>>>,
}

#[async_trait]
impl AsrProvider for LocalHttpAsrProvider {
    fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    async fn transcribe(&self, request: AsrRequest) -> ProviderResult<AsrOutput> {
        if request.streams.is_empty() {
            return Err(invalid_request("ASR request has no streams"));
        }

        // The local endpoint accepts one WAV per request. Send every captured
        // participant stream independently, then merge the relative timestamps
        // into one chronological transcript for the call.
        let mut segments = Vec::new();
        for stream in &request.streams {
            let wav = pcm_wav(stream);
            let form = Form::new().part(
                "file",
                Part::bytes(wav)
                    .file_name("capture.wav")
                    .mime_str("audio/wav")
                    .map_err(|error| invalid_config(error.to_string()))?,
            );
            let response = self
                .client
                .post(format!(
                    "{}/asr",
                    self.config.base_url.trim_end_matches('/')
                ))
                .multipart(form)
                .send()
                .await
                .map_err(|error| transport_error(error.to_string()))?;
            let status = response.status();
            let payload: LocalAsrResponse = response
                .json()
                .await
                .map_err(|error| invalid_response(error.to_string()))?;
            if !status.is_success() {
                return Err(http_status(status.as_u16(), &json!({"text": payload.text})));
            }
            let text = payload.text.unwrap_or_default();
            let stream_segments = payload
                .result
                .unwrap_or_default()
                .into_iter()
                .filter_map(|segment| {
                    let text = segment.text.filter(|value| !value.trim().is_empty())?;
                    let (start_ms, end_ms) = segment
                        .timestamp
                        .as_ref()
                        .and_then(|times| times.first())
                        .map(|range| {
                            (
                                range.first().copied().unwrap_or(0),
                                range.get(1).copied().unwrap_or(stream.duration_ms),
                            )
                        })
                        .unwrap_or((0, stream.duration_ms));
                    Some(TranscriptSegment {
                        participant_id: stream.participant_id.clone(),
                        start_ms,
                        end_ms,
                        text,
                        final_segment: true,
                    })
                })
                .collect::<Vec<_>>();
            if stream_segments.is_empty() && !text.trim().is_empty() {
                segments.push(TranscriptSegment {
                    participant_id: stream.participant_id.clone(),
                    start_ms: 0,
                    end_ms: stream.duration_ms,
                    text,
                    final_segment: true,
                });
            } else {
                segments.extend(stream_segments);
            }
        }
        segments.sort_by(|left, right| {
            left.start_ms
                .cmp(&right.start_ms)
                .then_with(|| left.end_ms.cmp(&right.end_ms))
                .then_with(|| {
                    left.participant_id
                        .as_str()
                        .cmp(right.participant_id.as_str())
                })
        });
        Ok(AsrOutput {
            request_id: Some(format!("local-asr:{}", request.operation_id)),
            segments,
        })
    }
}

fn pcm_wav(input: &AsrAudioInput) -> Vec<u8> {
    let pcm = match input.codec {
        ai_protocol::control::AudioCodec::Pcm16Le => input.payload.clone(),
        ai_protocol::control::AudioCodec::Pcmu => input
            .payload
            .iter()
            .map(|byte| ulaw_to_pcm(*byte))
            .flat_map(i16::to_le_bytes)
            .collect(),
        ai_protocol::control::AudioCodec::Pcma => input
            .payload
            .iter()
            .map(|byte| alaw_to_pcm(*byte))
            .flat_map(i16::to_le_bytes)
            .collect(),
    };
    let channels = input.channels.max(1) as u32;
    let sample_rate = input.sample_rate.max(8000);
    let byte_rate = sample_rate * channels * 2;
    let block_align = (channels * 2) as u16;
    let mut wav = Vec::with_capacity(44 + pcm.len());
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36u32.saturating_add(pcm.len() as u32)).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&(channels as u16).to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&block_align.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
    wav.extend_from_slice(&pcm);
    wav
}

fn ulaw_to_pcm(value: u8) -> i16 {
    let value = !value;
    let sign = value & 0x80;
    let exponent = ((value >> 4) & 7) as i16;
    let mantissa = (value & 0x0f) as i16;
    let sample = ((mantissa << 3) + 0x84) << exponent;
    if sign != 0 {
        0x84 - sample
    } else {
        sample - 0x84
    }
}
fn alaw_to_pcm(value: u8) -> i16 {
    let value = value ^ 0x55;
    let sign = value & 0x80;
    let exponent = ((value >> 4) & 7) as i16;
    let mantissa = (value & 0x0f) as i16;
    let sample = if exponent == 0 {
        (mantissa << 4) + 8
    } else {
        ((mantissa << 4) + 0x108) << (exponent - 1)
    };
    if sign != 0 { sample } else { -sample }
}
fn invalid_config(message: impl Into<String>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidConfiguration,
        code: "PROVIDER_CONFIG_INVALID",
        message: message.into(),
        retry_after_ms: None,
    }
}
fn invalid_request(message: impl Into<String>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        code: "PROVIDER_REQUEST_INVALID",
        message: message.into(),
        retry_after_ms: None,
    }
}
fn invalid_response(message: impl Into<String>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidResponse,
        code: "PROVIDER_RESPONSE_INVALID",
        message: message.into(),
        retry_after_ms: None,
    }
}
fn transport_error(message: impl Into<String>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Retryable,
        code: "PROVIDER_TRANSPORT_ERROR",
        message: message.into(),
        retry_after_ms: None,
    }
}
fn http_status(status: u16, _value: &Value) -> ProviderError {
    let kind = if status == 401 || status == 403 {
        ProviderErrorKind::Authentication
    } else if status == 429 {
        ProviderErrorKind::RateLimited
    } else if status >= 500 {
        ProviderErrorKind::Retryable
    } else {
        ProviderErrorKind::InvalidResponse
    };
    ProviderError {
        kind,
        code: "PROVIDER_HTTP_ERROR",
        message: format!("provider returned HTTP {status}"),
        retry_after_ms: None,
    }
}
