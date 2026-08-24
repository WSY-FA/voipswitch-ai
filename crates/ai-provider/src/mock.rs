use crate::{
    AsrOutput, AsrProvider, AsrRequest, LlmOutput, LlmProvider, LlmRequest, ProviderResult,
    TtsOutput, TtsProvider, TtsRequest,
};
use ai_protocol::control::{StructuredCallResult, TranscriptSegment};
use ai_protocol::id::ProviderId;
use async_trait::async_trait;

pub struct MockAsrProvider {
    provider_id: ProviderId,
}

impl MockAsrProvider {
    pub fn new(provider_id: ProviderId) -> Self {
        Self { provider_id }
    }
}

#[async_trait]
impl AsrProvider for MockAsrProvider {
    fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    async fn transcribe(&self, request: AsrRequest) -> ProviderResult<AsrOutput> {
        let segments = request
            .streams
            .iter()
            .map(|stream| TranscriptSegment {
                participant_id: stream.participant_id.clone(),
                start_ms: 0,
                end_ms: stream.duration_ms,
                text: format!(
                    "mock transcript for {} ({} bytes)",
                    stream.stream_id,
                    stream.payload.len()
                ),
                final_segment: true,
            })
            .collect();
        Ok(AsrOutput {
            request_id: Some(format!("mock-asr:{}", request.operation_id)),
            segments,
        })
    }
}

pub struct MockLlmProvider {
    provider_id: ProviderId,
}

pub struct MockTtsProvider {
    provider_id: ProviderId,
}

impl MockTtsProvider {
    pub fn new(provider_id: ProviderId) -> Self {
        Self { provider_id }
    }
}

#[async_trait]
impl TtsProvider for MockTtsProvider {
    fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    async fn synthesize(&self, request: TtsRequest) -> ProviderResult<TtsOutput> {
        let samples = request.text.chars().count().max(1) * 320;
        Ok(TtsOutput {
            pcm16_le: vec![0; samples * 2],
            sample_rate: 16_000,
        })
    }
}

impl MockLlmProvider {
    pub fn new(provider_id: ProviderId) -> Self {
        Self { provider_id }
    }
}

#[async_trait]
impl LlmProvider for MockLlmProvider {
    fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    async fn summarize(&self, request: LlmRequest) -> ProviderResult<LlmOutput> {
        Ok(LlmOutput {
            request_id: Some(format!("mock-llm:{}", request.operation_id)),
            result: StructuredCallResult {
                schema_version: 1,
                summary: format!("mock summary for {} segment(s)", request.transcript.len()),
                purpose: "mock-purpose".to_string(),
                outcome: "mock-outcome".to_string(),
                key_points: vec!["mock-key-point".to_string()],
                action_items: Vec::new(),
                tags: vec!["mock".to_string()],
                action: None,
            },
            input_tokens: None,
            output_tokens: None,
        })
    }
}
