mod config;
mod mock;
mod registry;

pub use config::{
    OpenAiCompatibleLlmConfig, StructuredOutputMode, VolcengineApiVariant, VolcengineAsrConfig,
};
pub use mock::{MockAsrProvider, MockLlmProvider};
pub use registry::ProviderRegistry;

use ai_protocol::control::{StructuredCallResult, TranscriptSegment};
use ai_protocol::id::{ParticipantId, ProviderId, StreamId};
use async_trait::async_trait;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorKind {
    Retryable,
    RateLimited,
    InvalidConfiguration,
    Authentication,
    InvalidRequest,
    InvalidResponse,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub code: &'static str,
    pub message: String,
    pub retry_after_ms: Option<u64>,
}

impl ProviderError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.kind,
            ProviderErrorKind::Retryable | ProviderErrorKind::RateLimited
        )
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for ProviderError {}

pub type ProviderResult<T> = Result<T, ProviderError>;

#[derive(Debug, Clone)]
pub struct AsrAudioInput {
    pub stream_id: StreamId,
    pub participant_id: ParticipantId,
    pub duration_ms: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct AsrRequest {
    pub operation_id: String,
    pub language: Option<String>,
    pub streams: Vec<AsrAudioInput>,
}

#[derive(Debug, Clone)]
pub struct AsrOutput {
    pub request_id: Option<String>,
    pub segments: Vec<TranscriptSegment>,
}

#[derive(Debug, Clone)]
pub struct LlmRequest {
    pub operation_id: String,
    pub transcript: Vec<TranscriptSegment>,
}

#[derive(Debug, Clone)]
pub struct LlmOutput {
    pub request_id: Option<String>,
    pub result: StructuredCallResult,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TtsRequest {
    pub operation_id: String,
    pub text: String,
    pub voice: String,
}

#[derive(Debug, Clone)]
pub struct TtsOutput {
    pub pcm16_le: Vec<u8>,
    pub sample_rate: u32,
}

#[async_trait]
pub trait AsrProvider: Send + Sync {
    fn provider_id(&self) -> &ProviderId;
    async fn transcribe(&self, request: AsrRequest) -> ProviderResult<AsrOutput>;
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    fn provider_id(&self) -> &ProviderId;
    async fn summarize(&self, request: LlmRequest) -> ProviderResult<LlmOutput>;
}

#[async_trait]
pub trait TtsProvider: Send + Sync {
    fn provider_id(&self) -> &ProviderId;
    async fn synthesize(&self, request: TtsRequest) -> ProviderResult<TtsOutput>;
}
