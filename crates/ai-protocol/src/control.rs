use crate::PROTOCOL_VERSION;
use crate::id::{
    ConnectorInstanceId, ConversationId, JobId, MessageId, OperationId, ParticipantId, ProfileId,
    StreamId, TenantId,
};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlEnvelope {
    pub protocol_version: u16,
    pub message_id: MessageId,
    pub timestamp_ms: u64,
    #[serde(flatten)]
    pub message: ControlMessage,
}

impl ControlEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION {
            bail!(
                "unsupported protocol version {}, expected {}",
                self.protocol_version,
                PROTOCOL_VERSION
            );
        }
        self.message.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum ControlMessage {
    ConnectorHello(ConnectorHello),
    GatewayHello(GatewayHello),
    ProfileCatalogRequest(ProfileCatalogRequest),
    ProfileCatalogSnapshot(ProfileCatalogSnapshot),
    SubmitPostCallJob(SubmitPostCallJob),
    DurableAccepted(DurableAccepted),
    AudioInputReady(AudioInputReady),
    EndAudioInput(EndAudioInput),
    JobCompleted(JobCompleted),
    ResultPersisted(ResultPersisted),
    CancelJob(CancelJob),
    JobStatusRequest(JobStatusRequest),
    JobStatus(JobStatus),
    JobResultRequest(JobResultRequest),
    Error(ProtocolError),
}

impl ControlMessage {
    fn validate(&self) -> Result<()> {
        match self {
            Self::SubmitPostCallJob(request) => request.validate(),
            Self::ProfileCatalogSnapshot(snapshot) => snapshot.validate(),
            Self::EndAudioInput(request) if request.final_sequences.is_empty() => {
                bail!("end_audio_input requires at least one final sequence")
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorHello {
    pub connector_instance_id: ConnectorInstanceId,
    pub connector_kind: String,
    pub supported_versions: Vec<u16>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayHello {
    pub selected_version: u16,
    pub gateway_instance_id: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileCatalogRequest {
    pub known_catalog_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileCatalogSnapshot {
    pub catalog_version: u64,
    pub profiles: Vec<AiProfileProjection>,
}

impl ProfileCatalogSnapshot {
    pub fn validate(&self) -> Result<()> {
        if self.catalog_version == 0 {
            bail!("profile catalog version must be greater than zero");
        }
        let mut ids = BTreeSet::new();
        for projection in &self.profiles {
            projection.profile.validate()?;
            if !ids.insert(projection.profile.profile_id.clone()) {
                bail!("duplicate profile {}", projection.profile.profile_id);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiProfileProjection {
    pub profile: AiProfileSnapshot,
    pub enabled: bool,
    pub executable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmitPostCallJob {
    pub job: JobRef,
    pub profile: AiProfileSnapshot,
    pub participants: Vec<Participant>,
    pub streams: Vec<StreamBinding>,
}

impl SubmitPostCallJob {
    pub fn validate(&self) -> Result<()> {
        if self.job.generation == 0 {
            bail!("generation must be greater than zero");
        }
        if self.participants.is_empty() || self.streams.is_empty() {
            bail!("job requires participants and streams");
        }
        self.profile.validate()?;
        if self.profile.pipeline_type != AiPipelineType::PostCallAnalysis {
            bail!("post-call job requires a post_call_analysis profile");
        }
        for stream in &self.streams {
            if !self
                .participants
                .iter()
                .any(|participant| participant.participant_id == stream.participant_id)
            {
                bail!("stream {} references unknown participant", stream.stream_id);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AiPipelineType {
    Transcription,
    #[default]
    PostCallAnalysis,
    LlmTask,
    VoiceAgent,
}

impl AiPipelineType {
    pub fn requires_asr(self) -> bool {
        matches!(
            self,
            Self::Transcription | Self::PostCallAnalysis | Self::VoiceAgent
        )
    }

    pub fn requires_llm(self) -> bool {
        matches!(
            self,
            Self::PostCallAnalysis | Self::LlmTask | Self::VoiceAgent
        )
    }

    pub fn requires_tts(self) -> bool {
        self == Self::VoiceAgent
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRef {
    pub job_id: JobId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub operation_id: OperationId,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiProfileSnapshot {
    pub profile_id: ProfileId,
    pub profile_version: u64,
    #[serde(default)]
    pub pipeline_type: AiPipelineType,
    pub asr_provider_id: Option<String>,
    pub llm_provider_id: Option<String>,
    #[serde(default)]
    pub tts_provider_id: Option<String>,
    pub capture_complete_ratio: f64,
    pub capture_process_min_ratio: f64,
    pub capture_complete_max_gap_ms: u64,
    pub capture_process_max_gap_ms: u64,
}

impl AiProfileSnapshot {
    pub fn validate(&self) -> Result<()> {
        if self.profile_version == 0 {
            bail!("profile version must be greater than zero");
        }
        validate_provider_combination(
            self.pipeline_type,
            self.asr_provider_id.as_deref(),
            self.llm_provider_id.as_deref(),
            self.tts_provider_id.as_deref(),
        )?;
        if !(0.0..=1.0).contains(&self.capture_complete_ratio)
            || !(0.0..=1.0).contains(&self.capture_process_min_ratio)
            || self.capture_complete_ratio < self.capture_process_min_ratio
        {
            bail!("invalid capture ratio thresholds");
        }
        if self.capture_complete_max_gap_ms > self.capture_process_max_gap_ms {
            bail!("complete gap threshold must not exceed process threshold");
        }
        Ok(())
    }
}

fn validate_provider_combination(
    pipeline_type: AiPipelineType,
    asr_provider_id: Option<&str>,
    llm_provider_id: Option<&str>,
    tts_provider_id: Option<&str>,
) -> Result<()> {
    for (capability, required, value) in [
        ("ASR", pipeline_type.requires_asr(), asr_provider_id),
        ("LLM", pipeline_type.requires_llm(), llm_provider_id),
        ("TTS", pipeline_type.requires_tts(), tts_provider_id),
    ] {
        let present = value.is_some_and(|id| !id.trim().is_empty());
        if required != present {
            if required {
                bail!("{capability} provider is required for {pipeline_type:?}");
            }
            bail!("{capability} provider is not used by {pipeline_type:?}");
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    pub participant_id: ParticipantId,
    pub role: String,
    pub display_number: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBinding {
    pub stream_id: StreamId,
    pub participant_id: ParticipantId,
    pub direction: MediaDirection,
    pub codec: AudioCodec,
    pub sample_rate: u32,
    pub channels: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaDirection {
    FromParticipant,
    ToParticipant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioCodec {
    Pcma,
    Pcmu,
    Pcm16Le,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableAccepted {
    pub job: JobRef,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioInputReady {
    pub job: JobRef,
    pub accepted_streams: Vec<StreamId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndAudioInput {
    pub job: JobRef,
    pub final_sequences: BTreeMap<StreamId, u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobCompleted {
    pub job: JobRef,
    pub result_version: u64,
    pub capture_quality: CaptureQuality,
    pub transcript: Vec<TranscriptSegment>,
    pub result: StructuredCallResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureQuality {
    Complete,
    IncompleteProcessable,
    Insufficient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptSegment {
    pub participant_id: ParticipantId,
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    pub final_segment: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredCallResult {
    pub schema_version: u32,
    pub summary: String,
    pub purpose: String,
    pub outcome: String,
    pub key_points: Vec<String>,
    pub action_items: Vec<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultPersisted {
    pub job: JobRef,
    pub result_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelJob {
    pub job: JobRef,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStatusRequest {
    pub job: JobRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResultRequest {
    pub job: JobRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStatus {
    pub job: JobRef,
    pub state: JobState,
    pub analysis_version: u32,
    pub result_version: Option<u64>,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Capturing,
    Queued,
    RunningAsr,
    RunningLlm,
    Completed,
    Persisted,
    Cancelled,
    Failed,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub related_message_id: Option<MessageId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id<T: TryFrom<String, Error = anyhow::Error>>(value: &str) -> T {
        value.to_string().try_into().unwrap()
    }

    #[test]
    fn rejects_stream_for_unknown_participant() {
        let request = SubmitPostCallJob {
            job: JobRef {
                job_id: id("job-1"),
                tenant_id: id("tenant-1"),
                conversation_id: id("conversation-1"),
                operation_id: id("operation-1"),
                generation: 1,
            },
            profile: AiProfileSnapshot {
                profile_id: id("profile-1"),
                profile_version: 1,
                pipeline_type: AiPipelineType::PostCallAnalysis,
                asr_provider_id: Some("mock-asr".to_string()),
                llm_provider_id: Some("mock-llm".to_string()),
                tts_provider_id: None,
                capture_complete_ratio: 0.995,
                capture_process_min_ratio: 0.95,
                capture_complete_max_gap_ms: 200,
                capture_process_max_gap_ms: 5_000,
            },
            participants: vec![Participant {
                participant_id: id("participant-1"),
                role: "caller".to_string(),
                display_number: None,
            }],
            streams: vec![StreamBinding {
                stream_id: id("stream-1"),
                participant_id: id("participant-2"),
                direction: MediaDirection::FromParticipant,
                codec: AudioCodec::Pcmu,
                sample_rate: 8_000,
                channels: 1,
            }],
        };

        assert!(request.validate().is_err());
    }

    #[test]
    fn validates_provider_requirements_by_pipeline_type() {
        let base = AiProfileSnapshot {
            profile_id: id("profile-1"),
            profile_version: 1,
            pipeline_type: AiPipelineType::Transcription,
            asr_provider_id: Some("mock-asr".to_string()),
            llm_provider_id: None,
            tts_provider_id: None,
            capture_complete_ratio: 0.995,
            capture_process_min_ratio: 0.95,
            capture_complete_max_gap_ms: 200,
            capture_process_max_gap_ms: 5_000,
        };
        assert!(base.validate().is_ok());
        assert!(
            AiProfileSnapshot {
                pipeline_type: AiPipelineType::LlmTask,
                asr_provider_id: None,
                llm_provider_id: Some("mock-llm".to_string()),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
        assert!(
            AiProfileSnapshot {
                pipeline_type: AiPipelineType::VoiceAgent,
                asr_provider_id: Some("mock-asr".to_string()),
                llm_provider_id: Some("mock-llm".to_string()),
                tts_provider_id: Some("mock-tts".to_string()),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
        assert!(
            AiProfileSnapshot {
                pipeline_type: AiPipelineType::PostCallAnalysis,
                llm_provider_id: None,
                ..base
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn connector_hello_fixture_matches_wire_type() {
        let envelope: ControlEnvelope =
            serde_json::from_str(include_str!("../fixtures/connector_hello.v1.json")).unwrap();
        envelope.validate().unwrap();
        assert!(matches!(
            envelope.message,
            ControlMessage::ConnectorHello(_)
        ));
    }
}
