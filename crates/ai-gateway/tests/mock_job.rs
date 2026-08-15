use ai_gateway::{Gateway, GatewayConfig, GatewayProfileConfig};
use ai_protocol::control::{
    AiPipelineType, AiProfileSnapshot, AudioCodec, ControlMessage, EndAudioInput, JobRef, JobState,
    MediaDirection, Participant, ResultPersisted, StreamBinding, SubmitPostCallJob,
};
use ai_protocol::id::{
    ConversationId, JobId, OperationId, ParticipantId, ProfileId, ProviderId, StreamId, TenantId,
};
use ai_protocol::media::{MediaFrame, MediaFrameMetadata};
use ai_provider::{MockAsrProvider, MockLlmProvider, ProviderRegistry};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn completes_and_acknowledges_mock_job() {
    let directory = tempdir().unwrap();
    let mut config = GatewayConfig::with_data_dir(directory.path().to_path_buf());
    config.storage.disk_min_free_mb = 1;
    config.profiles = vec![GatewayProfileConfig {
        profile_id: "profile-1".to_string(),
        profile_version: 1,
        enabled: true,
        pipeline_type: AiPipelineType::PostCallAnalysis,
        asr_provider_id: Some("mock-asr".to_string()),
        llm_provider_id: Some("mock-llm".to_string()),
        tts_provider_id: None,
        capture: Default::default(),
    }];
    let mut providers = ProviderRegistry::default();
    providers
        .register_asr(Arc::new(MockAsrProvider::new(
            ProviderId::new("mock-asr").unwrap(),
        )))
        .unwrap();
    providers
        .register_llm(Arc::new(MockLlmProvider::new(
            ProviderId::new("mock-llm").unwrap(),
        )))
        .unwrap();
    let gateway = Gateway::open(config, Arc::new(providers), "test-worker".to_string()).unwrap();
    let mut events = gateway.subscribe();
    let request = request();
    let duplicate_request = request.clone();
    let job = request.job.clone();
    let (_, ready) = gateway.submit(request).unwrap();
    let ready = ready.unwrap();
    assert_eq!(ready.accepted_streams.len(), 1);

    for sequence in 0..10 {
        gateway.ingest_media(frame(&job, sequence)).unwrap();
    }
    gateway
        .end_audio(EndAudioInput {
            job: job.clone(),
            final_sequences: BTreeMap::from([(StreamId::new("stream-1").unwrap(), 9)]),
        })
        .unwrap();

    let completed = loop {
        let event = tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .unwrap()
            .unwrap();
        if let ControlMessage::JobCompleted(completed) = event {
            break completed;
        }
    };
    assert_eq!(completed.transcript.len(), 1);
    assert_eq!(gateway.status(&job).unwrap().state, JobState::Completed);
    let (accepted, ready) = gateway.submit(duplicate_request).unwrap();
    assert!(accepted.duplicate);
    assert!(ready.is_none());
    gateway
        .result_persisted(&ResultPersisted {
            job: job.clone(),
            result_version: completed.result_version,
        })
        .unwrap();
    assert_eq!(gateway.status(&job).unwrap().state, JobState::Persisted);
}

#[tokio::test]
async fn rejects_profile_snapshot_that_differs_from_catalog() {
    let directory = tempdir().unwrap();
    let mut config = GatewayConfig::with_data_dir(directory.path().to_path_buf());
    config.storage.disk_min_free_mb = 1;
    config.profiles = vec![GatewayProfileConfig {
        profile_id: "profile-1".to_string(),
        profile_version: 1,
        enabled: true,
        pipeline_type: AiPipelineType::PostCallAnalysis,
        asr_provider_id: Some("mock-asr".to_string()),
        llm_provider_id: Some("mock-llm".to_string()),
        tts_provider_id: None,
        capture: Default::default(),
    }];
    let mut providers = ProviderRegistry::default();
    providers
        .register_asr(Arc::new(MockAsrProvider::new(
            ProviderId::new("mock-asr").unwrap(),
        )))
        .unwrap();
    providers
        .register_llm(Arc::new(MockLlmProvider::new(
            ProviderId::new("mock-llm").unwrap(),
        )))
        .unwrap();
    let gateway = Gateway::open(config, Arc::new(providers), "test-worker".to_string()).unwrap();
    let mut request = request();
    request.profile.profile_version += 1;

    let error = gateway.submit(request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("snapshot does not match gateway catalog")
    );
}

fn request() -> SubmitPostCallJob {
    let participant_id = ParticipantId::new("participant-1").unwrap();
    SubmitPostCallJob {
        job: JobRef {
            job_id: JobId::new("job-1").unwrap(),
            tenant_id: TenantId::new("tenant-1").unwrap(),
            conversation_id: ConversationId::new("call-1").unwrap(),
            operation_id: OperationId::new("operation-1").unwrap(),
            generation: 1,
        },
        profile: AiProfileSnapshot {
            profile_id: ProfileId::new("profile-1").unwrap(),
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
            participant_id: participant_id.clone(),
            role: "caller".to_string(),
            display_number: Some("1000".to_string()),
        }],
        streams: vec![StreamBinding {
            stream_id: StreamId::new("stream-1").unwrap(),
            participant_id,
            direction: MediaDirection::FromParticipant,
            codec: AudioCodec::Pcmu,
            sample_rate: 8_000,
            channels: 1,
        }],
    }
}

fn frame(job: &JobRef, sequence: u64) -> MediaFrame {
    MediaFrame {
        metadata: MediaFrameMetadata {
            job_id: job.job_id.clone(),
            tenant_id: job.tenant_id.clone(),
            conversation_id: job.conversation_id.clone(),
            participant_id: ParticipantId::new("participant-1").unwrap(),
            stream_id: StreamId::new("stream-1").unwrap(),
            sequence,
            generation: job.generation,
            direction: MediaDirection::FromParticipant,
            codec: AudioCodec::Pcmu,
            sample_rate: 8_000,
            channels: 1,
            media_timestamp: sequence * 160,
            duration_ms: 20,
            end_of_stream: false,
        },
        payload: vec![0x7f; 160],
    }
}
