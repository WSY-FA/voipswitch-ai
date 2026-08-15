use ai_gateway::{GatewayConfig, GatewayProfileConfig};
use ai_protocol::PROTOCOL_VERSION;
use ai_protocol::control::{
    AiPipelineType, AiProfileSnapshot, AudioCodec, ConnectorHello, ControlEnvelope, ControlMessage,
    EndAudioInput, JobRef, MediaDirection, Participant, ProfileCatalogRequest, ResultPersisted,
    StreamBinding, SubmitPostCallJob,
};
use ai_protocol::frame::{read_json_frame, write_json_frame};
use ai_protocol::id::{
    ConnectorInstanceId, ConversationId, JobId, MessageId, OperationId, ParticipantId, ProfileId,
    ProviderId, StreamId, TenantId,
};
use ai_protocol::media::{MediaFrame, MediaFrameMetadata, write_media_frame};
use ai_provider::{MockAsrProvider, MockLlmProvider, ProviderRegistry};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::UnixStream;
#[tokio::test]
async fn control_and_media_sockets_complete_mock_job() {
    let directory = tempdir().unwrap();
    let control_path = directory.path().join("control.sock");
    let media_path = directory.path().join("media.sock");
    let mut config = GatewayConfig::with_data_dir(directory.path().join("data"));
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
    let gateway =
        ai_gateway::Gateway::open(config, Arc::new(providers()), "socket-test".to_string())
            .unwrap();
    let control = tokio::spawn(vs_ai_gatewayd::server::run_control_socket(
        gateway.clone(),
        control_path.clone(),
    ));
    let media = tokio::spawn(vs_ai_gatewayd::server::run_media_socket(
        gateway,
        media_path.clone(),
    ));
    wait_for_socket(&control_path).await;
    wait_for_socket(&media_path).await;

    let mut control_client = UnixStream::connect(&control_path).await.unwrap();
    send(
        &mut control_client,
        1,
        ControlMessage::ConnectorHello(ConnectorHello {
            connector_instance_id: ConnectorInstanceId::new("test-connector").unwrap(),
            connector_kind: "test".to_string(),
            supported_versions: vec![PROTOCOL_VERSION],
            capabilities: vec!["audio_input".to_string()],
        }),
    )
    .await;
    assert!(matches!(
        recv(&mut control_client).await,
        ControlMessage::GatewayHello(_)
    ));
    send(
        &mut control_client,
        2,
        ControlMessage::ProfileCatalogRequest(ProfileCatalogRequest {
            known_catalog_version: None,
        }),
    )
    .await;
    let ControlMessage::ProfileCatalogSnapshot(catalog) = recv(&mut control_client).await else {
        panic!("profile catalog snapshot expected");
    };
    assert_eq!(catalog.catalog_version, 1);
    assert!(catalog.profiles[0].executable);

    let request = request();
    let job = request.job.clone();
    send(
        &mut control_client,
        3,
        ControlMessage::SubmitPostCallJob(request),
    )
    .await;
    assert!(matches!(
        recv(&mut control_client).await,
        ControlMessage::DurableAccepted(_)
    ));
    assert!(matches!(
        recv(&mut control_client).await,
        ControlMessage::AudioInputReady(_)
    ));

    let mut media_client = UnixStream::connect(&media_path).await.unwrap();
    for sequence in 0..10 {
        write_media_frame(&mut media_client, &frame(&job, sequence))
            .await
            .unwrap();
    }
    drop(media_client);
    tokio::time::sleep(Duration::from_millis(50)).await;

    send(
        &mut control_client,
        4,
        ControlMessage::EndAudioInput(EndAudioInput {
            job: job.clone(),
            final_sequences: BTreeMap::from([(StreamId::new("stream-1").unwrap(), 9)]),
        }),
    )
    .await;
    assert!(matches!(
        recv(&mut control_client).await,
        ControlMessage::JobStatus(_)
    ));
    let completed = loop {
        if let ControlMessage::JobCompleted(completed) = recv(&mut control_client).await {
            break completed;
        }
    };
    send(
        &mut control_client,
        5,
        ControlMessage::ResultPersisted(ResultPersisted {
            job,
            result_version: completed.result_version,
        }),
    )
    .await;
    assert!(matches!(
        recv(&mut control_client).await,
        ControlMessage::JobStatus(_)
    ));

    control.abort();
    media.abort();
}

async fn send(stream: &mut UnixStream, sequence: u64, message: ControlMessage) {
    write_json_frame(
        stream,
        &ControlEnvelope {
            protocol_version: PROTOCOL_VERSION,
            message_id: MessageId::new(format!("client-{sequence}")).unwrap(),
            timestamp_ms: 0,
            message,
        },
    )
    .await
    .unwrap();
}

async fn recv(stream: &mut UnixStream) -> ControlMessage {
    tokio::time::timeout(
        Duration::from_secs(3),
        read_json_frame::<_, ControlEnvelope>(stream),
    )
    .await
    .unwrap()
    .unwrap()
    .message
}

async fn wait_for_socket(path: &std::path::Path) {
    for _ in 0..100 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("socket {} was not created", path.display());
}

fn providers() -> ProviderRegistry {
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
    providers
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
