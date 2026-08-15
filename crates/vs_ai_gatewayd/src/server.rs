use ai_gateway::Gateway;
use ai_protocol::PROTOCOL_VERSION;
use ai_protocol::control::{
    ConnectorHello, ControlEnvelope, ControlMessage, GatewayHello, JobResultRequest,
    JobStatusRequest, ProfileCatalogRequest, ProtocolError,
};
use ai_protocol::frame::{read_json_frame, write_json_frame};
use ai_protocol::id::{JobId, MessageId};
use ai_protocol::media::read_media_frame;
use ai_protocol::time::unix_timestamp_ms;
use anyhow::{Context, Result, bail};
use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

pub async fn run_control_socket(gateway: Arc<Gateway>, path: PathBuf) -> Result<()> {
    let listener = bind_listener(&path).await?;
    info!(socket = %path.display(), "AI control socket listening");
    let sequence = Arc::new(AtomicU64::new(1));
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("accept AI control client")?;
        let gateway = gateway.clone();
        let sequence = sequence.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_control_client(stream, gateway, sequence).await {
                debug!(error = %error, "AI control client disconnected");
            }
        });
    }
}

pub async fn run_media_socket(gateway: Arc<Gateway>, path: PathBuf) -> Result<()> {
    let listener = bind_listener(&path).await?;
    info!(socket = %path.display(), "AI media socket listening");
    loop {
        let (mut stream, _) = listener.accept().await.context("accept AI media client")?;
        if let Ok(credentials) = stream.peer_cred() {
            debug!(
                uid = credentials.uid(),
                gid = credentials.gid(),
                "AI media peer connected"
            );
        }
        let gateway = gateway.clone();
        tokio::spawn(async move {
            loop {
                let frame = match read_media_frame(&mut stream).await {
                    Ok(frame) => frame,
                    Err(error) => {
                        debug!(error = %error, "AI media client disconnected");
                        return;
                    }
                };
                if let Err(error) = gateway.ingest_media(frame) {
                    warn!(error = %error, "AI media frame rejected");
                    return;
                }
            }
        });
    }
}

async fn handle_control_client(
    stream: UnixStream,
    gateway: Arc<Gateway>,
    sequence: Arc<AtomicU64>,
) -> Result<()> {
    if let Ok(credentials) = stream.peer_cred() {
        debug!(
            uid = credentials.uid(),
            gid = credentials.gid(),
            "AI control peer connected"
        );
    }
    let (mut reader, mut writer) = stream.into_split();
    let mut events = gateway.subscribe();
    let mut handshake_complete = false;
    let mut owned_jobs = BTreeSet::<JobId>::new();
    loop {
        tokio::select! {
            request = read_json_frame::<_, ControlEnvelope>(&mut reader) => {
                let request = request?;
                let related = request.message_id.clone();
                let responses = match dispatch(
                    &gateway,
                    request,
                    &mut handshake_complete,
                    &mut owned_jobs,
                ) {
                    Ok(responses) => responses,
                    Err(error) => vec![ControlMessage::Error(ProtocolError {
                        code: "REQUEST_REJECTED".to_string(),
                        message: error.to_string(),
                        retryable: false,
                        related_message_id: Some(related),
                    })],
                };
                for response in responses {
                    write_json_frame(&mut writer, &envelope(response, &sequence)?).await?;
                }
            }
            event = events.recv(), if handshake_complete => match event {
                Ok(ControlMessage::JobCompleted(completed))
                    if owned_jobs.contains(&completed.job.job_id) => {
                        write_json_frame(
                            &mut writer,
                            &envelope(ControlMessage::JobCompleted(completed), &sequence)?,
                        ).await?;
                    }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!(count, "AI control client event queue lagged; client must query job result");
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
}

fn dispatch(
    gateway: &Gateway,
    envelope: ControlEnvelope,
    handshake_complete: &mut bool,
    owned_jobs: &mut BTreeSet<JobId>,
) -> Result<Vec<ControlMessage>> {
    envelope.validate()?;
    if !*handshake_complete {
        let ControlMessage::ConnectorHello(hello) = envelope.message else {
            bail!("connector_hello must be the first message");
        };
        validate_hello(&hello)?;
        *handshake_complete = true;
        return Ok(vec![ControlMessage::GatewayHello(GatewayHello {
            selected_version: PROTOCOL_VERSION,
            gateway_instance_id: "local".to_string(),
            capabilities: vec![
                "audio_input".to_string(),
                "post_call_job".to_string(),
                "durable_result".to_string(),
                "profile_catalog".to_string(),
            ],
        })]);
    }

    match envelope.message {
        ControlMessage::ProfileCatalogRequest(ProfileCatalogRequest { .. }) => {
            Ok(vec![ControlMessage::ProfileCatalogSnapshot(
                gateway.profile_catalog()?,
            )])
        }
        ControlMessage::SubmitPostCallJob(request) => {
            owned_jobs.insert(request.job.job_id.clone());
            let (accepted, ready) = gateway.submit(request)?;
            let mut responses = vec![ControlMessage::DurableAccepted(accepted)];
            if let Some(ready) = ready {
                responses.push(ControlMessage::AudioInputReady(ready));
            }
            Ok(responses)
        }
        ControlMessage::EndAudioInput(request) => {
            owned_jobs.insert(request.job.job_id.clone());
            let job = request.job.clone();
            gateway.end_audio(request)?;
            Ok(vec![ControlMessage::JobStatus(gateway.status(&job)?)])
        }
        ControlMessage::CancelJob(request) => {
            owned_jobs.insert(request.job.job_id.clone());
            gateway.cancel(&request.job)?;
            Ok(vec![ControlMessage::JobStatus(
                gateway.status(&request.job)?,
            )])
        }
        ControlMessage::JobStatusRequest(JobStatusRequest { job }) => {
            owned_jobs.insert(job.job_id.clone());
            Ok(vec![ControlMessage::JobStatus(gateway.status(&job)?)])
        }
        ControlMessage::JobResultRequest(JobResultRequest { job }) => {
            owned_jobs.insert(job.job_id.clone());
            Ok(vec![ControlMessage::JobCompleted(
                gateway.completed_result(&job)?,
            )])
        }
        ControlMessage::ResultPersisted(message) => {
            let job = message.job.clone();
            gateway.result_persisted(&message)?;
            Ok(vec![ControlMessage::JobStatus(gateway.status(&job)?)])
        }
        _ => bail!("message type is not accepted from a connector"),
    }
}

fn validate_hello(hello: &ConnectorHello) -> Result<()> {
    if !hello.supported_versions.contains(&PROTOCOL_VERSION) {
        bail!("connector does not support protocol version {PROTOCOL_VERSION}");
    }
    if hello.connector_kind.trim().is_empty() {
        bail!("connector_kind is required");
    }
    Ok(())
}

fn envelope(message: ControlMessage, sequence: &AtomicU64) -> Result<ControlEnvelope> {
    Ok(ControlEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: MessageId::new(format!(
            "gateway-{}",
            sequence.fetch_add(1, Ordering::Relaxed)
        ))?,
        timestamp_ms: unix_timestamp_ms(),
        message,
    })
}

async fn bind_listener(path: &Path) -> Result<UnixListener> {
    let mode = 0o660;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create socket directory {}", parent.display()))?;
    }
    if path.exists() {
        match UnixStream::connect(path).await {
            Ok(_) => bail!("socket {} is already in use", path.display()),
            Err(_) => tokio::fs::remove_file(path)
                .await
                .with_context(|| format!("remove stale socket {}", path.display()))?,
        }
    }
    let listener =
        UnixListener::bind(path).with_context(|| format!("bind socket {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("set socket permissions {}", path.display()))?;
    Ok(listener)
}
