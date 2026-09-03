use crate::capture::CaptureStore;
use crate::catalog::{CatalogStore, GatewayCatalog, build_provider_registry};
use crate::completeness::CaptureManifest;
use crate::config::{
    CaptureThresholds, GatewayConfig, GatewayProfileConfig, ProviderUpsertRequest,
};
use crate::disk::{DiskAdmission, DiskAdmissionGuard, DiskUsage};
use crate::store::{JobStore, StoredJob};
use crate::voice_agent::VoiceAgentSession;
use ai_protocol::control::WelcomePrompt;
use ai_protocol::control::{
    ActionRequested, ActionResult, AgentAction, AiPipelineType, AiProfileProjection,
    AiProfileSnapshot, AsrFinal, AssistConversationReady, AssistSuggestion, AudioInputReady,
    CaptureQuality, ControlMessage, ConversationReady, ConversationStopped, DurableAccepted,
    EndAudioInput, JobCompleted, JobRef, JobState, JobStatus, ProfileCatalogSnapshot,
    ResultPersisted, StartAssistConversation, StartConversation, StopAssistConversation,
    StopConversation, SubmitPostCallJob, TtsState, TtsStateChanged,
};
use ai_protocol::id::{ConversationId, JobId, ProfileId};
use ai_protocol::media::{MediaFrame, MediaFrameMetadata};
use ai_protocol::time::unix_timestamp_ms;
use ai_provider::{
    AsrAudioInput, AsrRequest, LlmRequest, ProviderError, ProviderRegistry, ProviderResult,
    TtsRequest,
};
use anyhow::{Context, Result, bail};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tracing::{info, warn};

pub struct Gateway {
    config: GatewayConfig,
    catalog: Arc<CatalogStore>,
    store: Arc<JobStore>,
    capture: Arc<CaptureStore>,
    providers: RwLock<Arc<ProviderRegistry>>,
    ingest_lock: Mutex<()>,
    disk_admission: DiskAdmissionGuard,
    job_tx: mpsc::Sender<JobId>,
    events: broadcast::Sender<ControlMessage>,
    worker_instance_id: String,
    voice_sessions: Arc<Mutex<std::collections::BTreeMap<ConversationId, VoiceConversation>>>,
    assist_sessions: Arc<Mutex<std::collections::BTreeMap<ConversationId, AssistConversation>>>,
    assist_history: Arc<Mutex<BTreeMap<ConversationId, VecDeque<ControlMessage>>>>,
    assist_execution: Arc<tokio::sync::Mutex<()>>,
    media_events: broadcast::Sender<MediaFrame>,
}

/// Per-conversation state is intentionally in-memory.  The Core owns call lifetime and will
/// start a fresh generation after a connector restart; provider secrets and PBX state never
/// enter this structure.
struct VoiceConversation {
    session: VoiceAgentSession,
    profile: AiProfileSnapshot,
    participant: ai_protocol::control::Participant,
    input_stream: ai_protocol::control::StreamBinding,
    buffered_frames: Vec<MediaFrame>,
    speech_frames: u16,
    silent_frames: u16,
    barge_in_frames: u16,
    next_output_sequence: u64,
    next_action_sequence: u64,
    welcome_in_progress: bool,
}

struct VoiceTurn {
    conversation: JobRef,
    profile: AiProfileSnapshot,
    participant: ai_protocol::control::Participant,
    input_stream: ai_protocol::control::StreamBinding,
    frames: Vec<MediaFrame>,
    playback_generation: u64,
    first_timestamp: u64,
    output_sequence: u64,
}

struct AssistConversation {
    conversation: JobRef,
    profile: AiProfileSnapshot,
    streams:
        std::collections::BTreeMap<ai_protocol::id::StreamId, ai_protocol::control::StreamBinding>,
    buffered_frames: Vec<MediaFrame>,
    speech_frames: u16,
    silent_frames: u16,
    next_segment_id: u64,
    recent_transcript: Vec<ai_protocol::control::TranscriptSegment>,
}

struct AssistTurn {
    conversation: JobRef,
    profile: AiProfileSnapshot,
    stream: ai_protocol::control::StreamBinding,
    frames: Vec<MediaFrame>,
    segment_id: u64,
}

const VAD_MIN_SPEECH_FRAMES: u16 = 5;
const VAD_END_SILENCE_FRAMES: u16 = 10;
const BARGE_IN_MIN_SPEECH_FRAMES: u16 = 15;
const BARGE_IN_MIN_ENERGY: u32 = 1_000;
const VAD_MAX_SPEECH_FRAMES: usize = 250;
const VAD_MAX_BUFFERED_FRAMES: usize = 1_500;
const ASSIST_HISTORY_PER_CONVERSATION: usize = 128;
const ASSIST_HISTORY_CONVERSATIONS: usize = 256;

impl Gateway {
    pub fn open(
        config: GatewayConfig,
        providers: Arc<ProviderRegistry>,
        worker_instance_id: String,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        std::fs::create_dir_all(&config.data_dir).with_context(|| {
            format!(
                "create gateway data directory {}",
                config.data_dir.display()
            )
        })?;
        let store = Arc::new(JobStore::open(&config.data_dir.join("gateway.db"))?);
        let catalog = Arc::new(CatalogStore::open(
            &config.data_dir.join("gateway.db"),
            &config,
        )?);
        let capture = Arc::new(CaptureStore::new(config.data_dir.join("captures"))?);
        let (job_tx, job_rx) = mpsc::channel(config.worker_queue_capacity);
        let (events, _) = broadcast::channel(config.worker_queue_capacity);
        let (media_events, _) = broadcast::channel(config.worker_queue_capacity * 4);
        let gateway = Arc::new(Self {
            config,
            catalog,
            store,
            capture,
            providers: RwLock::new(providers),
            ingest_lock: Mutex::new(()),
            disk_admission: DiskAdmissionGuard::default(),
            job_tx,
            events,
            worker_instance_id,
            voice_sessions: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            assist_sessions: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            assist_history: Arc::new(Mutex::new(BTreeMap::new())),
            assist_execution: Arc::new(tokio::sync::Mutex::new(())),
            media_events,
        });
        tokio::spawn(Self::worker_dispatch(gateway.clone(), job_rx));
        tokio::spawn(Self::cleanup_loop(gateway.clone()));
        Ok(gateway)
    }

    pub fn open_configured(config: GatewayConfig, worker_instance_id: String) -> Result<Arc<Self>> {
        config.validate()?;
        std::fs::create_dir_all(&config.data_dir).with_context(|| {
            format!(
                "create gateway data directory {}",
                config.data_dir.display()
            )
        })?;
        let catalog = CatalogStore::open(&config.data_dir.join("gateway.db"), &config)?;
        let loaded_catalog = catalog.load()?;
        let providers = Arc::new(build_provider_registry(&catalog, &loaded_catalog)?);
        Self::open(config, providers, worker_instance_id)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ControlMessage> {
        self.events.subscribe()
    }

    /// Return the bounded replay window and a live receiver as one subscription operation.
    /// Assist publishers hold the same history lock while broadcasting, so events are not
    /// lost between the replay snapshot and the live stream.
    pub fn subscribe_assist(
        &self,
        conversation_id: &ConversationId,
    ) -> (Vec<ControlMessage>, broadcast::Receiver<ControlMessage>) {
        let history = self.assist_history.lock().unwrap();
        let receiver = self.events.subscribe();
        let replay = history
            .get(conversation_id)
            .map(|events| events.iter().cloned().collect())
            .unwrap_or_default();
        (replay, receiver)
    }

    /// TTS is sent only over the local media socket.  It is deliberately separate from the
    /// control broadcast so a slow RTP consumer cannot delay call/session events.
    pub fn subscribe_media(&self) -> broadcast::Receiver<MediaFrame> {
        self.media_events.subscribe()
    }

    pub fn start_conversation(&self, request: StartConversation) -> Result<ConversationReady> {
        request.validate()?;
        info!(conversation_id = %request.conversation.conversation_id, job_id = %request.conversation.job_id, profile_id = %request.profile.profile_id, "voice-agent conversation started");
        let mut sessions = self.voice_sessions.lock().unwrap();
        if sessions.contains_key(&request.conversation.conversation_id) {
            return Ok(ConversationReady {
                conversation: request.conversation,
                state: ai_protocol::control::ConversationState::Listening,
            });
        }
        let mut session = VoiceAgentSession::start(request.conversation.clone())?;
        session.ready()?;
        let ready = ConversationReady {
            conversation: request.conversation.clone(),
            state: session.state(),
        };
        let welcome_conversation = request.conversation.clone();
        let welcome_profile = request.profile.clone();
        let welcome_input_stream = request.input_stream.clone();
        let welcome_in_progress = request.welcome.is_some();
        sessions.insert(
            request.conversation.conversation_id.clone(),
            VoiceConversation {
                session,
                profile: request.profile.clone(),
                participant: request.participant,
                input_stream: request.input_stream.clone(),
                buffered_frames: Vec::new(),
                speech_frames: 0,
                silent_frames: 0,
                barge_in_frames: 0,
                next_output_sequence: 1,
                next_action_sequence: 1,
                welcome_in_progress,
            },
        );
        if let Some(welcome) = request.welcome {
            let conversation = welcome_conversation;
            let profile = welcome_profile;
            let input_stream = welcome_input_stream;
            let providers = self.providers.read().unwrap().clone();
            let events = self.events.clone();
            let media_events = self.media_events.clone();
            let sessions = self.voice_sessions.clone();
            let sessions_for_error = sessions.clone();
            let conversation_id = conversation.conversation_id.clone();
            tokio::spawn(async move {
                if let Err(error) = execute_welcome(
                    welcome,
                    conversation,
                    profile,
                    input_stream,
                    providers,
                    events.clone(),
                    media_events,
                    sessions,
                )
                .await
                {
                    warn!(error = %error, "voice-agent welcome prompt failed");
                    let mut sessions_guard = sessions_for_error.lock().unwrap();
                    if let Some(session) = sessions_guard.get_mut(&conversation_id) {
                        session.welcome_in_progress = false;
                        if session.session.state()
                            == ai_protocol::control::ConversationState::Speaking
                        {
                            let _ = session.session.ready();
                        }
                        let _ = events.send(ControlMessage::TtsStateChanged(TtsStateChanged {
                            conversation: session.session.conversation.clone(),
                            generation: session.session.playback_generation,
                            state: TtsState::Failed,
                            sample_rate: None,
                        }));
                    }
                }
            });
        }
        Ok(ready)
    }

    pub fn start_assist_conversation(
        &self,
        request: StartAssistConversation,
    ) -> Result<AssistConversationReady> {
        request.validate()?;
        let mut sessions = self.assist_sessions.lock().unwrap();
        sessions
            .entry(request.conversation.conversation_id.clone())
            .or_insert_with(|| AssistConversation {
                conversation: request.conversation.clone(),
                profile: request.profile.clone(),
                streams: request
                    .streams
                    .iter()
                    .cloned()
                    .map(|stream| (stream.stream_id.clone(), stream))
                    .collect(),
                buffered_frames: Vec::new(),
                speech_frames: 0,
                silent_frames: 0,
                next_segment_id: 1,
                recent_transcript: Vec::new(),
            });
        self.assist_history
            .lock()
            .unwrap()
            .entry(request.conversation.conversation_id.clone())
            .or_default();
        info!(conversation_id = %request.conversation.conversation_id, profile_id = %request.profile.profile_id, "realtime assist conversation started");
        Ok(AssistConversationReady {
            conversation: request.conversation,
        })
    }

    pub fn stop_assist_conversation(&self, request: StopAssistConversation) -> Result<()> {
        let pending = self
            .assist_sessions
            .lock()
            .unwrap()
            .remove(&request.conversation.conversation_id)
            .and_then(|session| {
                if session.speech_frames < VAD_MIN_SPEECH_FRAMES
                    || session.buffered_frames.is_empty()
                {
                    return None;
                }
                let stream_id = session.buffered_frames.first()?.metadata.stream_id.clone();
                let stream = session.streams.get(&stream_id)?.clone();
                Some(AssistTurn {
                    conversation: session.conversation,
                    profile: session.profile,
                    stream,
                    frames: session.buffered_frames,
                    segment_id: session.next_segment_id,
                })
            });
        if let Some(turn) = pending {
            self.spawn_assist_turn(turn);
        }
        info!(conversation_id = %request.conversation.conversation_id, reason = %request.reason, "realtime assist conversation stopped");
        Ok(())
    }

    pub fn stop_conversation(&self, request: StopConversation) -> Result<ConversationStopped> {
        let mut sessions = self.voice_sessions.lock().unwrap();
        if let Some(mut session) = sessions.remove(&request.conversation.conversation_id) {
            session.session.stop()?;
        }
        Ok(ConversationStopped {
            conversation: request.conversation,
            reason: request.reason,
        })
    }

    pub fn action_result(&self, result: &ActionResult) -> Result<()> {
        let mut sessions = self.voice_sessions.lock().unwrap();
        let Some(session) = sessions.get_mut(&result.conversation.conversation_id) else {
            return Ok(());
        };
        if session.session.conversation != result.conversation {
            bail!("voice-agent action result conversation does not match active session");
        }
        if !result.success
            && session.session.state() == ai_protocol::control::ConversationState::Thinking
        {
            session.session.resume_after_action_failure()?;
            session.speech_frames = 0;
            session.silent_frames = 0;
            session.barge_in_frames = 0;
            info!(
                conversation_id = %result.conversation.conversation_id,
                operation_id = %result.operation_id,
                code = %result.code,
                "voice-agent action failed; conversation resumed"
            );
        }
        Ok(())
    }

    pub async fn synthesize_tts(
        &self,
        request: ai_protocol::control::SynthesizeTts,
    ) -> Result<ai_protocol::control::TtsSynthesized> {
        let catalog = self.catalog.load()?;
        let profile = catalog
            .profiles
            .iter()
            .find(|profile| profile.profile_id == request.profile_id.as_str())
            .context("voice-agent profile not found")?;
        if profile.pipeline_type != AiPipelineType::VoiceAgent {
            bail!("profile is not a voice_agent profile");
        }
        let provider_id = profile
            .tts_provider_id
            .as_deref()
            .context("voice-agent TTS provider missing")?;
        let provider = self
            .providers
            .read()
            .unwrap()
            .tts(provider_id)
            .context("voice-agent TTS provider unavailable")?;
        let output = tokio::time::timeout(
            Duration::from_secs(75),
            provider.synthesize(TtsRequest {
                operation_id: format!("config-{}", request.request_id),
                text: request.text,
                voice: request.voice,
            }),
        )
        .await
        .context("TTS provider timed out")?
        .map_err(anyhow::Error::from)?;
        Ok(ai_protocol::control::TtsSynthesized {
            request_id: request.request_id,
            success: true,
            pcm16_le: output.pcm16_le,
            sample_rate: output.sample_rate,
            error: None,
        })
    }

    pub fn profile_catalog(&self) -> Result<ProfileCatalogSnapshot> {
        let catalog = self.catalog.load()?;
        let providers = self.providers.read().unwrap().clone();
        let profiles = catalog
            .profiles
            .iter()
            .map(|profile| {
                let snapshot = AiProfileSnapshot {
                    profile_id: ProfileId::new(profile.profile_id.clone())?,
                    profile_version: profile.profile_version,
                    pipeline_type: profile.pipeline_type,
                    asr_provider_id: profile.asr_provider_id.clone(),
                    llm_provider_id: profile.llm_provider_id.clone(),
                    tts_provider_id: profile.tts_provider_id.clone(),
                    capture_complete_ratio: f64::from(profile.capture.complete_ratio_ppm)
                        / 1_000_000.0,
                    capture_process_min_ratio: f64::from(profile.capture.process_min_ratio_ppm)
                        / 1_000_000.0,
                    capture_complete_max_gap_ms: profile.capture.complete_max_gap_ms,
                    capture_process_max_gap_ms: profile.capture.process_max_gap_ms,
                };
                let executable = match profile.pipeline_type {
                    AiPipelineType::RealtimeAssist => {
                        profile
                            .asr_provider_id
                            .as_deref()
                            .is_some_and(|id| providers.asr(id).is_some())
                            && profile
                                .llm_provider_id
                                .as_deref()
                                .is_some_and(|id| providers.llm(id).is_some())
                    }
                    AiPipelineType::PostCallAnalysis => {
                        profile
                            .asr_provider_id
                            .as_deref()
                            .is_some_and(|id| providers.asr(id).is_some())
                            && profile
                                .llm_provider_id
                                .as_deref()
                                .is_some_and(|id| providers.llm(id).is_some())
                    }
                    AiPipelineType::VoiceAgent => {
                        profile
                            .asr_provider_id
                            .as_deref()
                            .is_some_and(|id| providers.asr(id).is_some())
                            && profile
                                .llm_provider_id
                                .as_deref()
                                .is_some_and(|id| providers.llm(id).is_some())
                            && profile
                                .tts_provider_id
                                .as_deref()
                                .is_some_and(|id| providers.tts(id).is_some())
                    }
                    _ => false,
                };
                anyhow::Ok(AiProfileProjection {
                    profile: snapshot,
                    enabled: profile.enabled,
                    executable,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let snapshot = ProfileCatalogSnapshot {
            catalog_version: catalog.version,
            profiles,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn gateway_catalog(&self) -> Result<GatewayCatalog> {
        self.catalog.load()
    }

    pub fn upsert_provider(&self, provider: ProviderUpsertRequest) -> Result<GatewayCatalog> {
        let catalog = self.catalog.upsert_provider(provider)?;
        self.install_registry(&catalog)?;
        Ok(catalog)
    }

    pub fn upsert_profile(&self, profile: GatewayProfileConfig) -> Result<GatewayCatalog> {
        let catalog = self.catalog.upsert_profile(profile)?;
        self.install_registry(&catalog)?;
        Ok(catalog)
    }

    pub fn delete_provider(
        &self,
        provider_id: &str,
        expected_revision: u64,
    ) -> Result<GatewayCatalog> {
        let catalog = self
            .catalog
            .delete_provider(provider_id, expected_revision)?;
        self.install_registry(&catalog)?;
        Ok(catalog)
    }

    pub fn delete_profile(
        &self,
        profile_id: &str,
        expected_revision: u64,
    ) -> Result<GatewayCatalog> {
        let catalog = self.catalog.delete_profile(profile_id, expected_revision)?;
        self.install_registry(&catalog)?;
        Ok(catalog)
    }

    pub fn bootstrap_admin(&self, password: &str, created_at_ms: u64) -> Result<bool> {
        self.catalog.bootstrap_admin(password, created_at_ms)
    }

    pub fn authenticate_admin(&self, username: &str, password: &str) -> Result<bool> {
        self.catalog.authenticate_admin(username, password)
    }

    pub fn submit(
        &self,
        request: SubmitPostCallJob,
    ) -> Result<(DurableAccepted, Option<AudioInputReady>)> {
        request.validate()?;
        let projection = self
            .profile_catalog()?
            .profiles
            .into_iter()
            .find(|projection| projection.profile.profile_id == request.profile.profile_id)
            .with_context(|| format!("profile {} not found", request.profile.profile_id))?;
        if !projection.enabled || !projection.executable {
            bail!("profile {} is not executable", request.profile.profile_id);
        }
        if projection.profile != request.profile {
            bail!(
                "profile {} snapshot does not match gateway catalog version",
                request.profile.profile_id
            );
        }
        match self.disk_admission.evaluate(
            &DiskUsage::read(&self.config.data_dir)?,
            &self.config.storage,
        ) {
            DiskAdmission::Reject => bail!("AI_STORAGE_REJECT: storage watermark exceeded"),
            DiskAdmission::Warning => {
                warn!(job_id = %request.job.job_id, "AI storage warning watermark exceeded")
            }
            DiskAdmission::Accept => {}
        }
        self.capture
            .prepare(&request.job.job_id, &request.streams)?;
        let manifest = CaptureManifest::new(&request.streams);
        let now = unix_timestamp_ms();
        let deadline = now.saturating_add(
            self.config
                .execution
                .post_call_job_deadline_seconds
                .saturating_mul(1000),
        );
        let duplicate = self.store.submit(&request, &manifest, now, deadline)?;
        let accepts_media = self.store.load(&request.job.job_id)?.state == JobState::Capturing;
        let accepted_streams = request
            .streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect();
        Ok((
            DurableAccepted {
                job: request.job.clone(),
                duplicate,
            },
            accepts_media.then_some(AudioInputReady {
                job: request.job,
                accepted_streams,
            }),
        ))
    }

    pub fn ingest_media(&self, frame: MediaFrame) -> Result<()> {
        frame.validate()?;
        if frame.metadata.sequence <= 2 {
            info!(conversation_id = %frame.metadata.conversation_id, sequence = frame.metadata.sequence, direction = ?frame.metadata.direction, "voice-agent media frame received");
        }
        let (is_voice_conversation, turn) = self.ingest_voice_media(&frame)?;
        if let Some(turn) = turn {
            self.spawn_voice_turn(turn);
        }
        let (is_assist_conversation, assist_turn) = self.ingest_assist_media(&frame)?;
        if let Some(turn) = assist_turn {
            self.spawn_assist_turn(turn);
        }
        if is_voice_conversation || is_assist_conversation {
            return Ok(());
        }
        let _guard = self.ingest_lock.lock().unwrap();
        let mut stored = self.store.load(&frame.metadata.job_id)?;
        if stored.state != JobState::Capturing {
            bail!("job {} is not accepting media", frame.metadata.job_id);
        }
        validate_media_identity(&stored, &frame)?;
        if !stored.manifest.observe(&frame.metadata)? {
            return Ok(());
        }
        self.capture.append(&frame)?;
        self.store.update_manifest(
            &frame.metadata.job_id,
            &stored.manifest,
            unix_timestamp_ms(),
        )
    }

    fn ingest_assist_media(&self, frame: &MediaFrame) -> Result<(bool, Option<AssistTurn>)> {
        let mut sessions = self.assist_sessions.lock().unwrap();
        let Some(session) = sessions.get_mut(&frame.metadata.conversation_id) else {
            return Ok((false, None));
        };
        let Some(stream) = session.streams.get(&frame.metadata.stream_id).cloned() else {
            return Ok((true, None));
        };
        if frame.metadata.job_id != session.conversation.job_id
            || frame.metadata.tenant_id != session.conversation.tenant_id
            || frame.metadata.generation != session.conversation.generation
            || frame.metadata.participant_id != stream.participant_id
            || frame.metadata.direction != ai_protocol::control::MediaDirection::FromParticipant
        {
            bail!("assist media identity mismatch");
        }
        if frame_has_voice(frame) {
            session.speech_frames = session.speech_frames.saturating_add(1);
            session.silent_frames = 0;
        } else if session.speech_frames > 0 {
            session.silent_frames = session.silent_frames.saturating_add(1);
        }
        if session.speech_frames > 0 {
            session.buffered_frames.push(frame.clone());
        }
        let max_reached = session.buffered_frames.len() >= VAD_MAX_SPEECH_FRAMES;
        if session.speech_frames < VAD_MIN_SPEECH_FRAMES
            || (!max_reached && session.silent_frames < VAD_END_SILENCE_FRAMES)
        {
            return Ok((true, None));
        }
        let frames = std::mem::take(&mut session.buffered_frames);
        session.speech_frames = 0;
        session.silent_frames = 0;
        let segment_id = session.next_segment_id;
        session.next_segment_id = segment_id.saturating_add(1);
        Ok((
            true,
            Some(AssistTurn {
                conversation: session.conversation.clone(),
                profile: session.profile.clone(),
                stream,
                frames,
                segment_id,
            }),
        ))
    }

    fn spawn_assist_turn(&self, turn: AssistTurn) {
        let providers = self.providers.read().unwrap().clone();
        let events = self.events.clone();
        let sessions = self.assist_sessions.clone();
        let history = self.assist_history.clone();
        let execution = self.assist_execution.clone();
        tokio::spawn(async move {
            let _guard = execution.lock().await;
            if let Err(error) =
                execute_assist_turn(turn, providers, events, history, sessions).await
            {
                warn!(error = %error, "assist turn failed");
            }
        });
    }

    fn ingest_voice_media(&self, frame: &MediaFrame) -> Result<(bool, Option<VoiceTurn>)> {
        let mut sessions = self.voice_sessions.lock().unwrap();
        let Some(conversation) = sessions.get_mut(&frame.metadata.conversation_id) else {
            return Ok((false, None));
        };
        if frame.metadata.job_id != conversation.session.conversation.job_id
            || frame.metadata.tenant_id != conversation.session.conversation.tenant_id
            || frame.metadata.generation != conversation.session.conversation.generation
            || frame.metadata.direction != ai_protocol::control::MediaDirection::FromParticipant
            || frame.metadata.participant_id != conversation.input_stream.participant_id
            || frame.metadata.stream_id != conversation.input_stream.stream_id
        {
            bail!("voice conversation media identity does not match its input stream");
        }
        if conversation.session.state() == ai_protocol::control::ConversationState::Speaking {
            // Ordinary handset comfort noise and acoustic echo can pass the normal
            // VAD threshold. Only sustained, substantially stronger speech may
            // interrupt TTS; otherwise every response is chopped after a few
            // hundred milliseconds.
            if mean_abs_for_codec(frame.metadata.codec, &frame.payload) >= BARGE_IN_MIN_ENERGY {
                conversation.barge_in_frames = conversation.barge_in_frames.saturating_add(1);
            } else {
                conversation.barge_in_frames = 0;
            }
            if conversation.barge_in_frames >= BARGE_IN_MIN_SPEECH_FRAMES {
                conversation.barge_in_frames = 0;
                let generation = conversation.session.barge_in()?;
                info!(conversation_id = %frame.metadata.conversation_id, generation, "voice-agent barge-in interrupted TTS");
                let _ = self
                    .events
                    .send(ControlMessage::TtsStateChanged(TtsStateChanged {
                        conversation: conversation.session.conversation.clone(),
                        generation,
                        state: TtsState::Interrupted,
                        sample_rate: None,
                    }));
            }
        }
        if conversation.welcome_in_progress {
            return Ok((true, None));
        }
        if conversation.session.state() != ai_protocol::control::ConversationState::Listening {
            return Ok((true, None));
        }
        let voice = frame_has_voice(frame);
        if frame.metadata.sequence <= 5 || frame.metadata.sequence.is_multiple_of(25) {
            info!(
                conversation_id = %frame.metadata.conversation_id,
                sequence = frame.metadata.sequence,
                vad_energy = mean_abs_for_codec(frame.metadata.codec, &frame.payload),
                vad_voice = voice,
                speech_frames = conversation.speech_frames,
                silent_frames = conversation.silent_frames,
                "voice-agent VAD sample"
            );
        }
        if voice {
            conversation.speech_frames = conversation.speech_frames.saturating_add(1);
            conversation.silent_frames = 0;
        } else if conversation.speech_frames > 0 {
            conversation.silent_frames = conversation.silent_frames.saturating_add(1);
        }
        if conversation.speech_frames > 0 {
            conversation.buffered_frames.push(frame.clone());
            if conversation.buffered_frames.len() > VAD_MAX_BUFFERED_FRAMES {
                conversation.buffered_frames.remove(0);
            }
        }
        let max_speech_reached = conversation.buffered_frames.len() >= VAD_MAX_SPEECH_FRAMES;
        if conversation.speech_frames < VAD_MIN_SPEECH_FRAMES
            || (!max_speech_reached && conversation.silent_frames < VAD_END_SILENCE_FRAMES)
        {
            return Ok((true, None));
        }
        conversation.session.begin_thinking()?;
        let frames = std::mem::take(&mut conversation.buffered_frames);
        conversation.speech_frames = 0;
        conversation.silent_frames = 0;
        let first_timestamp = frames
            .first()
            .map_or(0, |item| item.metadata.media_timestamp);
        let playback_generation = conversation.session.playback_generation;
        let output_sequence = conversation.next_output_sequence;
        Ok((
            true,
            Some(VoiceTurn {
                conversation: conversation.session.conversation.clone(),
                profile: conversation.profile.clone(),
                participant: conversation.participant.clone(),
                input_stream: conversation.input_stream.clone(),
                frames,
                playback_generation,
                first_timestamp,
                output_sequence,
            }),
        ))
    }

    fn spawn_voice_turn(&self, turn: VoiceTurn) {
        let providers = self.providers.read().unwrap().clone();
        let events = self.events.clone();
        let media_events = self.media_events.clone();
        let sessions = self.voice_sessions.clone();
        tokio::spawn(async move {
            if let Err(error) =
                execute_voice_turn(turn, providers, events.clone(), media_events, sessions).await
            {
                warn!(error = %error, "voice-agent turn failed");
            }
        });
    }

    pub fn end_audio(&self, request: EndAudioInput) -> Result<()> {
        let _guard = self.ingest_lock.lock().unwrap();
        self.store
            .end_audio(&request.job, &request.final_sequences, unix_timestamp_ms())?;
        if let Err(error) = self.job_tx.try_send(request.job.job_id.clone()) {
            warn!(job_id = %request.job.job_id, error = %error, "job left for scanner pickup");
        }
        Ok(())
    }

    pub fn cancel(&self, job: &JobRef) -> Result<()> {
        self.store.cancel(job, unix_timestamp_ms())
    }

    pub fn status(&self, job: &JobRef) -> Result<JobStatus> {
        self.store.status(job)
    }

    pub fn completed_result(&self, job: &JobRef) -> Result<JobCompleted> {
        self.store.completed_result(job)
    }

    pub fn result_persisted(&self, message: &ResultPersisted) -> Result<()> {
        self.store
            .mark_persisted(&message.job, message.result_version, unix_timestamp_ms())
    }

    async fn worker_dispatch(gateway: Arc<Self>, mut job_rx: mpsc::Receiver<JobId>) {
        let semaphore = Arc::new(Semaphore::new(gateway.config.worker_count));
        let mut scan = tokio::time::interval(Duration::from_secs(1));
        loop {
            let jobs = tokio::select! {
                value = job_rx.recv() => match value {
                    Some(job_id) => vec![job_id],
                    None => break,
                },
                _ = scan.tick() => match gateway.store.pending_job_ids(unix_timestamp_ms(), 64) {
                    Ok(jobs) => jobs,
                    Err(error) => {
                        warn!(error = %error, "failed to scan pending AI jobs");
                        Vec::new()
                    }
                }
            };
            for job_id in jobs {
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => return,
                };
                let gateway = gateway.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = gateway.execute_job(job_id.clone()).await {
                        warn!(job_id = %job_id, error = %error, "AI job execution failed");
                        if let Ok(status) = gateway.store.load(&job_id)
                            && matches!(
                                status.state,
                                JobState::Queued | JobState::RunningAsr | JobState::RunningLlm
                            )
                        {
                            let _ = gateway.store.fail(
                                &job_id,
                                JobState::Failed,
                                "GATEWAY_EXECUTION_ERROR",
                                unix_timestamp_ms(),
                            );
                        }
                    }
                });
            }
        }
    }

    async fn cleanup_loop(gateway: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(error) = gateway.cleanup_once() {
                warn!(error = %error, "AI temporary asset cleanup failed");
            }
        }
    }

    fn cleanup_once(&self) -> Result<()> {
        let now = unix_timestamp_ms();
        let persisted_before =
            now.saturating_sub(hours_ms(self.config.storage.persisted_audio_grace_hours));
        let terminal_before = now.saturating_sub(hours_ms(
            self.config.storage.temporary_audio_retention_hours,
        ));
        for job_id in self
            .store
            .capture_purge_candidates(persisted_before, terminal_before, 128)?
        {
            self.capture.remove_job(&job_id)?;
            self.store.mark_capture_purged(&job_id)?;
            info!(job_id = %job_id, "AI temporary capture purged");
        }

        let result_before = now.saturating_sub(hours_ms(
            self.config.storage.unacknowledged_result_retention_hours,
        ));
        for job_id in self
            .store
            .expire_unacknowledged_results(result_before, now, 128)?
        {
            self.capture.remove_job(&job_id)?;
            self.store.mark_capture_purged(&job_id)?;
            warn!(job_id = %job_id, "unacknowledged AI result expired");
        }
        Ok(())
    }

    async fn execute_job(&self, job_id: JobId) -> Result<()> {
        let now = unix_timestamp_ms();
        let lease_ms = self
            .config
            .execution
            .claim_lease_seconds
            .saturating_mul(1000);
        if !self
            .store
            .claim(&job_id, &self.worker_instance_id, now, lease_ms)?
        {
            return Ok(());
        }
        let mut stored = self.store.load(&job_id)?;
        if now >= stored.deadline_at_ms {
            self.store
                .fail(&job_id, JobState::TimedOut, "JOB_DEADLINE_EXCEEDED", now)?;
            return Ok(());
        }
        let final_sequences = stored
            .final_sequences
            .clone()
            .context("queued job has no final sequences")?;
        stored.manifest = self
            .capture
            .rebuild_manifest(&job_id, &stored.request.streams)?;
        self.store
            .update_manifest(&job_id, &stored.manifest, unix_timestamp_ms())?;
        let evaluation = stored
            .manifest
            .evaluate(&final_sequences, &thresholds_for(&stored)?)?;
        if evaluation.quality == CaptureQuality::Insufficient {
            self.store.fail(
                &job_id,
                JobState::Failed,
                "CAPTURE_INSUFFICIENT",
                unix_timestamp_ms(),
            )?;
            return Ok(());
        }

        if stored.transcript.is_none()
            && stored.asr_attempts > self.config.execution.asr_max_retries
        {
            self.store.fail(
                &job_id,
                JobState::Failed,
                "ASR_RETRIES_EXHAUSTED",
                unix_timestamp_ms(),
            )?;
            return Ok(());
        }
        if stored.transcript.is_some()
            && stored.llm_attempts > self.config.execution.llm_max_retries
        {
            self.store.fail(
                &job_id,
                JobState::Failed,
                "LLM_RETRIES_EXHAUSTED",
                unix_timestamp_ms(),
            )?;
            return Ok(());
        }

        let transcript = match stored.transcript.clone() {
            Some(transcript) => transcript,
            None => self.execute_asr(&stored).await?,
        };
        let result = self.execute_llm(&stored, transcript.clone()).await?;
        let result_version =
            self.store
                .complete(&job_id, evaluation.quality, &result, unix_timestamp_ms())?;
        let completed = JobCompleted {
            job: stored.request.job,
            result_version,
            capture_quality: evaluation.quality,
            transcript,
            result,
        };
        let _ = self.events.send(ControlMessage::JobCompleted(completed));
        info!(job_id = %job_id, result_version, "AI job completed");
        Ok(())
    }

    async fn execute_asr(
        &self,
        stored: &StoredJob,
    ) -> Result<Vec<ai_protocol::control::TranscriptSegment>> {
        let providers = self.providers.read().unwrap().clone();
        let provider = providers
            .asr(
                stored
                    .request
                    .profile
                    .asr_provider_id
                    .as_deref()
                    .context("post-call profile has no ASR provider")?,
            )
            .with_context(|| {
                format!(
                    "ASR provider {:?} not found",
                    stored.request.profile.asr_provider_id
                )
            })?;
        let mut streams = Vec::with_capacity(stored.request.streams.len());
        for stream in &stored.request.streams {
            let stats = stored
                .manifest
                .streams
                .get(&stream.stream_id)
                .context("capture stream stats missing")?;
            streams.push(AsrAudioInput {
                stream_id: stream.stream_id.clone(),
                participant_id: stream.participant_id.clone(),
                duration_ms: stats.received_duration_ms,
                codec: stream.codec,
                sample_rate: stream.sample_rate,
                channels: stream.channels,
                payload: self
                    .capture
                    .read_payloads(&stored.request.job.job_id, &stream.stream_id)?,
            });
        }
        loop {
            let attempt = self
                .store
                .start_asr(&stored.request.job.job_id, unix_timestamp_ms())?;
            if attempt > self.config.execution.asr_max_retries.saturating_add(1) {
                self.store.fail(
                    &stored.request.job.job_id,
                    JobState::Failed,
                    "ASR_RETRIES_EXHAUSTED",
                    unix_timestamp_ms(),
                )?;
                bail!("ASR retries exhausted");
            }
            let request = AsrRequest {
                operation_id: format!("{}:asr:{attempt}", stored.request.job.operation_id),
                language: None,
                streams: streams.clone(),
            };
            match self
                .provider_call(
                    &stored.request.job.job_id,
                    stored.deadline_at_ms,
                    provider.transcribe(request),
                )
                .await?
            {
                Ok(output) => {
                    self.store.save_asr(
                        &stored.request.job.job_id,
                        &output.segments,
                        unix_timestamp_ms(),
                    )?;
                    return Ok(output.segments);
                }
                Err(error) => {
                    if !self.should_retry(&error, attempt, self.config.execution.asr_max_retries) {
                        self.fail_provider(&stored.request.job.job_id, &error)?;
                        return Err(error.into());
                    }
                    self.retry_delay(&error, attempt, stored.deadline_at_ms)
                        .await?;
                }
            }
        }
    }

    async fn execute_llm(
        &self,
        stored: &StoredJob,
        transcript: Vec<ai_protocol::control::TranscriptSegment>,
    ) -> Result<ai_protocol::control::StructuredCallResult> {
        let providers = self.providers.read().unwrap().clone();
        let provider = providers
            .llm(
                stored
                    .request
                    .profile
                    .llm_provider_id
                    .as_deref()
                    .context("post-call profile has no LLM provider")?,
            )
            .with_context(|| {
                format!(
                    "LLM provider {:?} not found",
                    stored.request.profile.llm_provider_id
                )
            })?;
        loop {
            let attempt = self
                .store
                .start_llm(&stored.request.job.job_id, unix_timestamp_ms())?;
            if attempt > self.config.execution.llm_max_retries.saturating_add(1) {
                self.store.fail(
                    &stored.request.job.job_id,
                    JobState::Failed,
                    "LLM_RETRIES_EXHAUSTED",
                    unix_timestamp_ms(),
                )?;
                bail!("LLM retries exhausted");
            }
            let request = LlmRequest {
                operation_id: format!("{}:llm:{attempt}", stored.request.job.operation_id),
                transcript: transcript.clone(),
                allow_actions: false,
            };
            match self
                .provider_call(
                    &stored.request.job.job_id,
                    stored.deadline_at_ms,
                    provider.summarize(request),
                )
                .await?
            {
                Ok(output) => {
                    if let Err(error) = validate_result(&output.result) {
                        self.store.fail(
                            &stored.request.job.job_id,
                            JobState::Failed,
                            "LLM_SCHEMA_INVALID",
                            unix_timestamp_ms(),
                        )?;
                        return Err(error);
                    }
                    return Ok(output.result);
                }
                Err(error) => {
                    if !self.should_retry(&error, attempt, self.config.execution.llm_max_retries) {
                        self.fail_provider(&stored.request.job.job_id, &error)?;
                        return Err(error.into());
                    }
                    self.retry_delay(&error, attempt, stored.deadline_at_ms)
                        .await?;
                }
            }
        }
    }

    async fn provider_call<F, T>(
        &self,
        job_id: &JobId,
        deadline_at_ms: u64,
        future: F,
    ) -> Result<ProviderResult<T>>
    where
        F: Future<Output = ProviderResult<T>>,
    {
        tokio::pin!(future);
        let lease_ms = self
            .config
            .execution
            .claim_lease_seconds
            .saturating_mul(1000);
        let mut renew = tokio::time::interval(Duration::from_secs(
            self.config.execution.claim_renew_interval_seconds,
        ));
        renew.tick().await;
        loop {
            let remaining = deadline_at_ms.saturating_sub(unix_timestamp_ms());
            if remaining == 0 {
                self.timeout_job(job_id)?;
                bail!("job deadline exceeded");
            }
            tokio::select! {
                response = &mut future => return Ok(response),
                _ = renew.tick() => self.store.renew_claim(
                    job_id,
                    &self.worker_instance_id,
                    unix_timestamp_ms(),
                    lease_ms,
                )?,
                _ = tokio::time::sleep(Duration::from_millis(remaining)) => {
                    self.timeout_job(job_id)?;
                    bail!("job deadline exceeded");
                }
            }
        }
    }

    fn should_retry(&self, error: &ProviderError, attempt: u32, max_retries: u32) -> bool {
        error.is_retryable() && attempt <= max_retries
    }

    async fn retry_delay(
        &self,
        error: &ProviderError,
        attempt: u32,
        deadline_at_ms: u64,
    ) -> Result<()> {
        let exponential = self
            .config
            .execution
            .retry_initial_delay_ms
            .saturating_mul(1_u64 << attempt.saturating_sub(1).min(16));
        let delay = error
            .retry_after_ms
            .unwrap_or(exponential)
            .min(self.config.execution.retry_max_delay_ms);
        if unix_timestamp_ms().saturating_add(delay) >= deadline_at_ms {
            bail!("retry delay would exceed job deadline");
        }
        tokio::time::sleep(Duration::from_millis(delay)).await;
        Ok(())
    }

    fn fail_provider(&self, job_id: &JobId, error: &ProviderError) -> Result<()> {
        self.store
            .fail(job_id, JobState::Failed, error.code, unix_timestamp_ms())
    }

    fn timeout_job(&self, job_id: &JobId) -> Result<()> {
        self.store.fail(
            job_id,
            JobState::TimedOut,
            "JOB_DEADLINE_EXCEEDED",
            unix_timestamp_ms(),
        )
    }

    fn install_registry(&self, catalog: &GatewayCatalog) -> Result<()> {
        let providers = Arc::new(build_provider_registry(&self.catalog, catalog)?);
        *self.providers.write().unwrap() = providers;
        Ok(())
    }
}

fn validate_media_identity(stored: &StoredJob, frame: &MediaFrame) -> Result<()> {
    let job = &stored.request.job;
    let metadata = &frame.metadata;
    if metadata.job_id != job.job_id
        || metadata.tenant_id != job.tenant_id
        || metadata.conversation_id != job.conversation_id
        || metadata.generation != job.generation
    {
        bail!("media frame identity does not match durable job");
    }
    Ok(())
}

fn frame_has_voice(frame: &MediaFrame) -> bool {
    // Handsets often send low-level comfort noise instead of the canonical G.711 silence byte.
    // Decode each frame and use mean absolute amplitude so VAD is stable across endpoints.
    mean_abs_for_codec(frame.metadata.codec, &frame.payload) >= 128
}

fn mean_abs_for_codec(codec: ai_protocol::control::AudioCodec, payload: &[u8]) -> u32 {
    let samples = match codec {
        ai_protocol::control::AudioCodec::Pcma => {
            return mean_abs(payload.iter().copied().map(alaw_to_pcm));
        }
        ai_protocol::control::AudioCodec::Pcmu => {
            return mean_abs(payload.iter().copied().map(ulaw_to_pcm));
        }
        ai_protocol::control::AudioCodec::Pcm16Le => payload
            .chunks_exact(2)
            .map(|sample| i16::from_le_bytes([sample[0], sample[1]])),
    };
    mean_abs(samples)
}

fn mean_abs(samples: impl Iterator<Item = i16>) -> u32 {
    let (sum, count) = samples.fold((0_u64, 0_u64), |(sum, count), sample| {
        (sum + i64::from(sample).unsigned_abs(), count + 1)
    });
    sum.checked_div(count).unwrap_or(0) as u32
}

fn ulaw_to_pcm(value: u8) -> i16 {
    let value = !value;
    let sign = value & 0x80;
    let exponent = i16::from((value >> 4) & 7);
    let mantissa = i16::from(value & 0x0f);
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
    let exponent = i16::from((value >> 4) & 7);
    let mantissa = i16::from(value & 0x0f);
    let sample = if exponent == 0 {
        (mantissa << 4) + 8
    } else {
        ((mantissa << 4) + 0x108) << (exponent - 1)
    };
    if sign != 0 { sample } else { -sample }
}

async fn execute_voice_turn(
    turn: VoiceTurn,
    providers: Arc<ProviderRegistry>,
    events: broadcast::Sender<ControlMessage>,
    media_events: broadcast::Sender<MediaFrame>,
    sessions: Arc<Mutex<std::collections::BTreeMap<ConversationId, VoiceConversation>>>,
) -> Result<()> {
    let asr_id = turn
        .profile
        .asr_provider_id
        .as_deref()
        .context("voice-agent ASR provider missing")?;
    let llm_id = turn
        .profile
        .llm_provider_id
        .as_deref()
        .context("voice-agent LLM provider missing")?;
    let tts_id = turn
        .profile
        .tts_provider_id
        .as_deref()
        .context("voice-agent TTS provider missing")?;
    let asr = providers
        .asr(asr_id)
        .context("voice-agent ASR provider unavailable")?;
    let llm = providers
        .llm(llm_id)
        .context("voice-agent LLM provider unavailable")?;
    let tts = providers
        .tts(tts_id)
        .context("voice-agent TTS provider unavailable")?;
    let duration_ms = turn
        .frames
        .iter()
        .map(|frame| u64::from(frame.metadata.duration_ms))
        .sum();
    let payload = turn
        .frames
        .iter()
        .flat_map(|frame| frame.payload.iter().copied())
        .collect();
    let transcript = asr
        .transcribe(AsrRequest {
            operation_id: format!(
                "{}-asr-{}",
                turn.conversation.operation_id, turn.playback_generation
            ),
            language: None,
            streams: vec![AsrAudioInput {
                stream_id: turn.input_stream.stream_id.clone(),
                participant_id: turn.participant.participant_id.clone(),
                duration_ms,
                codec: turn.input_stream.codec,
                sample_rate: turn.input_stream.sample_rate,
                channels: turn.input_stream.channels,
                payload,
            }],
        })
        .await
        .map_err(anyhow::Error::from)?;
    let transcript_text = transcript
        .segments
        .iter()
        .map(|segment| segment.text.as_str())
        .collect::<Vec<_>>();
    info!(
        conversation_id = %turn.conversation.conversation_id,
        generation = turn.playback_generation,
        segments = transcript.segments.len(),
        transcript = ?transcript_text,
        "voice-agent ASR completed"
    );
    let text = if transcript.segments.is_empty() {
        "抱歉，我没有听清，请您再说一遍。".to_string()
    } else {
        let llm_output = llm
            .summarize(LlmRequest {
                operation_id: format!(
                    "{}-llm-{}",
                    turn.conversation.operation_id, turn.playback_generation
                ),
                transcript: transcript.segments,
                allow_actions: true,
            })
            .await
            .map_err(anyhow::Error::from)?;
        info!(conversation_id = %turn.conversation.conversation_id, generation = turn.playback_generation, "voice-agent LLM completed");
        if let Some(action) = llm_output.result.action.clone() {
            let transfer_action = matches!(
                action,
                AgentAction::TransferToExtension { .. }
                    | AgentAction::TransferToBusinessTarget { .. }
            );
            let action_sequence = {
                let mut sessions = sessions.lock().unwrap();
                let session = sessions
                    .get_mut(&turn.conversation.conversation_id)
                    .context("voice conversation stopped before action request")?;
                if session.session.conversation != turn.conversation {
                    bail!("voice conversation identity mismatch before action request");
                }
                let sequence = session.next_action_sequence;
                session.next_action_sequence = sequence.saturating_add(1);
                sequence
            };
            let action_request = ActionRequested {
                conversation: turn.conversation.clone(),
                operation_id: ai_protocol::id::OperationId::new(format!(
                    "{}:action:{}",
                    turn.conversation.operation_id, action_sequence
                ))?,
                generation: turn.conversation.generation,
                action,
                deadline_at_ms: unix_timestamp_ms().saturating_add(30_000),
            };
            let _ = events.send(ControlMessage::ActionRequested(action_request));
            info!(conversation_id = %turn.conversation.conversation_id, generation = turn.playback_generation, "voice-agent action requested");
            if transfer_action {
                return Ok(());
            }
        }
        llm_output.result.summary
    };
    let text = if text.trim().is_empty() {
        warn!(
            conversation_id = %turn.conversation.conversation_id,
            generation = turn.playback_generation,
            "voice-agent LLM returned an empty response; using repeat prompt"
        );
        "抱歉，我没有听清，请您再说一遍。".to_string()
    } else {
        text
    };
    let audio = tts
        .synthesize(TtsRequest {
            operation_id: format!(
                "{}-tts-{}",
                turn.conversation.operation_id, turn.playback_generation
            ),
            text,
            voice: String::new(),
        })
        .await
        .map_err(anyhow::Error::from)?;
    info!(conversation_id = %turn.conversation.conversation_id, generation = turn.playback_generation, sample_rate = audio.sample_rate, pcm_bytes = audio.pcm16_le.len(), "voice-agent TTS completed");
    if audio.sample_rate != 16_000
        || audio.pcm16_le.is_empty()
        || !audio.pcm16_le.len().is_multiple_of(2)
    {
        bail!("voice-agent TTS must return non-empty PCM16LE at 16000 Hz");
    }
    let (generation, mut sequence) = {
        let mut sessions = sessions.lock().unwrap();
        let session = sessions
            .get_mut(&turn.conversation.conversation_id)
            .context("voice conversation stopped before TTS completed")?;
        if session.session.conversation != turn.conversation
            || session.session.state() != ai_protocol::control::ConversationState::Thinking
        {
            bail!("voice conversation generation is stale before TTS playback");
        }
        let generation = session.session.begin_speaking()?;
        let sequence = session.next_output_sequence.max(turn.output_sequence);
        session.next_output_sequence = sequence;
        (generation, sequence)
    };
    let _ = events.send(ControlMessage::TtsStateChanged(TtsStateChanged {
        conversation: turn.conversation.clone(),
        generation,
        state: TtsState::Started,
        sample_rate: Some(16_000),
    }));
    let output_stream =
        ai_protocol::id::StreamId::new(format!("{}-tts", turn.input_stream.stream_id.as_str()))?;
    let frame_bytes = 640; // 20 ms * 16 kHz * mono * PCM16LE
    // Pace against an absolute clock so IPC scheduling time does not accumulate and
    // stretch the audio. The Core RTP sender uses the same strategy.
    let mut next_frame_at = tokio::time::Instant::now();
    for (offset, payload) in audio.pcm16_le.chunks(frame_bytes).enumerate() {
        let still_speaking = {
            let sessions = sessions.lock().unwrap();
            sessions
                .get(&turn.conversation.conversation_id)
                .is_some_and(|session| {
                    session.session.playback_generation == generation
                        && session.session.state()
                            == ai_protocol::control::ConversationState::Speaking
                })
        };
        if !still_speaking {
            return Ok(());
        }
        tokio::time::sleep_until(next_frame_at).await;
        next_frame_at += Duration::from_millis(20);
        let frame = MediaFrame {
            metadata: MediaFrameMetadata {
                job_id: turn.conversation.job_id.clone(),
                tenant_id: turn.conversation.tenant_id.clone(),
                conversation_id: turn.conversation.conversation_id.clone(),
                participant_id: turn.participant.participant_id.clone(),
                stream_id: output_stream.clone(),
                sequence,
                generation,
                direction: ai_protocol::control::MediaDirection::ToParticipant,
                codec: ai_protocol::control::AudioCodec::Pcm16Le,
                sample_rate: 16_000,
                channels: 1,
                media_timestamp: turn.first_timestamp.saturating_add((offset as u64) * 320),
                duration_ms: 20,
                end_of_stream: false,
            },
            payload: payload.to_vec(),
        };
        if media_events.send(frame).is_err() {
            bail!("voice-agent media consumer is unavailable");
        }
        sequence = sequence.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let completed = {
        let mut sessions = sessions.lock().unwrap();
        let session = sessions
            .get_mut(&turn.conversation.conversation_id)
            .context("voice conversation stopped during TTS playback")?;
        if session.session.playback_generation != generation
            || session.session.state() != ai_protocol::control::ConversationState::Speaking
        {
            false
        } else {
            session.next_output_sequence = sequence;
            session.session.ready()?;
            true
        }
    };
    if completed {
        let _ = events.send(ControlMessage::TtsStateChanged(TtsStateChanged {
            conversation: turn.conversation,
            generation,
            state: TtsState::Stopped,
            sample_rate: Some(16_000),
        }));
    }
    Ok(())
}

async fn execute_assist_turn(
    turn: AssistTurn,
    providers: Arc<ProviderRegistry>,
    events: broadcast::Sender<ControlMessage>,
    history: Arc<Mutex<BTreeMap<ConversationId, VecDeque<ControlMessage>>>>,
    sessions: Arc<Mutex<std::collections::BTreeMap<ConversationId, AssistConversation>>>,
) -> Result<()> {
    let asr_id = turn
        .profile
        .asr_provider_id
        .as_deref()
        .context("assist ASR provider missing")?;
    let llm_id = turn
        .profile
        .llm_provider_id
        .as_deref()
        .context("assist LLM provider missing")?;
    let asr = providers
        .asr(asr_id)
        .context("assist ASR provider unavailable")?;
    let llm = providers
        .llm(llm_id)
        .context("assist LLM provider unavailable")?;
    let duration_ms = turn
        .frames
        .iter()
        .map(|frame| u64::from(frame.metadata.duration_ms))
        .sum();
    let payload = turn
        .frames
        .iter()
        .flat_map(|frame| frame.payload.iter().copied())
        .collect();
    let output = asr
        .transcribe(AsrRequest {
            operation_id: format!(
                "{}-assist-asr-{}",
                turn.conversation.operation_id, turn.segment_id
            ),
            language: None,
            streams: vec![AsrAudioInput {
                stream_id: turn.stream.stream_id.clone(),
                participant_id: turn.stream.participant_id.clone(),
                duration_ms,
                codec: turn.stream.codec,
                sample_rate: turn.stream.sample_rate,
                channels: turn.stream.channels,
                payload,
            }],
        })
        .await
        .map_err(anyhow::Error::from)?;
    info!(conversation_id = %turn.conversation.conversation_id, segment_id = turn.segment_id, segments = output.segments.len(), "realtime assist ASR completed");
    let mut final_segments = output.segments;
    if final_segments.is_empty() {
        return Ok(());
    }
    for segment in &mut final_segments {
        segment.final_segment = true;
    }
    let first = final_segments
        .first()
        .cloned()
        .context("missing ASR segment")?;
    publish_assist_event(
        &events,
        &history,
        ControlMessage::AsrPartial(ai_protocol::control::AsrPartial {
            conversation: turn.conversation.clone(),
            participant_id: first.participant_id.clone(),
            segment_id: turn.segment_id,
            text: first.text.clone(),
            start_ms: first.start_ms,
            end_ms: first.end_ms,
        }),
    );
    {
        let mut guard = sessions.lock().unwrap();
        if let Some(session) = guard.get_mut(&turn.conversation.conversation_id)
            && session.conversation == turn.conversation
        {
            session.recent_transcript.extend(final_segments.clone());
            if session.recent_transcript.len() > 20 {
                let drop = session.recent_transcript.len() - 20;
                session.recent_transcript.drain(0..drop);
            }
        }
    }
    publish_assist_event(
        &events,
        &history,
        ControlMessage::AsrFinal(AsrFinal {
            conversation: turn.conversation.clone(),
            participant_id: first.participant_id.clone(),
            segment_id: turn.segment_id,
            text: first.text.clone(),
            start_ms: first.start_ms,
            end_ms: first.end_ms,
        }),
    );
    let transcript = sessions
        .lock()
        .unwrap()
        .get(&turn.conversation.conversation_id)
        .filter(|session| session.conversation == turn.conversation)
        .map(|session| session.recent_transcript.clone())
        .unwrap_or_else(|| final_segments.clone());
    let suggestion = llm
        .summarize(LlmRequest {
            operation_id: format!(
                "{}-assist-llm-{}",
                turn.conversation.operation_id, turn.segment_id
            ),
            transcript,
            allow_actions: false,
        })
        .await
        .map_err(anyhow::Error::from)?;
    info!(conversation_id = %turn.conversation.conversation_id, segment_id = turn.segment_id, "realtime assist LLM completed");
    let text = if suggestion.result.summary.trim().is_empty() {
        first.text
    } else {
        suggestion.result.summary
    };
    publish_assist_event(
        &events,
        &history,
        ControlMessage::AssistSuggestion(AssistSuggestion {
            conversation: turn.conversation,
            suggestion_id: turn.segment_id,
            kind: "next_step".to_string(),
            text,
            confidence: None,
            source_segment_id: turn.segment_id,
        }),
    );
    Ok(())
}

fn publish_assist_event(
    events: &broadcast::Sender<ControlMessage>,
    history: &Arc<Mutex<BTreeMap<ConversationId, VecDeque<ControlMessage>>>>,
    message: ControlMessage,
) {
    let conversation_id = match &message {
        ControlMessage::AsrPartial(event) => event.conversation.conversation_id.clone(),
        ControlMessage::AsrFinal(event) => event.conversation.conversation_id.clone(),
        ControlMessage::AssistSuggestion(event) => event.conversation.conversation_id.clone(),
        _ => return,
    };
    let mut histories = history.lock().unwrap();
    if histories.len() >= ASSIST_HISTORY_CONVERSATIONS
        && !histories.contains_key(&conversation_id)
        && let Some(oldest) = histories.keys().next().cloned()
    {
        histories.remove(&oldest);
    }
    let entries = histories.entry(conversation_id).or_default();
    if entries.len() >= ASSIST_HISTORY_PER_CONVERSATION {
        entries.pop_front();
    }
    entries.push_back(message.clone());
    let _ = events.send(message);
}

#[allow(clippy::too_many_arguments)]
async fn execute_welcome(
    welcome: WelcomePrompt,
    conversation: JobRef,
    profile: AiProfileSnapshot,
    input_stream: ai_protocol::control::StreamBinding,
    providers: Arc<ProviderRegistry>,
    events: broadcast::Sender<ControlMessage>,
    media_events: broadcast::Sender<MediaFrame>,
    sessions: Arc<Mutex<std::collections::BTreeMap<ConversationId, VoiceConversation>>>,
) -> Result<()> {
    let pcm16_le = match welcome {
        WelcomePrompt::Text(text) => {
            let tts_id = profile
                .tts_provider_id
                .as_deref()
                .context("welcome text requires a TTS provider")?;
            let tts = providers
                .tts(tts_id)
                .context("welcome TTS provider unavailable")?;
            tts.synthesize(TtsRequest {
                operation_id: format!("{}-welcome", conversation.operation_id),
                text,
                voice: String::new(),
            })
            .await
            .map_err(anyhow::Error::from)?
            .pcm16_le
        }
        WelcomePrompt::PcmWav(bytes) => decode_pcm_wav(&bytes)?,
    };
    if pcm16_le.is_empty() || !pcm16_le.len().is_multiple_of(2) {
        bail!("welcome audio must contain non-empty PCM16LE");
    }
    let (generation, mut sequence) = {
        let mut sessions_guard = sessions.lock().unwrap();
        let session = sessions_guard
            .get_mut(&conversation.conversation_id)
            .context("welcome conversation is no longer active")?;
        if session.session.conversation != conversation {
            bail!("welcome conversation identity mismatch");
        }
        if session.session.state() == ai_protocol::control::ConversationState::Listening {
            session.session.begin_thinking()?;
        }
        let generation = session.session.begin_speaking()?;
        let sequence = session.next_output_sequence;
        (generation, sequence)
    };
    let _ = events.send(ControlMessage::TtsStateChanged(TtsStateChanged {
        conversation: conversation.clone(),
        generation,
        state: TtsState::Started,
        sample_rate: Some(16_000),
    }));
    let output_stream =
        ai_protocol::id::StreamId::new(format!("{}-welcome", input_stream.stream_id.as_str()))?;
    let mut next_at = tokio::time::Instant::now();
    for (offset, payload) in pcm16_le.chunks(640).enumerate() {
        let still_speaking = {
            let sessions_guard = sessions.lock().unwrap();
            sessions_guard
                .get(&conversation.conversation_id)
                .is_some_and(|session| {
                    session.session.playback_generation == generation
                        && session.session.state()
                            == ai_protocol::control::ConversationState::Speaking
                })
        };
        if !still_speaking {
            let mut sessions_guard = sessions.lock().unwrap();
            if let Some(session) = sessions_guard.get_mut(&conversation.conversation_id) {
                session.welcome_in_progress = false;
            }
            return Ok(());
        }
        tokio::time::sleep_until(next_at).await;
        let frame = MediaFrame {
            metadata: MediaFrameMetadata {
                job_id: conversation.job_id.clone(),
                tenant_id: conversation.tenant_id.clone(),
                conversation_id: conversation.conversation_id.clone(),
                participant_id: input_stream.participant_id.clone(),
                stream_id: output_stream.clone(),
                sequence,
                generation,
                direction: ai_protocol::control::MediaDirection::ToParticipant,
                codec: ai_protocol::control::AudioCodec::Pcm16Le,
                sample_rate: 16_000,
                channels: 1,
                media_timestamp: (offset as u64) * 320,
                duration_ms: 20,
                end_of_stream: false,
            },
            payload: payload.to_vec(),
        };
        if media_events.send(frame).is_err() {
            bail!("voice-agent welcome media consumer is unavailable");
        }
        sequence = sequence.saturating_add(1);
        next_at += Duration::from_millis(20);
    }
    let mut sessions_guard = sessions.lock().unwrap();
    if let Some(session) = sessions_guard.get_mut(&conversation.conversation_id)
        && session.session.playback_generation == generation
        && session.session.state() == ai_protocol::control::ConversationState::Speaking
    {
        session.next_output_sequence = sequence;
        session.session.ready()?;
        session.welcome_in_progress = false;
        let _ = events.send(ControlMessage::TtsStateChanged(TtsStateChanged {
            conversation,
            generation,
            state: TtsState::Stopped,
            sample_rate: Some(16_000),
        }));
    }
    Ok(())
}

#[allow(unused_assignments)]
fn decode_pcm_wav(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() < 44 || &bytes[..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        bail!("welcome audio must be a RIFF/WAVE file");
    }
    let mut offset = 12;
    let mut channels = None;
    let mut sample_rate = None;
    let mut bits = None;
    let mut data = None;
    while offset + 8 <= bytes.len() {
        let id = &bytes[offset..offset + 4];
        let len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        if offset + len > bytes.len() {
            bail!("welcome WAV chunk exceeds file size");
        }
        match id {
            b"fmt " if len >= 16 => {
                let format = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
                channels = Some(u16::from_le_bytes(
                    bytes[offset + 2..offset + 4].try_into().unwrap(),
                ));
                sample_rate = Some(u32::from_le_bytes(
                    bytes[offset + 4..offset + 8].try_into().unwrap(),
                ));
                bits = Some(u16::from_le_bytes(
                    bytes[offset + 14..offset + 16].try_into().unwrap(),
                ));
                if format != 1
                    || channels != Some(1)
                    || bits != Some(16)
                    || !matches!(sample_rate, Some(8_000 | 16_000))
                {
                    bail!("welcome WAV must be PCM16LE mono at 8000 or 16000 Hz");
                }
            }
            b"data" => data = Some(bytes[offset..offset + len].to_vec()),
            _ => {}
        }
        offset += len + (len % 2);
    }
    let pcm = data.context("welcome WAV data chunk is missing")?;
    if sample_rate == Some(8_000) {
        let mut upsampled = Vec::with_capacity(pcm.len() * 2);
        for sample in pcm.chunks_exact(2) {
            upsampled.extend_from_slice(sample);
            upsampled.extend_from_slice(sample);
        }
        Ok(upsampled)
    } else {
        Ok(pcm)
    }
}

fn thresholds_for(stored: &StoredJob) -> Result<CaptureThresholds> {
    let profile = &stored.request.profile;
    let thresholds = CaptureThresholds {
        complete_ratio_ppm: ratio_to_ppm(profile.capture_complete_ratio)?,
        process_min_ratio_ppm: ratio_to_ppm(profile.capture_process_min_ratio)?,
        complete_max_gap_ms: profile.capture_complete_max_gap_ms,
        process_max_gap_ms: profile.capture_process_max_gap_ms,
    };
    thresholds.validate()?;
    Ok(thresholds)
}

fn ratio_to_ppm(value: f64) -> Result<u32> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        bail!("capture ratio is outside 0..=1");
    }
    Ok((value * 1_000_000.0).round() as u32)
}

fn validate_result(result: &ai_protocol::control::StructuredCallResult) -> Result<()> {
    if result.schema_version != 1 {
        bail!("unsupported structured result schema version");
    }
    if result.summary.len() > 64 * 1024
        || result.purpose.len() > 8 * 1024
        || result.outcome.len() > 8 * 1024
        || result.key_points.len() > 100
        || result.action_items.len() > 100
        || result.tags.len() > 50
    {
        bail!("structured result exceeds local schema limits");
    }
    Ok(())
}

fn hours_ms(hours: u64) -> u64 {
    hours.saturating_mul(60 * 60 * 1000)
}

#[cfg(test)]
mod tests {
    use super::{
        ASSIST_HISTORY_PER_CONVERSATION, BARGE_IN_MIN_ENERGY, alaw_to_pcm, mean_abs_for_codec,
        publish_assist_event, ulaw_to_pcm,
    };
    use ai_protocol::control::JobRef;
    use ai_protocol::control::{AsrPartial, AudioCodec, ControlMessage};
    use ai_protocol::id::{ConversationId, JobId, OperationId, ParticipantId, TenantId};
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use tokio::sync::broadcast;

    #[test]
    fn vad_rejects_g711_silence_and_accepts_speech_energy() {
        assert!(mean_abs_for_codec(AudioCodec::Pcma, &[0xd5; 160]) < 128);
        assert!(mean_abs_for_codec(AudioCodec::Pcmu, &[0xff; 160]) < 128);
        assert!(mean_abs_for_codec(AudioCodec::Pcma, &[0x80; 160]) >= 128);
        assert!(mean_abs_for_codec(AudioCodec::Pcmu, &[0x00; 160]) >= 128);
    }

    #[test]
    fn g711_decoders_keep_silence_near_zero() {
        assert!(alaw_to_pcm(0xd5).unsigned_abs() <= 8);
        assert!(ulaw_to_pcm(0xff).unsigned_abs() <= 1);
    }

    #[test]
    fn barge_in_requires_stronger_energy_than_vad() {
        assert!(mean_abs_for_codec(AudioCodec::Pcma, &[0x80; 160]) >= BARGE_IN_MIN_ENERGY);
        assert!(mean_abs_for_codec(AudioCodec::Pcma, &[0xd5; 160]) < BARGE_IN_MIN_ENERGY);
    }

    #[test]
    fn assist_events_are_replayed_and_bounded() {
        let conversation_id = ConversationId::new("call-1").unwrap();
        let job = JobRef {
            job_id: JobId::new("job-1").unwrap(),
            tenant_id: TenantId::new("tenant-1").unwrap(),
            conversation_id: conversation_id.clone(),
            operation_id: OperationId::new("assist").unwrap(),
            generation: 1,
        };
        let participant_id = ParticipantId::new("caller").unwrap();
        let (events, _) = broadcast::channel(8);
        let history = Arc::new(Mutex::new(BTreeMap::<
            ConversationId,
            VecDeque<ControlMessage>,
        >::new()));
        for segment_id in 1..=(ASSIST_HISTORY_PER_CONVERSATION as u64 + 1) {
            publish_assist_event(
                &events,
                &history,
                ControlMessage::AsrPartial(AsrPartial {
                    conversation: job.clone(),
                    participant_id: participant_id.clone(),
                    segment_id,
                    text: segment_id.to_string(),
                    start_ms: 0,
                    end_ms: 20,
                }),
            );
        }
        let replay = history.lock().unwrap()[&conversation_id].clone();
        assert_eq!(replay.len(), ASSIST_HISTORY_PER_CONVERSATION);
        let ControlMessage::AsrPartial(first) = &replay[0] else {
            panic!("partial expected")
        };
        assert_eq!(first.segment_id, 2);
    }
}
