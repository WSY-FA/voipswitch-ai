use crate::capture::CaptureStore;
use crate::catalog::{CatalogStore, GatewayCatalog, build_provider_registry};
use crate::completeness::CaptureManifest;
use crate::config::{
    CaptureThresholds, GatewayConfig, GatewayProfileConfig, ProviderUpsertRequest,
};
use crate::disk::{DiskAdmission, DiskAdmissionGuard, DiskUsage};
use crate::store::{JobStore, StoredJob};
use ai_protocol::control::{
    AiPipelineType, AiProfileProjection, AiProfileSnapshot, AudioInputReady, CaptureQuality,
    ControlMessage, DurableAccepted, EndAudioInput, JobCompleted, JobRef, JobState, JobStatus,
    ProfileCatalogSnapshot, ResultPersisted, SubmitPostCallJob,
};
use ai_protocol::id::{JobId, ProfileId};
use ai_protocol::media::MediaFrame;
use ai_protocol::time::unix_timestamp_ms;
use ai_provider::{
    AsrAudioInput, AsrRequest, LlmRequest, ProviderError, ProviderRegistry, ProviderResult,
};
use anyhow::{Context, Result, bail};
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
}

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
        let providers = Arc::new(build_provider_registry(&catalog.load()?)?);
        Self::open(config, providers, worker_instance_id)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ControlMessage> {
        self.events.subscribe()
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
                let executable = profile.pipeline_type == AiPipelineType::PostCallAnalysis
                    && profile
                        .asr_provider_id
                        .as_deref()
                        .is_some_and(|id| providers.asr(id).is_some())
                    && profile
                        .llm_provider_id
                        .as_deref()
                        .is_some_and(|id| providers.llm(id).is_some());
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
        let providers = Arc::new(build_provider_registry(catalog)?);
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
