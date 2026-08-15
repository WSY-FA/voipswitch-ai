use crate::completeness::CaptureManifest;
use ai_protocol::control::{
    CaptureQuality, JobCompleted, JobRef, JobState, JobStatus, StructuredCallResult,
    SubmitPostCallJob, TranscriptSegment,
};
use ai_protocol::id::{JobId, StreamId};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

pub struct JobStore {
    connection: Mutex<Connection>,
}

#[derive(Debug, Clone)]
pub struct StoredJob {
    pub request: SubmitPostCallJob,
    pub state: JobState,
    pub analysis_version: u32,
    pub result_version: Option<u64>,
    pub manifest: CaptureManifest,
    pub final_sequences: Option<BTreeMap<StreamId, u64>>,
    pub transcript: Option<Vec<TranscriptSegment>>,
    pub result: Option<StructuredCallResult>,
    pub capture_quality: Option<CaptureQuality>,
    pub asr_attempts: u32,
    pub llm_attempts: u32,
    pub deadline_at_ms: u64,
    pub error_code: Option<String>,
}

impl JobStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create database directory {}", parent.display()))?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("open gateway database {}", path.display()))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS ai_jobs (
               job_id TEXT PRIMARY KEY,
               tenant_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               profile_id TEXT NOT NULL DEFAULT '',
               request_json TEXT NOT NULL,
               state TEXT NOT NULL,
               analysis_version INTEGER NOT NULL,
               result_version INTEGER,
               capture_manifest_json TEXT NOT NULL,
               final_sequences_json TEXT,
               transcript_json TEXT,
               result_json TEXT,
               capture_quality TEXT,
               asr_attempts INTEGER NOT NULL DEFAULT 0,
               llm_attempts INTEGER NOT NULL DEFAULT 0,
               deadline_at_ms INTEGER NOT NULL,
               claim_owner TEXT,
               claim_until_ms INTEGER,
               error_code TEXT,
               result_persisted INTEGER NOT NULL DEFAULT 0,
               capture_purged INTEGER NOT NULL DEFAULT 0,
               created_at_ms INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS ai_jobs_state_claim
               ON ai_jobs(state, claim_until_ms, updated_at_ms);
             CREATE INDEX IF NOT EXISTS ai_jobs_tenant_conversation
               ON ai_jobs(tenant_id, conversation_id, created_at_ms);",
        )?;
        ensure_column(
            &connection,
            "ai_jobs",
            "profile_id",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        connection.execute_batch(
            "CREATE INDEX IF NOT EXISTS ai_jobs_profile_state
               ON ai_jobs(profile_id, state);",
        )?;
        backfill_profile_ids(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn submit(
        &self,
        request: &SubmitPostCallJob,
        manifest: &CaptureManifest,
        now_ms: u64,
        deadline_at_ms: u64,
    ) -> Result<bool> {
        request.validate()?;
        let request_json = serde_json::to_string(request)?;
        let manifest_json = serde_json::to_string(manifest)?;
        let connection = self.connection.lock().unwrap();
        let inserted = connection.execute(
            "INSERT OR IGNORE INTO ai_jobs
             (job_id, tenant_id, conversation_id, profile_id, request_json, state,
              analysis_version, capture_manifest_json, deadline_at_ms, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, 'capturing', 1, ?6, ?7, ?8, ?8)",
            params![
                request.job.job_id.as_str(),
                request.job.tenant_id.as_str(),
                request.job.conversation_id.as_str(),
                request.profile.profile_id.as_str(),
                request_json,
                manifest_json,
                as_i64(deadline_at_ms)?,
                as_i64(now_ms)?,
            ],
        )?;
        if inserted == 1 {
            return Ok(false);
        }
        let existing: String = connection.query_row(
            "SELECT request_json FROM ai_jobs WHERE job_id = ?1",
            [request.job.job_id.as_str()],
            |row| row.get(0),
        )?;
        if existing != request_json {
            bail!(
                "job_id {} conflicts with another request",
                request.job.job_id
            );
        }
        Ok(true)
    }

    pub fn update_manifest(
        &self,
        job_id: &JobId,
        manifest: &CaptureManifest,
        now_ms: u64,
    ) -> Result<()> {
        let manifest = serde_json::to_string(manifest)?;
        let connection = self.connection.lock().unwrap();
        let updated = connection.execute(
            "UPDATE ai_jobs SET capture_manifest_json = ?2, updated_at_ms = ?3
             WHERE job_id = ?1 AND state NOT IN
               ('completed', 'persisted', 'cancelled', 'failed', 'timed_out')",
            params![job_id.as_str(), manifest, as_i64(now_ms)?],
        )?;
        if updated != 1 {
            bail!("job {job_id} no longer accepts manifest updates");
        }
        Ok(())
    }

    pub fn end_audio(
        &self,
        job: &JobRef,
        final_sequences: &BTreeMap<StreamId, u64>,
        now_ms: u64,
    ) -> Result<()> {
        self.ensure_job_ref(job)?;
        let final_sequences_json = serde_json::to_string(final_sequences)?;
        let connection = self.connection.lock().unwrap();
        let current: (String, Option<String>) = connection.query_row(
            "SELECT state, final_sequences_json FROM ai_jobs WHERE job_id = ?1",
            [job.job_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if current.0 == "capturing" {
            connection.execute(
                "UPDATE ai_jobs SET state = 'queued', final_sequences_json = ?2,
                 updated_at_ms = ?3 WHERE job_id = ?1",
                params![job.job_id.as_str(), final_sequences_json, as_i64(now_ms)?],
            )?;
            return Ok(());
        }
        if current.1.as_deref() == Some(final_sequences_json.as_str()) {
            return Ok(());
        }
        bail!(
            "job {} already ended with different final sequences",
            job.job_id
        )
    }

    pub fn pending_job_ids(&self, now_ms: u64, limit: usize) -> Result<Vec<JobId>> {
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT job_id FROM ai_jobs
             WHERE state IN ('queued', 'running_asr', 'running_llm')
               AND (claim_until_ms IS NULL OR claim_until_ms < ?1)
             ORDER BY updated_at_ms LIMIT ?2",
        )?;
        let rows = statement.query_map(params![as_i64(now_ms)?, limit as i64], |row| {
            row.get::<_, String>(0)
        })?;
        rows.map(|row| JobId::new(row?).context("invalid stored job id"))
            .collect()
    }

    pub fn claim(&self, job_id: &JobId, owner: &str, now_ms: u64, lease_ms: u64) -> Result<bool> {
        let connection = self.connection.lock().unwrap();
        let updated = connection.execute(
            "UPDATE ai_jobs SET claim_owner = ?2, claim_until_ms = ?3, updated_at_ms = ?4
             WHERE job_id = ?1
               AND state IN ('queued', 'running_asr', 'running_llm')
               AND (claim_until_ms IS NULL OR claim_until_ms < ?4)",
            params![
                job_id.as_str(),
                owner,
                as_i64(now_ms.saturating_add(lease_ms))?,
                as_i64(now_ms)?,
            ],
        )?;
        Ok(updated == 1)
    }

    pub fn renew_claim(
        &self,
        job_id: &JobId,
        owner: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<()> {
        let connection = self.connection.lock().unwrap();
        let updated = connection.execute(
            "UPDATE ai_jobs SET claim_until_ms = ?3, updated_at_ms = ?4
             WHERE job_id = ?1 AND claim_owner = ?2
               AND state IN ('queued', 'running_asr', 'running_llm')",
            params![
                job_id.as_str(),
                owner,
                as_i64(now_ms.saturating_add(lease_ms))?,
                as_i64(now_ms)?,
            ],
        )?;
        if updated != 1 {
            bail!("claim for job {job_id} is no longer owned by {owner}");
        }
        Ok(())
    }

    pub fn start_asr(&self, job_id: &JobId, now_ms: u64) -> Result<u32> {
        self.increment_attempt(job_id, "asr_attempts", "running_asr", now_ms)
    }

    pub fn save_asr(
        &self,
        job_id: &JobId,
        transcript: &[TranscriptSegment],
        now_ms: u64,
    ) -> Result<()> {
        let connection = self.connection.lock().unwrap();
        let updated = connection.execute(
            "UPDATE ai_jobs SET transcript_json = ?2, state = 'running_llm',
             updated_at_ms = ?3 WHERE job_id = ?1 AND state = 'running_asr'",
            params![
                job_id.as_str(),
                serde_json::to_string(transcript)?,
                as_i64(now_ms)?,
            ],
        )?;
        if updated != 1 {
            bail!("job {job_id} no longer accepts ASR checkpoint");
        }
        Ok(())
    }

    pub fn start_llm(&self, job_id: &JobId, now_ms: u64) -> Result<u32> {
        self.increment_attempt(job_id, "llm_attempts", "running_llm", now_ms)
    }

    pub fn complete(
        &self,
        job_id: &JobId,
        quality: CaptureQuality,
        result: &StructuredCallResult,
        now_ms: u64,
    ) -> Result<u64> {
        let connection = self.connection.lock().unwrap();
        let analysis_version: u32 = connection.query_row(
            "SELECT analysis_version FROM ai_jobs WHERE job_id = ?1",
            [job_id.as_str()],
            |row| row.get(0),
        )?;
        let result_version = u64::from(analysis_version);
        let updated = connection.execute(
            "UPDATE ai_jobs SET state = 'completed', result_version = ?2,
             capture_quality = ?3, result_json = ?4, claim_owner = NULL,
             claim_until_ms = NULL, updated_at_ms = ?5
             WHERE job_id = ?1 AND state = 'running_llm'",
            params![
                job_id.as_str(),
                as_i64(result_version)?,
                quality_str(quality),
                serde_json::to_string(result)?,
                as_i64(now_ms)?,
            ],
        )?;
        if updated != 1 {
            bail!("job {job_id} no longer accepts completed result");
        }
        Ok(result_version)
    }

    pub fn fail(&self, job_id: &JobId, state: JobState, code: &str, now_ms: u64) -> Result<()> {
        if !matches!(state, JobState::Failed | JobState::TimedOut) {
            bail!("invalid failure state");
        }
        let connection = self.connection.lock().unwrap();
        let updated = connection.execute(
            "UPDATE ai_jobs SET state = ?2, error_code = ?3, claim_owner = NULL,
             claim_until_ms = NULL, updated_at_ms = ?4 WHERE job_id = ?1
             AND state IN ('queued', 'running_asr', 'running_llm')",
            params![job_id.as_str(), state_str(state), code, as_i64(now_ms)?],
        )?;
        if updated == 0 {
            let state: String = connection.query_row(
                "SELECT state FROM ai_jobs WHERE job_id = ?1",
                [job_id.as_str()],
                |row| row.get(0),
            )?;
            if !matches!(
                state.as_str(),
                "completed" | "persisted" | "cancelled" | "failed" | "timed_out"
            ) {
                bail!("job {job_id} cannot transition to failure from {state}");
            }
        }
        Ok(())
    }

    pub fn cancel(&self, job: &JobRef, now_ms: u64) -> Result<()> {
        self.ensure_job_ref(job)?;
        let connection = self.connection.lock().unwrap();
        connection.execute(
            "UPDATE ai_jobs SET state = 'cancelled', claim_owner = NULL,
             claim_until_ms = NULL, updated_at_ms = ?2
             WHERE job_id = ?1
               AND state NOT IN ('completed', 'persisted', 'cancelled', 'failed', 'timed_out')",
            params![job.job_id.as_str(), as_i64(now_ms)?],
        )?;
        Ok(())
    }

    pub fn mark_persisted(&self, job: &JobRef, result_version: u64, now_ms: u64) -> Result<()> {
        self.ensure_job_ref(job)?;
        let connection = self.connection.lock().unwrap();
        let updated = connection.execute(
            "UPDATE ai_jobs SET state = 'persisted', result_persisted = 1,
             updated_at_ms = ?3 WHERE job_id = ?1 AND result_version = ?2
               AND state IN ('completed', 'persisted')",
            params![
                job.job_id.as_str(),
                as_i64(result_version)?,
                as_i64(now_ms)?
            ],
        )?;
        if updated != 1 {
            bail!("result version does not match completed job");
        }
        Ok(())
    }

    pub fn load(&self, job_id: &JobId) -> Result<StoredJob> {
        let connection = self.connection.lock().unwrap();
        connection
            .query_row(
                "SELECT request_json, state, analysis_version, result_version,
                 capture_manifest_json, final_sequences_json, transcript_json,
                 result_json, capture_quality, asr_attempts, llm_attempts,
                 deadline_at_ms, error_code FROM ai_jobs WHERE job_id = ?1",
                [job_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, u32>(9)?,
                        row.get::<_, u32>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, Option<String>>(12)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("job {job_id} not found"))
            .and_then(StoredJob::decode)
    }

    pub fn status(&self, job: &JobRef) -> Result<JobStatus> {
        self.ensure_job_ref(job)?;
        let stored = self.load(&job.job_id)?;
        Ok(JobStatus {
            job: job.clone(),
            state: stored.state,
            analysis_version: stored.analysis_version,
            result_version: stored.result_version,
            error_code: stored.error_code,
        })
    }

    pub fn completed_result(&self, job: &JobRef) -> Result<JobCompleted> {
        self.ensure_job_ref(job)?;
        let stored = self.load(&job.job_id)?;
        if !matches!(stored.state, JobState::Completed | JobState::Persisted) {
            bail!("job {} has no completed result", job.job_id);
        }
        Ok(JobCompleted {
            job: job.clone(),
            result_version: stored.result_version.context("missing result version")?,
            capture_quality: stored.capture_quality.context("missing capture quality")?,
            transcript: stored.transcript.context("missing transcript")?,
            result: stored.result.context("missing structured result")?,
        })
    }

    pub fn capture_purge_candidates(
        &self,
        persisted_before_ms: u64,
        terminal_before_ms: u64,
        limit: usize,
    ) -> Result<Vec<JobId>> {
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT job_id FROM ai_jobs
             WHERE capture_purged = 0 AND (
               (state = 'persisted' AND updated_at_ms <= ?1) OR
               (state IN ('completed', 'cancelled', 'failed', 'timed_out')
                 AND updated_at_ms <= ?2)
             ) ORDER BY updated_at_ms LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![
                as_i64(persisted_before_ms)?,
                as_i64(terminal_before_ms)?,
                limit as i64,
            ],
            |row| row.get::<_, String>(0),
        )?;
        rows.map(|row| JobId::new(row?).context("invalid stored job id"))
            .collect()
    }

    pub fn mark_capture_purged(&self, job_id: &JobId) -> Result<()> {
        let connection = self.connection.lock().unwrap();
        connection.execute(
            "UPDATE ai_jobs SET capture_purged = 1 WHERE job_id = ?1",
            [job_id.as_str()],
        )?;
        Ok(())
    }

    pub fn expire_unacknowledged_results(
        &self,
        before_ms: u64,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<JobId>> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction()?;
        let job_ids = {
            let mut statement = transaction.prepare(
                "SELECT job_id FROM ai_jobs
                 WHERE state = 'completed' AND result_persisted = 0 AND updated_at_ms <= ?1
                 ORDER BY updated_at_ms LIMIT ?2",
            )?;
            let rows = statement.query_map(params![as_i64(before_ms)?, limit as i64], |row| {
                row.get::<_, String>(0)
            })?;
            rows.map(|row| JobId::new(row?).context("invalid stored job id"))
                .collect::<Result<Vec<_>>>()?
        };
        for job_id in &job_ids {
            transaction.execute(
                "UPDATE ai_jobs SET state = 'failed', result_version = NULL,
                 transcript_json = NULL, result_json = NULL, capture_quality = NULL,
                 error_code = 'RESULT_RETENTION_EXPIRED', updated_at_ms = ?2
                 WHERE job_id = ?1 AND state = 'completed' AND result_persisted = 0",
                params![job_id.as_str(), as_i64(now_ms)?],
            )?;
        }
        transaction.commit()?;
        Ok(job_ids)
    }

    fn ensure_job_ref(&self, job: &JobRef) -> Result<()> {
        let connection = self.connection.lock().unwrap();
        let request_json: String = connection
            .query_row(
                "SELECT request_json FROM ai_jobs WHERE job_id = ?1",
                [job.job_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("job {} not found", job.job_id))?;
        let request: SubmitPostCallJob = serde_json::from_str(&request_json)?;
        if request.job != *job {
            bail!("job reference does not match durable job identity");
        }
        Ok(())
    }

    fn increment_attempt(
        &self,
        job_id: &JobId,
        column: &str,
        state: &str,
        now_ms: u64,
    ) -> Result<u32> {
        let sql = match column {
            "asr_attempts" => {
                "UPDATE ai_jobs SET asr_attempts = asr_attempts + 1, state = ?2,
                 updated_at_ms = ?3 WHERE job_id = ?1
                 AND state IN ('queued', 'running_asr') RETURNING asr_attempts"
            }
            "llm_attempts" => {
                "UPDATE ai_jobs SET llm_attempts = llm_attempts + 1, state = ?2,
                 updated_at_ms = ?3 WHERE job_id = ?1
                 AND state = 'running_llm' RETURNING llm_attempts"
            }
            _ => bail!("unsupported attempt column"),
        };
        let connection = self.connection.lock().unwrap();
        connection
            .query_row(
                sql,
                params![job_id.as_str(), state, as_i64(now_ms)?],
                |row| row.get(0),
            )
            .context("increment provider attempt")
    }
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .any(|name| name == column);
    if !exists {
        connection.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))?;
    }
    Ok(())
}

fn backfill_profile_ids(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT job_id, request_json FROM ai_jobs WHERE profile_id = '' OR profile_id IS NULL",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (job_id, request_json) in rows {
        let request: SubmitPostCallJob = serde_json::from_str(&request_json)
            .with_context(|| format!("decode stored job request {job_id} for profile migration"))?;
        connection.execute(
            "UPDATE ai_jobs SET profile_id = ?2 WHERE job_id = ?1",
            params![job_id, request.profile.profile_id.as_str()],
        )?;
    }
    Ok(())
}

type StoredRow = (
    String,
    String,
    u32,
    Option<i64>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    u32,
    u32,
    i64,
    Option<String>,
);

impl StoredJob {
    fn decode(row: StoredRow) -> Result<Self> {
        Ok(Self {
            request: serde_json::from_str(&row.0)?,
            state: parse_state(&row.1)?,
            analysis_version: row.2,
            result_version: row.3.map(|value| value as u64),
            manifest: serde_json::from_str(&row.4)?,
            final_sequences: row
                .5
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
            transcript: row
                .6
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
            result: row
                .7
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
            capture_quality: row.8.map(|value| parse_quality(&value)).transpose()?,
            asr_attempts: row.9,
            llm_attempts: row.10,
            deadline_at_ms: u64::try_from(row.11).context("negative deadline")?,
            error_code: row.12,
        })
    }
}

fn state_str(state: JobState) -> &'static str {
    match state {
        JobState::Capturing => "capturing",
        JobState::Queued => "queued",
        JobState::RunningAsr => "running_asr",
        JobState::RunningLlm => "running_llm",
        JobState::Completed => "completed",
        JobState::Persisted => "persisted",
        JobState::Cancelled => "cancelled",
        JobState::Failed => "failed",
        JobState::TimedOut => "timed_out",
    }
}

fn parse_state(value: &str) -> Result<JobState> {
    match value {
        "capturing" => Ok(JobState::Capturing),
        "queued" => Ok(JobState::Queued),
        "running_asr" => Ok(JobState::RunningAsr),
        "running_llm" => Ok(JobState::RunningLlm),
        "completed" => Ok(JobState::Completed),
        "persisted" => Ok(JobState::Persisted),
        "cancelled" => Ok(JobState::Cancelled),
        "failed" => Ok(JobState::Failed),
        "timed_out" => Ok(JobState::TimedOut),
        _ => bail!("unknown stored job state {value}"),
    }
}

fn quality_str(quality: CaptureQuality) -> &'static str {
    match quality {
        CaptureQuality::Complete => "complete",
        CaptureQuality::IncompleteProcessable => "incomplete_processable",
        CaptureQuality::Insufficient => "insufficient",
    }
}

fn parse_quality(value: &str) -> Result<CaptureQuality> {
    match value {
        "complete" => Ok(CaptureQuality::Complete),
        "incomplete_processable" => Ok(CaptureQuality::IncompleteProcessable),
        "insufficient" => Ok(CaptureQuality::Insufficient),
        _ => bail!("unknown capture quality {value}"),
    }
}

fn as_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("value exceeds sqlite integer range")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_protocol::control::{
        AiPipelineType, AiProfileSnapshot, AudioCodec, MediaDirection, Participant, StreamBinding,
    };
    use ai_protocol::id::{ConversationId, OperationId, ParticipantId, ProfileId, TenantId};
    use tempfile::tempdir;

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

    #[test]
    fn duplicate_submission_is_idempotent() {
        let directory = tempdir().unwrap();
        let store = JobStore::open(&directory.path().join("jobs.db")).unwrap();
        let request = request();
        let manifest = CaptureManifest::new(&request.streams);
        assert!(!store.submit(&request, &manifest, 1, 1000).unwrap());
        assert!(store.submit(&request, &manifest, 2, 1000).unwrap());

        let mut conflicting = request;
        conflicting.profile.profile_version = 2;
        assert!(store.submit(&conflicting, &manifest, 3, 1000).is_err());
    }

    #[test]
    fn active_claim_cannot_be_reentered_by_same_worker() {
        let directory = tempdir().unwrap();
        let store = JobStore::open(&directory.path().join("jobs.db")).unwrap();
        let request = request();
        let manifest = CaptureManifest::new(&request.streams);
        store.submit(&request, &manifest, 1, 10_000).unwrap();
        store
            .end_audio(
                &request.job,
                &BTreeMap::from([(StreamId::new("stream-1").unwrap(), 0)]),
                2,
            )
            .unwrap();
        assert!(
            store
                .claim(&request.job.job_id, "worker-1", 3, 1_000)
                .unwrap()
        );
        assert!(
            !store
                .claim(&request.job.job_id, "worker-1", 4, 1_000)
                .unwrap()
        );
    }

    #[test]
    fn cancellation_rejects_late_asr_checkpoint() {
        let directory = tempdir().unwrap();
        let store = JobStore::open(&directory.path().join("jobs.db")).unwrap();
        let request = request();
        let manifest = CaptureManifest::new(&request.streams);
        store.submit(&request, &manifest, 1, 10_000).unwrap();
        store
            .end_audio(
                &request.job,
                &BTreeMap::from([(StreamId::new("stream-1").unwrap(), 0)]),
                2,
            )
            .unwrap();
        store.start_asr(&request.job.job_id, 3).unwrap();
        store.cancel(&request.job, 4).unwrap();
        assert!(store.save_asr(&request.job.job_id, &[], 5).is_err());
        assert_eq!(
            store.status(&request.job).unwrap().state,
            JobState::Cancelled
        );
    }
}
