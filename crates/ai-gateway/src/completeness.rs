use crate::config::CaptureThresholds;
use ai_protocol::control::{CaptureQuality, StreamBinding};
use ai_protocol::id::StreamId;
use ai_protocol::media::MediaFrameMetadata;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureManifest {
    pub streams: BTreeMap<StreamId, StreamCaptureStats>,
}

impl CaptureManifest {
    pub fn new(streams: &[StreamBinding]) -> Self {
        Self {
            streams: streams
                .iter()
                .map(|stream| {
                    (
                        stream.stream_id.clone(),
                        StreamCaptureStats {
                            stream: stream.clone(),
                            first_sequence: None,
                            highest_sequence: None,
                            received_frames: 0,
                            duplicate_or_late_frames: 0,
                            max_observed_gap_ms: 0,
                            received_duration_ms: 0,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn observe(&mut self, metadata: &MediaFrameMetadata) -> Result<bool> {
        let stats = self
            .streams
            .get_mut(&metadata.stream_id)
            .ok_or_else(|| anyhow::anyhow!("unknown stream {}", metadata.stream_id))?;
        if stats.stream.participant_id != metadata.participant_id
            || stats.stream.codec != metadata.codec
            || stats.stream.sample_rate != metadata.sample_rate
            || stats.stream.channels != metadata.channels
            || stats.stream.direction != metadata.direction
        {
            bail!("media metadata does not match stream binding");
        }
        Ok(stats.observe(metadata.sequence, u64::from(metadata.duration_ms)))
    }

    pub fn evaluate(
        &self,
        final_sequences: &BTreeMap<StreamId, u64>,
        thresholds: &CaptureThresholds,
    ) -> Result<CaptureEvaluation> {
        thresholds.validate()?;
        if final_sequences.len() != self.streams.len() {
            bail!("final sequence set does not match configured streams");
        }
        let mut streams = BTreeMap::new();
        let mut quality = CaptureQuality::Complete;
        for (stream_id, stats) in &self.streams {
            let final_sequence = final_sequences
                .get(stream_id)
                .ok_or_else(|| anyhow::anyhow!("missing final sequence for {stream_id}"))?;
            let evaluation = stats.evaluate(*final_sequence, thresholds);
            quality = worst_quality(quality, evaluation.quality);
            streams.insert(stream_id.clone(), evaluation);
        }
        Ok(CaptureEvaluation { quality, streams })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCaptureStats {
    pub stream: StreamBinding,
    pub first_sequence: Option<u64>,
    pub highest_sequence: Option<u64>,
    pub received_frames: u64,
    pub duplicate_or_late_frames: u64,
    pub max_observed_gap_ms: u64,
    pub received_duration_ms: u64,
}

impl StreamCaptureStats {
    fn observe(&mut self, sequence: u64, duration_ms: u64) -> bool {
        match self.highest_sequence {
            None => {
                self.first_sequence = Some(sequence);
                self.highest_sequence = Some(sequence);
                self.max_observed_gap_ms = sequence.saturating_mul(duration_ms);
            }
            Some(highest) if sequence > highest => {
                let missing = sequence.saturating_sub(highest).saturating_sub(1);
                self.max_observed_gap_ms = self
                    .max_observed_gap_ms
                    .max(missing.saturating_mul(duration_ms));
                self.highest_sequence = Some(sequence);
            }
            Some(_) => {
                self.duplicate_or_late_frames = self.duplicate_or_late_frames.saturating_add(1);
                return false;
            }
        }
        self.received_frames = self.received_frames.saturating_add(1);
        self.received_duration_ms = self.received_duration_ms.saturating_add(duration_ms);
        true
    }

    fn evaluate(
        &self,
        final_sequence: u64,
        thresholds: &CaptureThresholds,
    ) -> StreamCaptureEvaluation {
        let expected_frames = final_sequence.saturating_add(1);
        let received_frames = self.received_frames.min(expected_frames);
        let ratio_ppm = (received_frames.saturating_mul(1_000_000) / expected_frames.max(1)) as u32;
        let nominal_duration_ms = self
            .received_duration_ms
            .checked_div(self.received_frames.max(1))
            .unwrap_or(20)
            .max(1);
        let tail_gap_ms = match self.highest_sequence {
            Some(highest) => final_sequence
                .saturating_sub(highest)
                .saturating_mul(nominal_duration_ms),
            None => expected_frames.saturating_mul(nominal_duration_ms),
        };
        let max_gap_ms = self.max_observed_gap_ms.max(tail_gap_ms);
        let quality = if ratio_ppm >= thresholds.complete_ratio_ppm
            && max_gap_ms <= thresholds.complete_max_gap_ms
        {
            CaptureQuality::Complete
        } else if ratio_ppm >= thresholds.process_min_ratio_ppm
            && max_gap_ms <= thresholds.process_max_gap_ms
        {
            CaptureQuality::IncompleteProcessable
        } else {
            CaptureQuality::Insufficient
        };
        StreamCaptureEvaluation {
            expected_frames,
            received_frames,
            ratio_ppm,
            max_gap_ms,
            quality,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureEvaluation {
    pub quality: CaptureQuality,
    pub streams: BTreeMap<StreamId, StreamCaptureEvaluation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCaptureEvaluation {
    pub expected_frames: u64,
    pub received_frames: u64,
    pub ratio_ppm: u32,
    pub max_gap_ms: u64,
    pub quality: CaptureQuality,
}

fn worst_quality(left: CaptureQuality, right: CaptureQuality) -> CaptureQuality {
    use CaptureQuality::{Complete, IncompleteProcessable, Insufficient};
    match (left, right) {
        (Insufficient, _) | (_, Insufficient) => Insufficient,
        (IncompleteProcessable, _) | (_, IncompleteProcessable) => IncompleteProcessable,
        _ => Complete,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_protocol::control::{AudioCodec, MediaDirection};
    use ai_protocol::id::ParticipantId;

    fn stream() -> StreamBinding {
        StreamBinding {
            stream_id: StreamId::new("stream-1").unwrap(),
            participant_id: ParticipantId::new("participant-1").unwrap(),
            direction: MediaDirection::FromParticipant,
            codec: AudioCodec::Pcmu,
            sample_rate: 8_000,
            channels: 1,
        }
    }

    fn metadata(sequence: u64) -> MediaFrameMetadata {
        MediaFrameMetadata {
            job_id: ai_protocol::id::JobId::new("job-1").unwrap(),
            tenant_id: ai_protocol::id::TenantId::new("tenant-1").unwrap(),
            conversation_id: ai_protocol::id::ConversationId::new("call-1").unwrap(),
            participant_id: ParticipantId::new("participant-1").unwrap(),
            stream_id: StreamId::new("stream-1").unwrap(),
            sequence,
            generation: 1,
            direction: MediaDirection::FromParticipant,
            codec: AudioCodec::Pcmu,
            sample_rate: 8_000,
            channels: 1,
            media_timestamp: sequence * 160,
            duration_ms: 20,
            end_of_stream: false,
        }
    }

    #[test]
    fn detects_processable_gap() {
        let stream = stream();
        let mut manifest = CaptureManifest::new(std::slice::from_ref(&stream));
        for sequence in 0..100 {
            if sequence != 50 {
                assert!(manifest.observe(&metadata(sequence)).unwrap());
            }
        }
        let evaluation = manifest
            .evaluate(
                &BTreeMap::from([(stream.stream_id, 99)]),
                &CaptureThresholds::default(),
            )
            .unwrap();
        assert_eq!(evaluation.quality, CaptureQuality::IncompleteProcessable);
        assert_eq!(
            evaluation.streams.values().next().unwrap().ratio_ppm,
            990_000
        );
    }
}
