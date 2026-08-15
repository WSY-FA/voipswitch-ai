use crate::completeness::CaptureManifest;
use ai_protocol::control::StreamBinding;
use ai_protocol::id::{JobId, StreamId};
use ai_protocol::media::{MediaFrame, MediaFrameMetadata};
use anyhow::{Context, Result, bail};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

pub struct CaptureStore {
    root: PathBuf,
}

impl CaptureStore {
    pub fn new(root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root)
            .with_context(|| format!("create capture directory {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn prepare(&self, job_id: &JobId, streams: &[StreamBinding]) -> Result<()> {
        let job_dir = self.job_dir(job_id);
        fs::create_dir_all(&job_dir)
            .with_context(|| format!("create job capture directory {}", job_dir.display()))?;
        for stream in streams {
            let path = self.stream_path(job_id, &stream.stream_id);
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("prepare capture stream {}", path.display()))?;
        }
        Ok(())
    }

    pub fn append(&self, frame: &MediaFrame) -> Result<()> {
        if frame.payload.is_empty() {
            return Ok(());
        }
        let path = self.stream_path(&frame.metadata.job_id, &frame.metadata.stream_id);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .with_context(|| format!("open capture stream {}", path.display()))?;
        let metadata = serde_json::to_vec(&frame.metadata)?;
        let metadata_len = u32::try_from(metadata.len()).context("capture metadata too large")?;
        let payload_len =
            u32::try_from(frame.payload.len()).context("capture payload too large")?;
        file.write_all(&metadata_len.to_be_bytes())?;
        file.write_all(&metadata)?;
        file.write_all(&payload_len.to_be_bytes())?;
        file.write_all(&frame.payload)?;
        Ok(())
    }

    pub fn read_payloads(&self, job_id: &JobId, stream_id: &StreamId) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        for (_, payload) in self.read_records(job_id, stream_id)? {
            output.extend_from_slice(&payload);
        }
        Ok(output)
    }

    pub fn remove_job(&self, job_id: &JobId) -> Result<()> {
        let path = self.job_dir(job_id);
        match fs::remove_dir_all(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                Err(error).with_context(|| format!("remove capture directory {}", path.display()))
            }
        }
    }

    pub fn rebuild_manifest(
        &self,
        job_id: &JobId,
        streams: &[StreamBinding],
    ) -> Result<CaptureManifest> {
        let mut manifest = CaptureManifest::new(streams);
        for stream in streams {
            for (metadata, _) in self.read_records(job_id, &stream.stream_id)? {
                let _ = manifest.observe(&metadata)?;
            }
        }
        Ok(manifest)
    }

    fn job_dir(&self, job_id: &JobId) -> PathBuf {
        self.root.join(job_id.as_str())
    }

    fn stream_path(&self, job_id: &JobId, stream_id: &StreamId) -> PathBuf {
        self.job_dir(job_id)
            .join(format!("{}.frames", stream_id.as_str()))
    }

    fn read_records(
        &self,
        job_id: &JobId,
        stream_id: &StreamId,
    ) -> Result<Vec<(MediaFrameMetadata, Vec<u8>)>> {
        let path = self.stream_path(job_id, stream_id);
        let mut file =
            File::open(&path).with_context(|| format!("open capture stream {}", path.display()))?;
        let mut records = Vec::new();
        loop {
            let Some(metadata_len) = read_len(&mut file)? else {
                break;
            };
            if metadata_len == 0 || metadata_len > ai_protocol::media::MAX_MEDIA_METADATA_LEN {
                bail!("invalid stored capture metadata length {metadata_len}");
            }
            let mut metadata = vec![0_u8; metadata_len];
            file.read_exact(&mut metadata)
                .context("read stored capture metadata")?;
            let metadata: MediaFrameMetadata = serde_json::from_slice(&metadata)?;
            let payload_len =
                read_len(&mut file)?.context("missing stored capture payload length")?;
            if payload_len == 0 || payload_len > ai_protocol::media::MAX_MEDIA_FRAME_LEN {
                bail!("invalid stored capture payload length {payload_len}");
            }
            let mut payload = vec![0_u8; payload_len];
            file.read_exact(&mut payload)
                .context("read stored capture payload")?;
            records.push((metadata, payload));
        }
        Ok(records)
    }
}

fn read_len(file: &mut File) -> Result<Option<usize>> {
    let mut len = [0_u8; 4];
    match file.read(&mut len[..1]) {
        Ok(0) => Ok(None),
        Ok(1) => {
            file.read_exact(&mut len[1..])
                .context("truncated stored capture length")?;
            Ok(Some(u32::from_be_bytes(len) as usize))
        }
        Ok(_) => unreachable!(),
        Err(error) => Err(error.into()),
    }
}
