use crate::config::StorageLimits;
use anyhow::{Context, Result, bail};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskUsage {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub used_percent: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskAdmission {
    Accept,
    Warning,
    Reject,
}

#[derive(Debug, Default)]
pub struct DiskAdmissionGuard {
    rejected: AtomicBool,
}

impl DiskAdmissionGuard {
    pub fn evaluate(&self, usage: &DiskUsage, limits: &StorageLimits) -> DiskAdmission {
        let minimum_free = limits.disk_min_free_mb.saturating_mul(1024 * 1024);
        if self.rejected.load(Ordering::Acquire) {
            if usage.used_percent < limits.disk_resume_percent
                && usage.available_bytes >= minimum_free
            {
                self.rejected.store(false, Ordering::Release);
            } else {
                return DiskAdmission::Reject;
            }
        }
        if usage.used_percent >= limits.disk_reject_percent || usage.available_bytes < minimum_free
        {
            self.rejected.store(true, Ordering::Release);
            DiskAdmission::Reject
        } else if usage.used_percent >= limits.disk_warning_percent {
            DiskAdmission::Warning
        } else {
            DiskAdmission::Accept
        }
    }
}

impl DiskUsage {
    pub fn read(path: &Path) -> Result<Self> {
        let path = CString::new(path.as_os_str().as_bytes()).context("data path contains NUL")?;
        let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: path is NUL-terminated and stats points to writable memory.
        if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error()).context("read data filesystem usage");
        }
        // SAFETY: statvfs returned success and initialized stats.
        let stats = unsafe { stats.assume_init() };
        let block_size = stats.f_frsize;
        let total_bytes = stats.f_blocks.saturating_mul(block_size);
        let available_bytes = stats.f_bavail.saturating_mul(block_size);
        if total_bytes == 0 {
            bail!("filesystem reports zero capacity");
        }
        let used_bytes = total_bytes.saturating_sub(available_bytes);
        let used_percent = used_bytes.saturating_mul(100) / total_bytes;
        Ok(Self {
            total_bytes,
            available_bytes,
            used_percent: used_percent.min(100) as u8,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_state_uses_resume_hysteresis() {
        let guard = DiskAdmissionGuard::default();
        let limits = StorageLimits {
            disk_min_free_mb: 1,
            ..StorageLimits::default()
        };
        let usage = |used_percent| DiskUsage {
            total_bytes: 100,
            available_bytes: 10 * 1024 * 1024,
            used_percent,
        };
        assert_eq!(guard.evaluate(&usage(85), &limits), DiskAdmission::Reject);
        assert_eq!(guard.evaluate(&usage(80), &limits), DiskAdmission::Reject);
        assert_eq!(guard.evaluate(&usage(74), &limits), DiskAdmission::Warning);
    }
}
