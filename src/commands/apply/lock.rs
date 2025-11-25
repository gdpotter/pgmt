use crate::constants::LOCK_FILE_STALE_TIMEOUT;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::warn;

/// Concurrent run protection using a lock file
pub struct ApplyLock {
    lock_file_path: PathBuf,
}

impl ApplyLock {
    pub fn new(root_dir: &Path) -> Self {
        Self {
            lock_file_path: root_dir.join(".pgmt_apply.lock"),
        }
    }

    pub fn acquire(&self) -> Result<()> {
        if self.lock_file_path.exists() {
            // Check if the lock file is stale
            if let Ok(metadata) = fs::metadata(&self.lock_file_path)
                && let Ok(modified) = metadata.modified()
                && let Ok(elapsed) = modified.elapsed()
            {
                if elapsed > LOCK_FILE_STALE_TIMEOUT {
                    warn!("Removing stale lock file (older than 10 minutes)");
                    let _ = fs::remove_file(&self.lock_file_path);
                } else {
                    return Err(anyhow::anyhow!(
                        "Another pgmt apply operation is currently running.\n\n💡 If you're sure no other apply is running, remove: {}",
                        self.lock_file_path.display()
                    ));
                }
            }
        }

        // Create lock file with current process info
        let lock_content = format!(
            "PID: {}\nStarted: {}",
            std::process::id(),
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        );

        fs::write(&self.lock_file_path, lock_content)
            .map_err(|e| anyhow::anyhow!("Failed to create lock file: {}", e))?;

        Ok(())
    }
}

impl Drop for ApplyLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock_file_path);
    }
}
