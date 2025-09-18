use super::Result;
use super::sled_storage::{CleanupFun, SledStorageDB};
use anyhow::Error;
use convert::Bytesize;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
/// Configuration for Sled storage backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Path to database directory
    pub path: String,
    /// Cache capacity in bytes
    pub cache_capacity: Bytesize,
    /// Cleanup function for expired keys
    #[serde(skip, default = "Config::cleanup_f_default")]
    pub cleanup_f: CleanupFun,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            path: String::default(),
            cache_capacity: Bytesize::from(1024 * 1024 * 1024),
            cleanup_f: def_cleanup,
        }
    }
}

impl Config {
    /// Converts to Sled's native configuration
    #[inline]
    pub fn to_sled_config(&self) -> Result<sled::Config> {
        if self.path.trim().is_empty() {
            return Err(Error::msg("storage dir is empty"));
        }
        let sled_cfg = sled::Config::default()
            .path(self.path.trim())
            .cache_capacity(self.cache_capacity.as_u64())
            .flush_every_ms(Some(3000))
            .mode(sled::Mode::LowSpace);
        Ok(sled_cfg)
    }

    /// Returns default cleanup function
    #[inline]
    fn cleanup_f_default() -> CleanupFun {
        def_cleanup
    }
}

/// Default cleanup function that runs in background thread
fn def_cleanup(_db: &SledStorageDB) {
    #[cfg(feature = "ttl")]
    {
        let db = _db.clone();

        tokio::spawn(async move {
            let limit = 200;
            loop {
                sleep(std::time::Duration::from_secs(10)).await;
                let mut total_cleanups = 0;
                let now = std::time::Instant::now();
                loop {
                    let now = std::time::Instant::now();
                    let count = db.cleanup(limit);
                    total_cleanups += count;
                    if count > 0 {
                        log::debug!(
                            "def_cleanup: {}, total cleanups: {}, active_count(): {}, cost time: {:?}",
                            count,
                            total_cleanups,
                            db.active_count(),
                            now.elapsed()
                        );
                    }
                    if count < limit {
                        break;
                    }
                    if db.active_count() > 50 {
                        sleep(std::time::Duration::from_millis(500)).await;
                    } else {
                        sleep(std::time::Duration::from_millis(0)).await;
                    }
                }
                if now.elapsed().as_secs() > 3 {
                    log::info!(
                        "total cleanups: {}, cost time: {:?}",
                        total_cleanups,
                        now.elapsed()
                    );
                }
            }
        });
    }
}
