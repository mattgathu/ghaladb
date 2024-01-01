use bincode::{Decode, Encode};
use typed_builder::TypedBuilder;

/// Database Configuration
#[derive(Debug, Copy, Clone, TypedBuilder, Encode, Decode)]
pub struct DatabaseOptions {
    /// Maximum vlog size in bytes: default 1gb
    #[builder(default = 1_000_000_000)]
    pub max_vlog_size: usize,
    /// vlog memory buffer
    #[builder(default = true)]
    pub vlog_mem_buf_enabled: bool,
    /// vlog memory buffer size in bytes: default 8mb
    #[builder(default = 8_000_000)]
    pub vlog_mem_buf_size: usize,
    /// Synchronous write IO flag. If enabled all writes will be flushed to
    /// disk.
    #[builder(default = false)]
    pub sync: bool,
    /// enable vlog compaction
    #[builder(default = true)]
    pub compact: bool,
    /// enable data compression
    #[builder(default = true)]
    pub compress: bool,
    /// keys sync interval in seconds
    #[builder(default = 10)]
    pub keys_sync_interval: u128,
}
