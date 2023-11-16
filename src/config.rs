use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct DatabaseOptions {
    /// Maximum in-memory table size in bytes
    #[builder(default = 4_000_096_000)]
    pub max_mem_table_size: usize,
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
    /// Read Timeout. Timeout in milliseconds for data lookups.
    #[builder(default = 1000)]
    pub read_timeout: u64,
    /// Maximum SS tables
    #[builder(default = 10)]
    pub max_ssts: usize,
    /// vlog compaction enabled
    #[builder(default = true)]
    pub vlog_compaction_enabled: bool,
    /// compress data
    #[builder(default = true)]
    pub compress: bool,
}
