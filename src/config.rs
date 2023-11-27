use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
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
    /// vlog compaction enabled
    #[builder(default = true)]
    pub vlog_compaction_enabled: bool,
    /// data compression enabled
    #[builder(default = true)]
    pub compress_data: bool,
}
