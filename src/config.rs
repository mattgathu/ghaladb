use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct DatabaseOptions {
    /// Maximum in-memory table size in bytes
    #[builder(default = 4_096_000)]
    pub max_mem_table_size: usize,
    /// Synchronous write IO flag. If enabled all writes will be flushed to disk.
    #[builder(default = false)]
    pub sync: bool,
    /// Read Timeout. Timeout in milliseconds for data lookups.
    #[builder(default = 1000)]
    pub read_timeout: u64,
    /// Maximum SS tables
    #[builder(default = 10)]
    pub max_ssts: usize,
}
