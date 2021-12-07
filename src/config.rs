use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
pub struct DatabaseOptions {
    /// Maximum in-memory table size in bytes
    #[builder(default = 2_048_000)]
    pub max_mem_table_size: usize, //FIXME how to avoid really low val that makes very kv bigger
    /// Synchronous write IO flag. If enabled all writes will be flushed to disk.
    #[builder(default = false)]
    pub sync: bool,
}
