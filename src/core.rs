use bincode::{Decode, Encode};

#[cfg(test)]
use rand::{distributions::Standard, thread_rng, Rng};

pub type Bytes = Vec<u8>;
pub type KeyRef<'a> = &'a [u8];
pub type VlogNum = u64;
pub type DataEntrySz = u32;

pub(crate) trait MemSize {
    fn mem_sz(&self) -> usize;
}

/// A list of possible data pointer states.
#[derive(Debug, Copy, Clone, PartialEq, Encode, Decode)]
pub(crate) enum ValueEntry {
    /// A deleted value entry
    Tombstone,
    /// A valid pointer to data on disk.
    Val(DataPtr),
}
impl Default for ValueEntry {
    fn default() -> Self {
        Self::Tombstone
    }
}
impl MemSize for ValueEntry {
    fn mem_sz(&self) -> usize {
        match self {
            Self::Val(dp) => dp.mem_sz(),
            Self::Tombstone => 0usize,
        }
    }
}

impl MemSize for Bytes {
    fn mem_sz(&self) -> usize {
        self.len()
    }
}

/// A data pointer for data on disk.
#[derive(
    Debug, Clone, Copy, Encode, Decode, PartialEq, Hash, PartialOrd, Ord, Eq,
)]
pub struct DataPtr {
    /// The value log number
    pub vlog: VlogNum,
    //TODO: we limit vlog size to 4gb and use a u32 offset
    /// Data offset in the value log file.
    pub offset: u64,
    /// Data size
    pub len: DataEntrySz,
    /// Data compression flag.
    pub compressed: bool,
}
impl DataPtr {
    /// Create a data pointer.
    pub fn new(vlog: VlogNum, offset: u64, len: u32, compressed: bool) -> Self {
        Self {
            vlog,
            offset,
            len,
            compressed,
        }
    }
    pub fn mem_sz(&self) -> usize {
        std::mem::size_of_val(self)
    }
    pub fn serde_sz() -> usize {
        // u64 + u64 + u32 +bool
        21
    }
}

#[cfg(test)]
pub trait FixtureGen<T> {
    fn gen() -> T;
}

#[cfg(test)]
impl FixtureGen<Bytes> for Bytes {
    fn gen() -> Bytes {
        let mut rng = thread_rng();
        let len = rng.gen_range(32..4097);
        rng.sample_iter(Standard).take(len).collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::{dec::Dec, error::GhalaDbResult};

    use super::*;

    #[test]
    fn dp_serde_sz() -> GhalaDbResult<()> {
        let dp = DataPtr::new(0, 0, 0, true);
        let serde_sz = DataPtr::serde_sz();
        let bytes = Dec::ser_raw(&dp)?;
        assert_eq!(
            bytes.len(),
            serde_sz,
            "dp bytes not match expected len. Got: {} Expected: {}",
            serde_sz,
            bytes.len()
        );
        Ok(())
    }
}
