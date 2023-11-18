use serde::{Deserialize, Serialize};

#[cfg(test)]
use rand::{distributions::Standard, thread_rng, Rng};

pub type Bytes = Vec<u8>;
pub type KeyRef<'a> = &'a [u8];
pub type VlogNum = u64;
pub type DataEntrySz = u32;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ValueEntry {
    Tombstone,
    Val(DataPtr),
}
impl ValueEntry {
    pub fn mem_sz(&self) -> usize {
        match &self {
            ValueEntry::Tombstone => 0,
            ValueEntry::Val(dp) => dp.mem_sz(),
        }
    }
}
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Hash, PartialOrd, Ord, Eq,
)]
pub struct DataPtr {
    pub vlog: VlogNum,
    pub offset: u64,
    pub len: DataEntrySz,
    pub compressed: bool,
}
impl DataPtr {
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
    use crate::error::GhalaDbResult;

    use super::*;

    #[test]
    fn dp_serde_sz() -> GhalaDbResult<()> {
        let dp = DataPtr::new(0, 0, 0, true);
        let serde_sz = DataPtr::serde_sz();
        let bytes = bincode::serialize(&dp)?;
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
