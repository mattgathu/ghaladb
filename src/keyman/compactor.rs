use crate::{
    config::DatabaseOptions,
    core::Bytes,
    error::{GhalaDBError, GhalaDbResult},
    keyman::{merge_iter, SSTable, SSTableWriter, SeqNum, ValueEntry},
    utils::t,
};
use std::{collections::HashSet, path::PathBuf, sync::mpsc::Sender};

use super::sstable::SSTableIter;

pub(crate) struct CompactionResult {
    pub level: usize,
    pub sst: SSTable,
    pub seq_nums: HashSet<SeqNum>,
}

/// SST Compactor Message
#[allow(clippy::large_enum_variant)]
pub(crate) enum CompactorMsg {
    /// Compaction Error
    Error(GhalaDBError),
    /// Compaction Result
    Ok(CompactionResult),
}
impl CompactorMsg {
    pub fn is_err(&self) -> bool {
        match self {
            Self::Ok(_) => false,
            Self::Error(_) => true,
        }
    }
}

/// SST Compactor
///
/// The compactor merges several SSTs into one.
#[derive(Debug)]
pub(crate) struct Compactor {
    path: PathBuf,
    level: usize,
    seq_nums: HashSet<SeqNum>,
    seq_num: SeqNum,
    conf: DatabaseOptions,
}

impl Compactor {
    pub fn new(
        path: PathBuf,
        level: usize,
        seq_nums: HashSet<SeqNum>,
        seq_num: SeqNum,
        conf: DatabaseOptions,
    ) -> Compactor {
        Self {
            path,
            level,
            seq_nums,
            seq_num,
            conf,
        }
    }

    pub(crate) fn compact(
        &self,
        ssts: Vec<SSTableIter>,
        tx: Sender<CompactorMsg>,
    ) -> GhalaDbResult<()> {
        let msg = match self._compact(ssts) {
            Ok(res) => CompactorMsg::Ok(res),
            Err(e) => CompactorMsg::Error(e),
        };
        if msg.is_err() {
            t!("Compactor::clean_up", self.clean_up()).ok();
        }
        tx.send(msg)
            .map_err(|e| GhalaDBError::SstSendError(e.to_string()))?;

        Ok(())
    }

    fn _compact(&self, ssts: Vec<SSTableIter>) -> GhalaDbResult<CompactionResult> {
        let mut merged: Box<
            dyn Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>,
        > = Box::new(vec![].into_iter());
        for sst in ssts {
            let inter = merge_iter(merged, sst);
            merged = Box::new(inter);
        }
        let mut sst_wtr =
            SSTableWriter::new(&self.path, self.seq_num, self.conf.compress)?;
        for entry in merged {
            let (k, v) = entry?;
            sst_wtr.write(k.to_vec(), v.clone())?;
        }
        let sst = sst_wtr.into_sstable()?;

        debug!(
            "compacted ssts to {}. Total bytes: {}",
            self.path.display(),
            sst.mem_size()
        );

        Ok(CompactionResult {
            level: self.level,
            sst,
            seq_nums: self.seq_nums.clone(),
        })
    }

    fn clean_up(&self) -> GhalaDbResult<()> {
        Ok(std::fs::remove_file(&self.path)?)
    }
}
