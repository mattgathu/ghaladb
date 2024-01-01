use std::path::Path;

use crate::{
    core::VlogNum,
    error::GhalaDbResult,
    keys::Keys,
    vlog::{DataEntry, VlogReader},
};

/// A Lightweight Garbage Collector
///
/// The janitor reads data entries in a values log and checks
/// if they are still valid by doing a lookup for the current data
/// pointer associated to a key and comparing it to the one on the log.
///
/// If the data pointers do not much, it means that the data associated to
/// the key got updated and we can safely delete the stale entry in the values logs.
/// If the pointers match then the data is still live and valid.
///
/// The janitor returns a live data entry when found. The database will re-insert it
/// and trigger more sweeping later.
///
/// Once the janitor goes through an entire values log, the database will drop it.
///
/// NOTE
/// --
/// The sweeping is driven by the database.
///
/// The janitor will stop every time a live data entry is found.
/// The idea is we can do this sweeping inline when doing writes.
///
/// If the vlog entries are all stale, then the vlog will not stop until it goes through
/// the entire vlog. This could be problematic.
/// TODO: handle this edge case
pub(crate) struct Janitor {
    vnum: VlogNum,
    vlog_iter: VlogReader,
}

impl Janitor {
    pub fn new(vnum: VlogNum, path: &Path) -> GhalaDbResult<Self> {
        debug!("init janitor for vlog: {vnum} at: {path:?}");
        let vlog_iter = VlogReader::from_path(path)?;
        Ok(Self { vnum, vlog_iter })
    }

    pub fn sweep(&mut self, keys: &mut Keys) -> GhalaDbResult<Option<DataEntry>> {
        trace!("Janitor::sweep");
        loop {
            match self.vlog_iter.next_entry()? {
                None => return Ok(None),
                Some((dp, de)) => {
                    match keys.get(&de.key) {
                        None => continue,
                        Some(cur_dp) => {
                            if cur_dp == dp {
                                // data is live and should move to tail
                                return Ok(Some(de));
                            } else {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
    pub fn vnum(&self) -> VlogNum {
        self.vnum
    }
}
