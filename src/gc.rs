use std::path::Path;

use crate::{
    core::VlogNum,
    error::GhalaDbResult,
    keys::Skt,
    vlog::{DataEntry, VlogIter},
};

pub(crate) struct Janitor {
    vnum: VlogNum,
    vlog_iter: VlogIter,
}

impl Janitor {
    pub fn new(vnum: VlogNum, path: &Path) -> GhalaDbResult<Self> {
        debug!("init janitor for vlog: {vnum} at: {path:?}");
        let vlog_iter = VlogIter::from_path(path)?;
        Ok(Self { vnum, vlog_iter })
    }

    pub fn sweep(&mut self, keys: &mut Skt) -> GhalaDbResult<Option<DataEntry>> {
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
