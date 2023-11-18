use std::path::Path;

use crate::{
    core::{ValueEntry, VlogNum},
    error::GhalaDbResult,
    keyman::KeyMan,
    utils::t,
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

    pub fn step(&mut self, keyman: &mut KeyMan) -> GhalaDbResult<Option<DataEntry>> {
        loop {
            match self.vlog_iter.next_entry()? {
                None => return Ok(None),
                Some((dp, de)) => {
                    let ve = t!("keyman::get_ve", keyman.get_ve(&de.key))?;
                    match ve {
                        ValueEntry::Tombstone => continue,
                        ValueEntry::Val(cur_dp) => {
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