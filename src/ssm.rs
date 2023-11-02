//! Storage System Manager

use crate::{
    error::{GhalaDBError, GhalaDbResult},
    memtable::{Bytes, KeyRef, MemTable, ValueEntry},
    sstable::{SSTable, SSTableIter, SSTableWriter},
};
use tap::tap::Tap;
//use rand::Rng;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs::OpenOptions,
    io::{BufReader, BufWriter},
    iter::Peekable,
    path::{Path, PathBuf},
    sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender},
    thread,
    time::Duration,
};

type LevelsOnDisk = BTreeMap<usize, Vec<PathBuf>>;
type Levels = BTreeMap<usize, VecDeque<SSTable>>;
type SequenceNumber = u64;
struct CompactionResult {
    level: usize,
    sst: SSTable,
    seq_nums: HashSet<SequenceNumber>,
}

pub(crate) struct StoreSysMan {
    base_path: PathBuf,
    max_tables: usize,
    sequence: SequenceNumber,
    rx: Option<Receiver<CompactionResult>>,
    levels: Levels,
}

impl StoreSysMan {
    pub fn new<P: AsRef<Path>>(path: P, max_tables: usize) -> GhalaDbResult<StoreSysMan> {
        let base_path = path.as_ref().to_path_buf();

        debug!("Loading SSM from dir: {:?}", base_path);

        let mut levels: Levels = BTreeMap::new();
        let mut sequence = 0u64;
        let lvl_info = Self::load_levels_info(base_path.join("lvl_info"))?;
        for (lvl, paths) in lvl_info {
            let mut ssts = paths
                .iter()
                .map(|p| {
                    debug!("Loading sst from: {:?}", p);
                    SSTable::from_path(p)
                })
                .collect::<GhalaDbResult<Vec<SSTable>>>()?;
            ssts.sort_unstable_by(|l, r| r.seq_num.cmp(&l.seq_num));
            sequence = std::cmp::max(sequence, ssts.iter().map(|t| t.seq_num).max().unwrap_or(0));
            let ssts: VecDeque<_> = ssts.into();
            levels.insert(lvl, ssts);
        }

        Ok(StoreSysMan {
            base_path,
            max_tables,
            sequence,
            rx: None,
            levels,
        })
    }

    pub fn get(&mut self, key: KeyRef) -> GhalaDbResult<Option<Bytes>> {
        for (level, ssts) in self.levels.iter_mut() {
            trace!("checking for key in level: {}", level);
            for sst in ssts.iter_mut() {
                if let Some(val) = sst.get(key)? {
                    match val {
                        ValueEntry::Val(s) => {
                            return Ok(Some(s));
                        }
                        ValueEntry::Tombstone => return Ok(None),
                    }
                }
            }
        }

        Ok(None)
    }

    fn build_sst_path(&mut self) -> GhalaDbResult<(SequenceNumber, PathBuf)> {
        self.sequence += 1;
        let sst_name = format!("{}.sst", self.sequence);
        Ok((self.sequence, self.base_path.join(sst_name)))
    }

    pub fn flush_mem_table(&mut self, table: impl MemTable) -> GhalaDbResult<PathBuf> {
        debug_assert!(!table.is_empty(), "cannot flush empty mem table");

        let (seq_num, sst_path) = self.build_sst_path()?;
        debug_assert!(
            !sst_path.exists(),
            "sst path already exists: {}",
            sst_path.display()
        );
        debug!(
            "flushig mem table with sequence: {} to {}",
            seq_num,
            sst_path.display()
        );
        let mut wtr = SSTableWriter::new(&sst_path, seq_num)?;
        for entry in table.into_iter() {
            let (k, v) = entry?;
            wtr.write(k.to_vec(), v.clone())?;
        }
        let sst = wtr.into_sstable().map_err(|e| {
            debug!("got error when writing sst: {:#?}", e);
            e
        })?;
        trace!("got sst: {:?}", sst);
        let ssts = self.levels.entry(0usize).or_default();
        ssts.push_front(sst);

        self.shrink()?;

        Ok(sst_path)
    }

    fn shrink(&mut self) -> GhalaDbResult<()> {
        if self.rx.is_some() {
            return self.handle_compaction();
        }
        if let Some(lvl) = self.check_full_level() {
            debug!("compacting ssts in level: {}", lvl);
            debug_assert!(self.rx.is_none(), "compaction already ongoing");
            let (seq_num, sst_path) = self.build_sst_path()?;
            let ssts = self.levels.get_mut(&lvl).unwrap();

            let (tx, rx) = channel();
            let mut ssts_to_compact = vec![];
            let mut seq_nums = HashSet::new();
            for sst in ssts {
                ssts_to_compact.push(sst.iter()?);
                seq_nums.insert(sst.seq_num);
            }
            thread::spawn(move || {
                Self::compact(lvl, ssts_to_compact, seq_nums, tx, sst_path, seq_num)
                    .tap(|r| debug!("compaction result: {:?}", r))
            });
            self.rx = Some(rx);
        }
        Ok(())
    }

    fn check_full_level(&self) -> Option<usize> {
        self.levels
            .iter()
            .filter(|(_, ssts)| ssts.len() > self.max_tables)
            .map(|(lvl, _ssts)| *lvl)
            .next()
    }

    pub fn iter(&self) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>> {
        let mut accum: Box<dyn Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>> =
            Box::new(vec![].into_iter());
        for ssts in self.levels.values() {
            for sst in ssts {
                let inter = merge_iter(accum, sst.iter()?);
                accum = Box::new(inter);
            }
        }

        Ok(accum)
    }
    fn handle_compaction(&mut self) -> GhalaDbResult<()> {
        if let Some(ref rx) = self.rx {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Err(recv_err) => match recv_err {
                    RecvTimeoutError::Disconnected => {
                        trace!("compaction thread disconnected");
                        Err(GhalaDBError::CompactionError(
                            "compaction thread disconnected.".to_string(),
                        ))
                    }
                    RecvTimeoutError::Timeout => {
                        debug!("timed out waiting for compaction result");
                        Ok(())
                    }
                },
                Ok(CompactionResult {
                    level,
                    sst,
                    seq_nums,
                }) => {
                    debug!("got result from compaction thread");
                    trace!(
                        "adding sst at {} to level {}",
                        sst.path.display(),
                        level + 1
                    );
                    self.levels.entry(level + 1).or_default().push_front(sst);
                    self.levels.entry(level).or_default().retain_mut(|t| {
                        if seq_nums.contains(&t.seq_num) {
                            t.deactivate();
                            debug!("deactivated sst: {}", t.path.display());
                            false
                        } else {
                            true
                        }
                    });

                    self.rx = None;
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    fn merge_ssts(
        &self,
        new: &SSTable,
        old: &SSTable,
        sst_path: PathBuf,
        seq_num: u64,
    ) -> GhalaDbResult<SSTable> {
        trace!("merging ssts");
        let new = new.iter()?;
        let old = old.iter()?;
        let merged = merge_iter(new, old);

        let mut sst_wtr = SSTableWriter::new(&sst_path, seq_num)?;
        for entry in merged {
            let (k, v) = entry?;
            sst_wtr.write(k.to_vec(), v.clone())?;
        }
        let sst = sst_wtr.into_sstable()?;

        Ok(sst)
    }
    fn compact(
        level: usize,
        ssts: Vec<SSTableIter>,
        seq_nums: HashSet<SequenceNumber>,
        tx: Sender<CompactionResult>,
        path: PathBuf,
        seq_num: u64,
    ) -> GhalaDbResult<()> {
        let mut merged: Box<dyn Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>> =
            Box::new(vec![].into_iter());
        for sst in ssts {
            let inter = merge_iter(merged, sst);
            merged = Box::new(inter);
        }
        let mut sst_wtr = SSTableWriter::new(&path, seq_num)?;
        for entry in merged {
            let (k, v) = entry?;
            sst_wtr.write(k.to_vec(), v.clone())?;
        }
        let sst = sst_wtr.into_sstable()?;

        debug!(
            "compacted ssts to {}. Total bytes: {}",
            path.display(),
            sst.mem_size()
        );

        tx.send(CompactionResult {
            level,
            sst,
            seq_nums,
        })
        .map_err(|e| GhalaDBError::SstSendError(e.to_string()))?;

        Ok(())
    }

    fn load_levels_info<P: AsRef<Path>>(path: P) -> GhalaDbResult<LevelsOnDisk> {
        if path.as_ref().exists() {
            let reader = BufReader::new(OpenOptions::new().read(true).open(&path)?);
            let info: LevelsOnDisk = serde_json::from_reader(reader)?;
            Ok(info)
        } else {
            Ok(LevelsOnDisk::new())
        }
    }

    fn dump_levels_info(&self) -> GhalaDbResult<()> {
        let lvl_info: LevelsOnDisk = self
            .levels
            .iter()
            .map(|(lvl, ssts)| (*lvl, ssts.iter().map(|t| t.path.clone()).collect()))
            .collect();
        debug!("dumping levels info: \n {:#?}", lvl_info);
        let path = self.base_path.join("lvl_info");
        let wtr = BufWriter::new(OpenOptions::new().write(true).create(true).open(path)?);
        serde_json::to_writer_pretty(wtr, &lvl_info)?;
        Ok(())
    }
}

impl Drop for StoreSysMan {
    fn drop(&mut self) {
        debug!("checking pending compaction before shutdown");
        while self.rx.is_some() {
            self.handle_compaction().ok();
        }
        debug!("persisting levels info on disk");
        self.dump_levels_info().ok();
    }
}

pub(crate) fn merge_iter<I, J>(
    lhs: I,
    rhs: J,
) -> impl Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>
where
    I: Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>,
    J: Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>,
{
    MergeSorted {
        lhs: lhs.peekable(),
        rhs: rhs.peekable(),
    }
}

struct MergeSorted<I, J>
where
    I: Iterator,
    J: Iterator,
{
    lhs: Peekable<I>,
    rhs: Peekable<J>,
}

impl<I: Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>, J> Iterator for MergeSorted<I, J>
where
    J: Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>,
{
    type Item = GhalaDbResult<(Bytes, ValueEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.lhs.peek(), self.rhs.peek()) {
            (Some(&Err(_)), _) => self.lhs.next(),
            (_, Some(&Err(_))) => self.rhs.next(),
            (Some(Ok(ref l)), Some(Ok(ref r))) if l.0 == r.0 => {
                _ = self.rhs.next();
                self.lhs.next()
            }
            (Some(Ok(ref l)), Some(Ok(ref r))) if l.0 < r.0 => self.lhs.next(),
            (Some(&Ok(_)), Some(&Ok(_))) => self.rhs.next(),
            (Some(&Ok(_)), None) => self.lhs.next(),
            (None, Some(&Ok(_))) => self.rhs.next(),
            (None, None) => None,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::BTreeMemTable;
    use rand::{
        distributions::{Alphanumeric, DistString},
        prelude::ThreadRng,
        thread_rng,
    };
    use tempdir::TempDir;
    fn gen_string(rng: &mut ThreadRng, len: usize) -> String {
        Alphanumeric {}.sample_string(rng, len)
    }

    fn gen_tmp_dir() -> GhalaDbResult<TempDir> {
        let mut rng = thread_rng();
        Ok(TempDir::new(&gen_string(&mut rng, 16))?)
    }

    fn dummy_vals() -> Vec<(Bytes, Bytes)> {
        let vals = [
            "Mike Tyson",
            "Deontay Wilder",
            "Anthony Joshua",
            "Muhammad Ali",
            "Vladimir Klitschko",
        ];
        vals.iter().map(|b| ((*b).into(), (*b).into())).collect()
    }

    #[test]
    fn sst_serde() -> GhalaDbResult<()> {
        let tmp_dir = gen_tmp_dir()?;
        let mut mt = BTreeMemTable::new();
        for (k, v) in dummy_vals() {
            mt.insert(k, v)
        }
        mt.delete("Tyson Fury".as_bytes().to_vec());

        let mut ssm = StoreSysMan::new(tmp_dir.path(), 1)?;
        let sst_path = ssm.flush_mem_table(mt.clone())?;
        let loaded_sst = SSTable::from_path(sst_path)?;
        let mt2 = loaded_sst.as_memtable()?;
        assert_eq!(mt, mt2);
        Ok(())
    }
}
