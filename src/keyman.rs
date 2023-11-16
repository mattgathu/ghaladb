use crate::{
    config::DatabaseOptions,
    core::{Bytes, DataPtr, KeyRef, ValueEntry},
    error::{GhalaDBError, GhalaDbResult},
    memtable::{BTreeMemTable, MemTable},
    sstable::{SSTable, SSTableIter, SSTableWriter},
    utils::t,
};
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
use tap::tap::Tap;

#[derive(Debug)]
pub(crate) struct KeyMan {
    mem: BTreeMemTable,
    ssm: StoreSysMan,
    conf: DatabaseOptions,
}

impl KeyMan {
    pub fn new(path: &Path, conf: DatabaseOptions) -> GhalaDbResult<KeyMan> {
        let ssm = StoreSysMan::new(path, 10)?;
        let mem = BTreeMemTable::new();
        Ok(Self { mem, ssm, conf })
    }

    pub fn get(&mut self, key: KeyRef) -> GhalaDbResult<Option<DataPtr>> {
        if let Some(entry) = self.mem.get(key) {
            match entry {
                ValueEntry::Tombstone => Ok(None),
                ValueEntry::Val(dp) => Ok(Some(dp)),
            }
        } else {
            self.ssm.get(key)
        }
    }

    pub fn get_ve(&mut self, key: KeyRef) -> GhalaDbResult<ValueEntry> {
        if let Some(entry) = self.mem.get(key) {
            Ok(entry)
        } else {
            self.ssm.get_ve(key)
        }
    }

    pub fn delete(&mut self, key: Bytes) -> GhalaDbResult<()> {
        self.mem.delete(key);
        Ok(())
    }

    pub fn put(&mut self, key: Bytes, val: DataPtr) -> GhalaDbResult<()> {
        if self.mem_at_capacity(key.len() + val.mem_sz()) {
            self.flush_mem()?;
        }
        self.mem.insert(key, val);
        Ok(())
    }
    pub fn iter(
        &self,
    ) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>>
    {
        let mem_iter = self.mem.iter();
        let ssm_iter = self.ssm.iter()?;
        let merged = merge_iter(mem_iter, ssm_iter);
        Ok(Box::new(merged))
    }
    /// Attempts to sync in-memory data to disk.
    pub fn sync(&mut self) -> GhalaDbResult<()> {
        if !self.mem.is_empty() {
            t!("keyman::flush_mem", self.flush_mem())?;
        }
        t!("ssm::sync", self.ssm.sync())?;
        Ok(())
    }
    fn flush_mem(&mut self) -> GhalaDbResult<()> {
        if self.mem.is_empty() {
            warn!("got empty memtable to flush.");
            return Ok(());
        }
        debug!("flushing mem table");

        let mut mem_table = BTreeMemTable::new();
        std::mem::swap(&mut self.mem, &mut mem_table);
        let path = self.ssm.flush_mem_table(mem_table)?;
        debug!("flushed mem table to: {:?}", path);

        Ok(())
    }
    fn mem_at_capacity(&self, kv_size: usize) -> bool {
        if self.mem.is_empty() {
            return false;
        }
        self.mem.mem_size() + kv_size > self.conf.max_mem_table_size
    }
}
impl Drop for KeyMan {
    fn drop(&mut self) {
        t!("keyman::sync", self.sync()).ok();
    }
}
type LevelsOnDisk = BTreeMap<usize, Vec<PathBuf>>;
type Levels = BTreeMap<usize, VecDeque<SSTable>>;
type SequenceNumber = u64;
struct CompactionResult {
    level: usize,
    sst: SSTable,
    seq_nums: HashSet<SequenceNumber>,
}

#[derive(Debug)]
pub(crate) struct StoreSysMan {
    base_path: PathBuf,
    max_tables: usize,
    sequence: SequenceNumber,
    rx: Option<Receiver<CompactionResult>>,
    levels: Levels,
}

impl StoreSysMan {
    pub fn new<P: AsRef<Path>>(
        path: P,
        max_tables: usize,
    ) -> GhalaDbResult<StoreSysMan> {
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
            sequence = std::cmp::max(
                sequence,
                ssts.iter().map(|t| t.seq_num).max().unwrap_or(0),
            );
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

    pub fn get(&mut self, key: KeyRef) -> GhalaDbResult<Option<DataPtr>> {
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
    pub fn get_ve(&mut self, key: KeyRef) -> GhalaDbResult<ValueEntry> {
        for (_level, ssts) in self.levels.iter_mut() {
            for sst in ssts.iter_mut() {
                if let Some(val) = sst.get(key)? {
                    return Ok(val);
                }
            }
        }
        Err(GhalaDBError::MissingValueEntry(key.to_vec()))
    }
    fn build_sst_path(&mut self) -> GhalaDbResult<(SequenceNumber, PathBuf)> {
        self.sequence += 1;
        let sst_name = format!("{}.sst", self.sequence);
        Ok((self.sequence, self.base_path.join(sst_name)))
    }

    pub fn flush_mem_table(
        &mut self,
        table: impl MemTable,
    ) -> GhalaDbResult<PathBuf> {
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

    pub fn iter(
        &self,
    ) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>>
    {
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
    pub fn sync(&mut self) -> GhalaDbResult<()> {
        debug!("checking pending compaction before shutdown");
        while self.rx.is_some() {
            self.handle_compaction()?;
        }
        debug!("persisting levels info on disk");
        self.dump_levels_info()?;
        Ok(())
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
        let mut merged: Box<
            dyn Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>,
        > = Box::new(vec![].into_iter());
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
        let wtr =
            BufWriter::new(OpenOptions::new().write(true).create(true).open(path)?);
        serde_json::to_writer_pretty(wtr, &lvl_info)?;
        Ok(())
    }
}

impl Drop for StoreSysMan {
    fn drop(&mut self) {
        t!("ssm::drop", self.sync()).ok();
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

impl<I: Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>, J> Iterator
    for MergeSorted<I, J>
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
    use crate::core::FixtureGen;
    use tempfile::tempdir;

    #[test]
    fn read_after_compaction() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let opts = DatabaseOptions::builder().max_mem_table_size(1000).build();
        let mut kman = KeyMan::new(tmp_dir.path(), opts.clone())?;
        let data: Vec<Bytes> = (0..1000).map(|_| Bytes::gen()).collect();
        for k in &data {
            let dp = DataPtr::new(0, k.len() as u64, k.len() as u32, false);
            kman.put(k.clone(), dp)?;
        }
        drop(kman);
        let mut kman = KeyMan::new(tmp_dir.path(), opts)?;
        for k in &data {
            assert!(kman.get(k)?.is_some());
        }
        Ok(())
    }
}
