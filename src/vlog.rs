use crate::{
    config::DatabaseOptions,
    core::{DataEntrySz, DataPtr, VlogNum},
    utils::t,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::error::{GhalaDBError, GhalaDbResult};

const VLOG_INFO_FILE: &str = "vlog_info";

pub type Bytes = Vec<u8>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValEntry {
    Tombstone,
    Value(Bytes),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataEntry {
    pub key: Bytes,
    pub val: Bytes,
}
impl DataEntry {
    pub fn new(key: Bytes, val: Bytes) -> DataEntry {
        Self { key, val }
    }
    pub fn mem_sz(&self) -> usize {
        std::mem::size_of_val(self)
    }
    pub fn serde_sz(&self) -> usize {
        //TODO: test bicode 16 extra bytes thing
        //or use mem size
        self.key.len() + self.val.len() + 16
    }
}

pub(crate) struct Vlog {
    rdr: BufReader<File>,
    wtr: BufWriter<File>,
    num: VlogNum,
    write_offset: u64,
    buf: Vec<(DataPtr, DataEntry)>,
    conf: VlogConfig,
    buf_size: usize,
    path: PathBuf,
    active: bool,
}

impl Vlog {
    fn new(
        rdr: BufReader<File>,
        wtr: BufWriter<File>,
        num: VlogNum,
        offset: u64,
        conf: VlogConfig,
        path: PathBuf,
    ) -> Vlog {
        Vlog {
            rdr,
            wtr,
            num,
            write_offset: offset,
            buf: vec![],
            conf,
            buf_size: 0usize,
            path,
            active: true,
        }
    }

    fn from_path(
        path: PathBuf,
        num: VlogNum,
        conf: VlogConfig,
    ) -> GhalaDbResult<Vlog> {
        let mut wtr = BufWriter::new(
            OpenOptions::new().create(true).append(true).open(&path)?,
        );
        let rdr = BufReader::new(OpenOptions::new().read(true).open(&path)?);
        wtr.seek(SeekFrom::End(0))?;
        let offset = wtr.stream_position()?;
        Ok(Vlog::new(rdr, wtr, num, offset, conf, path))
    }

    fn deactivate(&mut self) {
        self.active = false;
    }

    // TODO: can we cache hot entries in mem
    fn get(&mut self, dp: &DataPtr) -> GhalaDbResult<DataEntry> {
        if let Some(de) = self.get_from_buf(dp) {
            return Ok(de);
        }
        let mut buf = vec![0u8; dp.len as usize];
        self.rdr.seek(SeekFrom::Start(dp.offset))?;
        self.rdr.read_exact(&mut buf)?;
        let data = bincode::deserialize(&buf)?;
        Ok(data)
    }

    fn get_from_buf(&self, dp: &DataPtr) -> Option<DataEntry> {
        if let Ok(index) = self
            .buf
            .binary_search_by(|item| item.0.offset.cmp(&dp.offset))
        {
            let (_dp, de) = self.buf.get(index).unwrap();
            Some(de.clone())
        } else {
            None
        }
    }

    fn write_to_buf(&mut self, de: DataEntry) -> GhalaDbResult<DataPtr> {
        debug_assert!(self.conf.mem_buf_enabled, "mem buf not enabled");
        let de_sz = de.serde_sz();
        if self.buf_size + de_sz > self.conf.mem_buf_size {
            self.flush_buf()?;
        }
        let dp_sz = DataPtr::serde_sz() as u64;
        let offset = self.write_offset;
        self.write_offset += dp_sz + de_sz as u64;
        let dp = DataPtr::new(self.num, offset + dp_sz, de_sz as u32);
        self.buf_size += de.mem_sz() + dp.mem_sz();
        self.buf.push((dp, de));
        debug_assert!(self.buf_invariant_ok(), "buf invariant violated");

        Ok(dp)
    }

    fn put(&mut self, entry: DataEntry) -> GhalaDbResult<DataPtr> {
        if self.conf.mem_buf_enabled {
            self.write_to_buf(entry)
        } else {
            let dp = t!("vlog::write_entry", self.write_entry(&entry, None))?;
            self.wtr.flush()?;
            Ok(dp)
        }
    }

    fn size(&self) -> usize {
        self.write_offset as usize
    }

    fn flush_buf(&mut self) -> GhalaDbResult<()> {
        let mut buf = vec![];
        std::mem::swap(&mut buf, &mut self.buf);
        for (dp, de) in buf {
            self.write_entry(&de, Some(dp))?;
        }
        self.wtr.flush()?;
        debug_assert!(self.buf.is_empty(), "buf not empty after flush");
        Ok(())
    }

    fn write_entry(
        &mut self,
        de: &DataEntry,
        dp_opt: Option<DataPtr>,
    ) -> GhalaDbResult<DataPtr> {
        let de_bytes = bincode::serialize(&de)?;
        let de_sz: DataEntrySz = de_bytes.len() as u32;
        let dp_sz = DataPtr::serde_sz() as u64;

        let dp = dp_opt.unwrap_or_else(|| {
            DataPtr::new(self.num, self.write_offset + dp_sz, de_sz)
        });
        let dp_bytes = bincode::serialize(&dp)?;

        let offset = dp.offset - dp_sz;
        self.wtr.seek(SeekFrom::Start(offset))?;
        self.wtr.write_all(&dp_bytes)?;
        self.wtr.write_all(&de_bytes)?;
        self.write_offset += dp_sz;
        self.write_offset += de_bytes.len() as u64;
        Ok(dp)
    }

    fn buf_invariant_ok(&self) -> bool {
        // check buf entries as sorted by dp offset
        let mut prev = None;
        for item in &self.buf {
            let cur = item.0.offset;
            if let Some(ofst) = prev {
                if cur <= ofst {
                    return false;
                }
            }
            prev = Some(cur)
        }
        true
    }
    fn delete(&self) -> GhalaDbResult<()> {
        debug_assert!(!self.active, "cannot del active vlog");
        debug!("deleting vlog at: {}", self.path.display(),);
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}
impl Drop for Vlog {
    fn drop(&mut self) {
        if self.conf.mem_buf_enabled {
            debug!("flushing mem buf");
            self.flush_buf().ok();
            debug_assert!(self.buf.is_empty(), "buf not empty at drop");
        }
        if !self.active {
            t!("vlog::drop", self.delete()).ok();
        }
    }
}
pub(crate) struct VlogIter {
    rdr: BufReader<File>,
}
impl VlogIter {
    pub fn from_path(path: &Path) -> GhalaDbResult<Self> {
        let rdr = BufReader::new(OpenOptions::new().read(true).open(path)?);
        Ok(Self { rdr })
    }
    fn read_de(&mut self, sz: DataEntrySz) -> GhalaDbResult<DataEntry> {
        let mut buf = vec![0u8; sz as usize];
        self.rdr.read_exact(&mut buf)?;
        let de: DataEntry = bincode::deserialize(&buf)?;
        Ok(de)
    }
    fn read_dp(&mut self) -> GhalaDbResult<Option<DataPtr>> {
        let dp_sz = DataPtr::serde_sz();
        let mut buf = vec![0u8; dp_sz];
        let res = self.rdr.read_exact(&mut buf);
        if let Err(e) = res {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            } else {
                return Err(GhalaDBError::IOError(e));
            }
        }
        let dp: DataPtr = bincode::deserialize(&buf)?;
        Ok(Some(dp))
    }
    pub fn next_entry(&mut self) -> GhalaDbResult<Option<(DataPtr, DataEntry)>> {
        if let Some(dp) = self.read_dp()? {
            let de = self.read_de(dp.len)?;
            Ok(Some((dp, de)))
        } else {
            Ok(None)
        }
    }
}
impl Iterator for VlogIter {
    type Item = GhalaDbResult<(DataPtr, DataEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.next_entry() {
            Ok(None) => None,
            Ok(Some(val)) => Some(Ok(val)),
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VlogConfig {
    mem_buf_enabled: bool,
    mem_buf_size: usize,
}
impl From<&DatabaseOptions> for VlogConfig {
    fn from(opts: &DatabaseOptions) -> Self {
        Self {
            mem_buf_enabled: opts.vlog_mem_buf_enabled,
            mem_buf_size: opts.vlog_mem_buf_size,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VlogsInfo {
    vlogs: Vec<VlogNum>,
}

pub(crate) struct VlogsMan {
    base_path: PathBuf,
    vlogs: BTreeMap<VlogNum, Vlog>,
    seq: VlogNum,
    conf: DatabaseOptions,
}

impl VlogsMan {
    pub fn new(path: &Path, conf: DatabaseOptions) -> GhalaDbResult<VlogsMan> {
        let base_path = path.to_path_buf();
        let info = Self::load_vlogs_info(base_path.join(VLOG_INFO_FILE))?;
        let mut vlogs = BTreeMap::new();
        let mut seq = VlogNum::MIN;
        for vnum in info.vlogs {
            let lpath = base_path.join(format!("{}.vlog", vnum));
            let vlog = Vlog::from_path(lpath, vnum, VlogConfig::from(&conf))?;
            vlogs.insert(vnum, vlog);
            seq = std::cmp::max(vnum, seq);
        }
        Ok(VlogsMan {
            base_path,
            vlogs,
            seq,
            conf,
        })
    }
    pub fn drop_vlog(&mut self, vnum: VlogNum) -> GhalaDbResult<()> {
        if let Some(mut vlog) = self.vlogs.remove(&vnum) {
            vlog.deactivate();
        } else {
            error!("vlog: {vnum} not found when dropping");
        }

        Ok(())
    }

    fn dump_vlogs_info(&self) -> GhalaDbResult<()> {
        let path = self.base_path.join(VLOG_INFO_FILE);
        let wtr =
            BufWriter::new(OpenOptions::new().create(true).write(true).open(path)?);
        let info = VlogsInfo {
            vlogs: self.vlogs.keys().copied().collect(),
        };
        bincode::serialize_into(wtr, &info)?;
        Ok(())
    }

    fn load_vlogs_info(path: PathBuf) -> GhalaDbResult<VlogsInfo> {
        if path.exists() {
            let reader = BufReader::new(OpenOptions::new().read(true).open(&path)?);
            let info: VlogsInfo = bincode::deserialize_from(reader)?;
            Ok(info)
        } else {
            Ok(VlogsInfo { vlogs: vec![] })
        }
    }

    pub fn get(&mut self, dp: &DataPtr) -> GhalaDbResult<DataEntry> {
        let vlog = self
            .vlogs
            .get_mut(&dp.vlog)
            .ok_or_else(|| GhalaDBError::MissingVlog(dp.vlog))?;
        vlog.get(dp)
    }
    pub fn put(&mut self, entry: DataEntry) -> GhalaDbResult<DataPtr> {
        let vlog = self.get_tail()?;
        vlog.put(entry)
    }

    //TODO: find better heuristic
    pub fn needs_gc(&mut self) -> GhalaDbResult<Option<(VlogNum, PathBuf)>> {
        if self.vlogs.len() > 3 {
            let vnum = self.vlogs.keys().next().unwrap();
            let path = self.base_path.join(format!("{}.vlog", vnum));
            Ok(Some((*vnum, path)))
        } else {
            Ok(None)
        }
    }

    /// Attempts to sync in-memory data to disk.
    pub fn sync(&mut self) -> GhalaDbResult<()> {
        t!("vlogsman::dump_vlogs_info", self.dump_vlogs_info())?;
        for (_vnum, vlog) in self.vlogs.iter_mut() {
            t!("vlog::flush_buf", vlog.flush_buf())?;
        }
        Ok(())
    }

    fn get_tail(&mut self) -> GhalaDbResult<&mut Vlog> {
        if let Some(vlog) = self.vlogs.get_mut(&self.seq) {
            if vlog.size() > self.conf.max_vlog_size {
                vlog.flush_buf()?;
                self.seq += 1;
                debug_assert!(!self.vlogs.contains_key(&self.seq));
                let next_vlog = Self::create_new_vlog(
                    self.seq,
                    &self.base_path,
                    VlogConfig::from(&self.conf),
                )?;
                Ok(self.vlogs.entry(self.seq).or_insert(next_vlog))
            } else {
                Ok(self.vlogs.get_mut(&self.seq).unwrap())
            }
        } else {
            let vlog = Self::create_new_vlog(
                self.seq,
                &self.base_path,
                VlogConfig::from(&self.conf),
            )?;
            Ok(self.vlogs.entry(self.seq).or_insert(vlog))
        }
    }

    fn create_new_vlog(
        num: VlogNum,
        base_path: &Path,
        conf: VlogConfig,
    ) -> GhalaDbResult<Vlog> {
        debug!("creating new vlog: {num}");
        debug_assert!(base_path.exists(), "base path not found");
        let path = base_path.join(format!("{}.vlog", num));
        let vlog = Vlog::from_path(path, num, conf)?;
        Ok(vlog)
    }
}

impl Drop for VlogsMan {
    fn drop(&mut self) {
        t!("vlogsman::drop", self.sync()).ok();
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::core::FixtureGen;
    use tempfile::tempdir;

    #[test]
    fn vlog_iter() -> GhalaDbResult<()> {
        let tmp_dir = tempdir()?;
        let path = tmp_dir.path().join("1.vlog");
        let conf = VlogConfig {
            mem_buf_size: 1_000_000,
            mem_buf_enabled: true,
        };
        let mut vlog = Vlog::from_path(path.clone(), 1, conf)?;
        let data: Vec<DataEntry> = (0..100)
            .map(|_| DataEntry::new(Bytes::gen(), Bytes::gen()))
            .collect();
        for de in &data {
            vlog.put(de.clone())?;
        }
        drop(vlog);
        let vlog_iter = VlogIter::from_path(&path)?;
        let iter_data: Vec<DataEntry> = vlog_iter
            .into_iter()
            .map(|i| i.map(|(_dp, de)| de))
            .collect::<GhalaDbResult<Vec<DataEntry>>>()?;

        assert_eq!(data.len(), iter_data.len(), "data len not eq");
        for (l, r) in data.into_iter().zip(iter_data.into_iter()) {
            assert_eq!(
                l, r,
                "iter data does not match expected. Found: {:?}, Expected: {:?}",
                r, l
            );
        }

        Ok(())
    }
}
