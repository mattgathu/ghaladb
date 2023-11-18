#[cfg(test)]
use crate::core::FixtureGen;
use crate::{
    config::DatabaseOptions,
    core::{DataEntrySz, DataPtr, VlogNum},
    error::{GhalaDBError, GhalaDbResult},
    utils::t,
};
#[allow(unused_imports)]
use contracts::*;
use serde::{Deserialize, Serialize};
use snap::raw::{Decoder, Encoder};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

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
}
#[cfg(test)]
impl FixtureGen<DataEntry> for DataEntry {
    fn gen() -> DataEntry {
        DataEntry {
            key: Bytes::gen(),
            val: Bytes::gen(),
        }
    }
}

pub(crate) struct Vlog {
    rdr: BufReader<File>,
    wtr: BufWriter<File>,
    num: VlogNum,
    w_off: u64,
    buf: Vec<(DataPtr, Bytes)>,
    conf: VlogConfig,
    buf_sz: usize,
    path: PathBuf,
    active: bool,
    encoder: Option<Encoder>,
    decoder: Option<Decoder>,
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
        let (encoder, decoder) = if conf.compress {
            (Some(Encoder::new()), Some(Decoder::new()))
        } else {
            (None, None)
        };
        Vlog {
            rdr,
            wtr,
            num,
            w_off: offset,
            buf: vec![],
            conf,
            buf_sz: 0usize,
            path,
            active: true,
            encoder,
            decoder,
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

    #[debug_requires(self.active, "vlog not active")]
    fn deactivate(&mut self) {
        self.active = false;
    }

    // TODO: can we cache hot entries in mem
    fn get(&mut self, dp: &DataPtr) -> GhalaDbResult<DataEntry> {
        if let Some(de) = self.get_from_buf(dp)? {
            return Ok(de);
        }
        self.get_from_disk(dp)
    }

    fn get_from_buf(&mut self, dp: &DataPtr) -> GhalaDbResult<Option<DataEntry>> {
        if let Ok(index) = self
            .buf
            .binary_search_by(|item| item.0.offset.cmp(&dp.offset))
        {
            let (_dp, de_bytes) = self.buf.get(index).cloned().unwrap();
            let de = self.de(&de_bytes)?;
            Ok(Some(de))
        } else {
            Ok(None)
        }
    }

    // requires that dp not in buf
    fn get_from_disk(&mut self, dp: &DataPtr) -> GhalaDbResult<DataEntry> {
        let mut buf = vec![0u8; dp.len as usize];
        self.rdr.seek(SeekFrom::Start(dp.offset))?;
        self.rdr.read_exact(&mut buf)?;
        t!("vlog::de", self.de(&buf))
    }

    #[debug_invariant(self.buf_entries_sorted())]
    #[debug_ensures(self.w_off > old(self.w_off), "w_off did not inc")]
    fn write_to_buf(&mut self, de: &DataEntry) -> GhalaDbResult<DataPtr> {
        let offset = self.w_off;
        let de_bytes = self.ser(de)?;
        let dp_sz = DataPtr::serde_sz() as u64;
        //TODO: write to disk if buffer too small
        if self.buf_sz + de_bytes.len() > self.conf.mem_buf_size
            && !self.buf.is_empty()
        {
            self.flush()?;
        }
        let dp = DataPtr::new(
            self.num,
            offset + dp_sz,
            de_bytes.len() as u32,
            self.conf.compress,
        );
        self.buf_sz += de_bytes.len() + dp_sz as usize;
        self.w_off += dp_sz + de_bytes.len() as u64;
        self.buf.push((dp, de_bytes));

        Ok(dp)
    }

    fn put(&mut self, entry: &DataEntry) -> GhalaDbResult<DataPtr> {
        if self.conf.mem_buf_enabled {
            let dp = self.write_to_buf(entry)?;
            debug_assert!(self.buf_has_dp(&dp));
            Ok(dp)
        } else {
            let dp = t!("vlog::write_entry", self.write_de(entry))?;
            self.wtr.flush()?;
            Ok(dp)
        }
    }

    fn size(&self) -> usize {
        self.w_off as usize
    }

    #[debug_ensures(self.buf.is_empty(), "buffer not flushed")]
    #[debug_ensures(self.buf_sz == 0, "buffer size not reset")]
    fn flush(&mut self) -> GhalaDbResult<()> {
        for (dp, de_bytes) in &self.buf {
            // write to file
            let dp_offset = dp.offset - DataPtr::serde_sz() as u64;
            let s_pos = self.wtr.stream_position().ok();
            debug_assert!(Some(dp_offset) == s_pos, "offset do not match");
            self.wtr.seek(SeekFrom::Start(dp_offset))?;
            self.wtr.write_all(&bincode::serialize(dp)?)?;
            self.wtr.write_all(de_bytes)?;
        }
        self.wtr.flush()?;
        self.buf.clear();
        self.buf_sz = 0;
        Ok(())
    }

    fn ser(&mut self, de: &DataEntry) -> GhalaDbResult<Bytes> {
        let de_bytes = bincode::serialize(de)?;
        let ret = if let Some(ref mut enc) = self.encoder {
            enc.compress_vec(&de_bytes)?
        } else {
            de_bytes
        };
        Ok(ret)
    }

    fn de(&mut self, buf: &[u8]) -> GhalaDbResult<DataEntry> {
        let de: DataEntry = if let Some(ref mut dec) = self.decoder {
            bincode::deserialize(&dec.decompress_vec(buf)?)?
        } else {
            bincode::deserialize(buf)?
        };
        Ok(de)
    }

    fn write_de(&mut self, de: &DataEntry) -> GhalaDbResult<DataPtr> {
        let offset = self.w_off;
        let de_bytes = self.ser(de)?;
        let dp_sz = DataPtr::serde_sz() as u64;
        let dp = DataPtr::new(
            self.num,
            offset + dp_sz,
            de_bytes.len() as u32,
            self.conf.compress,
        );
        let dp_bytes = bincode::serialize(&dp)?;
        self.wtr.write_all(&dp_bytes)?;
        self.wtr.write_all(&de_bytes)?;
        self.w_off += (de_bytes.len() + dp_bytes.len()) as u64;

        Ok(dp)
    }

    #[cfg(debug_assertions)]
    fn buf_entries_sorted(&self) -> bool {
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

    #[cfg(debug_assertions)]
    fn buf_has_dp(&self, dp: &DataPtr) -> bool {
        self.buf.binary_search_by_key(dp, |(odp, _)| *odp).is_ok()
    }

    #[debug_requires(!self.active, "cannot del active vlog")]
    #[debug_ensures(!self.path.exists())]
    fn delete(&self) -> GhalaDbResult<()> {
        debug!("deleting vlog at: {}", self.path.display(),);
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}
impl Drop for Vlog {
    fn drop(&mut self) {
        if self.conf.mem_buf_enabled {
            debug!("flushing mem buf");
            self.flush().ok();
            debug_assert!(self.buf.is_empty(), "buf not empty at drop");
        }
        if !self.active {
            t!("vlog::drop", self.delete()).ok();
        }
    }
}
pub(crate) struct VlogIter {
    rdr: BufReader<File>,
    decoder: Decoder,
}
impl VlogIter {
    pub fn from_path(path: &Path) -> GhalaDbResult<Self> {
        let rdr = BufReader::new(OpenOptions::new().read(true).open(path)?);
        let decoder = Decoder::new();
        Ok(Self { rdr, decoder })
    }
    fn read_de(
        &mut self,
        sz: DataEntrySz,
        compressed: bool,
    ) -> GhalaDbResult<DataEntry> {
        let mut buf = vec![0u8; sz as usize];
        self.rdr.read_exact(&mut buf)?;
        let buf = if compressed {
            self.decoder.decompress_vec(&buf)?
        } else {
            buf
        };
        Ok(bincode::deserialize(&buf)?)
    }
    fn read_dp(&mut self) -> GhalaDbResult<Option<DataPtr>> {
        let dp_sz = DataPtr::serde_sz();
        let mut buf = vec![0u8; dp_sz];
        let res = self.rdr.read_exact(&mut buf);
        if let Err(e) = res {
            //TODO: is it better to track offset and know the EOF
            // and avoid handling this error
            // this error might be due to something else
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
            let de = self.read_de(dp.len, dp.compressed)?;
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
    compress: bool,
}
impl From<&DatabaseOptions> for VlogConfig {
    fn from(opts: &DatabaseOptions) -> Self {
        Self {
            mem_buf_enabled: opts.vlog_mem_buf_enabled,
            mem_buf_size: opts.vlog_mem_buf_size,
            compress: opts.compress,
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

    #[debug_ensures(!self.vlogs.contains_key(&vnum))]
    pub fn drop_vlog(&mut self, vnum: VlogNum) -> GhalaDbResult<()> {
        if let Some(mut vlog) = self.vlogs.remove(&vnum) {
            vlog.deactivate();
        } else {
            error!("vlog: {vnum} not found when dropping");
        }

        Ok(())
    }

    #[debug_ensures(self.base_path.join(VLOG_INFO_FILE).exists())]
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
    pub fn put(&mut self, entry: &DataEntry) -> GhalaDbResult<DataPtr> {
        let vlog = self.get_tail()?;
        vlog.put(entry)
    }

    //TODO: find better heuristic
    //
    // 1: Use age threshold
    //
    // The basic idea of our algorithm is that segments that have been recently
    // filled by writes from the system should be forced to wait for a certain
    // amount of time (the age-threshold) before they are allowed to become
    // candidates for garbage collect
    //
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
            t!("vlog::flush_buf", vlog.flush())?;
        }
        Ok(())
    }

    fn get_tail(&mut self) -> GhalaDbResult<&mut Vlog> {
        if let Some(vlog) = self.vlogs.get_mut(&self.seq) {
            if vlog.size() > self.conf.max_vlog_size {
                vlog.flush()?;
                self.seq += 1;
                let next_vlog = self.create_new_vlog()?;
                Ok(self.vlogs.entry(self.seq).or_insert(next_vlog))
            } else {
                Ok(self.vlogs.get_mut(&self.seq).unwrap())
            }
        } else {
            let vlog = self.create_new_vlog()?;
            Ok(self.vlogs.entry(self.seq).or_insert(vlog))
        }
    }

    #[debug_requires(!self.vlogs.contains_key(&self.seq))]
    fn create_new_vlog(&self) -> GhalaDbResult<Vlog> {
        let path = self.base_path.join(format!("{}.vlog", self.seq));
        let conf = VlogConfig::from(&self.conf);
        let vlog = Vlog::from_path(path, self.seq, conf)?;
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
    use tempfile::{tempdir, TempDir};
    fn init_vlog(temp_dir: &TempDir) -> GhalaDbResult<Vlog> {
        let file_path = temp_dir.path().join("test_vlog.db");
        let conf = VlogConfig {
            mem_buf_enabled: true,
            mem_buf_size: 1024,
            compress: true,
        };

        Vlog::from_path(file_path.clone(), 1, conf)
    }

    #[test]
    fn vlog_iter() -> GhalaDbResult<()> {
        let tmp_dir = tempdir()?;
        let path = tmp_dir.path().join("1.vlog");
        let conf = VlogConfig {
            mem_buf_size: 1_000_000,
            mem_buf_enabled: true,
            compress: true,
        };
        let mut vlog = Vlog::from_path(path.clone(), 1, conf)?;
        let data: Vec<DataEntry> = (0..100)
            .map(|_| DataEntry::new(Bytes::gen(), Bytes::gen()))
            .collect();
        for de in &data {
            vlog.put(de)?;
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

    #[test]
    fn vlog_write_and_read() -> GhalaDbResult<()> {
        let mut vlog = init_vlog(&tempdir()?)?;
        let test_entry = DataEntry {
            key: vec![1, 2, 3],
            val: vec![4, 5, 6],
        };
        let data_ptr = vlog.put(&test_entry)?;
        let read_entry = vlog.get(&data_ptr)?;
        assert_eq!(read_entry, test_entry);
        Ok(())
    }

    #[test]
    fn vlog_flush() -> GhalaDbResult<()> {
        let mut vlog = init_vlog(&tempdir()?)?;
        let test_entry = DataEntry {
            key: vec![1, 2, 3],
            val: vec![4, 5, 6],
        };
        vlog.put(&test_entry)?;
        vlog.flush()?;
        Ok(())
    }

    #[test]
    fn vlog_deactivate() -> GhalaDbResult<()> {
        let temp_dir = tempdir()?;
        let mut vlog = init_vlog(&temp_dir)?;
        vlog.deactivate();
        drop(vlog);
        assert!(!temp_dir.path().join("test_vlog.db").exists());
        Ok(())
    }

    #[test]
    fn vlog_flush_buf() -> GhalaDbResult<()> {
        let mut vlog = init_vlog(&tempdir()?)?;

        vlog.write_to_buf(&DataEntry::gen())?;
        vlog.write_to_buf(&DataEntry::gen())?;
        vlog.flush()?;

        assert!(vlog.buf.is_empty());
        assert_eq!(vlog.buf_sz, 0);
        Ok(())
    }
}