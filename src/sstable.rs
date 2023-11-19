use crate::dec::Dec;
use contracts::*;
use patricia_tree::{map::IntoIter, GenericPatriciaMap};
use serde::{Deserialize, Serialize};
use std::{
    convert::TryInto,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    core::DataPtr,
    error::{GhalaDBError, GhalaDbResult},
};

use crate::core::{Bytes, KeyRef, ValueEntry};
const FOOTER_SIZE: i64 = 8;

pub type SstIndex = GenericPatriciaMap<Bytes, DiskEntry>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DiskEntry {
    Tombstone,
    Value { offset: u64, len: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstMetadata {
    index: SstIndex,
    seq_num: u64,
}

#[derive(Debug)]
pub(crate) struct SSTable {
    pub path: PathBuf,
    pub index: SstIndex,
    first_key: Bytes,
    last_key: Bytes,
    mem_size: usize,
    pub seq_num: u64,
    rdr: BufReader<File>,
    pub active: bool,
}

impl SSTable {
    #[debug_requires(!index.is_empty(), "sst index should not be empty")]
    pub fn new(
        path: PathBuf,
        index: SstIndex,
        seq_num: u64,
        rdr: BufReader<File>,
    ) -> SSTable {
        let first_key = index.keys().next().unwrap().to_owned();
        let last_key = index.keys().last().unwrap().to_owned();
        let mem_size: usize = index
            .iter()
            .map(|(k, v)| {
                let vlen = match v {
                    DiskEntry::Value { offset: _, len } => len,
                    DiskEntry::Tombstone => &0,
                };
                k.len() + vlen
            })
            .sum();
        SSTable {
            path,
            index,
            first_key,
            last_key,
            mem_size,
            seq_num,
            rdr,
            active: true,
        }
    }

    #[debug_requires(path.as_ref().exists())]
    pub fn from_path<P: AsRef<Path>>(path: P) -> GhalaDbResult<SSTable> {
        let meta = Self::load_meta(&path)?;
        let reader = Self::get_rdr(&path)?;

        Ok(SSTable::new(
            path.as_ref().to_path_buf(),
            meta.index,
            meta.seq_num,
            reader,
        ))
    }

    pub fn get(&mut self, key: KeyRef) -> GhalaDbResult<Option<ValueEntry>> {
        if self.key_is_covered(key) {
            match self.index.get(key) {
                Some(de) => match de {
                    DiskEntry::Tombstone => Ok(Some(ValueEntry::Tombstone)),
                    DiskEntry::Value { offset, len } => {
                        Ok(self.read_val(*offset, *len)?.map(ValueEntry::Val))
                    }
                },
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub fn mem_size(&self) -> usize {
        self.mem_size
    }

    #[debug_requires(self.active)]
    pub fn deactivate(&mut self) {
        self.active = false;
    }

    fn key_is_covered(&self, k: KeyRef) -> bool {
        k >= &self.first_key && k <= &self.last_key
    }

    fn get_rdr<P: AsRef<Path>>(path: P) -> GhalaDbResult<BufReader<File>> {
        Ok(BufReader::new(OpenOptions::new().read(true).open(path)?))
    }

    #[debug_requires(path.as_ref().exists())]
    fn load_meta<P: AsRef<Path>>(path: P) -> GhalaDbResult<SstMetadata> {
        let mut rdr = Self::get_rdr(&path)?;
        rdr.seek(SeekFrom::End(-FOOTER_SIZE))?;
        let mut buf = vec![];
        let _ = rdr.read_to_end(&mut buf)?;
        let footer: [u8; 8] = buf.try_into().map_err(|e| {
            let error = format!("Failed to read sst footer. Reason: {:?}", e);
            GhalaDBError::SstLoadError(error)
        })?;
        let meta_sz = usize::from_le_bytes(footer) as i64;

        let off_set = 0i64 - (meta_sz + FOOTER_SIZE);
        rdr.seek(SeekFrom::End(off_set))?;
        let mut buf = vec![0u8; meta_sz as usize];
        rdr.read_exact(&mut buf)?;
        let meta: SstMetadata = Dec::deser_raw(&buf).map_err(|e| {
            error!("failed to deser sst metadata. Reason: {:?}", e);
            e
        })?;

        Ok(meta)
    }

    #[inline]
    fn load_index(p: &Path) -> GhalaDbResult<SstIndex> {
        Self::load_meta(p).map(|m| m.index)
    }

    fn read_val(
        &mut self,
        offset: u64,
        len: usize,
    ) -> GhalaDbResult<Option<DataPtr>> {
        let bytes = Self::read_val_inner(&mut self.rdr, offset, len)?;
        let dp: DataPtr = Dec::deser_raw(&bytes)?;
        Ok(Some(dp))
    }

    fn read_val_inner(
        rdr: &mut BufReader<File>,
        offset: u64,
        len: usize,
    ) -> GhalaDbResult<Bytes> {
        rdr.seek(SeekFrom::Start(offset))?;
        let mut v = vec![0u8; len];
        rdr.read_exact(&mut v)?;
        Ok(v)
    }

    pub(crate) fn iter(&self) -> GhalaDbResult<SSTableIter> {
        let index = Self::load_index(&self.path)?;
        let buf = Self::get_rdr(&self.path)?;
        SSTableIter::new(buf, index)
    }

    #[debug_requires(!self.active, "cannot del active sst")]
    #[debug_ensures(!self.path.exists())]
    fn delete(&self) -> GhalaDbResult<()> {
        debug!(
            "deleting sst at: {} active: {}",
            self.path.display(),
            self.active
        );
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}

impl Drop for SSTable {
    fn drop(&mut self) {
        if !self.active {
            self.delete()
                .map_err(|e| {
                    error!("failed to delete sst. Reason: {:#?}", e);
                    e
                })
                .ok();
        }
    }
}

pub(crate) struct SSTableIter {
    buf: BufReader<File>,
    index: IntoIter<Bytes, DiskEntry>,
}
impl SSTableIter {
    pub fn new(buf: BufReader<File>, index: SstIndex) -> GhalaDbResult<SSTableIter> {
        Ok(SSTableIter {
            buf,
            index: index.into_iter(),
        })
    }
    fn read_val(&mut self, offset: u64, len: usize) -> GhalaDbResult<ValueEntry> {
        let bytes = SSTable::read_val_inner(&mut self.buf, offset, len)?;
        let dp: DataPtr = Dec::deser_raw(&bytes)?;
        Ok(ValueEntry::Val(dp))
    }
}
impl Iterator for SSTableIter {
    type Item = GhalaDbResult<(Bytes, ValueEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, de)) = self.index.next() {
            match de {
                DiskEntry::Tombstone => Some(Ok((k, ValueEntry::Tombstone))),
                DiskEntry::Value { offset, len } => {
                    let kv = self.read_val(offset, len).map(|v| (k, v));
                    Some(kv)
                }
            }
        } else {
            None
        }
    }
}

pub(crate) struct SSTableWriter {
    path: PathBuf,
    buf: BufWriter<File>,
    offset: u64,
    index: SstIndex,
    seq_num: u64,
}

impl SSTableWriter {
    pub fn new(path: &PathBuf, seq_num: u64) -> GhalaDbResult<SSTableWriter> {
        Ok(SSTableWriter {
            path: path.clone(),
            buf: BufWriter::new(
                OpenOptions::new().write(true).create(true).open(path)?,
            ),
            offset: 0u64,
            index: SstIndex::new(),
            seq_num,
        })
    }

    pub fn write(&mut self, k: Bytes, v: ValueEntry) -> GhalaDbResult<()> {
        match v {
            ValueEntry::Tombstone => {
                self.index.insert(k, DiskEntry::Tombstone);
            }
            ValueEntry::Val(dp) => {
                let bytes = Dec::ser_raw(&dp)?;
                self.buf.write_all(&bytes)?;
                self.index.insert(
                    k,
                    DiskEntry::Value {
                        offset: self.offset,
                        len: bytes.len(),
                    },
                );
                self.offset += bytes.len() as u64;
            }
        }
        Ok(())
    }

    pub fn into_sstable(mut self) -> GhalaDbResult<SSTable> {
        // write metadata
        let metadata = SstMetadata {
            index: self.index,
            seq_num: self.seq_num,
        };
        let meta = Dec::ser_raw(&metadata)?;
        self.buf.write_all(&meta)?;
        // write footer
        let meta_sz = meta.len() as u64;
        let footer = meta_sz.to_le_bytes();
        self.buf.write_all(&footer)?;
        self.buf.flush()?;

        SSTable::from_path(self.path).map_err(|e| {
            debug!("got error loading sst from path: {}", e);
            e
        })
    }
}
