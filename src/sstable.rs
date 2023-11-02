use serde::{Deserialize, Serialize};
pub(crate) use std::collections::btree_map::IntoIter;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{GhalaDBError, GhalaDbResult};
use crate::memtable::{BTreeMemTable, Bytes, EntryType, KeyRef, MemTable, ValueEntry};

const FOOTER_SIZE: i64 = 8;

pub type SstIndex = BTreeMap<Bytes, IndexVal>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct IndexVal {
    offset: u64,
    len: usize,
    entry_type: EntryType,
}
impl IndexVal {
    pub fn new(offset: u64, len: usize, entry_type: EntryType) -> IndexVal {
        Self {
            offset,
            len,
            entry_type,
        }
    }
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
    pub fn new(path: PathBuf, index: SstIndex, seq_num: u64, rdr: BufReader<File>) -> SSTable {
        debug_assert!(!index.is_empty());
        let first_key = index.keys().next().unwrap().to_owned();
        let last_key = index.keys().last().unwrap().to_owned();
        let mem_size: usize = index.iter().map(|(k, v)| k.len() + v.len).sum();
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
                Some(IndexVal {
                    offset,
                    len,
                    entry_type,
                }) => match entry_type {
                    EntryType::Tombstone => Ok(Some(ValueEntry::Tombstone)),
                    EntryType::Value => Ok(self.read_val(*offset, *len)?.map(ValueEntry::Val)),
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

    pub fn deactivate(&mut self) {
        self.active = false;
    }

    fn key_is_covered(&self, k: KeyRef) -> bool {
        k >= &self.first_key && k <= &self.last_key
    }

    fn get_rdr<P: AsRef<Path>>(path: P) -> GhalaDbResult<BufReader<File>> {
        Ok(BufReader::new(OpenOptions::new().read(true).open(path)?))
    }

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
        let meta: SstMetadata = bincode::deserialize(&buf).map_err(|e| {
            error!("failed to deser sst metadata. Reason: {:?}", e);
            e
        })?;

        Ok(meta)
    }

    fn load_index(p: &Path) -> GhalaDbResult<SstIndex> {
        Self::load_meta(p).map(|m| m.index)
    }

    fn read_val(&mut self, offset: u64, len: usize) -> GhalaDbResult<Option<Bytes>> {
        let bytes = Self::read_val_inner(&mut self.rdr, offset, len)?;
        Ok(Some(bytes))
    }

    fn read_val_inner(rdr: &mut BufReader<File>, offset: u64, len: usize) -> GhalaDbResult<Bytes> {
        rdr.seek(SeekFrom::Start(offset))?;
        let mut v = vec![0u8; len];
        rdr.read_exact(&mut v)?;
        Ok(v)
    }

    #[allow(dead_code)]
    pub fn as_memtable(&self) -> GhalaDbResult<BTreeMemTable> {
        let mut buf = Self::get_rdr(&self.path)?;
        let mut table = BTreeMemTable::new();
        for (k, idx_val) in self.index.iter() {
            match idx_val.entry_type {
                EntryType::Value => {
                    let mut val = vec![0u8; idx_val.len];
                    buf.read_exact(&mut val)?;
                    table.insert(k.clone(), val);
                }
                EntryType::Tombstone => table.delete(k.clone()),
            }
        }

        Ok(table)
    }

    pub(crate) fn iter(&self) -> GhalaDbResult<SSTableIter> {
        let index = Self::load_index(&self.path)?;
        let buf = Self::get_rdr(&self.path)?;
        SSTableIter::new(buf, index)
    }

    fn delete(&self) -> GhalaDbResult<()> {
        debug_assert!(!self.active, "deleting active sst");
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
    index: IntoIter<Bytes, IndexVal>,
}
impl SSTableIter {
    pub(crate) fn new(buf: BufReader<File>, index: SstIndex) -> GhalaDbResult<SSTableIter> {
        Ok(SSTableIter {
            buf,
            index: index.into_iter(),
        })
    }
    fn read_val(&mut self, offset: u64, len: usize) -> GhalaDbResult<Bytes> {
        SSTable::read_val_inner(&mut self.buf, offset, len)
    }
}
impl Iterator for SSTableIter {
    type Item = GhalaDbResult<(Bytes, ValueEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((
            k,
            IndexVal {
                offset,
                len,
                entry_type,
            },
        )) = self.index.next()
        {
            match entry_type {
                EntryType::Tombstone => Some(Ok((k, ValueEntry::Tombstone))),
                EntryType::Value => Some(
                    self.read_val(offset, len)
                        .map(|bytes| (k, ValueEntry::Val(bytes))),
                ),
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
            buf: BufWriter::new(OpenOptions::new().write(true).create(true).open(path)?),
            offset: 0u64,
            index: SstIndex::new(),
            seq_num,
        })
    }

    pub fn write(&mut self, k: Bytes, v: ValueEntry) -> GhalaDbResult<()> {
        match v {
            ValueEntry::Tombstone => {
                self.index
                    .insert(k, IndexVal::new(0, 0, EntryType::Tombstone));
            }
            ValueEntry::Val(bytes) => {
                self.buf.write_all(&bytes)?;
                self.index
                    .insert(k, IndexVal::new(self.offset, bytes.len(), EntryType::Value));
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
        let meta = bincode::serialize(&metadata)?;
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
