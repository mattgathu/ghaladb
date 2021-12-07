#![allow(dead_code)]
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::{File, Metadata, OpenOptions};
use std::io::BufReader;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
//use std::thread;
//
//

use crate::error::GhalaDbResult;
use crate::memtable::{BTreeMemTable, MemTable, OpType};

const FOOTER_SIZE: i64 = 8;

pub type SstIndex = BTreeMap<String, (usize, usize, OpType)>;
pub trait SSTable {
    fn get(&mut self, key: &str) -> GhalaDbResult<Option<String>>;
    fn len(&self) -> usize;
    fn size(self) -> usize;
    //fn iter(&self) -> SSTableIter;
}

// Things to implement:
// creation from memtable
// - key-val blocks
// - metadata blocks
// - stats meta block
// - fixed length footer
//      - ref metadata/indices
// ref: https://github.com/google/leveldb/blob/master/doc/table_format.md
pub struct NaiveSSTable {
    path: PathBuf,
    //TODO: since values are meant to be small, we can use isize for the length and
    // have a negative value for tombstones, thereby eliminating the OpType in the index
    index: SstIndex,
    name: String,
}

impl NaiveSSTable {
    pub fn new(path: PathBuf, index: SstIndex, name: String) -> NaiveSSTable {
        NaiveSSTable { path, index, name }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> GhalaDbResult<NaiveSSTable> {
        let mut reader = BufReader::new(OpenOptions::new().read(true).open(&path)?);
        // read footer
        // - seek to end - footer_sz
        reader.seek(SeekFrom::End(-FOOTER_SIZE))?;
        let mut buf = vec![];
        let _ = reader.read_to_end(&mut buf)?;
        let footer: [u8; 8] = buf.try_into().expect("footer size not 8");
        let index_sz = usize::from_le_bytes(footer) as i64;

        // load index
        let index_offst = 0i64 - (index_sz + FOOTER_SIZE);
        reader.seek(SeekFrom::End(index_offst))?;
        let mut buf = vec![0u8; index_sz as usize];
        reader.read_exact(&mut buf)?;
        let index: SstIndex = bincode::deserialize(&buf)?;
        // load values
        reader.seek(SeekFrom::Start(0u64))?;

        // name
        let name = path
            .as_ref()
            .iter()
            .last()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        Ok(NaiveSSTable::new(path.as_ref().to_path_buf(), index, name))
    }

    pub fn first_key(&self) -> &str {
        debug_assert!(!self.index.is_empty());
        self.index.keys().next().unwrap()
    }
    pub fn last_key(&self) -> &str {
        self.index.keys().last().unwrap()
    }

    fn get_index_entry(&self, k: &str) -> Option<(usize, usize, OpType)> {
        self.index.get(k).map(|e| *e)
    }

    fn get_buf(&mut self) -> GhalaDbResult<BufReader<File>> {
        Ok(BufReader::new(
            OpenOptions::new().read(true).open(&self.path)?,
        ))
    }

    fn read_val(&mut self, offset: usize, len: usize) -> GhalaDbResult<Option<String>> {
        let mut buf = self.get_buf()?;
        buf.seek(SeekFrom::Start(offset as u64))?;
        let mut vbuf = vec![0u8; len];
        buf.read_exact(&mut vbuf)?;
        Ok(Some(String::from_utf8(vbuf)?))
    }

    #[allow(unused)]
    pub fn into_memtable(&mut self) -> GhalaDbResult<BTreeMemTable> {
        let mut buf = self.get_buf()?;
        let mut table = BTreeMemTable::new();
        for (k, (_, v_len, op_type)) in self.index.iter() {
            match op_type {
                OpType::Put => {
                    let mut b = vec![0u8; *v_len];
                    buf.read_exact(&mut b)?;
                    let val = String::from_utf8(b)?;
                    table.insert(k.to_string(), val);
                }
                OpType::Delete => table.delete(k.to_string()),
            }
        }

        Ok(table)
    }

    fn get_file_handle_with_metadata<P: AsRef<Path>>(path: P) -> GhalaDbResult<(File, Metadata)> {
        let f = OpenOptions::new().read(true).open(path)?;
        let m = f.metadata()?;
        Ok((f, m))
    }
}

impl Debug for NaiveSSTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NaiveSSTable")
            .field("index", &self.index)
            .field("name", &self.name)
            .finish()
    }
}

impl SSTable for NaiveSSTable {
    fn len(&self) -> usize {
        self.index.len()
    }

    fn size(self) -> usize {
        todo!()
    }

    fn get(&mut self, key: &str) -> GhalaDbResult<Option<String>> {
        match self.get_index_entry(key) {
            Some((offset, len, op_type)) => match op_type {
                OpType::Delete => Ok(None),
                OpType::Put => self.read_val(offset, len),
            },
            None => Ok(None),
        }
    }
}
