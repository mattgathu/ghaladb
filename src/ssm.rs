//! Storage System Manager

use std::{
    collections::{BTreeMap, VecDeque},
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    mem::swap,
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, SyncSender},
    sync::Arc,
    time::SystemTime,
};

use crate::{
    error::{GhalaDBError, GhalaDbResult},
    memtable::{merge_mem_tables, BTreeMemTable, MemTable, OpType},
    sstable::{NaiveSSTable, SSTable, SstIndex},
};

#[derive(Debug, Clone)]
pub enum SsmCmd {
    FlushMemTable(Arc<BTreeMemTable>),
    Get(String),
    Shutdown,
}

pub(crate) enum SsmResp {
    TableFlushed,
    FlushingError(GhalaDBError),
    GetError(GhalaDBError),
    GetResp { key: String, val: Option<String> },
}

// functions:
// - write mem tables to fs
// - compact ss tables
// - track ss tables
pub(crate) struct Ssm {
    ss_tables: VecDeque<NaiveSSTable>,
    base_path: PathBuf,
    tx: SyncSender<SsmResp>,
    rx: Receiver<SsmCmd>,
}

impl Ssm {
    pub fn new<P: AsRef<Path>>(
        ss_tables: VecDeque<NaiveSSTable>,
        path: P,
        tx: SyncSender<SsmResp>,
        rx: Receiver<SsmCmd>,
    ) -> Ssm {
        let base_path = path.as_ref().to_path_buf();

        Ssm {
            ss_tables,
            base_path,
            tx,
            rx,
        }
    }

    pub fn work_loop(&mut self) {
        loop {
            match self.rx.recv().unwrap() {
                SsmCmd::FlushMemTable(table) => match self.flush_mem_table(dbg!(table).as_ref()) {
                    Ok(_) => {
                        //FIXME
                        self.tx.send(SsmResp::TableFlushed).ok();
                    }
                    Err(e) => {
                        //FIXME
                        self.tx.send(SsmResp::FlushingError(e)).ok();
                    }
                },
                SsmCmd::Get(key) => match self.get(&key) {
                    Ok(val) => self.tx.send(SsmResp::GetResp { key, val }).unwrap(),
                    Err(e) => self.tx.send(SsmResp::GetError(e)).unwrap(),
                },
                SsmCmd::Shutdown => {
                    break;
                }
            }
            // check if we can compact/merge SSTs
            self.merge_ssts().ok();
        }
    }
    fn merge_ssts(&mut self) -> GhalaDbResult<()> {
        if dbg!(self.ss_tables.is_empty() || self.ss_tables.len() == 1) {
            return Ok(());
        }
        let base_path = self.base_path.clone();
        let mut sst_n = self.ss_tables.pop_back().unwrap();
        dbg!(self.ss_tables.is_empty());
        for ref mut sst_x in &mut self.ss_tables {
            if dbg!(Self::ssts_overlap(&sst_n, &sst_x)) {
                let mut merged = { Self::_merge_ssts(&mut sst_n, sst_x, base_path)? };
                swap(*sst_x, &mut merged);
                break;
            }
        }
        dbg!(&self.ss_tables);
        Ok(())
    }

    fn _merge_ssts(
        new: &mut NaiveSSTable,
        old: &mut NaiveSSTable,
        base_path: PathBuf,
    ) -> GhalaDbResult<NaiveSSTable> {
        // naive approach
        // read both sst as mem tables -> merge them -> flush back to disk
        let new = new.into_memtable()?;
        let old = old.into_memtable()?;
        let merged = merge_mem_tables(old, new);

        // FIXME: duplicated
        let sst_name = Ssm::gen_sst_name()?;
        let sst_path = base_path.join(&sst_name);
        let index = Ssm::flush_mem_table_to_disk(&merged, &sst_path)?;
        let sst = NaiveSSTable::new(sst_path, index, sst_name);

        Ok(sst)
    }

    fn ssts_overlap(left: &NaiveSSTable, right: &NaiveSSTable) -> bool {
        if left.last_key() > right.first_key() || right.last_key() > left.first_key() {
            true
        } else {
            false
        }
    }

    fn get(&mut self, key: &str) -> GhalaDbResult<Option<String>> {
        for sst in &mut self.ss_tables {
            if let Some(val) = sst.get(key)? {
                return Ok(Some(val));
            }
        }
        Ok(None)
    }

    fn flush_mem_table(&mut self, table: &dyn MemTable) -> GhalaDbResult<PathBuf> {
        let sst_name = Ssm::gen_sst_name()?;
        let sst_path = dbg!(self.base_path.join(&sst_name));
        let index = dbg!(Ssm::flush_mem_table_to_disk(table, &sst_path)?);
        let sst = NaiveSSTable::new(sst_path.clone(), index, sst_name);
        self.ss_tables.push_back(sst);

        Ok(sst_path)
    }

    pub fn flush_mem_table_to_disk<P: AsRef<Path>>(
        table: &dyn MemTable,
        path: P,
    ) -> GhalaDbResult<SstIndex> {
        let mut writer = Ssm::get_write_buf(&path)?;
        let mut cur_val_offset = 0usize;
        let mut index = BTreeMap::new();
        // write values + populate index
        for (k, v) in table.iter() {
            if let Some(val) = v {
                writer.write_all(val.as_bytes())?;
                index.insert(k, (cur_val_offset, val.len(), OpType::Put));
                cur_val_offset += val.len();
            } else {
                index.insert(k, (0, 0, OpType::Delete));
            }
        }
        // write index
        let index_ser = bincode::serialize(&index)?;
        writer.write_all(&index_ser)?;
        // write footer
        // fixed size footer [u8;8] holding size of index
        let index_sz = index_ser.len() as u64;
        let footer = index_sz.to_le_bytes();
        writer.write_all(&footer)?;

        Ok(index)
    }
    fn gen_sst_name() -> GhalaDbResult<String> {
        Ok(format!(
            "ghaladb_{}.sst",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis()
        ))
    }

    fn get_write_buf<P: AsRef<Path>>(path: P) -> GhalaDbResult<BufWriter<File>> {
        //TODO: implement sanity checks
        // - path should not exist
        Ok(BufWriter::new(
            OpenOptions::new().write(true).create(true).open(path)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::sync_channel;

    fn dummy_vals() -> Vec<(String, String)> {
        let vals = vec![
            "Mike Tyson",
            "Deontay Wilder",
            "Anthony Joshua",
            "Muhammad Ali",
            "Vladimir Klitschko",
        ];
        vals.iter()
            .map(|b| (b.to_string(), b.to_string()))
            .collect()
    }

    #[test]
    fn sst_serde() -> GhalaDbResult<()> {
        let path = "/tmp/sst_serde";
        std::fs::create_dir(path).ok();
        let mut mt = BTreeMemTable::new();
        for (k, v) in dummy_vals() {
            mt.insert(k, v)
        }
        mt.delete("Tyson Fury".to_string());
        let (_, rx) = sync_channel::<SsmCmd>(1);
        let (tx, _) = sync_channel::<SsmResp>(1);

        let mut ssm = Ssm::new(VecDeque::new(), path, tx, rx);
        let sst_path = ssm.flush_mem_table(&mt)?;
        let mut loaded_sst = NaiveSSTable::from_path(&sst_path)?;
        let mt2 = loaded_sst.into_memtable()?;
        assert_eq!(mt, mt2);
        Ok(())
    }
}
