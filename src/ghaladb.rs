use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::config::DatabaseOptions;
use crate::error::{GhalaDBError, GhalaDbResult};
use crate::memtable::{BTreeMemTable, MemTable, MemTableIter, OpType};
use crate::ssm::{Ssm, SsmCmd, SsmResp};
use crate::wal::{LogRecord, OnDiskWal, WriteAheadLog};

struct SsmInbox {
    out_going: SyncSender<SsmCmd>,
    incoming: Receiver<SsmResp>,
}
pub struct GhalaDB {
    log: Box<dyn WriteAheadLog>,
    mem: BTreeMemTable,
    flushing_mem: Arc<BTreeMemTable>,
    #[allow(unused)]
    flush_buf: BTreeMap<String, String>,
    db_options: DatabaseOptions,
    ssm_inbox: SsmInbox,
    #[allow(unused)]
    ssm_handler: JoinHandle<()>,
}

impl GhalaDB {
    pub fn new<P: AsRef<Path>>(
        path: P,
        options: Option<DatabaseOptions>,
    ) -> GhalaDbResult<GhalaDB> {
        let db_options = options.unwrap_or_else(|| DatabaseOptions::builder().build());
        Self::init_db_dir(path.as_ref())?;
        let (tx, ssm_rx) = sync_channel::<SsmCmd>(10);
        let (ssm_tx, rx) = sync_channel::<SsmResp>(10);
        let ssm_inbox = SsmInbox {
            out_going: tx,
            incoming: rx,
        };
        let mut ssm = Ssm::new(VecDeque::new(), &path, ssm_tx, ssm_rx);
        let ssm_handler = thread::spawn(move || ssm.work_loop());
        let db = GhalaDB {
            log: Box::new(OnDiskWal::new(path.as_ref())?),
            mem: BTreeMemTable::new(),
            flushing_mem: Arc::new(BTreeMemTable::new()),
            flush_buf: BTreeMap::new(),
            db_options,
            ssm_inbox,
            ssm_handler,
        };
        Ok(db)
    }

    pub fn delete<K: Into<String>>(&mut self, k: K) -> GhalaDbResult<()> {
        let k = k.into();
        self.log.log(LogRecord::new(&k, "", OpType::Delete))?;
        self.mem.delete(k);
        Ok(())
    }

    pub fn get<K: Into<String>>(&mut self, k: K) -> GhalaDbResult<Option<String>> {
        let key = k.into();
        if let Some(val) = self.mem.get(&key) {
            return Ok(Some(val));
        }
        if let Some(val) = self.flushing_mem.get(&key) {
            return Ok(Some(val));
        }

        self.get_from_ssm(&key)
    }

    pub fn put<K: Into<String>, V: Into<String>>(&mut self, k: K, v: V) -> GhalaDbResult<()> {
        let k = k.into();
        let v = v.into();
        if self.mem_at_capacity(k.len() + v.len()) {
            self.flush_mem()?;
        }
        // write to WAL
        let tag = OpType::Put;
        let log_record = LogRecord::new(&k, &v, tag);
        self.log.log(log_record)?;
        self.mem.insert(k, v);
        Ok(())
    }

    pub fn iter(&self) -> GhalaDBIter {
        GhalaDBIter {
            iter: self.mem.iter(),
        }
    }

    fn get_from_ssm(&self, k: &str) -> GhalaDbResult<Option<String>> {
        self.ssm_inbox
            .out_going
            .send(SsmCmd::Get(k.to_string()))
            .unwrap();
        loop {
            //TODO: this recv should have a timeout
            match self.ssm_inbox.incoming.recv().unwrap() {
                SsmResp::GetResp { key, val } => {
                    debug_assert_eq!(&key, k);
                    return Ok(val);
                }
                SsmResp::GetError(e) => {
                    return Err(e);
                }
                _ => {
                    //FIXME: we do not care for now what to do in these case
                    // we should probably park these messages somewhere or just handle them here.
                    // very least log them
                    continue;
                }
            }
        }
    }

    fn init_db_dir(path: &Path) -> GhalaDbResult<()> {
        match std::fs::create_dir(path) {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    if std::fs::metadata(path).map(|m| m.is_dir())? {
                        Ok(())
                    } else {
                        Err(GhalaDBError::DbPathNotDirectory(path.to_path_buf()))
                    }
                }
                _ => Err(GhalaDBError::IOError(e)),
            },
        }?;
        Ok(())
    }

    fn mem_at_capacity(&self, kv_size: usize) -> bool {
        if self.mem.is_empty() {
            return false;
        }
        dbg!((dbg!(self.mem.size()) + kv_size) > dbg!(self.db_options.max_mem_table_size))
    }

    fn flush_mem(&mut self) -> GhalaDbResult<()> {
        //FIXME what happens to existing flushing_mem
        // check if we swap before swapping
        //std::mem::swap(&mut self.mem, &mut self.flushing_mem);
        let mut mem_table = BTreeMemTable::new();
        std::mem::swap(&mut self.mem, &mut mem_table);
        self.flushing_mem = Arc::new(mem_table);
        self.ssm_inbox
            .out_going
            .send(SsmCmd::FlushMemTable(self.flushing_mem.clone()))
            .unwrap();
        Ok(())
    }

    fn shut_down_ssm(&self) -> GhalaDbResult<()> {
        self.ssm_inbox
            .out_going
            .send(SsmCmd::Shutdown)
            .map_err(|_| GhalaDBError::SsmCmdSendError(SsmCmd::Shutdown))?;
        loop {
            match self.ssm_inbox.incoming.recv() {
                Err(_) => {
                    break;
                }
                Ok(_) => {
                    //TODO
                }
            }
        }
        Ok(())
    }
}

impl Drop for GhalaDB {
    // TODO
    // Things to do when closing database
    // - flush values in memory
    // - sync data to disk
    // - clear wal
    fn drop(&mut self) {
        //TODO: is the minion live at this point or already dropped?
        self.log.flush().ok();
        self.shut_down_ssm().ok();
    }
}

pub struct GhalaDBIter<'a> {
    iter: MemTableIter<'a>,
}

impl<'a> Iterator for GhalaDBIter<'a> {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((k, v)) = self.iter.next() {
                match v {
                    None => continue,
                    Some(val) => return Some((k.to_string(), val.to_string())),
                }
            } else {
                return None;
            }
        }
    }
}

//impl IntoIterator for GhalaDB {
//    type Item = (String, String);
//    type IntoIter = IntoIter<String, String>;
//
//    fn into_iter(self) -> Self::IntoIter {
//        self.mem.into_iter()
//    }
//}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn key_lookup() -> GhalaDbResult<()> {
        let mut db = GhalaDB::new("/tmp/foo", None)?;
        let k = "hello";
        let v = "world";
        db.put(k, v)?;
        assert_eq!(db.get(k)?, Some(v.to_string()));
        Ok(())
    }

    #[test]
    fn put_delete_get() -> GhalaDbResult<()> {
        let mut db = GhalaDB::new("/tmp/foo", None)?;
        let k = "hello";
        let v = "world";
        db.put(k, v)?;
        assert_eq!(db.get(k)?, Some(v.to_string()));
        db.delete(k)?;
        assert_eq!(db.get(k)?, None);
        Ok(())
    }

    #[test]
    fn flush_restore() -> GhalaDbResult<()> {
        let mut db = GhalaDB::new("/tmp/foo", None)?;
        db.put("hello", "world")?;
        //db.flush(1)?;
        Ok(())
    }

    #[test]
    fn kv_iter() -> GhalaDbResult<()> {
        let mut db = GhalaDB::new("/tmp/iter", None)?;
        db.put("king", "queen")?;
        db.put("man", "woman")?;
        db.delete("king")?;
        let entries: Vec<(String, String)> = db.iter().collect();
        assert!(!entries.contains(&("king".to_string(), "queen".to_string())));
        assert!(entries.contains(&("man".to_string(), "woman".to_string())));

        let mut db = GhalaDB::new("/tmp/iter2", None)?;
        for (k, v) in [("bee", "honey"), ("fish", "water")] {
            db.put(k, v)?;
        }
        let mut counter = 0;
        for _ in db.iter() {
            counter += 1;
        }
        assert_eq!(counter, 2);
        Ok(())
    }

    #[test]
    fn get_from_ssm() -> GhalaDbResult<()> {
        let mut db = GhalaDB::new("/tmp/ssm", None)?;
        db.put("left", "right")?;
        db.put("man", "woman")?;
        db.flush_mem()?;
        assert_eq!(db.get("man")?, Some("woman".to_string()));
        Ok(())
    }

    #[test]
    fn sst_merges() -> GhalaDbResult<()> {
        let opts = DatabaseOptions::builder()
            .max_mem_table_size(16)
            .sync(false)
            .build();
        let mut db = GhalaDB::new("/tmp/ssm_merges", Some(opts))?;
        for (k, v) in dummy_vals() {
            db.put(k, v)?;
        }
        Ok(())
    }
}
