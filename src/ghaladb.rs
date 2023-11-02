use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::Path;

use crate::config::DatabaseOptions;
use crate::error::{GhalaDBError, GhalaDbResult};
use crate::memtable::{BTreeMemTable, Bytes, KeyRef, MemTable, ValueEntry};
use crate::ssm::{merge_iter, StoreSysMan};

pub struct GhalaDB<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    mem: BTreeMemTable,
    db_options: DatabaseOptions,
    ssm: StoreSysMan,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> GhalaDB<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    pub fn new<P: AsRef<Path>>(
        path: P,
        options: Option<DatabaseOptions>,
    ) -> GhalaDbResult<GhalaDB<K, V>> {
        debug!("database init. path: {:?}", path.as_ref());
        debug!("database init with options: {:#?}", options);
        let db_options = options.unwrap_or_else(|| DatabaseOptions::builder().build());
        Self::init_dir(path.as_ref())?;

        let ssm = StoreSysMan::new(&path, db_options.max_ssts)?;
        let db = GhalaDB {
            mem: BTreeMemTable::new(),
            db_options,
            ssm,
            _k: PhantomData,
            _v: PhantomData,
        };
        Ok(db)
    }

    pub fn delete(&mut self, k: K) -> GhalaDbResult<()> {
        let key: Bytes = bincode::serialize(&k)?;
        trace!("deleting: {:?}", key);
        self.mem.delete(key);
        Ok(())
    }

    pub fn get(&mut self, k: K) -> GhalaDbResult<Option<V>> {
        let key: Bytes = bincode::serialize(&k)?;
        if let Some(val_entry) = self.mem.get(&key) {
            match val_entry {
                ValueEntry::Tombstone => Ok(None),
                ValueEntry::Val(bytes) => Ok(Some(bincode::deserialize(bytes)?)),
            }
        } else {
            self.get_from_ssm(&key)
        }
    }

    pub fn put(&mut self, k: K, v: V) -> GhalaDbResult<()> {
        let key: Bytes = bincode::serialize(&k)?;
        let val: Bytes = bincode::serialize(&v)?;
        trace!("inserting: {:?}", key);
        if self.mem_at_capacity(key.len() + val.len()) {
            self.flush_mem()?;
        }
        self.mem.insert(key, val);
        Ok(())
    }

    pub fn iter(&self) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<(K, V)>> + '_> {
        let mem_iter = self.mem.iter();
        let ssm_iter = self.ssm.iter()?;
        let merged = merge_iter(mem_iter, ssm_iter);
        let db_iter: GhalaDBIter<K, V> = GhalaDBIter {
            iter: Box::new(merged),
            _k: PhantomData,
            _v: PhantomData,
        };

        Ok(db_iter.into_iter())
    }

    fn get_from_ssm(&mut self, k: KeyRef) -> GhalaDbResult<Option<V>> {
        trace!("reading from ssm");
        match self.ssm.get(k)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
        }
    }

    fn init_dir(path: &Path) -> GhalaDbResult<()> {
        trace!("initializing db directory: {:?}", path);
        match std::fs::create_dir_all(path) {
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
        self.mem.mem_size() + kv_size > self.db_options.max_mem_table_size
    }

    fn flush_mem(&mut self) -> GhalaDbResult<()> {
        if self.mem.is_empty() {
            debug!("got empty memtable to flush. NOP");
            return Ok(());
        }
        debug!("flushing mem table");
        let mut mem_table = BTreeMemTable::new();
        std::mem::swap(&mut self.mem, &mut mem_table);
        let path = self.ssm.flush_mem_table(mem_table)?;
        debug!("flushed mem table to: {:?}", path);

        Ok(())
    }
}

impl<K, V> Drop for GhalaDB<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    // TODO
    // Things to do when closing database
    // - flush values in memory
    // - sync data to disk
    // - clear wal
    fn drop(&mut self) {
        debug!("flushing mem before shutdown");
        self.flush_mem().ok();
    }
}

pub struct GhalaDBIter<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    iter: Box<dyn Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> Iterator for GhalaDBIter<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Item = GhalaDbResult<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.nxt() {
            Err(e) => Some(Err(e)),
            Ok(None) => None,
            Ok(Some(kv)) => Some(Ok(kv)),
        }
    }
}

impl<K, V> GhalaDBIter<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn nxt(&mut self) -> GhalaDbResult<Option<(K, V)>> {
        loop {
            if let Some(kv) = self.iter.next() {
                let (k, v) = kv?;
                match v {
                    ValueEntry::Tombstone => continue,
                    //ValueEntry::Val(val) => return Some((k, val)),
                    ValueEntry::Val(val) => {
                        let key: K = bincode::deserialize(&k)?;
                        let val: V = bincode::deserialize(&val)?;
                        return Ok(Some((key, val)));
                    }
                }
            } else {
                return Ok(None);
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
    use std::collections::HashSet;

    use rand::{
        distributions::{Alphanumeric, DistString},
        prelude::ThreadRng,
        random, thread_rng,
    };
    use tempdir::TempDir;

    use super::*;

    fn gen_tmp_dir() -> GhalaDbResult<TempDir> {
        let mut rng = thread_rng();
        Ok(TempDir::new(&gen_string(&mut rng, 16))?)
    }

    fn gen_string(rng: &mut ThreadRng, len: usize) -> String {
        Alphanumeric {}.sample_string(rng, len)
    }

    fn dummy_vals() -> Vec<(String, String)> {
        let vals = [
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
        env_logger::try_init().ok();
        let tmp_dir = gen_tmp_dir()?;
        let mut db: GhalaDB<String, String> = GhalaDB::new(tmp_dir.path(), None)?;
        let k = "hello".to_string();
        let v = "world".to_string();
        db.put(k.clone(), v.clone())?;
        assert_eq!(db.get(k)?, Some(v));
        Ok(())
    }

    #[test]
    fn put_delete_get() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = gen_tmp_dir()?;
        let mut db: GhalaDB<String, String> = GhalaDB::new(tmp_dir.path(), None)?;
        let k = "hello".to_string();
        let v = "world".to_string();
        db.put(k.clone(), v.clone())?;
        assert_eq!(db.get(k.clone())?, Some(v));
        db.delete(k.clone())?;
        assert_eq!(db.get(k)?, None);
        Ok(())
    }

    #[test]
    fn flush_restore() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = gen_tmp_dir()?;
        info!("DB init");
        let mut db: GhalaDB<String, String> = GhalaDB::new(tmp_dir.path(), None)?;
        db.put("hello".to_string(), "world".to_string())?;
        info!("dropping DB");
        drop(db);
        info!("Reloading DB");
        let mut db: GhalaDB<String, String> = GhalaDB::new(tmp_dir.path(), None)?;
        assert_eq!(db.get("hello".to_string())?, Some("world".to_string()));
        Ok(())
    }

    #[test]
    fn kv_iter() -> GhalaDbResult<()> {
        let tmp_dir = gen_tmp_dir()?;
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        db.put("king".to_owned(), "queen".to_owned())?;
        db.put("man".to_owned(), "woman".to_owned())?;
        db.delete("king".to_owned())?;
        let entries: Vec<(String, String)> = db
            .iter()?
            .collect::<GhalaDbResult<Vec<(String, String)>>>()?;
        assert!(!entries.contains(&("king".to_string(), "queen".to_string())));
        assert!(entries.contains(&("man".to_string(), "woman".to_string())));

        let tmp_dir = gen_tmp_dir()?;
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        for (k, v) in [("bee", "honey"), ("fish", "water")] {
            db.put(k.to_owned(), v.to_owned())?;
        }
        let mut counter = 0;
        for _ in db.iter()? {
            counter += 1;
        }
        assert_eq!(counter, 2);
        Ok(())
    }

    #[test]
    fn get_from_ssm() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = gen_tmp_dir()?;
        let mut db: GhalaDB<String, String> = GhalaDB::new(tmp_dir.path(), None)?;
        db.put("left".to_string(), "right".to_string())?;
        db.put("man".to_string(), "woman".to_string())?;
        db.flush_mem()?;
        assert_eq!(db.get("man".to_string())?, Some("woman".to_string()));
        Ok(())
    }

    #[test]
    fn sst_merges() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = gen_tmp_dir()?;
        let opts = DatabaseOptions::builder()
            .max_mem_table_size(16)
            .sync(false)
            .build();
        let mut db = GhalaDB::new(tmp_dir.path(), Some(opts))?;
        for (k, v) in dummy_vals() {
            db.put(k, v)?;
        }
        db.flush_mem()?;

        for (k, v) in dummy_vals() {
            db.delete(k)?;
            db.delete(v)?;
        }
        db.flush_mem()?;
        Ok(())
    }

    #[test]
    fn data_integrity_1() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = gen_tmp_dir()?;
        let opts = DatabaseOptions::builder()
            .max_mem_table_size(1024)
            .sync(false)
            .build();
        let mut rng = thread_rng();
        let unchanged: HashSet<String> = (0..1000).map(|_| gen_string(&mut rng, 64)).collect();
        let deleted: HashSet<String> = (0..1000).map(|_| gen_string(&mut rng, 64)).collect();
        let updated: HashSet<String> = (0..1000).map(|_| gen_string(&mut rng, 64)).collect();
        let mut db: GhalaDB<String, String> = GhalaDB::new(tmp_dir.path(), Some(opts.clone()))?;
        assert!(unchanged.is_disjoint(&deleted));
        assert!(unchanged.is_disjoint(&updated));
        assert!(deleted.is_disjoint(&updated));

        for k in unchanged.iter().chain(deleted.iter()).chain(updated.iter()) {
            db.put(k.into(), k.into())?;
        }

        for k in &unchanged {
            assert_eq!(db.get(k.into())?, Some(k.to_string()))
        }
        for k in &deleted {
            db.delete(k.into())?;
        }
        for k in &updated {
            db.put(k.into(), gen_string(&mut rng, random::<u8>() as usize))?;
        }
        for k in &unchanged {
            assert_eq!(db.get(k.into())?, Some(k.to_string()))
        }
        for k in &deleted {
            assert_eq!(db.get(k.into())?, None, "testing that key: {:?} is del", k)
        }
        for k in &updated {
            assert_ne!(
                db.get(k.into())?,
                Some(k.to_string()),
                "key: {:?} should have been updated.",
                k
            )
        }
        std::mem::drop(db);
        let mut db: GhalaDB<String, String> = GhalaDB::new(tmp_dir.path(), Some(opts))?;
        for k in &unchanged {
            assert_eq!(db.get(k.into())?, Some(k.to_string()))
        }
        for k in &deleted {
            assert_eq!(db.get(k.into())?, None, "testing that key: {:?} is del", k)
        }
        for k in &updated {
            assert_ne!(
                db.get(k.into())?,
                Some(k.to_string()),
                "key: {:?} should have been updated.",
                k
            )
        }

        Ok(())
    }
}
