use bincode::Decode;
use bincode::Encode;

use crate::{
    config::DatabaseOptions,
    core::{Bytes, ValueEntry},
    dec::Dec,
    error::{GhalaDbError, GhalaDbResult},
    gc::Janitor,
    keys::Skt,
    utils::t,
    vlog::{DataEntry, VlogsMan},
};
use std::marker::PhantomData;
use std::path::Path;

/// An LSM key value store with keys and values separation.
pub struct GhalaDb<K, V>
where
    K: Encode + Decode,
    V: Encode + Decode,
{
    keys: Skt,
    vlogs_man: VlogsMan,
    janitor: Option<Janitor>,
    opts: DatabaseOptions,
    sweeping: bool,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> GhalaDb<K, V>
where
    K: Encode + Decode,
    V: Encode + Decode,
{
    /// Creates a new data store or loads an existing one.
    pub fn new<P: AsRef<Path>>(
        path: P,
        options: Option<DatabaseOptions>,
    ) -> GhalaDbResult<GhalaDb<K, V>> {
        info!("database init. path: {:?}", path.as_ref());
        debug!("database init with options: {:#?}", options);
        let opts = options.unwrap_or_else(|| DatabaseOptions::builder().build());
        Self::init_dir(path.as_ref())?;
        let skt_path = path.as_ref().join("skt");

        let vlogs_man = VlogsMan::new(path.as_ref(), opts)?;
        let keys = Skt::from_path(skt_path, opts)?;
        let janitor = None;
        let db = GhalaDb {
            keys,
            vlogs_man,
            janitor,
            opts,
            sweeping: false,
            _k: PhantomData,
            _v: PhantomData,
        };
        Ok(db)
    }

    /// Deletes a key from the data store.
    pub fn delete(&mut self, key: &K) -> GhalaDbResult<()> {
        let key = Dec::ser_raw(key)?;
        t!("keys::del", self.keys.delete(&key))?;
        Ok(())
    }

    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: &K) -> GhalaDbResult<Option<V>> {
        let key = Dec::ser_raw(key)?;
        if let Some(dp) = self.keys.get(&key) {
            let bytes = t!("vlogman::get", self.vlogs_man.get(&dp))?.val;
            let val: V = Dec::deser_raw(&bytes)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    /// Inserts a key-value pair into the data store.
    pub fn put(&mut self, k: &K, v: &V) -> GhalaDbResult<()> {
        let key = Dec::ser_raw(k)?;
        let val = Dec::ser_raw(v)?;
        self.put_raw(key, val)
    }

    fn put_raw(&mut self, key: Bytes, val: Bytes) -> GhalaDbResult<()> {
        trace!("updating: {key:?}");
        let de = DataEntry::new(key.clone(), val);
        let dp = t!("vlogman::put", self.vlogs_man.put(&de))?;
        t!("keys::put", self.keys.put(key, ValueEntry::Val(dp)))?;
        t!("gc", self.gc())?;

        Ok(())
    }

    /// An iterator visiting all key-value pairs in an ordered manner.
    pub fn iter(
        &mut self,
    ) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<(K, V)>> + '_> {
        let db_iter: GhalaDbIter<K, V> = GhalaDbIter {
            iter: Box::new(self.keys.iter()),
            valman: &mut self.vlogs_man,
            _k: PhantomData,
            _v: PhantomData,
        };

        Ok(db_iter.into_iter())
    }

    /// Attempts to sync all data to disk.
    pub fn sync(&mut self) -> GhalaDbResult<()> {
        self.keys.sync()?;
        self.vlogs_man.sync()?;
        Ok(())
    }

    fn gc(&mut self) -> GhalaDbResult<()> {
        if !self.opts.compact || self.sweeping {
            return Ok(());
        }
        self.sweeping = true;
        if let Some(ref mut jan) = self.janitor {
            if let Some(de) = jan.sweep(&mut self.keys)? {
                t!("gc::put_raw", self.put_raw(de.key, de.val))?;
            } else {
                t!("vlogs_man::drop_vlog", self.vlogs_man.drop_vlog(jan.vnum()))?;
                self.janitor = None;
            }
        } else if let Some((vnum, path)) = self.vlogs_man.get_gc_cand()? {
            let janitor = t!("janitor::new", Janitor::new(vnum, &path))?;
            self.janitor = Some(janitor);
        }
        self.sweeping = false;

        Ok(())
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
                        Err(GhalaDbError::DbPathNotDirectory(path.to_path_buf()))
                    }
                }
                _ => Err(GhalaDbError::IOError(e)),
            },
        }?;
        Ok(())
    }
}

pub struct GhalaDbIter<'a, K, V> {
    iter: Box<dyn Iterator<Item = (&'a Bytes, &'a ValueEntry)> + 'a>,
    valman: &'a mut VlogsMan,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> Iterator for GhalaDbIter<'_, K, V>
where
    K: Decode,
    V: Decode,
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

impl<K, V> GhalaDbIter<'_, K, V>
where
    K: Decode,
    V: Decode,
{
    fn nxt(&mut self) -> GhalaDbResult<Option<(K, V)>> {
        loop {
            if let Some((_, v)) = self.iter.next() {
                match v {
                    ValueEntry::Tombstone => continue,
                    ValueEntry::Val(dp) => {
                        let v = self.valman.get(dp)?;
                        let key: K = Dec::deser_raw(&v.key)?;
                        let val: V = Dec::deser_raw(&v.val)?;
                        return Ok(Some((key, val)));
                    }
                }
            } else {
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::core::FixtureGen;

    use super::*;
    use tempfile::tempdir;

    macro_rules! s {
        ($expr:expr) => {
            $expr.to_owned()
        };
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
        let tmp_dir = tempdir()?;
        let mut db = GhalaDb::new(tmp_dir.path(), None)?;
        let k = "hello".to_owned();
        let v = "world".to_owned();
        db.put(&k, &v)?;
        assert_eq!(db.get(&k)?, Some(v));
        Ok(())
    }

    #[test]
    fn put_delete_get() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let mut db = GhalaDb::new(tmp_dir.path(), None)?;
        let k = "hello".to_owned();
        let v = "world".to_owned();
        db.put(&k, &v)?;
        assert_eq!(db.get(&k)?, Some(v));
        db.delete(&k)?;
        assert_eq!(db.get(&k)?, None);
        Ok(())
    }

    #[test]
    fn flush_restore() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        info!("DB init");
        let mut db = GhalaDb::new(tmp_dir.path(), None)?;
        let k = "hello".to_owned();
        let v = "world".to_owned();
        db.put(&k, &v)?;
        info!("dropping DB");
        drop(db);
        info!("Reloading DB");
        let mut db = GhalaDb::new(tmp_dir.path(), None)?;
        assert_eq!(db.get(&k)?, Some(v));
        Ok(())
    }

    #[test]
    fn kv_iter() -> GhalaDbResult<()> {
        let tmp_dir = tempdir()?;
        let mut db = GhalaDb::new(tmp_dir.path(), None)?;

        db.put(&s!("king"), &s!("queen"))?;
        db.put(&s!("man"), &s!("woman"))?;
        db.delete(&s!("king"))?;
        let entries: Vec<(String, String)> = db
            .iter()?
            .collect::<GhalaDbResult<Vec<(String, String)>>>()?;
        assert!(!entries.contains(&(s!("king"), s!("queen"))));
        assert!(entries.contains(&(s!("man"), s!("woman"))));

        let tmp_dir = tempdir()?;
        let mut db = GhalaDb::new(tmp_dir.path(), None)?;
        for (k, v) in [("bee", "honey"), ("fish", "water")] {
            db.put(&s!(k), &s!(v))?;
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
        let tmp_dir = tempdir()?;
        let mut db = GhalaDb::new(tmp_dir.path(), None)?;

        db.put(&s!("left"), &s!("right"))?;
        db.put(&s!("man"), &s!("woman"))?;
        assert_eq!(db.get(&s!("man"))?, Some(s!("woman")));
        Ok(())
    }

    #[test]
    fn sst_merges() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let opts = DatabaseOptions::builder().sync(false).build();
        let mut db = GhalaDb::new(tmp_dir.path(), Some(opts))?;
        for (k, v) in dummy_vals() {
            db.put(&k, &v)?;
        }

        for (k, v) in &dummy_vals() {
            db.delete(k)?;
            db.delete(v)?;
        }
        Ok(())
    }

    #[test]
    fn gc() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let opts = DatabaseOptions::builder()
            .max_vlog_size(4 * 1024)
            .sync(false)
            .build();
        let mut db = GhalaDb::new(tmp_dir.path(), Some(opts))?;
        let data = (0..100).map(|_| Bytes::gen()).collect::<Vec<_>>();
        for entry in &data {
            db.put(entry, entry)?;
        }
        for entry in data {
            let val = db.get(&entry)?.unwrap();
            assert!(entry == val);
        }

        Ok(())
    }

    #[test]
    fn data_integrity_1() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let opts = DatabaseOptions::builder()
            .max_vlog_size(10000 * 1024)
            .compact(true)
            .sync(false)
            .build();
        let unchanged: HashSet<Bytes> = (0..1000).map(|_| Bytes::gen()).collect();
        let deleted: HashSet<Bytes> = (0..1000).map(|_| Bytes::gen()).collect();
        let updated: HashSet<Bytes> = (0..1000).map(|_| Bytes::gen()).collect();
        let mut db = GhalaDb::new(tmp_dir.path(), Some(opts))?;
        assert!(unchanged.is_disjoint(&deleted));
        assert!(unchanged.is_disjoint(&updated));
        assert!(deleted.is_disjoint(&updated));

        for k in unchanged.iter().chain(deleted.iter()).chain(updated.iter()) {
            db.put(k, k)?;
        }

        for k in &unchanged {
            assert_eq!(db.get(k)?, Some(k.clone()))
        }
        for k in &deleted {
            db.delete(k)?;
        }
        for k in &updated {
            db.put(k, &Bytes::gen())?;
        }
        for k in &unchanged {
            assert_eq!(db.get(k)?, Some(k.clone()))
        }
        for k in &deleted {
            assert_eq!(db.get(k)?, None, "testing that key: {:?} is del", k)
        }
        for k in &updated {
            assert_ne!(
                db.get(k)?,
                Some(k.clone()),
                "key: {:?} should have been updated.",
                k
            )
        }
        drop(db);
        let mut db = GhalaDb::new(tmp_dir.path(), Some(opts))?;
        for k in &unchanged {
            assert_eq!(db.get(k)?, Some(k.clone()))
        }
        for k in &deleted {
            assert_eq!(db.get(k)?, None, "testing that key: {:?} is del", k)
        }
        for k in &updated {
            assert_ne!(
                db.get(k)?,
                Some(k.clone()),
                "key: {:?} should have been updated.",
                k
            )
        }

        Ok(())
    }
}
