use bincode::{Decode, Encode};

use crate::{
    config::DatabaseOptions,
    core::{Bytes, DataPtr},
    dec::Dec,
    error::{GhalaDbError, GhalaDbResult},
    gc::GarbageCollector,
    keys::Keys,
    utils::t,
    vlog::{DataEntry, VlogsMan},
};
use std::{borrow::Borrow, marker::PhantomData, path::Path};

/// An LSM key value store with keys and values separation.
pub struct GhalaDb<K, V>
where
    K: Encode + Decode,
    V: Encode + Decode,
{
    /// Keys Table
    keys: Keys,
    /// Values logs manager
    vlogs_man: VlogsMan,
    /// Garbage Collector
    gc: Option<GarbageCollector>,
    /// Database Configs
    opts: DatabaseOptions,
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
        trace!("GhalaDb::new path: {}", path.as_ref().display());
        let opts = options.unwrap_or_else(|| DatabaseOptions::builder().build());
        Self::init_dir(path.as_ref())?;
        let keys_path = path.as_ref().join("keys");

        let vlogs_man = VlogsMan::new(path.as_ref(), opts)?;
        let keys = Keys::from_path(keys_path, opts)?;
        let db = GhalaDb {
            keys,
            vlogs_man,
            gc: None,
            opts,
            _k: PhantomData,
            _v: PhantomData,
        };
        Ok(db)
    }

    /// Check if a key is present in the data store.
    ///
    /// Returns `true` if the store contains a value for the specified key.
    pub fn exists<Q: ?Sized>(&mut self, k: &Q) -> GhalaDbResult<bool>
    where
        K: Borrow<Q>,
        Q: bincode::Encode,
    {
        trace!("GhalaDb::contains_key");
        let key = Dec::ser_raw(k)?;
        Ok(self.keys.exists(&key))
    }

    /// Deletes a key from the data store.
    ///
    /// We simply remove the key from the in-memory keys table.
    pub fn delete<Q: ?Sized>(&mut self, key: &Q) -> GhalaDbResult<()>
    where
        K: Borrow<Q>,
        Q: bincode::Encode,
    {
        trace!("GhalaDb::delete");
        let key = Dec::ser_raw(key)?;
        t!("keys::del", self.keys.delete(&key))?;
        t!("gc", self.gc())?;
        Ok(())
    }

    /// Returns the value corresponding to the key.
    ///
    /// We first do a data pointer lookup in the in-memory keys table
    /// and then use the pointer to read the actual data entry from a
    /// vlog on disk.
    pub fn get<Q: ?Sized>(&mut self, key: &Q) -> GhalaDbResult<Option<V>>
    where
        K: Borrow<Q>,
        Q: bincode::Encode,
    {
        trace!("GhalaDb::get");
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
    pub fn put<Q: ?Sized>(&mut self, k: &Q, v: &V) -> GhalaDbResult<()>
    where
        K: Borrow<Q>,
        Q: bincode::Encode,
    {
        let key = Dec::ser_raw(k)?;
        let val = Dec::ser_raw(v)?;
        self.put_raw(key, val, false)
    }

    fn put_raw(
        &mut self,
        key: Bytes,
        val: Bytes,
        from_gc: bool,
    ) -> GhalaDbResult<()> {
        trace!("GhalaDb::put_raw key:{key:?}");
        let de = DataEntry::new(key.clone(), val);
        let dp = t!("vlogman::put", self.vlogs_man.put(&de))?;
        t!("keys::put", self.keys.put(key, dp))?;
        if !from_gc {
            t!("gc", self.gc())?;
        }

        Ok(())
    }

    /// An iterator visiting all key-value pairs in an ordered manner.
    pub fn iter(
        &mut self,
    ) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<(K, V)>> + '_> {
        trace!("GhalaDb::iter");
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
        trace!("GhalaDb::sync");
        self.keys.sync()?;
        self.vlogs_man.sync()?;
        Ok(())
    }

    fn gc(&mut self) -> GhalaDbResult<()> {
        trace!("GhalaDb::gc");
        if !self.opts.compact {
            return Ok(());
        }
        if let Some(ref mut gc) = self.gc {
            if let Some(de) = gc.sweep(&mut self.keys)? {
                // GC found a live data entry. Re-insert it.
                t!("gc::put_raw", self.put_raw(de.key, de.val, true))?;
            } else {
                // GC has finished going through the vlog.
                t!("vlogs_man::drop_vlog", self.vlogs_man.drop_vlog(gc.vnum()))?;
                self.gc = None;
            }
        } else if let Some((vnum, path)) = self.vlogs_man.get_gc_cand()? {
            let gc = t!("gc::new", GarbageCollector::new(vnum, &path))?;
            self.gc = Some(gc);
        }

        Ok(())
    }

    fn init_dir(path: &Path) -> GhalaDbResult<()> {
        trace!("GhalaDb::init_dir : {}", path.display());
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
    iter: Box<dyn Iterator<Item = (&'a Bytes, &'a DataPtr)> + 'a>,
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
        if let Some((_, dp)) = self.iter.next() {
            let v = self.valman.get(dp)?;
            let key: K = Dec::deser_raw(&v.key)?;
            let val: V = Dec::deser_raw(&v.val)?;
            Ok(Some((key, val)))
        } else {
            Ok(None)
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
        let mut db: GhalaDb<String, String> = GhalaDb::new(tmp_dir.path(), None)?;
        let k = "hello".to_owned();
        let v = "world".to_owned();
        db.put(&k, &v)?;
        assert_eq!(db.get(&k)?, Some(v));
        Ok(())
    }

    #[test]
    fn exists() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let mut db: GhalaDb<String, String> = GhalaDb::new(tmp_dir.path(), None)?;
        let k = "hello".to_owned();
        let v = "world".to_owned();
        db.put(&k, &v)?;
        assert!(db.exists(&k)?);
        Ok(())
    }

    #[test]
    fn put_delete_get() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let mut db: GhalaDb<String, String> = GhalaDb::new(tmp_dir.path(), None)?;
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
        let mut db: GhalaDb<String, String> = GhalaDb::new(tmp_dir.path(), None)?;
        let k = "hello".to_owned();
        let v = "world".to_owned();
        db.put(&k, &v)?;
        info!("dropping DB");
        drop(db);
        info!("Reloading DB");
        let mut db: GhalaDb<String, String> = GhalaDb::new(tmp_dir.path(), None)?;
        assert_eq!(db.get(&k)?, Some(v));
        Ok(())
    }

    #[test]
    fn kv_iter() -> GhalaDbResult<()> {
        let tmp_dir = tempdir()?;
        let mut db: GhalaDb<String, String> = GhalaDb::new(tmp_dir.path(), None)?;

        db.put(&s!("king"), &s!("queen"))?;
        db.put(&s!("man"), &s!("woman"))?;
        db.delete(&s!("king"))?;
        let entries: Vec<(String, String)> = db
            .iter()?
            .collect::<GhalaDbResult<Vec<(String, String)>>>()?;
        assert!(!entries.contains(&(s!("king"), s!("queen"))));
        assert!(entries.contains(&(s!("man"), s!("woman"))));

        let tmp_dir = tempdir()?;
        let mut db: GhalaDb<String, String> = GhalaDb::new(tmp_dir.path(), None)?;
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
        let mut db: GhalaDb<String, String> = GhalaDb::new(tmp_dir.path(), None)?;

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
        let mut db: GhalaDb<String, String> =
            GhalaDb::new(tmp_dir.path(), Some(opts))?;
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
    fn gc_shrinks_vlogs() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let opts = DatabaseOptions::builder()
            .max_vlog_size(4 * 1024)
            .sync(false)
            .build();
        let mut db: GhalaDb<Vec<u8>, Vec<u8>> =
            GhalaDb::new(tmp_dir.path(), Some(opts))?;
        let data = (0..100).map(|_| Bytes::gen()).collect::<Vec<_>>();
        for entry in &data {
            db.put(entry, entry)?;
        }
        let old_count = db.vlogs_man.vlogs_count();
        for key in data.iter().take(50) {
            db.delete(key)?;
        }
        let count = db.vlogs_man.vlogs_count();
        assert!(
            count < old_count,
            "vlogs count wrong: old {old_count} > cur {count}"
        );

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
        let mut db: GhalaDb<Vec<u8>, Vec<u8>> =
            GhalaDb::new(tmp_dir.path(), Some(opts))?;
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
        let mut db: GhalaDb<Vec<u8>, Vec<u8>> =
            GhalaDb::new(tmp_dir.path(), Some(opts))?;
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
        let mut db: GhalaDb<Vec<u8>, Vec<u8>> =
            GhalaDb::new(tmp_dir.path(), Some(opts))?;
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
