use crate::{
    config::DatabaseOptions,
    core::{Bytes, KeyRef, ValueEntry},
    error::{GhalaDBError, GhalaDbResult},
    gc::Janitor,
    keyman::KeyMan,
    utils::t,
    vlog::{DataEntry, VlogsMan},
};
use std::path::Path;

/// ///
/// ///          _|                  _|                  _|  _|
/// ///  _|_|_|  _|_|_|      _|_|_|  _|    _|_|_|    _|_|_|  _|_|_|
/// ///_|    _|  _|    _|  _|    _|  _|  _|    _|  _|    _|  _|    _|
/// ///_|    _|  _|    _|  _|    _|  _|  _|    _|  _|    _|  _|    _|
/// ///  _|_|_|  _|    _|    _|_|_|  _|    _|_|_|    _|_|_|  _|_|_|
/// ///      _|
/// ///  _|_|
/// ///
pub struct GhalaDB {
    keyman: KeyMan,
    vlogs_man: VlogsMan,
    janitor: Option<Janitor>,
    opts: DatabaseOptions,
    gc_on: bool,
}

impl GhalaDB {
    pub fn new<P: AsRef<Path>>(
        path: P,
        options: Option<DatabaseOptions>,
    ) -> GhalaDbResult<GhalaDB> {
        debug!("database init. path: {:?}", path.as_ref());
        debug!("database init with options: {:#?}", options);
        let db_options =
            options.unwrap_or_else(|| DatabaseOptions::builder().build());
        Self::init_dir(path.as_ref())?;

        let vlogs_man = VlogsMan::new(path.as_ref(), db_options.clone())?;
        let keyman = KeyMan::new(path.as_ref(), db_options.clone())?;
        let janitor = None;
        let db = GhalaDB {
            keyman,
            vlogs_man,
            janitor,
            opts: db_options,
            gc_on: false,
        };
        Ok(db)
    }

    pub fn delete(&mut self, key: Bytes) -> GhalaDbResult<()> {
        trace!("deleting: {:?}", key);
        t!("keyman::del", self.keyman.delete(key))?;
        Ok(())
    }

    pub fn get(&mut self, key: KeyRef) -> GhalaDbResult<Option<Bytes>> {
        if let Some(dp) = t!("keyman::get", self.keyman.get(key))? {
            let bytes = t!("vlogman::get", self.vlogs_man.get(&dp))?.val;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    pub fn put(&mut self, key: Bytes, val: Bytes) -> GhalaDbResult<()> {
        trace!("updating: {key:?}");
        let de = DataEntry::new(key.clone(), val);
        let dp = t!("vlogman::put", self.vlogs_man.put(&de))?;
        t!("keyman::put", self.keyman.put(key, dp))?;
        t!("gc", self.gc())?;

        Ok(())
    }

    pub fn iter(
        self,
    ) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<(Bytes, Bytes)>>> {
        let db_iter: GhalaDBIter = GhalaDBIter {
            iter: Box::new(self.keyman.iter()?),
            valman: self.vlogs_man,
        };

        Ok(db_iter.into_iter())
    }

    /// Attempts to sync all data to disk.
    pub fn sync(&mut self) -> GhalaDbResult<()> {
        self.keyman.sync()?;
        self.vlogs_man.sync()?;
        Ok(())
    }

    fn gc(&mut self) -> GhalaDbResult<()> {
        if !self.opts.vlog_compaction_enabled || self.gc_on {
            return Ok(());
        }
        self.gc_on = true;
        if let Some(ref mut jan) = self.janitor {
            if let Some(de) = jan.step(&mut self.keyman)? {
                t!("gc::put", self.put(de.key, de.val))?;
            } else {
                t!("vlogs_man::drop_vlog", self.vlogs_man.drop_vlog(jan.vnum()))?;
                self.janitor = None;
            }
        } else if let Some((vnum, path)) = self.vlogs_man.needs_gc()? {
            let janitor = t!("janitor::new", Janitor::new(vnum, &path))?;
            self.janitor = Some(janitor);
        }
        self.gc_on = false;

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
                        Err(GhalaDBError::DbPathNotDirectory(path.to_path_buf()))
                    }
                }
                _ => Err(GhalaDBError::IOError(e)),
            },
        }?;
        Ok(())
    }
}

pub struct GhalaDBIter {
    iter: Box<dyn Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>>>,
    valman: VlogsMan,
}

impl Iterator for GhalaDBIter {
    type Item = GhalaDbResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.nxt() {
            Err(e) => Some(Err(e)),
            Ok(None) => None,
            Ok(Some(kv)) => Some(Ok(kv)),
        }
    }
}

impl GhalaDBIter {
    fn nxt(&mut self) -> GhalaDbResult<Option<(Bytes, Bytes)>> {
        loop {
            if let Some(kv) = self.iter.next() {
                let (_, v) = kv?;
                match v {
                    ValueEntry::Tombstone => continue,
                    ValueEntry::Val(dp) => {
                        let v = self.valman.get(&dp)?;
                        return Ok(Some((v.key, v.val)));
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

    use crate::core::FixtureGen;

    use super::*;
    use tempfile::tempdir;

    fn dummy_vals() -> Vec<(Bytes, Bytes)> {
        let vals = [
            "Mike Tyson",
            "Deontay Wilder",
            "Anthony Joshua",
            "Muhammad Ali",
            "Vladimir Klitschko",
        ];
        vals.iter()
            .map(|b| (b.as_bytes().to_vec(), b.as_bytes().to_vec()))
            .collect()
    }

    #[test]
    fn key_lookup() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        let k = "hello".as_bytes().to_vec();
        let v = "world".as_bytes().to_vec();
        db.put(k.clone(), v.clone())?;
        assert_eq!(db.get(&k)?, Some(v));
        Ok(())
    }

    #[test]
    fn put_delete_get() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        let k = "hello".as_bytes().to_vec();
        let v = "world".as_bytes().to_vec();
        db.put(k.clone(), v.clone())?;
        assert_eq!(db.get(&k)?, Some(v));
        db.delete(k.clone())?;
        assert_eq!(db.get(&k)?, None);
        Ok(())
    }

    #[test]
    fn flush_restore() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        info!("DB init");
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        db.put("hello".as_bytes().to_vec(), "world".as_bytes().to_vec())?;
        info!("dropping DB");
        drop(db);
        info!("Reloading DB");
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        assert_eq!(
            db.get("hello".as_bytes())?,
            Some("world".as_bytes().to_vec())
        );
        Ok(())
    }

    #[test]
    fn kv_iter() -> GhalaDbResult<()> {
        let tmp_dir = tempdir()?;
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        db.put("king".as_bytes().to_vec(), "queen".as_bytes().to_vec())?;
        db.put("man".as_bytes().to_vec(), "woman".as_bytes().to_vec())?;
        db.delete("king".as_bytes().to_vec())?;
        let entries: Vec<(Bytes, Bytes)> =
            db.iter()?.collect::<GhalaDbResult<Vec<(Bytes, Bytes)>>>()?;
        assert!(!entries
            .contains(&("king".as_bytes().to_vec(), "queen".as_bytes().to_vec())));
        assert!(entries
            .contains(&("man".as_bytes().to_vec(), "woman".as_bytes().to_vec())));

        let tmp_dir = tempdir()?;
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        for (k, v) in [("bee", "honey"), ("fish", "water")] {
            db.put(k.as_bytes().to_vec(), v.as_bytes().to_vec())?;
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
        let mut db = GhalaDB::new(tmp_dir.path(), None)?;
        db.put("left".as_bytes().to_vec(), "right".as_bytes().to_vec())?;
        db.put("man".as_bytes().to_vec(), "woman".as_bytes().to_vec())?;
        assert_eq!(db.get("man".as_bytes())?, Some("woman".as_bytes().to_vec()));
        Ok(())
    }

    #[test]
    fn sst_merges() -> GhalaDbResult<()> {
        env_logger::try_init().ok();
        let tmp_dir = tempdir()?;
        let opts = DatabaseOptions::builder()
            .max_mem_table_size(16)
            .sync(false)
            .build();
        let mut db = GhalaDB::new(tmp_dir.path(), Some(opts))?;
        for (k, v) in dummy_vals() {
            db.put(k, v)?;
        }

        for (k, v) in dummy_vals() {
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
            .max_mem_table_size(1024)
            .max_vlog_size(4 * 1024)
            .sync(false)
            .build();
        let mut db = GhalaDB::new(tmp_dir.path(), Some(opts.clone()))?;
        let data = (0..100).map(|_| Bytes::gen()).collect::<Vec<_>>();
        for entry in &data {
            db.put(entry.clone(), entry.clone())?;
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
            .max_mem_table_size(1000 * 1024)
            .max_vlog_size(10000 * 1024)
            .vlog_compaction_enabled(true)
            .sync(false)
            .build();
        let unchanged: HashSet<Bytes> = (0..1000).map(|_| Bytes::gen()).collect();
        let deleted: HashSet<Bytes> = (0..1000).map(|_| Bytes::gen()).collect();
        let updated: HashSet<Bytes> = (0..1000).map(|_| Bytes::gen()).collect();
        let mut db = GhalaDB::new(tmp_dir.path(), Some(opts.clone()))?;
        assert!(unchanged.is_disjoint(&deleted));
        assert!(unchanged.is_disjoint(&updated));
        assert!(deleted.is_disjoint(&updated));

        for k in unchanged.iter().chain(deleted.iter()).chain(updated.iter()) {
            db.put(k.clone(), k.clone())?;
        }

        for k in &unchanged {
            assert_eq!(db.get(k)?, Some(k.clone()))
        }
        for k in &deleted {
            db.delete(k.clone())?;
        }
        for k in &updated {
            db.put(k.clone(), Bytes::gen())?;
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
        let mut db = GhalaDB::new(tmp_dir.path(), Some(opts))?;
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
