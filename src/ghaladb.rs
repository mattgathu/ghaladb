use crate::{
    config::DatabaseOptions,
    core::{Bytes, KeyRef, ValueEntry},
    error::{GhalaDBError, GhalaDbResult},
    gc::Janitor,
    keys::Skt,
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
    keys: Skt,
    vlogs_man: VlogsMan,
    janitor: Option<Janitor>,
    opts: DatabaseOptions,
    sweeping: bool,
}

impl GhalaDB {
    pub fn new<P: AsRef<Path>>(
        path: P,
        options: Option<DatabaseOptions>,
    ) -> GhalaDbResult<GhalaDB> {
        info!("database init. path: {:?}", path.as_ref());
        debug!("database init with options: {:#?}", options);
        let opts = options.unwrap_or_else(|| DatabaseOptions::builder().build());
        Self::init_dir(path.as_ref())?;
        let skt_path = path.as_ref().join("skt");

        let vlogs_man = VlogsMan::new(path.as_ref(), opts.clone())?;
        let keys = Skt::from_path(skt_path, opts.clone())?;
        let janitor = None;
        let db = GhalaDB {
            keys,
            vlogs_man,
            janitor,
            opts,
            sweeping: false,
        };
        Ok(db)
    }

    pub fn delete(&mut self, key: KeyRef) -> GhalaDbResult<()> {
        trace!("deleting: {:?}", key);
        t!("keys::del", self.keys.delete(key))?;
        Ok(())
    }

    pub fn get(&mut self, key: KeyRef) -> GhalaDbResult<Option<Bytes>> {
        if let Some(dp) = self.keys.get(key) {
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
        t!("keys::put", self.keys.put(key, ValueEntry::Val(dp)))?;
        t!("gc", self.gc())?;

        Ok(())
    }

    pub fn iter(
        &mut self,
    ) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<(Bytes, Bytes)>> + '_>
    {
        let db_iter: GhalaDBIter = GhalaDBIter {
            iter: Box::new(self.keys.iter()),
            valman: &mut self.vlogs_man,
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
                t!("gc::put", self.put(de.key, de.val))?;
            } else {
                t!("vlogs_man::drop_vlog", self.vlogs_man.drop_vlog(jan.vnum()))?;
                self.janitor = None;
            }
        } else if let Some((vnum, path)) = self.vlogs_man.needs_gc()? {
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
                        Err(GhalaDBError::DbPathNotDirectory(path.to_path_buf()))
                    }
                }
                _ => Err(GhalaDBError::IOError(e)),
            },
        }?;
        Ok(())
    }
}

pub struct GhalaDBIter<'a> {
    iter: Box<dyn Iterator<Item = (&'a Bytes, &'a ValueEntry)> + 'a>,
    valman: &'a mut VlogsMan,
}

impl Iterator for GhalaDBIter<'_> {
    type Item = GhalaDbResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.nxt() {
            Err(e) => Some(Err(e)),
            Ok(None) => None,
            Ok(Some(kv)) => Some(Ok(kv)),
        }
    }
}

impl GhalaDBIter<'_> {
    fn nxt(&mut self) -> GhalaDbResult<Option<(Bytes, Bytes)>> {
        loop {
            if let Some((_, v)) = self.iter.next() {
                match v {
                    ValueEntry::Tombstone => continue,
                    ValueEntry::Val(dp) => {
                        let v = self.valman.get(dp)?;
                        return Ok(Some((v.key, v.val)));
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
        db.delete(&k)?;
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
        db.delete("king".as_bytes())?;
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
        let opts = DatabaseOptions::builder().sync(false).build();
        let mut db = GhalaDB::new(tmp_dir.path(), Some(opts))?;
        for (k, v) in dummy_vals() {
            db.put(k, v)?;
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
            .max_vlog_size(10000 * 1024)
            .compact(true)
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
            db.delete(k)?;
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
