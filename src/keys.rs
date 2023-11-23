use crate::{
    config::DatabaseOptions,
    core::{Bytes, DataPtr, KeyRef, ValueEntry},
    dec::Dec,
    error::{GhalaDBError, GhalaDbResult},
    utils::t,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Skt {
    map: BTreeMap<Bytes, ValueEntry>,
    path: PathBuf,
    magic: u128,
}

impl Skt {
    pub fn new(path: PathBuf) -> Skt {
        let map = BTreeMap::new();
        let magic = 0;
        Self { map, path, magic }
    }

    #[allow(unused)]
    pub fn from_path<P: AsRef<Path>>(
        path: P,
        conf: DatabaseOptions,
    ) -> GhalaDbResult<Skt> {
        let skt = if path.as_ref().exists() {
            let mut rdr =
                BufReader::new(OpenOptions::new().read(true).open(path.as_ref())?);
            let mut buf = vec![];
            rdr.read_to_end(&mut buf)?;
            Dec::deser_raw(&buf)?
        } else {
            Skt::new(path.as_ref().to_path_buf())
        };
        Ok(skt)
    }

    pub fn delete(&mut self, key: KeyRef) -> GhalaDbResult<()> {
        self.map.remove(key);
        Ok(())
    }

    pub fn get(&mut self, key: KeyRef) -> Option<DataPtr> {
        self.map.get(key).and_then(|ve| match ve {
            ValueEntry::Val(dp) => Some(*dp),
            ValueEntry::Tombstone => None,
        })
    }

    pub fn get_ve(&mut self, key: KeyRef) -> GhalaDbResult<ValueEntry> {
        if let Some(ve) = self.map.get(key) {
            Ok(*ve)
        } else {
            Err(GhalaDBError::MissingValueEntry(key.to_vec()))
        }
    }

    pub fn put(&mut self, k: Bytes, v: ValueEntry) -> GhalaDbResult<()> {
        self.map.insert(k, v);
        self.magic = Self::time()?;
        //TODO: sync if delta is large
        Ok(())
    }

    //TODO: verify
    #[allow(unused)]
    pub fn mem_size(&self) -> usize {
        std::mem::size_of_val(&self.map)
    }

    //TODO: remove clone
    pub fn iter(
        &self,
    ) -> Box<dyn Iterator<Item = GhalaDbResult<(Bytes, ValueEntry)>> + '_> {
        let it = self.map.iter().map(|(k, v)| Ok((k.clone(), *v)));
        Box::new(it)
    }

    pub fn sync(&self) -> GhalaDbResult<()> {
        let mut wtr = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&self.path)?,
        );
        let bytes = Dec::ser_raw(&self)?;
        wtr.write_all(&bytes)?;
        Ok(())
    }

    fn time() -> GhalaDbResult<u128> {
        Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())
    }
}
impl Drop for Skt {
    fn drop(&mut self) {
        t!("Skt::sync", self.sync()).ok();
    }
}
