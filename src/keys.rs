use crate::{
    config::DatabaseOptions,
    core::{Bytes, DataPtr, KeyRef},
    dec::Dec,
    error::GhalaDbResult,
    utils::t,
};
use bincode::{Decode, Encode};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// Sorted Keys Table
///
/// This is an in-memory map that stores keys and their data pointer.
/// It is automatically synced to disk during datastore shutdown (when GhalaDb is dropped)
/// but it can also be synced manually using the `sync` method of GhalaDb.
#[derive(Encode, Decode, Debug)]
pub(crate) struct Skt {
    map: BTreeMap<Bytes, DataPtr>,
    path: PathBuf,
    magic: u128,
    conf: DatabaseOptions,
}

impl Skt {
    pub fn new(path: PathBuf, conf: DatabaseOptions) -> Skt {
        let map = BTreeMap::new();
        let magic = 0;
        Self {
            map,
            path,
            magic,
            conf,
        }
    }

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
            Skt::new(path.as_ref().to_path_buf(), conf)
        };
        Ok(skt)
    }

    pub fn delete(&mut self, key: KeyRef) -> GhalaDbResult<()> {
        trace!("Skt::delete");
        self.map.remove(key);
        Ok(())
    }

    pub fn get(&mut self, key: KeyRef) -> Option<DataPtr> {
        trace!("Skt::get");
        self.map.get(key).copied()
    }

    pub fn put(&mut self, k: Bytes, v: DataPtr) -> GhalaDbResult<()> {
        trace!("Skt::put");
        self.map.insert(k, v);
        let elapsed = Self::time()? - self.magic;

        if elapsed > (self.conf.keys_sync_interval * 10u128.pow(9)) {
            self.sync()?;
        }
        self.magic = Self::time()?;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &DataPtr)> {
        self.map.iter()
    }

    // TODO: implement partial sync to only update changes instead of entire table
    pub fn sync(&self) -> GhalaDbResult<()> {
        trace!("Skt::sync");
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
