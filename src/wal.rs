use serde::{Deserialize, Serialize};

use std::{
    convert::identity,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    path::PathBuf,
};

use crate::{
    error::{GhalaDBError, GhalaDbResult},
    memtable::{Bytes, EntryType, KeyRef},
};

pub trait WriteAheadLog {
    fn flush(&mut self) -> GhalaDbResult<()>;
    fn log(&mut self, record: LogRecord) -> GhalaDbResult<()>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRecord {
    pub key: Bytes,
    pub val: Option<Bytes>,
    pub tag: EntryType,
}
impl LogRecord {
    pub fn new(key: KeyRef, val: Option<Bytes>, tag: EntryType) -> LogRecord {
        LogRecord {
            key: key.to_vec(),
            val,
            tag,
        }
    }
}
pub struct OnDiskWal {
    f: BufWriter<File>,
    path: PathBuf,
}

impl OnDiskWal {
    pub fn new(path: &Path) -> GhalaDbResult<OnDiskWal> {
        let wal_path = path.join("wal");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(wal_path.clone())?;
        Ok(OnDiskWal {
            //TODO: provide db option to set WAL size capacity
            // and use `with_capacity` to create buffer
            f: BufWriter::new(f),
            path: wal_path,
        })
    }

    // FIXME
    // avoid flushing all the time
    // use a size limit + a timeout
    // if size is big enough or timeout as expired, flush to disk
    pub fn log(&mut self, record: LogRecord) -> GhalaDbResult<()> {
        self.f.write_all(&bincode::serialize(&record)?)?;
        self.f.write_all(b"\n")?;
        Ok(())
    }

    #[allow(unused)]
    pub fn entries(&self) -> GhalaDbResult<impl Iterator<Item = GhalaDbResult<LogRecord>>> {
        let buf = BufReader::new(File::open(self.path.clone())?);
        Ok(buf.split(b'\n').map(|l| {
            l.map(|li| {
                bincode::deserialize::<LogRecord>(&li).map_err(|e| GhalaDBError::SerdeError(e))
            })
            .map_err(|e| GhalaDBError::IOError(e))
            .and_then(identity)
        }))
    }

    fn flush(&mut self) -> GhalaDbResult<()> {
        Ok(self.f.flush()?)
    }
}

impl WriteAheadLog for OnDiskWal {
    fn flush(&mut self) -> GhalaDbResult<()> {
        self.flush()
    }
    fn log(&mut self, record: LogRecord) -> GhalaDbResult<()> {
        self.log(record)
    }
}

impl Drop for OnDiskWal {
    fn drop(&mut self) {
        self.flush().ok();
    }
}
