use serde::{Deserialize, Serialize};

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

use crate::{error::GhalaDbResult, memtable::OpType};

pub trait WriteAheadLog {
    fn flush(&mut self) -> GhalaDbResult<()>;
    fn log(&mut self, record: LogRecord) -> GhalaDbResult<()>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRecord<'a> {
    pub key: &'a str,
    pub val: &'a str,
    pub tag: OpType,
}
impl<'a> LogRecord<'a> {
    pub fn new(key: &'a str, val: &'a str, tag: OpType) -> LogRecord<'a> {
        LogRecord { key, val, tag }
    }
}
pub struct OnDiskWal {
    f: BufWriter<File>,
}

impl OnDiskWal {
    pub fn new(path: &Path) -> GhalaDbResult<OnDiskWal> {
        let wal_path = path.join("wal");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(wal_path)?;
        Ok(OnDiskWal {
            f: BufWriter::new(f),
        })
    }

    //TODO
    // avoid flushing all the time
    // use a size limit + a timeout
    // if size is big enough or timeout as expired, flush to disk
    pub fn log(&mut self, record: LogRecord) -> GhalaDbResult<()> {
        self.f.write_all(&bincode::serialize(&record)?)?;
        self.f.write_all(b"\n")?;
        Ok(())
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
