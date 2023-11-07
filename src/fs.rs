use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

#[cfg(not(feature = "mmap"))]
use std::io::BufWriter;

#[cfg(feature = "mmap")]
use memmap2::MmapMut;

use crate::error::GhalaDbResult;

#[cfg(not(feature = "mmap"))]
pub(crate) struct FileWriter {
    inner: BufWriter<File>,
}

#[cfg(feature = "mmap")]
pub(crate) struct FileWriter {
    inner: MmapMut,
    pos: usize,
    file: File,
}

impl FileWriter {
    fn get_file(path: &Path) -> GhalaDbResult<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(file)
    }
    pub fn flush(&mut self) -> GhalaDbResult<()> {
        let len = self.inner.len();
        let pos = self.pos;
        error!("self pos: {pos} mmap len: {len}");
        if self.inner.len() > 0usize {
            self.inner.flush().map_err(|e| {
                error!("flush failed. Reason: {e}");
                e
            })?;
        }
        Ok(())
    }
}

#[cfg(not(feature = "mmap"))]
impl FileWriter {
    pub fn new(path: &Path) -> GhalaDbResult<Self> {
        let file = Self::get_file(path)?;
        let wtr = BufWriter::new(file);
        Ok(Self { inner: wtr })
    }
    pub fn seek(&mut self, pos: SeekFrom) -> GhalaDbResult<()> {
        self.inner.seek(pos)?;
        Ok(())
    }
    pub fn write_all(&mut self, buf: &[u8]) -> GhalaDbResult<()> {
        self.inner.write_all(buf)?;
        Ok(())
    }

    pub fn stream_position(&mut self) -> GhalaDbResult<u64> {
        let pos = self.inner.stream_position()?;
        Ok(pos)
    }
}
#[cfg(feature = "mmap")]
impl FileWriter {
    pub fn new(path: &Path) -> GhalaDbResult<Self> {
        let file = Self::get_file(path)?;
        let inner = unsafe {
            MmapMut::map_mut(&file).map_err(|e| {
                error!("failed to create mmap. Reason: {}", e);
                e
            })?
        };
        let pos = 0usize;
        Ok(Self { inner, pos, file })
    }
    pub fn seek(&mut self, seek: SeekFrom) -> GhalaDbResult<()> {
        //TODO
        match seek {
            SeekFrom::Start(pos) => {
                self.pos = pos as usize;
            }
            SeekFrom::Current(pos) => {
                self.pos += pos as usize;
            }
            SeekFrom::End(_pos) => {
                // TODO
                // not sure what to do here
            }
        }
        Ok(())
    }
    pub fn write_all(&mut self, buf: &[u8]) -> GhalaDbResult<()> {
        debug_assert!(!buf.is_empty(), "write buf empty");
        let mlen = self.inner.len();
        error!("write_all buf len: {} mmap len: {mlen}", buf.len());
        (&mut self.inner[self.pos..]).write_all(buf).map_err(|e| {
            error!("failed to write_all. Reason: {e}");
            e
        })?;
        self.pos += buf.len();
        Ok(())
    }
    //pub fn write_all(&mut self, mut buf: &[u8]) -> GhalaDbResult<()> {
    //        while !buf.is_empty() {
    //            match self.write(buf) {
    //                Ok(0) => {
    //                    return Err(error::const_io_error!(
    //                        ErrorKind::WriteZero,
    //                        "failed to write whole buffer",
    //                    ));
    //                }
    //                Ok(n) => buf = &buf[n..],
    //                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
    //                Err(e) => return Err(e),
    //            }
    //        }
    //        Ok(())
    //    }
    pub fn stream_position(&mut self) -> GhalaDbResult<u64> {
        let pos = self.file.stream_position().map_err(|e| {
            error!("failed to get stream pos");
            e
        })?;
        Ok(pos)
    }
}
