/*!
GhalaDb is a key value datastore that implements keys and values separation
inspired by the [WiscKey](https://pages.cs.wisc.edu/~ll/papers/wisckey.pdf) paper.

GhalaDb implements a SSD-conscious data layout by decoupling the storage of
keys from values. An in-memory tree stores the keys along with pointers to
the values, while the values are stored in a separate log file.
This significantly reduces write amplification during ingestion,
while facilitating faster data loading.

Since GhalaDb keeps all its keys and data pointers in memory, it is suitable
for applications that have small-sized keys.

<div class="warning">!! GhalaDb is experimental software and might not be suitable for your use case.</div>


```ignore
use ghaladb::{GhalaDb, GhalaDbResult};

fn main() -> GhalaDbResult<()> {
    let mut db = GhalaDb::new("/tmp/ghaladb", None)?;
    let key = "king".to_owned();
    let val = "queen".to_owned();
    db.put(&key, &val)?;
    assert_eq!(db.get(&key)?.unwrap(), val);
    Ok(())
}
```
*/
#![deny(missing_docs, unused)]
#[macro_use]
extern crate log;
mod config;
mod core;
mod dec;
mod error;
mod gc;
mod ghaladb;
mod keys;
mod utils;
mod vlog;
pub use crate::{
    config::DatabaseOptions,
    error::{GhalaDbError, GhalaDbResult},
    ghaladb::GhalaDb,
};

//
//          _|                  _|                  _|  _|
//  _|_|_|  _|_|_|      _|_|_|  _|    _|_|_|    _|_|_|  _|_|_|
//_|    _|  _|    _|  _|    _|  _|  _|    _|  _|    _|  _|    _|
//_|    _|  _|    _|  _|    _|  _|  _|    _|  _|    _|  _|    _|
//  _|_|_|  _|    _|    _|_|_|  _|    _|_|_|    _|_|_|  _|_|_|
//      _|
//  _|_|
//
