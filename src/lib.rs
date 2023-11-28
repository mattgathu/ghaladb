/*!
 GhalaDB is a Log Structured Merge trees (LSM) based key value datastore that implements keys and values
 separation inspired by the [WiscKey](https://pages.cs.wisc.edu/~ll/papers/wisckey.pdf) paper.

 It keeps all its keys in memory with pointers to values on disk. It's therefore suitable
 for applications that have small-sized keys.


```rust
use ghaladb::{GhalaDb, GhalaDbResult};

fn main() -> GhalaDbResult<()> {
    let mut db = GhalaDb::new("/tmp/ghaladb", None)?;
    db.put("king".into(), "queen".into())?;
    assert_eq!(
        db.get("king".as_bytes())?.unwrap(),
        "queen".as_bytes().to_vec()
    );

    Ok(())
}
```
*/
#![deny(missing_docs)]
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
pub use crate::error::{GhalaDbError, GhalaDbResult};
pub use crate::ghaladb::GhalaDb;

//
//          _|                  _|                  _|  _|
//  _|_|_|  _|_|_|      _|_|_|  _|    _|_|_|    _|_|_|  _|_|_|
//_|    _|  _|    _|  _|    _|  _|  _|    _|  _|    _|  _|    _|
//_|    _|  _|    _|  _|    _|  _|  _|    _|  _|    _|  _|    _|
//  _|_|_|  _|    _|    _|_|_|  _|    _|_|_|    _|_|_|  _|_|_|
//      _|
//  _|_|
//
