GhalaDb
--
![docs.rs](https://img.shields.io/docsrs/ghaladb)
![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/mattgathu/ghaladb/tests.yml)
![Crates.io](https://img.shields.io/crates/d/ghaladb)


A key value datastore that implements keys and values separation inspired by
the [WiscKey](https://pages.cs.wisc.edu/~ll/papers/wisckey.pdf) paper.

GhalaDb implements a SSD-conscious data layout by decoupling the storage of
keys from values. An in-memory tree stores the keys along with pointers to
the values, while the values are stored in a separate log file.
This significantly reduces write amplification during ingestion,
while facilitating faster data loading.

Since GhalaDb keeps all its keys and data pointers in memory, it is suitable
for applications that have small-sized keys.


```rust
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


References
--
- https://arxiv.org/pdf/1812.07527.pdf LSM-based Storage Techniques: A Survey
- http://www.benstopford.com/2015/02/14/log-structured-merge-trees/ Log Structured Merge Trees
- https://dl.acm.org/doi/pdf/10.1145/3514221.3522563 - Dissecting, Designing, and Optimizing LSM-based Data Stores
- https://www.infoq.com/articles/API-Design-Joshua-Bloch/ Joshua Bloch: Bumper-Sticker API Design
