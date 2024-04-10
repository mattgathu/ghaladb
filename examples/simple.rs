use ghaladb::{GhalaDb, GhalaDbResult};

fn main() -> GhalaDbResult<()> {
    let mut db: GhalaDb<String, String> = GhalaDb::new("/tmp/ghaladb", None)?;
    let key = "king".to_owned();
    let val = "queen".to_owned();
    db.put(&key, &val)?;
    assert_eq!(db.get(&key)?.unwrap(), val);
    Ok(())
}
