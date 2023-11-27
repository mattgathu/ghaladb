use ghaladb::{error::GhalaDbResult, GhalaDB};

fn main() -> GhalaDbResult<()> {
    let mut db = GhalaDB::new("/tmp/ghaladb", None)?;
    db.put("king".into(), "queen".into())?;
    assert_eq!(
        db.get("king".as_bytes())?.unwrap(),
        "queen".as_bytes().to_vec()
    );

    Ok(())
}
