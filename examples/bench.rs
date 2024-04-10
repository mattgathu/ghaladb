use ghaladb::{GhalaDb, GhalaDbResult};
use rand::{distributions::Alphanumeric, prelude::ThreadRng, Rng};
use tempfile::tempdir;

fn gen_bytes(rng: &mut ThreadRng, len: usize) -> Vec<u8> {
    rng.sample_iter(Alphanumeric).take(len).collect()
}

fn main() -> GhalaDbResult<()> {
    let mut rng = rand::thread_rng();
    let tmp_dir = tempdir()?;
    let mut db: GhalaDb<Vec<u8>, Vec<u8>> = GhalaDb::new(tmp_dir.path(), None)?;
    let mut data = (0usize..1_000_000)
        .map(|_| {
            let (k, v) =
                (gen_bytes(&mut rng, 36usize), gen_bytes(&mut rng, 1000usize));
            (k, v)
        })
        .collect::<Vec<_>>();
    for (k, v) in &data {
        db.put(k, v)?;
    }
    for (k, _) in &data {
        db.get(k)?;
    }
    data.sort_unstable();
    for (k, _) in data {
        db.get(&k)?;
    }

    Ok(())
}
