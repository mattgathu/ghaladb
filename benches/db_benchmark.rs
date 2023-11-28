use criterion::{criterion_group, criterion_main, Criterion};
use ghaladb::GhalaDb;
use rand::{distributions::Alphanumeric, prelude::ThreadRng, Rng};
use tempfile::tempdir;

fn gen_bytes(rng: &mut ThreadRng, len: usize) -> Vec<u8> {
    rng.sample_iter(Alphanumeric).take(len).collect()
}

pub fn small_kv_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let tmp_dir = tempdir().expect("failed to create temp dir");
    let mut db = GhalaDb::new(tmp_dir.path(), None).unwrap();

    let mut data = (0usize..)
        .map(|_| (gen_bytes(&mut rng, 36usize), gen_bytes(&mut rng, 1000usize)));

    let mut group = c.benchmark_group("small_kv");
    group.throughput(criterion::Throughput::Bytes(1000u64));
    group.bench_function("put", |b| {
        b.iter_batched(
            || data.next().unwrap(),
            |(k, v)| db.put(k, v),
            criterion::BatchSize::SmallInput,
        )
    });
    let tmp_dir = tempdir().expect("failed to create temp dir");
    let mut db = GhalaDb::new(tmp_dir.path(), None).unwrap();
    let mut keys = (0usize..1_000_000)
        .map(|_| {
            let (k, v) =
                (gen_bytes(&mut rng, 36usize), gen_bytes(&mut rng, 1000usize));
            db.put(k.clone(), v).ok();
            k
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    let mut keys = keys.into_iter();
    group.bench_function("get", |b| {
        b.iter_batched(
            || keys.next().unwrap_or_else(|| gen_bytes(&mut rng, 36usize)),
            |k| db.get(&k),
            criterion::BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, small_kv_benchmark);
criterion_main!(benches);
