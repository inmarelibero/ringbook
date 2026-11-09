//! Baseline ring buffer benchmarks.

#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ringbook_ring::channel;

fn benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("ring");
    group.throughput(Throughput::Elements(1));

    group.bench_function("push_pop_same_thread", |bench| {
        let (mut producer, mut consumer) = channel::<[u64; 7]>(1 << 12).unwrap();
        bench.iter(|| {
            producer.push(black_box([1; 7])).unwrap();
            black_box(consumer.pop().unwrap());
        });
    });

    group.bench_function("transfer_across_threads", |bench| {
        let (mut producer, mut consumer) = channel::<[u64; 7]>(1 << 12).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader_stop = std::sync::Arc::clone(&stop);
        let reader = std::thread::spawn(move || {
            while !reader_stop.load(std::sync::atomic::Ordering::Relaxed) {
                black_box(consumer.pop());
            }
        });
        bench.iter(|| {
            while producer.push(black_box([1; 7])).is_err() {
                std::hint::spin_loop();
            }
        });
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().unwrap();
    });
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
