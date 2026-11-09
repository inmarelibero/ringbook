//! Baseline order book benchmarks.

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use ringbook_book::Book;
use ringbook_core::{Price, Qty, Side};

/// A repeatable mix of updates near the top of the book.
fn updates() -> Vec<(Side, i64, i64)> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    (0..4096)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let offset = (state % 64) as i64 + 1;
            let qty = ((state >> 8) % 8) as i64;
            if state & (1 << 40) == 0 {
                (Side::Buy, 1_000_000 - offset, qty)
            } else {
                (Side::Sell, 1_000_000 + offset, qty)
            }
        })
        .collect()
}

fn benchmarks(criterion: &mut Criterion) {
    let updates = updates();

    criterion.bench_function("book/flat_array/apply_4096", |bench| {
        let mut book = Book::new(1, 1 << 17).unwrap();
        bench.iter(|| {
            for &(side, price, qty) in &updates {
                book.apply(side, Price(price), Qty(qty));
            }
            black_box(book.best_bid());
        });
    });

    criterion.bench_function("book/btreemap/apply_4096", |bench| {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        bench.iter(|| {
            for &(side, price, qty) in &updates {
                let levels = if side == Side::Buy {
                    &mut bids
                } else {
                    &mut asks
                };
                if qty == 0 {
                    levels.remove(&price);
                } else {
                    levels.insert(price, qty);
                }
            }
            black_box(bids.last_key_value());
        });
    });
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
