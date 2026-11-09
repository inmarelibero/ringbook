//! Proves that book updates never touch the heap, even across recentres.

use ringbook_book::Book;
use ringbook_core::{Price, Qty, Side};
use ringbook_testkit::{CountingAllocator, count_allocations};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn updates_clears_and_recentres_never_allocate() {
    let mut book = Book::new(1, 1024).unwrap();

    let allocations = count_allocations(|| {
        for step in 0..50_000_i64 {
            let mid = 1_000_000 + step;
            book.apply(Side::Buy, Price(mid - 1), Qty(step % 5 + 1));
            book.apply(Side::Sell, Price(mid + 1), Qty(step % 7 + 1));
            book.apply(Side::Buy, Price(mid - 2), Qty::ZERO);
            assert!(book.mid().is_some());
            assert!(book.levels(Side::Buy).take(5).count() > 0);
            if step % 10_000 == 9_999 {
                book.clear();
            }
        }
    });

    assert!(book.stats().recentres > 0);
    assert_eq!(allocations, 0);
}
