//! Proves that pushing and popping never touch the heap.

#![cfg(not(any(loom, miri)))]

use ringbook_ring::channel;
use ringbook_testkit::{CountingAllocator, count_allocations};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn push_and_pop_never_allocate() {
    const CAPACITY: u64 = 64;
    let (mut producer, mut consumer) = channel::<[u64; 7]>(CAPACITY as usize).unwrap();

    let mut received = 0_u64;
    let allocations = count_allocations(|| {
        for round in 0..10_000_u64 {
            // Filling and draining exercises the paths that refresh cached indices.
            for _ in 0..CAPACITY {
                assert!(producer.push([round; 7]).is_ok());
            }
            assert!(producer.push([round; 7]).is_err());
            while let Some(item) = consumer.pop() {
                received += item[6];
            }
        }
    });

    assert_eq!(received, (0..10_000_u64).sum::<u64>() * CAPACITY);
    assert_eq!(allocations, 0);
}
