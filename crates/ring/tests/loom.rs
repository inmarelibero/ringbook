//! Exhaustive thread-interleaving tests.
//!
//! Run with `RUSTFLAGS="--cfg loom" cargo test -p ringbook-ring --test loom --release`.

#![cfg(loom)]

use loom::thread;
use ringbook_ring::channel;

fn transfer(capacity: usize, count: usize) {
    loom::model(move || {
        let (mut producer, mut consumer) = channel::<[usize; 2]>(capacity).unwrap();

        let writer = thread::spawn(move || {
            for value in 0..count {
                while producer.push([value, !value]).is_err() {
                    thread::yield_now();
                }
            }
        });

        let mut expected = 0;
        while expected < count {
            match consumer.pop() {
                Some(item) => {
                    assert_eq!(item, [expected, !expected]);
                    expected += 1;
                }
                None => thread::yield_now(),
            }
        }

        writer.join().unwrap();
        assert_eq!(consumer.pop(), None);
    });
}

#[test]
fn values_are_never_lost_duplicated_or_torn() {
    transfer(4, 3);
}

#[test]
fn slots_are_reused_safely_when_the_ring_wraps() {
    transfer(2, 3);
}

#[test]
fn a_single_slot_hands_over_correctly() {
    transfer(1, 3);
}
