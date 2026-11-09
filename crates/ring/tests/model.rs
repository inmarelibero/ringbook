//! Checks the ring against a simple queue over generated operation sequences.

#![cfg(not(any(loom, miri)))]

use std::collections::VecDeque;

use proptest::prelude::*;
use ringbook_ring::channel;

#[derive(Clone, Debug)]
enum Operation {
    Push(u32),
    Pop,
}

fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![any::<u32>().prop_map(Operation::Push), Just(Operation::Pop)]
}

proptest! {
    #[test]
    fn behaves_like_a_bounded_queue(
        capacity_power in 0_u32..6,
        operations in proptest::collection::vec(operation(), 0..512),
    ) {
        let capacity = 1_usize << capacity_power;
        let (mut producer, mut consumer) = channel::<u32>(capacity).unwrap();
        let mut model = VecDeque::new();

        for operation in operations {
            match operation {
                Operation::Push(value) if model.len() < capacity => {
                    prop_assert_eq!(producer.push(value), Ok(()));
                    model.push_back(value);
                }
                Operation::Push(value) => prop_assert_eq!(producer.push(value), Err(value)),
                Operation::Pop => prop_assert_eq!(consumer.pop(), model.pop_front()),
            }
            prop_assert_eq!(consumer.pending(), model.len());
            prop_assert_eq!(producer.free_slots(), capacity - model.len());
        }
    }
}
