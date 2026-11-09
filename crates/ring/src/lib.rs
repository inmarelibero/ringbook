//! A bounded, lock-free, single-producer single-consumer ring buffer.
//!
//! All memory is allocated by [`channel`]. After that, [`Producer::push`] and
//! [`Consumer::pop`] never allocate, lock or wait, which makes them safe to
//! call from the hot thread.
//!
//! The head and tail indices increase monotonically and wrap at `usize::MAX`;
//! a slot's position is the index masked by the capacity. Each side keeps a
//! private copy of the index it owns and a cached copy of the other side's
//! index, so it reads the shared cache line only when the cache runs out.

mod sync;

use std::fmt;
use std::mem::MaybeUninit;

use sync::{Arc, AtomicUsize, Ordering, UnsafeCell};

/// Keeps a value on its own cache line.
///
/// 128 bytes covers Apple silicon's line size and the adjacent-line prefetcher
/// on x86-64.
#[repr(align(128))]
struct CacheAligned<T>(T);

struct Shared<T> {
    /// The next index to read. Written only by the consumer.
    head: CacheAligned<AtomicUsize>,
    /// The next index to write. Written only by the producer.
    tail: CacheAligned<AtomicUsize>,
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
}

// SAFETY: `Producer` and `Consumer` are the only handles to `Shared`. Neither
// is `Clone`, and their methods take `&mut self`, so there is one writer and
// one reader. The head and tail protocol gives each side exclusive access to
// the slots it touches, and transferring a `T` between threads requires
// `T: Send`.
unsafe impl<T: Send> Sync for Shared<T> {}

/// The capacity passed to [`channel`] was not a power of two.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapacityError(pub usize);

impl fmt::Display for CapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ring capacity must be a non-zero power of two, got {}",
            self.0
        )
    }
}

impl std::error::Error for CapacityError {}

/// Creates a ring holding up to `capacity` items.
///
/// Slot memory is written during construction, so its pages are already
/// mapped when the hot thread first touches them.
///
/// # Errors
///
/// Returns a [`CapacityError`] unless `capacity` is a non-zero power of two.
pub fn channel<T: Copy>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), CapacityError> {
    if !capacity.is_power_of_two() {
        return Err(CapacityError(capacity));
    }

    let shared = Arc::new(Shared {
        head: CacheAligned(AtomicUsize::new(0)),
        tail: CacheAligned(AtomicUsize::new(0)),
        slots: (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::zeroed()))
            .collect(),
        mask: capacity - 1,
    });

    let producer = Producer {
        shared: Arc::clone(&shared),
        tail: 0,
        cached_head: 0,
        capacity,
    };
    let consumer = Consumer {
        shared,
        head: 0,
        cached_tail: 0,
        capacity,
    };
    Ok((producer, consumer))
}

/// The writing half of a ring.
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
    tail: usize,
    cached_head: usize,
    capacity: usize,
}

impl<T: Copy> Producer<T> {
    /// Appends a value, or returns it if the ring is full.
    ///
    /// # Errors
    ///
    /// Returns the value unchanged if the ring is full.
    #[inline]
    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.tail.wrapping_sub(self.cached_head) == self.capacity {
            // Acquire pairs with the consumer's Release of head, so a slot is
            // reused only after the consumer has finished reading it.
            self.cached_head = self.shared.head.0.load(Ordering::Acquire);
            if self.tail.wrapping_sub(self.cached_head) == self.capacity {
                return Err(value);
            }
        }

        // SAFETY: `mask` is `slots.len() - 1` and `slots.len()` is a power of
        // two, so the masked index is in bounds.
        let slot = unsafe {
            self.shared
                .slots
                .get_unchecked(self.tail & self.shared.mask)
        };
        // SAFETY: the check above shows this slot is outside the consumer's
        // readable range, and `&mut self` makes this the only producer, so
        // nothing else can access it until tail is published below.
        slot.with_mut(|cell| unsafe { cell.write(MaybeUninit::new(value)) });

        self.tail = self.tail.wrapping_add(1);
        // Release makes the slot write visible to the consumer's Acquire.
        self.shared.tail.0.store(self.tail, Ordering::Release);
        Ok(())
    }

    /// Returns the number of slots currently free.
    ///
    /// The consumer may free more at any moment, so this is a lower bound.
    #[inline]
    pub fn free_slots(&mut self) -> usize {
        self.cached_head = self.shared.head.0.load(Ordering::Acquire);
        self.capacity - self.tail.wrapping_sub(self.cached_head)
    }

    /// Returns the ring's capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// The reading half of a ring.
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    head: usize,
    cached_tail: usize,
    capacity: usize,
}

impl<T: Copy> Consumer<T> {
    /// Removes and returns the oldest value, or `None` if the ring is empty.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.head == self.cached_tail {
            // Acquire pairs with the producer's Release of tail, so the slot's
            // contents are visible before it is read.
            self.cached_tail = self.shared.tail.0.load(Ordering::Acquire);
            if self.head == self.cached_tail {
                return None;
            }
        }

        // SAFETY: `mask` is `slots.len() - 1` and `slots.len()` is a power of
        // two, so the masked index is in bounds.
        let slot = unsafe {
            self.shared
                .slots
                .get_unchecked(self.head & self.shared.mask)
        };
        // SAFETY: the check above shows the producer has published this slot
        // and will not overwrite it until head moves past it below. The slot
        // was initialised by `push`, and `T: Copy` means reading it by value
        // can't lead to a double drop.
        let value = slot.with(|cell| unsafe { cell.read().assume_init() });

        self.head = self.head.wrapping_add(1);
        // Release tells the producer's Acquire that this read has finished.
        self.shared.head.0.store(self.head, Ordering::Release);
        Some(value)
    }

    /// Returns the number of values waiting to be read.
    ///
    /// The producer may add more at any moment, so this is a lower bound.
    #[inline]
    pub fn pending(&mut self) -> usize {
        self.cached_tail = self.shared.tail.0.load(Ordering::Acquire);
        self.cached_tail.wrapping_sub(self.head)
    }

    /// Returns the ring's capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn rejects_capacities_that_are_not_powers_of_two() {
        for capacity in [0, 3, 6, 1000] {
            assert_eq!(channel::<u8>(capacity).err(), Some(CapacityError(capacity)));
        }
    }

    #[test]
    fn delivers_values_in_order() {
        let (mut producer, mut consumer) = channel::<u32>(4).unwrap();
        for value in 0..4 {
            producer.push(value).unwrap();
        }
        for value in 0..4 {
            assert_eq!(consumer.pop(), Some(value));
        }
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn a_full_ring_returns_the_value_and_recovers() {
        let (mut producer, mut consumer) = channel::<u32>(2).unwrap();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert_eq!(producer.push(3), Err(3));

        assert_eq!(consumer.pop(), Some(1));
        producer.push(3).unwrap();
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
    }

    #[test]
    fn capacity_of_one_works() {
        let (mut producer, mut consumer) = channel::<u32>(1).unwrap();
        for value in 0..10 {
            producer.push(value).unwrap();
            assert_eq!(producer.push(99), Err(99));
            assert_eq!(consumer.pop(), Some(value));
        }
    }

    #[test]
    fn reports_free_slots_and_pending_values() {
        let (mut producer, mut consumer) = channel::<u32>(4).unwrap();
        assert_eq!(producer.capacity(), 4);
        assert_eq!(consumer.capacity(), 4);
        assert_eq!(producer.free_slots(), 4);
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert_eq!(producer.free_slots(), 2);
        assert_eq!(consumer.pending(), 2);
        consumer.pop().unwrap();
        assert_eq!(producer.free_slots(), 3);
        assert_eq!(consumer.pending(), 1);
    }

    #[test]
    fn indices_survive_wrapping_at_the_integer_limit() {
        let (mut producer, mut consumer) = channel::<usize>(4).unwrap();
        let start = usize::MAX - 1;
        producer.tail = start;
        producer.cached_head = start;
        producer.shared.tail.0.store(start, Ordering::Relaxed);
        producer.shared.head.0.store(start, Ordering::Relaxed);
        consumer.head = start;
        consumer.cached_tail = start;

        for value in 0..16 {
            producer.push(value).unwrap();
            producer.push(value + 100).unwrap();
            assert_eq!(consumer.pop(), Some(value));
            assert_eq!(consumer.pop(), Some(value + 100));
        }
    }

    #[test]
    fn head_and_tail_do_not_share_a_cache_line() {
        let (producer, _consumer) = channel::<u8>(2).unwrap();
        let head = std::ptr::from_ref(&producer.shared.head) as usize;
        let tail = std::ptr::from_ref(&producer.shared.tail) as usize;
        assert!(head.abs_diff(tail) >= 128);
    }

    #[test]
    fn transfers_across_threads_without_loss_or_reordering() {
        const COUNT: u64 = if cfg!(miri) { 2_000 } else { 2_000_000 };

        let (mut producer, mut consumer) = channel::<[u64; 2]>(64).unwrap();
        let writer = thread::spawn(move || {
            for value in 0..COUNT {
                let mut item = [value, !value];
                while let Err(returned) = producer.push(item) {
                    item = returned;
                    thread::yield_now();
                }
            }
        });

        for expected in 0..COUNT {
            loop {
                if let Some(item) = consumer.pop() {
                    assert_eq!(item, [expected, !expected]);
                    break;
                }
                thread::yield_now();
            }
        }
        writer.join().unwrap();
        assert_eq!(consumer.pop(), None);
    }
}
