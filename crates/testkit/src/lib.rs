//! Test support shared across the ringbook workspace.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    // Counted per thread, so the test harness's own threads can't interfere.
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

/// A global allocator that counts each thread's allocator calls.
///
/// Install it in an integration test with `#[global_allocator]`.
pub struct CountingAllocator;

fn count() {
    ALLOCATIONS.with(|allocations| allocations.set(allocations.get() + 1));
}

// SAFETY: every call forwards unchanged to `System`, which upholds the
// `GlobalAlloc` contract; counting doesn't allocate or touch the memory.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: the caller's guarantees for `layout` are forwarded.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        count();
        // SAFETY: the caller's guarantees are forwarded, and the memory came
        // from `System`.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        // SAFETY: the caller's guarantees are forwarded, and the memory came
        // from `System`.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

/// Returns how many times `operation` called the allocator on this thread.
pub fn count_allocations(operation: impl FnOnce()) -> usize {
    let before = ALLOCATIONS.with(Cell::get);
    operation();
    ALLOCATIONS.with(Cell::get) - before
}
