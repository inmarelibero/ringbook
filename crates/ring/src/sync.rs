//! Synchronisation primitives, swapped for loom's models under `--cfg loom`.

#[cfg(loom)]
pub(crate) use loom::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
#[cfg(not(loom))]
pub(crate) use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// `std::cell::UnsafeCell` behind the closure-based API that loom requires.
#[cfg(not(loom))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    #[inline(always)]
    pub(crate) fn with<R>(&self, operation: impl FnOnce(*const T) -> R) -> R {
        operation(self.0.get())
    }

    #[inline(always)]
    pub(crate) fn with_mut<R>(&self, operation: impl FnOnce(*mut T) -> R) -> R {
        operation(self.0.get())
    }
}
