//! The two clocks ringbook runs on.
//!
//! [`Clock::event_time`] stamps frames on receipt, and that stamp is the only
//! time trading logic reads. [`Clock::measure`] is used only for measuring
//! latency and never influences a decision.

#![forbid(unsafe_code)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ringbook_core::{EventTime, MeasureStamp};

/// A cheaply cloned source of event times and measurement stamps.
#[derive(Clone, Debug)]
pub struct Clock {
    origin: Instant,
    origin_unix_ns: u64,
    counter: quanta::Clock,
}

impl Clock {
    /// Creates a clock anchored to the current wall-clock time.
    #[must_use]
    pub fn new() -> Self {
        let origin_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos() as u64);
        Self {
            origin: Instant::now(),
            origin_unix_ns,
            counter: quanta::Clock::new(),
        }
    }

    /// Returns a monotonic timestamp in nanoseconds since the Unix epoch.
    #[inline]
    #[must_use]
    pub fn event_time(&self) -> EventTime {
        EventTime(self.origin_unix_ns + self.origin.elapsed().as_nanos() as u64)
    }

    /// Returns a raw stamp for measuring latency.
    #[inline]
    #[must_use]
    pub fn measure(&self) -> MeasureStamp {
        MeasureStamp(self.counter.raw())
    }

    /// Returns the nanoseconds between two measurement stamps.
    #[inline]
    #[must_use]
    pub fn elapsed_ns(&self, start: MeasureStamp, end: MeasureStamp) -> u64 {
        self.counter.delta_as_nanos(start.0, end.0)
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_time_never_goes_backwards() {
        let clock = Clock::new();
        let first = clock.event_time();
        let second = clock.event_time();
        assert!(second >= first);
    }

    #[test]
    fn measures_a_positive_interval() {
        let clock = Clock::new();
        let start = clock.measure();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(clock.elapsed_ns(start, clock.measure()) >= 1_000_000);
    }
}
