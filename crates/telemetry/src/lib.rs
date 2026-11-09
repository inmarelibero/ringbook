//! Collects latency samples and counters from the hot thread.
//!
//! The hot thread only pushes to a ring and drops records if it is full, so
//! it can never wait for this code. Once a second the collected numbers go to
//! a caller-supplied sink, which is how metrics and storage stay separate.

#![forbid(unsafe_code)]

pub mod metrics_export;
pub mod storage;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use ringbook_engine::{EngineCounters, TelemetryRecord};
use ringbook_ring::Consumer;

const PUBLISH_PERIOD: Duration = Duration::from_secs(1);
const IDLE_SLEEP: Duration = Duration::from_micros(200);
const HIGHEST_TRACKABLE_NS: u64 = 3_600_000_000_000;
const SIGNIFICANT_FIGURES: u8 = 3;

/// A latency distribution in nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Percentiles {
    /// The number of samples.
    pub count: u64,
    /// The median.
    pub p50: u64,
    /// The 99th percentile.
    pub p99: u64,
    /// The 99.9th percentile.
    pub p999: u64,
    /// The largest sample.
    pub max: u64,
}

fn percentiles(histogram: &Histogram<u64>) -> Percentiles {
    Percentiles {
        count: histogram.len(),
        p50: histogram.value_at_quantile(0.50),
        p99: histogram.value_at_quantile(0.99),
        p999: histogram.value_at_quantile(0.999),
        max: histogram.max(),
    }
}

fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HIGHEST_TRACKABLE_NS, SIGNIFICANT_FIGURES)
        .expect("constant bounds are valid")
}

#[derive(Clone)]
struct Latencies {
    tick_to_decision: Histogram<u64>,
    batch: Histogram<u64>,
    tick_to_wire: Histogram<u64>,
}

impl Latencies {
    fn new() -> Self {
        Self {
            tick_to_decision: histogram(),
            batch: histogram(),
            tick_to_wire: histogram(),
        }
    }

    fn record(&mut self, record: &TelemetryRecord) {
        match *record {
            TelemetryRecord::TickToDecision(ns) => self.tick_to_decision.saturating_record(ns),
            TelemetryRecord::BatchLatency(ns) => self.batch.saturating_record(ns),
            TelemetryRecord::TickToWire(ns) => self.tick_to_wire.saturating_record(ns),
            TelemetryRecord::Counters(_) => {}
        }
    }

    fn clear(&mut self) {
        self.tick_to_decision.reset();
        self.batch.reset();
        self.tick_to_wire.reset();
    }

    fn summary(&self) -> LatencySummary {
        LatencySummary {
            tick_to_decision: percentiles(&self.tick_to_decision),
            batch: percentiles(&self.batch),
            tick_to_wire: percentiles(&self.tick_to_wire),
        }
    }
}

/// The latency distributions tracked for a period.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LatencySummary {
    /// From a frame reaching the parser to an order decision.
    pub tick_to_decision: Percentiles,
    /// From a frame reaching the parser to the end of its batch.
    pub batch: Percentiles,
    /// From a frame reaching the parser to the order reaching the socket.
    pub tick_to_wire: Percentiles,
}

/// What is published once a second.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Latencies since the previous snapshot.
    pub interval: LatencySummary,
    /// The latest engine counters.
    pub counters: EngineCounters,
}

/// What the telemetry thread reports when it finishes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TelemetryReport {
    /// Latencies over the whole run.
    pub total: LatencySummary,
    /// The final engine counters.
    pub counters: EngineCounters,
}

/// Starts the telemetry thread.
///
/// It runs until `stop` is set and the rings are empty, then returns the
/// whole-run report.
///
/// # Errors
///
/// Returns an error if the thread can't be started.
pub fn start(
    mut rings: Vec<Consumer<TelemetryRecord>>,
    mut sink: impl FnMut(&Snapshot) + Send + 'static,
    stop: Arc<AtomicBool>,
) -> std::io::Result<JoinHandle<TelemetryReport>> {
    std::thread::Builder::new()
        .name("telemetry".into())
        .spawn(move || {
            let mut interval = Latencies::new();
            let mut total = Latencies::new();
            let mut counters = EngineCounters::default();
            let mut last_publish = Instant::now();

            loop {
                let stopping = stop.load(Ordering::Acquire);
                let mut busy = false;
                for ring in &mut rings {
                    while let Some(record) = ring.pop() {
                        busy = true;
                        if let TelemetryRecord::Counters(latest) = record {
                            counters = latest;
                        }
                        interval.record(&record);
                        total.record(&record);
                    }
                }

                if last_publish.elapsed() >= PUBLISH_PERIOD {
                    last_publish = Instant::now();
                    sink(&Snapshot {
                        interval: interval.summary(),
                        counters,
                    });
                    interval.clear();
                }

                if !busy {
                    if stopping {
                        sink(&Snapshot {
                            interval: interval.summary(),
                            counters,
                        });
                        return TelemetryReport {
                            total: total.summary(),
                            counters,
                        };
                    }
                    std::thread::sleep(IDLE_SLEEP);
                }
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbook_ring::channel;
    use std::sync::Mutex;

    #[test]
    fn summarises_samples_from_every_ring() {
        let (mut first, first_rx) = channel(16).unwrap();
        let (mut second, second_rx) = channel(16).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let snapshots = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&snapshots);
        let thread = start(
            vec![first_rx, second_rx],
            move |snapshot: &Snapshot| seen.lock().unwrap().push(*snapshot),
            Arc::clone(&stop),
        )
        .unwrap();

        first.push(TelemetryRecord::TickToDecision(1_000)).unwrap();
        first.push(TelemetryRecord::TickToDecision(3_000)).unwrap();
        second.push(TelemetryRecord::TickToWire(9_000)).unwrap();
        let counters = EngineCounters {
            fills: 7,
            ..EngineCounters::default()
        };
        first.push(TelemetryRecord::Counters(counters)).unwrap();
        stop.store(true, Ordering::Release);

        let report = thread.join().unwrap();
        assert_eq!(report.total.tick_to_decision.count, 2);
        assert_eq!(report.total.tick_to_wire.count, 1);
        assert_eq!(report.counters.fills, 7);
        assert!(report.total.tick_to_decision.max >= 2_999);
        assert_eq!(snapshots.lock().unwrap().last().unwrap().counters.fills, 7);
    }

    #[test]
    fn an_empty_run_reports_zeroes() {
        let stop = Arc::new(AtomicBool::new(true));
        let report = start(Vec::new(), |_| {}, stop).unwrap().join().unwrap();
        assert_eq!(report, TelemetryReport::default());
    }
}
