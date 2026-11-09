//! Streams recorded inputs through the same parsers as a live run.
//!
//! Venues are merged by receive timestamp, then venue, then per-venue
//! sequence, giving one total order. Trading logic only sees event time, so
//! the pacing affects measurements but never decisions.

#![forbid(unsafe_code)]

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ringbook_clock::Clock;
use ringbook_core::{Event, EventTime, VenueId};
use ringbook_feeds::recording::EXTENSION;
use ringbook_feeds::{
    ConnEvent, FrameParser, Input, ParseOutput, RecordKind, Recording, RecordingError,
    RecordingReader,
};
use ringbook_ring::Producer;

/// How fast a recording is played back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pacing {
    /// At the original pace; used for latency measurement.
    Recorded,
    /// As fast as possible; used for throughput measurement.
    Max,
}

/// One venue's recording and parser.
pub struct ReplayVenue {
    /// The venue's runtime identifier.
    pub venue: VenueId,
    /// The venue's parser.
    pub parser: Box<dyn FrameParser>,
    /// The venue's recording.
    pub reader: RecordingReader,
}

/// Returns the path of a venue's file inside a recording directory.
#[must_use]
pub fn venue_file(directory: &Path, venue_name: &str) -> std::path::PathBuf {
    directory.join(format!("{venue_name}.{EXTENSION}"))
}

/// Reads the metadata record that starts a venue's recording.
///
/// # Errors
///
/// Returns a [`RecordingError`] if the file is unreadable or doesn't start
/// with metadata.
pub fn read_metadata(path: &Path) -> Result<Vec<u8>, RecordingError> {
    let mut reader = RecordingReader::open(path)?;
    match reader.next_record()? {
        Some(record) if record.kind == RecordKind::Metadata => Ok(record.payload),
        _ => Err(RecordingError::BadHeader),
    }
}

/// What a replay did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplaySummary {
    /// Records read from disk.
    pub records: u64,
    /// Events published to the engine.
    pub events: u64,
    /// Places where the recorder lost data.
    pub recording_gaps: u64,
}

struct Head {
    received: EventTime,
    venue_index: usize,
    sequence: u64,
    record: Recording,
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for Head {}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

impl Head {
    const fn key(&self) -> (EventTime, usize, u64) {
        (self.received, self.venue_index, self.sequence)
    }
}

fn input_from(record: &Recording) -> Option<Input<'_>> {
    match record.kind {
        RecordKind::Frame => Some(Input::Frame {
            stream: record.stream,
            data: &record.payload,
        }),
        RecordKind::Rest => Some(Input::Rest {
            tag: record.tag,
            body: &record.payload,
        }),
        RecordKind::Conn => {
            let event = record
                .payload
                .first()
                .copied()
                .and_then(ConnEvent::from_code)?;
            Some(Input::Conn {
                stream: record.stream,
                tag: record.tag,
                event,
            })
        }
        RecordKind::Metadata | RecordKind::Gap => None,
    }
}

/// Plays recordings into `producer`, waiting for ring space rather than
/// dropping, until the recordings end or `stop` is set.
///
/// Parser actions are ignored, because the responses are in the recording.
///
/// # Errors
///
/// Returns a [`RecordingError`] if a recording is corrupt.
pub fn run_replay(
    mut venues: Vec<ReplayVenue>,
    pacing: Pacing,
    clock: &Clock,
    producer: &mut Producer<Event>,
    stop: &Arc<AtomicBool>,
) -> Result<ReplaySummary, RecordingError> {
    let mut heap = BinaryHeap::with_capacity(venues.len());
    for (venue_index, venue) in venues.iter_mut().enumerate() {
        push_next(&mut heap, venue_index, &mut venue.reader)?;
    }

    let mut summary = ReplaySummary::default();
    let mut output = ParseOutput::default();
    let started = Instant::now();
    let first_received = heap.peek().map(|head| head.0.received);

    while let Some(Reverse(head)) = heap.pop() {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        summary.records += 1;
        let venue_index = head.venue_index;
        let record = head.record;

        if record.kind == RecordKind::Gap {
            summary.recording_gaps += 1;
        }
        if pacing == Pacing::Recorded
            && let Some(first) = first_received
        {
            pace(started, record.received.0.saturating_sub(first.0));
        }
        if let Some(input) = input_from(&record) {
            let venue = &mut venues[venue_index];
            output.begin(record.received, clock.measure());
            venue.parser.on_input(input, &mut output);
            for event in output.events.drain(..) {
                let mut pending = event;
                while let Err(returned) = producer.push(pending) {
                    if stop.load(Ordering::Relaxed) {
                        return Ok(summary);
                    }
                    pending = returned;
                    std::hint::spin_loop();
                }
                summary.events += 1;
            }
        }
        push_next(&mut heap, venue_index, &mut venues[venue_index].reader)?;
    }
    Ok(summary)
}

fn push_next(
    heap: &mut BinaryHeap<Reverse<Head>>,
    venue_index: usize,
    reader: &mut RecordingReader,
) -> Result<(), RecordingError> {
    if let Some(record) = reader.next_record()? {
        heap.push(Reverse(Head {
            received: record.received,
            venue_index,
            sequence: record.sequence,
            record,
        }));
    }
    Ok(())
}

fn pace(started: Instant, offset_ns: u64) {
    let target = Duration::from_nanos(offset_ns);
    loop {
        let elapsed = started.elapsed();
        if elapsed >= target {
            return;
        }
        let remaining = target - elapsed;
        if remaining > Duration::from_millis(2) {
            std::thread::sleep(remaining - Duration::from_millis(1));
        } else {
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbook_core::{InstrumentId, Payload, Price, Qty, Side};
    use ringbook_feeds::{ParserStats, start_recorder};
    use ringbook_ring::channel;

    struct EchoParser {
        venue: VenueId,
    }

    impl FrameParser for EchoParser {
        fn on_input(&mut self, input: Input<'_>, output: &mut ParseOutput) {
            if let Input::Frame { data, .. } = input {
                output.push(
                    self.venue,
                    InstrumentId(0),
                    None,
                    Payload::BookLevel {
                        side: Side::Buy,
                        price: Price(i64::from(data[0])),
                        qty: Qty(1),
                    },
                );
                output.end_batch();
            }
        }

        fn stats(&self) -> ParserStats {
            ParserStats::default()
        }
    }

    fn record(received: u64, sequence: u64, value: u8) -> Recording {
        Recording {
            received: EventTime(received),
            sequence,
            kind: RecordKind::Frame,
            stream: 0,
            tag: 0,
            payload: vec![value],
        }
    }

    fn write(name: &str, records: &[Recording]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ringbook-replay-{}-{name}.{EXTENSION}",
            std::process::id()
        ));
        let (handle, thread) = start_recorder(&path).unwrap();
        for record in records {
            handle.record(record.clone());
        }
        drop(handle);
        thread.join().unwrap();
        path
    }

    fn replay(paths: &[std::path::PathBuf], pacing: Pacing) -> Vec<i64> {
        let venues = paths
            .iter()
            .enumerate()
            .map(|(index, path)| ReplayVenue {
                venue: VenueId(index as u8),
                parser: Box::new(EchoParser {
                    venue: VenueId(index as u8),
                }),
                reader: RecordingReader::open(path).unwrap(),
            })
            .collect();
        let (mut producer, mut consumer) = channel(64).unwrap();
        run_replay(
            venues,
            pacing,
            &Clock::new(),
            &mut producer,
            &Arc::new(AtomicBool::new(false)),
        )
        .unwrap();

        let mut prices = Vec::new();
        while let Some(event) = consumer.pop() {
            if let Payload::BookLevel { price, .. } = event.payload {
                prices.push(price.0);
            }
        }
        prices
    }

    #[test]
    fn venues_merge_by_time_with_a_deterministic_tie_break() {
        let first = write(
            "merge-a",
            &[record(10, 1, 1), record(30, 2, 3), record(30, 3, 4)],
        );
        let second = write("merge-b", &[record(20, 1, 2), record(30, 2, 5)]);
        let paths = [first, second];

        let order = replay(&paths, Pacing::Max);
        assert_eq!(order, [1, 2, 3, 4, 5]);
        assert_eq!(replay(&paths, Pacing::Recorded), order);

        for path in paths {
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn stops_on_request_even_when_the_ring_is_full() {
        let path = write(
            "stop",
            &(0..32).map(|i| record(i, i, 1)).collect::<Vec<_>>(),
        );
        let venues = vec![ReplayVenue {
            venue: VenueId(0),
            parser: Box::new(EchoParser { venue: VenueId(0) }),
            reader: RecordingReader::open(&path).unwrap(),
        }];
        let (mut producer, _consumer) = channel(4).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopper = Arc::clone(&stop);
        // Creating a clock calibrates it, which takes longer than the timer.
        let clock = Clock::new();
        let timer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            stopper.store(true, Ordering::Relaxed);
        });

        let summary = run_replay(venues, Pacing::Max, &clock, &mut producer, &stop).unwrap();
        timer.join().unwrap();
        assert_eq!(summary.events, 4);
        std::fs::remove_file(path).unwrap();
    }
}
