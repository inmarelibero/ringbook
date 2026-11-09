//! The versioned on-disk format for raw market data, and its writer thread.
//!
//! A recording is a directory with one file per venue. Each record stores
//! exactly what arrived, so a parser fix can be replayed against old data.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread::JoinHandle;

use ringbook_core::EventTime;

const MAGIC: &[u8; 8] = b"RBREC\x001\n";
const HEADER_LEN: usize = 8 + 8 + 1 + 1 + 4 + 4 + 4;
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;
const QUEUE_DEPTH: usize = 1 << 16;

/// The file extension for per-venue recording files.
pub const EXTENSION: &str = "rbr";

/// What a record contains.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    /// The venue's metadata response.
    Metadata = 1,
    /// A WebSocket text frame.
    Frame = 2,
    /// A REST response.
    Rest = 3,
    /// A connection event; the payload is its code.
    Conn = 4,
    /// The recorder lost records here because it couldn't keep up.
    Gap = 5,
}

impl RecordKind {
    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Metadata),
            2 => Some(Self::Frame),
            3 => Some(Self::Rest),
            4 => Some(Self::Conn),
            5 => Some(Self::Gap),
            _ => None,
        }
    }
}

/// One recorded input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recording {
    /// When the input was received.
    pub received: EventTime,
    /// The per-venue sequence number.
    pub sequence: u64,
    /// What the record contains.
    pub kind: RecordKind,
    /// The endpoint's index.
    pub stream: u8,
    /// The REST request's tag.
    pub tag: u32,
    /// The raw bytes.
    pub payload: Vec<u8>,
}

fn write_record(writer: &mut impl Write, record: &Recording) -> io::Result<()> {
    let mut header = [0_u8; HEADER_LEN];
    header[0..8].copy_from_slice(&record.received.0.to_le_bytes());
    header[8..16].copy_from_slice(&record.sequence.to_le_bytes());
    header[16] = record.kind as u8;
    header[17] = record.stream;
    header[18..22].copy_from_slice(&record.tag.to_le_bytes());
    let length = u32::try_from(record.payload.len()).unwrap_or(u32::MAX);
    header[22..26].copy_from_slice(&length.to_le_bytes());

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[..26]);
    hasher.update(&record.payload);
    header[26..30].copy_from_slice(&hasher.finalize().to_le_bytes());

    writer.write_all(&header)?;
    writer.write_all(&record.payload)
}

/// Why a recording could not be read.
#[derive(Debug)]
pub enum RecordingError {
    /// The file could not be read.
    Io(io::Error),
    /// The file isn't a recording in a supported format.
    BadHeader,
    /// A record failed its checksum or is malformed before the end of the file.
    Corrupt {
        /// The sequence number of the last good record.
        after_sequence: Option<u64>,
    },
}

impl fmt::Display for RecordingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "recording I/O error: {error}"),
            Self::BadHeader => formatter.write_str("not a supported recording file"),
            Self::Corrupt { after_sequence } => {
                write!(formatter, "corrupt record after {after_sequence:?}")
            }
        }
    }
}

impl std::error::Error for RecordingError {}

impl From<io::Error> for RecordingError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Reads one venue's recording file.
#[derive(Debug)]
pub struct RecordingReader {
    reader: BufReader<File>,
    last_sequence: Option<u64>,
}

impl RecordingReader {
    /// Opens a recording file and checks its format.
    ///
    /// # Errors
    ///
    /// Returns a [`RecordingError`] if the file can't be read or isn't a
    /// supported recording.
    pub fn open(path: &Path) -> Result<Self, RecordingError> {
        let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
        let mut magic = [0_u8; MAGIC.len()];
        reader
            .read_exact(&mut magic)
            .map_err(|_| RecordingError::BadHeader)?;
        if &magic != MAGIC {
            return Err(RecordingError::BadHeader);
        }
        Ok(Self {
            reader,
            last_sequence: None,
        })
    }

    /// Reads the next record, or `None` at the end of the file.
    ///
    /// A truncated final record, as left by an interrupted recorder, counts
    /// as the end of the file.
    ///
    /// # Errors
    ///
    /// Returns a [`RecordingError`] if a complete record fails its checksum.
    pub fn next_record(&mut self) -> Result<Option<Recording>, RecordingError> {
        let mut header = [0_u8; HEADER_LEN];
        if !read_full(&mut self.reader, &mut header)? {
            return Ok(None);
        }

        let corrupt = RecordingError::Corrupt {
            after_sequence: self.last_sequence,
        };
        let length = u32::from_le_bytes([header[22], header[23], header[24], header[25]]) as usize;
        if length > MAX_PAYLOAD {
            return Err(corrupt);
        }
        let mut payload = vec![0; length];
        if !read_full(&mut self.reader, &mut payload)? {
            return Ok(None);
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[..26]);
        hasher.update(&payload);
        let checksum = u32::from_le_bytes([header[26], header[27], header[28], header[29]]);
        if hasher.finalize() != checksum {
            return Err(corrupt);
        }

        let Some(kind) = RecordKind::from_code(header[16]) else {
            return Err(corrupt);
        };
        let field = |range: std::ops::Range<usize>| -> [u8; 8] {
            let mut bytes = [0; 8];
            bytes.copy_from_slice(&header[range]);
            bytes
        };
        let sequence = u64::from_le_bytes(field(8..16));
        self.last_sequence = Some(sequence);
        Ok(Some(Recording {
            received: EventTime(u64::from_le_bytes(field(0..8))),
            sequence,
            kind,
            stream: header[17],
            tag: u32::from_le_bytes([header[18], header[19], header[20], header[21]]),
            payload,
        }))
    }
}

/// Fills `buffer`, returning `false` if the file ends first.
fn read_full(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<bool> {
    match reader.read_exact(buffer) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

/// Counters shared with a recorder thread.
#[derive(Debug, Default)]
pub struct RecorderStats {
    /// Records written.
    pub written: AtomicU64,
    /// Records discarded because the writer couldn't keep up.
    pub dropped: AtomicU64,
    /// Write failures; recording stops after the first.
    pub write_errors: AtomicU64,
}

/// A feed's non-blocking connection to its recorder thread.
#[derive(Clone, Debug)]
pub struct RecorderHandle {
    sender: SyncSender<Recording>,
    stats: Arc<RecorderStats>,
}

impl RecorderHandle {
    /// Queues a record without ever blocking the feed.
    ///
    /// If the queue is full the record is dropped and counted, and the writer
    /// marks the loss in the file.
    pub fn record(&self, record: Recording) {
        match self.sender.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns the recorder's counters.
    #[must_use]
    pub fn stats(&self) -> &Arc<RecorderStats> {
        &self.stats
    }
}

/// Starts a recorder thread writing to `path`.
///
/// Dropping every [`RecorderHandle`] flushes the file and ends the thread.
///
/// # Errors
///
/// Returns an error if the file can't be created.
pub fn start_recorder(path: &Path) -> io::Result<(RecorderHandle, JoinHandle<()>)> {
    let mut writer = BufWriter::with_capacity(1 << 20, File::create(path)?);
    writer.write_all(MAGIC)?;

    let stats = Arc::new(RecorderStats::default());
    let (sender, receiver) = sync_channel(QUEUE_DEPTH);
    let thread_stats = Arc::clone(&stats);
    let thread = std::thread::Builder::new()
        .name("recorder".into())
        .spawn(move || write_loop(&receiver, writer, &thread_stats))?;
    Ok((RecorderHandle { sender, stats }, thread))
}

fn write_loop(receiver: &Receiver<Recording>, mut writer: impl Write, stats: &RecorderStats) {
    let mut reported_drops = 0;
    let mut failed = false;
    while let Ok(record) = receiver.recv() {
        if failed {
            continue;
        }
        let result = (|| {
            let drops = stats.dropped.load(Ordering::Relaxed);
            if drops != reported_drops {
                reported_drops = drops;
                write_record(
                    &mut writer,
                    &Recording {
                        kind: RecordKind::Gap,
                        payload: Vec::new(),
                        ..record.clone()
                    },
                )?;
            }
            write_record(&mut writer, &record)
        })();
        match result {
            Ok(()) => {
                stats.written.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                failed = true;
                stats.write_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(%error, "recording stopped: write failed");
            }
        }
    }
    if let Err(error) = writer.flush() {
        stats.write_errors.fetch_add(1, Ordering::Relaxed);
        tracing::error!(%error, "recording flush failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let unique = format!("ringbook-{}-{name}.{EXTENSION}", std::process::id());
        std::env::temp_dir().join(unique)
    }

    fn record(sequence: u64, kind: RecordKind, payload: &[u8]) -> Recording {
        Recording {
            received: EventTime(sequence * 10),
            sequence,
            kind,
            stream: 1,
            tag: 7,
            payload: payload.to_vec(),
        }
    }

    fn write_file(path: &Path, records: &[Recording]) {
        let (handle, thread) = start_recorder(path).unwrap();
        for record in records {
            handle.record(record.clone());
        }
        drop(handle);
        thread.join().unwrap();
    }

    fn read_all(path: &Path) -> Vec<Recording> {
        let mut reader = RecordingReader::open(path).unwrap();
        let mut records = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            records.push(record);
        }
        records
    }

    #[test]
    fn records_round_trip_exactly() {
        let path = temp_path("round-trip");
        let records = [
            record(1, RecordKind::Metadata, b"{}"),
            record(2, RecordKind::Frame, b"hello"),
            record(3, RecordKind::Conn, &[2]),
            record(4, RecordKind::Rest, b""),
        ];
        write_file(&path, &records);
        assert_eq!(read_all(&path), records);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_truncated_final_record_ends_the_file() {
        let path = temp_path("truncated");
        write_file(
            &path,
            &[
                record(1, RecordKind::Frame, b"first"),
                record(2, RecordKind::Frame, b"second"),
            ],
        );
        let length = std::fs::metadata(&path).unwrap().len();
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(length - 3)
            .unwrap();
        assert_eq!(read_all(&path).len(), 1);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_flipped_byte_is_detected() {
        let path = temp_path("corrupt");
        write_file(&path, &[record(1, RecordKind::Frame, b"payload")]);
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&path, bytes).unwrap();

        let mut reader = RecordingReader::open(&path).unwrap();
        assert!(matches!(
            reader.next_record(),
            Err(RecordingError::Corrupt { .. })
        ));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_files_in_another_format() {
        let path = temp_path("bad-header");
        std::fs::write(&path, b"something else entirely").unwrap();
        assert!(matches!(
            RecordingReader::open(&path),
            Err(RecordingError::BadHeader)
        ));
        std::fs::remove_file(path).unwrap();
    }
}
