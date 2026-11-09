//! The append-only journal.
//!
//! The hot thread pushes fixed-size [`JournalRecord`]s onto a ring, and a
//! journal thread appends them to a binary file. Each record carries a length
//! and a checksum. Measurement stamps are excluded, so two replays of the same
//! recording produce byte-identical journals.

#![forbid(unsafe_code)]

mod codec;

use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ringbook_core::{ControlCommand, EventTime, ExecReport, HaltReason, NewOrder, OrderCommand};
use ringbook_ring::Consumer;

const MAGIC: &[u8; 8] = b"RBJRN\x002\n";
const FRAME_HEADER: usize = 2 + 4;
/// Longer than any encoded record, so a larger length field must be damage.
const MAX_RECORD: usize = 256;
const FSYNC_PERIOD: Duration = Duration::from_secs(1);
const IDLE_SLEEP: Duration = Duration::from_micros(200);
const WRITE_BATCH: usize = 4096;

/// Why pre-trade risk refused an order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RiskRejection {
    /// The order would breach the position limit.
    Position,
    /// The order would breach the notional limit.
    Notional,
    /// The order rate limit is exhausted.
    OrderRate,
    /// The order exceeds the maximum order size.
    OrderSize,
    /// The price is too far from the mid.
    PriceCollar,
    /// The order would trade against our own resting order.
    SelfTrade,
    /// The order table is full.
    OrderTableFull,
    /// The outbound order ring is full.
    OrderPathFull,
}

/// What a journal record describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalBody {
    /// A command the engine sent.
    Sent(OrderCommand),
    /// An order risk refused.
    RiskRejected {
        /// The refused order.
        order: NewOrder,
        /// Why it was refused.
        reason: RiskRejection,
    },
    /// A venue report the engine applied.
    Report(ExecReport),
    /// An operator command the engine applied.
    Control(ControlCommand),
    /// The engine entered a global halt.
    Halted(HaltReason),
    /// The venue session the run is attached to.
    Session(u64),
}

/// One journal entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalRecord {
    /// The market data batch this record belongs to.
    pub batch: u64,
    /// The engine's event time.
    pub time: EventTime,
    /// What happened.
    pub body: JournalBody,
}

/// What starts every journal file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalHeader {
    /// The full config text the run used.
    pub config: String,
    /// The binary's version.
    pub binary_version: String,
    /// The run epoch.
    pub epoch: u16,
    /// Identifies the run in storage, and stays the same across recoveries.
    pub run_id: u64,
}

impl JournalHeader {
    /// Returns the checksum identifying the run's config.
    #[must_use]
    pub fn config_hash(&self) -> u32 {
        crc32fast::hash(self.config.as_bytes())
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.epoch.to_le_bytes());
        bytes.extend_from_slice(&self.config_hash().to_le_bytes());
        bytes.extend_from_slice(&self.run_id.to_le_bytes());
        for text in [&self.config, &self.binary_version] {
            bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
            bytes.extend_from_slice(text.as_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let epoch = u16::from_le_bytes(bytes.get(0..2)?.try_into().ok()?);
        let hash = u32::from_le_bytes(bytes.get(2..6)?.try_into().ok()?);
        let run_id = u64::from_le_bytes(bytes.get(6..14)?.try_into().ok()?);
        let mut offset = 14;
        let mut text = || -> Option<String> {
            let length =
                u32::from_le_bytes(bytes.get(offset..offset + 4)?.try_into().ok()?) as usize;
            offset += 4;
            let value = String::from_utf8(bytes.get(offset..offset + length)?.to_vec()).ok()?;
            offset += length;
            Some(value)
        };
        let header = Self {
            config: text()?,
            binary_version: text()?,
            epoch,
            run_id,
        };
        (header.config_hash() == hash).then_some(header)
    }
}

/// Why a journal could not be read.
#[derive(Debug)]
pub enum JournalError {
    /// The file could not be read.
    Io(io::Error),
    /// The file isn't a journal, or its header is damaged.
    BadHeader,
    /// A record before the end of the file is damaged.
    Corrupt {
        /// The byte offset of the damaged record.
        offset: usize,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "journal I/O error: {error}"),
            Self::BadHeader => formatter.write_str("not a valid journal file"),
            Self::Corrupt { offset } => write!(formatter, "corrupt journal record at {offset}"),
        }
    }
}

impl std::error::Error for JournalError {}

impl From<io::Error> for JournalError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// A journal read back from disk.
#[derive(Debug, PartialEq, Eq)]
pub struct JournalContents {
    /// The run's header.
    pub header: JournalHeader,
    /// Every usable record.
    pub records: Vec<JournalRecord>,
    /// Whether the file ended with an incomplete or damaged record.
    pub truncated_tail: bool,
}

/// Reads a journal.
///
/// An incomplete or damaged record at the end marks the end of the usable
/// journal, because a crash can leave one there.
///
/// # Errors
///
/// Returns a [`JournalError`] if the header is invalid or a record is damaged
/// anywhere but the end.
pub fn read_journal(path: &Path) -> Result<JournalContents, JournalError> {
    decode_journal(&std::fs::read(path)?)
}

fn decode_journal(bytes: &[u8]) -> Result<JournalContents, JournalError> {
    if bytes.get(..MAGIC.len()) != Some(MAGIC.as_slice()) {
        return Err(JournalError::BadHeader);
    }
    let mut offset = MAGIC.len();
    let read_u32 = |at: usize| -> Option<u32> {
        Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
    };
    let header_length = read_u32(offset).ok_or(JournalError::BadHeader)? as usize;
    let header_checksum = read_u32(offset + 4).ok_or(JournalError::BadHeader)?;
    offset += 8;
    let header_bytes = bytes
        .get(offset..offset + header_length)
        .ok_or(JournalError::BadHeader)?;
    if crc32fast::hash(header_bytes) != header_checksum {
        return Err(JournalError::BadHeader);
    }
    let header = JournalHeader::decode(header_bytes).ok_or(JournalError::BadHeader)?;
    offset += header_length;

    let mut records = Vec::new();
    let mut truncated_tail = false;
    while offset < bytes.len() {
        let Some(frame) = bytes.get(offset..offset + FRAME_HEADER) else {
            truncated_tail = true;
            break;
        };
        let length = usize::from(u16::from_le_bytes([frame[0], frame[1]]));
        let checksum = u32::from_le_bytes([frame[2], frame[3], frame[4], frame[5]]);
        if length > MAX_RECORD {
            // Damage this far from the end can't be a torn final write.
            if bytes.len() - offset > FRAME_HEADER + MAX_RECORD {
                return Err(JournalError::Corrupt { offset });
            }
            truncated_tail = true;
            break;
        }
        let body_start = offset + FRAME_HEADER;
        let Some(body) = bytes.get(body_start..body_start + length) else {
            truncated_tail = true;
            break;
        };
        let record = (crc32fast::hash(body) == checksum)
            .then(|| codec::decode(body))
            .flatten();
        let Some(record) = record else {
            if body_start + length == bytes.len() {
                truncated_tail = true;
                break;
            }
            return Err(JournalError::Corrupt { offset });
        };
        records.push(record);
        offset = body_start + length;
    }
    Ok(JournalContents {
        header,
        records,
        truncated_tail,
    })
}

fn encode_header(header: &JournalHeader) -> Vec<u8> {
    let body = header.encode();
    let mut bytes = Vec::with_capacity(MAGIC.len() + 8 + body.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    bytes.extend_from_slice(&body);
    bytes
}

/// Runs the journal thread.
pub struct JournalWriter {
    thread: JoinHandle<io::Result<u64>>,
}

impl JournalWriter {
    /// Creates a journal and starts appending records from `records`.
    ///
    /// The thread ends after `stop` is set and the ring is empty. If a write
    /// fails, it sets `failed`, which the engine treats as a global halt.
    ///
    /// # Errors
    ///
    /// Returns an error if the file can't be created.
    pub fn start(
        path: &Path,
        header: &JournalHeader,
        records: Consumer<JournalRecord>,
        stop: Arc<AtomicBool>,
        failed: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let mut file = File::create(path)?;
        file.write_all(&encode_header(header))?;
        file.sync_data()?;
        Self::spawn(file, records, stop, failed)
    }

    /// Continues appending to an existing journal after recovery.
    ///
    /// # Errors
    ///
    /// Returns an error if the file can't be opened.
    pub fn resume(
        path: &Path,
        usable_length: u64,
        records: Consumer<JournalRecord>,
        stop: Arc<AtomicBool>,
        failed: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let file = File::options().append(true).open(path)?;
        file.set_len(usable_length)?;
        Self::spawn(file, records, stop, failed)
    }

    fn spawn(
        file: File,
        records: Consumer<JournalRecord>,
        stop: Arc<AtomicBool>,
        failed: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let thread = std::thread::Builder::new()
            .name("journal".into())
            .spawn(move || {
                let result = write_loop(file, records, &stop);
                if result.is_err() {
                    failed.store(true, Ordering::Release);
                }
                result
            })?;
        Ok(Self { thread })
    }

    /// Waits for the thread and returns how many records it wrote.
    ///
    /// # Errors
    ///
    /// Returns the write error that stopped the thread.
    ///
    /// # Panics
    ///
    /// Panics if the journal thread panicked.
    pub fn finish(self) -> io::Result<u64> {
        self.thread.join().expect("the journal thread panicked")
    }
}

/// Returns the length of a journal's usable contents.
///
/// # Errors
///
/// Returns a [`JournalError`] under the conditions of [`read_journal`].
pub fn usable_length(path: &Path) -> Result<u64, JournalError> {
    let bytes = std::fs::read(path)?;
    let contents = decode_journal(&bytes)?;
    let mut length = encode_header(&contents.header).len() as u64;
    for record in &contents.records {
        length += (FRAME_HEADER + codec::encode(record).len()) as u64;
    }
    Ok(length)
}

fn write_loop(
    mut file: File,
    mut records: Consumer<JournalRecord>,
    stop: &AtomicBool,
) -> io::Result<u64> {
    let mut buffer = Vec::with_capacity(WRITE_BATCH * 160);
    let mut written = 0;
    let mut last_sync = Instant::now();
    let mut dirty = false;

    loop {
        // Read the flag first, so an empty drain after it means truly done.
        let stopping = stop.load(Ordering::Acquire);
        buffer.clear();
        let mut drained = 0;
        while drained < WRITE_BATCH {
            let Some(record) = records.pop() else {
                break;
            };
            let body = codec::encode(&record);
            buffer.extend_from_slice(&(body.len() as u16).to_le_bytes());
            buffer.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
            buffer.extend_from_slice(&body);
            drained += 1;
        }

        if drained > 0 {
            file.write_all(&buffer)?;
            written += drained as u64;
            dirty = true;
        }
        if dirty && last_sync.elapsed() >= FSYNC_PERIOD {
            file.sync_data()?;
            last_sync = Instant::now();
            dirty = false;
        }
        if drained == 0 {
            if stopping {
                file.sync_data()?;
                return Ok(written);
            }
            std::thread::sleep(IDLE_SLEEP);
        }
    }
}

#[cfg(test)]
mod tests;
