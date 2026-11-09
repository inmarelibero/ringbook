//! Hosts the paper exchange core behind the binary order protocol.
//!
//! The venue keeps a bounded log of reports so a client can resume after a
//! disconnect, and cancels a session's open orders when its connection drops.
//! It runs on wall-clock time, so it isn't deterministic.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ringbook_clock::Clock;
use ringbook_core::{Event, EventTime, ExecReport, InstrumentId, OrderCommand, VenueId};
use ringbook_gateway::{FRAME_LEN, Frame, FrameReader, decode, encode};
use ringbook_paper_exchange::{PaperExchange, QueueModel};
use ringbook_ring::Consumer;

const HEARTBEAT_PERIOD: Duration = Duration::from_millis(500);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);

/// How a mock venue is set up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MockVenueConfig {
    /// The session identifier; a restarted venue must use a new one.
    pub session: u64,
    /// How many reports to keep for retransmission.
    pub retained_reports: usize,
    /// The number of venues the exchange knows.
    pub venues: u8,
    /// The number of instruments the exchange knows.
    pub instruments: u16,
    /// Injected fault: drop the connection after this many reports.
    pub drop_connection_after: Option<u64>,
}

struct Connection {
    stream: TcpStream,
    reader: FrameReader,
    outgoing: Vec<u8>,
    written: usize,
    last_received: Instant,
    last_sent: Instant,
    introduced: bool,
}

/// The mock venue.
pub struct MockVenue<Q> {
    exchange: PaperExchange<Q>,
    config: MockVenueConfig,
    inputs: Vec<Consumer<Event>>,
    clock: Clock,
    listener: TcpListener,
    retained: VecDeque<ExecReport>,
    clock_offset: Option<u64>,
    reports_sent: u64,
    fault_fired: bool,
}

/// What a finished venue did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MockVenueSummary {
    /// Reports produced.
    pub reports: u64,
}

impl<Q: QueueModel> MockVenue<Q> {
    /// Creates a venue listening on `listener`.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener can't be made non-blocking.
    pub fn new(
        exchange: PaperExchange<Q>,
        config: MockVenueConfig,
        inputs: Vec<Consumer<Event>>,
        clock: Clock,
        listener: TcpListener,
    ) -> io::Result<Self> {
        listener.set_nonblocking(true)?;
        Ok(Self {
            exchange,
            retained: VecDeque::with_capacity(config.retained_reports),
            config,
            inputs,
            clock,
            listener,
            clock_offset: None,
            reports_sent: 0,
            fault_fired: false,
        })
    }

    fn now(&self) -> EventTime {
        let wall = self.clock.event_time().0;
        EventTime(wall - self.clock_offset.unwrap_or(0))
    }

    fn drain_market(&mut self) {
        for ring in 0..self.inputs.len() {
            while let Some(event) = self.inputs[ring].pop() {
                if self.clock_offset.is_none() {
                    // A replay's events carry old timestamps, so run on a
                    // clock aligned with the first event.
                    let wall = self.clock.event_time().0;
                    self.clock_offset = Some(wall.saturating_sub(event.event_time.0));
                }
                self.exchange.on_market(&event);
            }
        }
    }

    fn collect_reports(&mut self, connection: Option<&mut Connection>) {
        let now = self.now();
        self.exchange.advance(now);
        let mut produced = std::mem::take(&mut self.retained);
        let before = produced.len();
        let capacity = self.config.retained_reports;
        self.exchange.poll(now, |report| {
            if produced.len() == capacity {
                produced.pop_front();
            }
            produced.push_back(report);
        });
        let new_reports = produced.len() - before.min(produced.len());
        self.retained = produced;

        if let Some(connection) = connection
            && connection.introduced
        {
            let start = self.retained.len() - new_reports.min(self.retained.len());
            for report in self.retained.iter().skip(start) {
                queue_frame(connection, &Frame::Report(*report));
                self.reports_sent += 1;
            }
        }
    }

    fn on_frame(&mut self, frame: Frame, connection: &mut Connection) {
        match frame {
            Frame::Hello {
                session,
                last_sequence,
            } => self.on_hello(session, last_sequence, connection),
            Frame::Command(command) => self.on_command(command),
            Frame::QueryState => self.on_query(connection),
            _ => {}
        }
    }

    fn on_hello(&mut self, session: u64, last_sequence: u64, connection: &mut Connection) {
        if session == 0 {
            // A client with no session is a new run, so it starts flat.
            self.config.session += 1;
            self.exchange.reset_session();
            self.retained.clear();
        }
        queue_frame(
            connection,
            &Frame::HelloAck {
                session: self.config.session,
            },
        );
        connection.introduced = true;
        if session != 0 && session != self.config.session {
            return;
        }

        let oldest = self.retained.front().map(|report| report.sequence);
        let newest = self.retained.back().map(|report| report.sequence);
        let missing = newest.is_some_and(|newest| newest > last_sequence)
            && oldest.is_some_and(|oldest| oldest > last_sequence + 1);
        if missing {
            queue_frame(connection, &Frame::ResumeFailed);
            return;
        }
        for report in self
            .retained
            .iter()
            .filter(|report| report.sequence > last_sequence)
        {
            queue_frame(connection, &Frame::Report(*report));
        }
    }

    fn on_command(&mut self, command: OrderCommand) {
        let now = self.now();
        self.exchange.submit(command, now);
    }

    fn on_query(&mut self, connection: &mut Connection) {
        self.exchange.open_orders(|open| {
            queue_frame(
                connection,
                &Frame::OpenOrder {
                    id: open.order.id,
                    remaining: open.remaining,
                },
            );
        });
        for venue in 0..self.config.venues {
            for instrument in 0..self.config.instruments {
                let venue = VenueId(venue);
                let instrument = InstrumentId(instrument);
                queue_frame(
                    connection,
                    &Frame::Position {
                        venue,
                        instrument,
                        qty: self.exchange.position(venue, instrument),
                    },
                );
            }
        }
        queue_frame(connection, &Frame::StateEnd);
    }

    fn pump(&mut self, connection: &mut Connection) -> io::Result<()> {
        loop {
            match connection.stream.read(connection.reader.unfilled()) {
                Ok(0) => return Err(ErrorKind::ConnectionReset.into()),
                Ok(count) => {
                    connection.reader.advance(count);
                    connection.last_received = Instant::now();
                    if let Some(bytes) = connection.reader.take() {
                        let frame =
                            decode(bytes).ok_or_else(|| io::Error::from(ErrorKind::InvalidData))?;
                        self.on_frame(frame, connection);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }

        let now = Instant::now();
        if now.duration_since(connection.last_sent) >= HEARTBEAT_PERIOD {
            queue_frame(connection, &Frame::Heartbeat);
            connection.last_sent = now;
        }
        if now.duration_since(connection.last_received) >= HEARTBEAT_TIMEOUT {
            return Err(ErrorKind::TimedOut.into());
        }
        flush(connection)
    }

    /// Runs until `stop` is set.
    pub fn run(&mut self, stop: &Arc<AtomicBool>) -> MockVenueSummary {
        let mut connection: Option<Connection> = None;
        while !stop.load(Ordering::Acquire) {
            self.drain_market();

            if connection.is_none() {
                match self.listener.accept() {
                    Ok((stream, _)) => match prepare(stream) {
                        Ok(accepted) => connection = Some(accepted),
                        Err(error) => eprintln!("mock venue: accept failed: {error}"),
                    },
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                    Err(error) => eprintln!("mock venue: accept failed: {error}"),
                }
            }

            self.collect_reports(connection.as_mut());
            if let Some(mut active) = connection.take() {
                let fault = self
                    .config
                    .drop_connection_after
                    .is_some_and(|limit| !self.fault_fired && self.reports_sent >= limit);
                if fault {
                    self.fault_fired = true;
                }
                match if fault {
                    Err(ErrorKind::ConnectionAborted.into())
                } else {
                    self.pump(&mut active)
                } {
                    Ok(()) => connection = Some(active),
                    Err(error) => {
                        eprintln!("mock venue: connection lost: {error}; cancelling open orders");
                        let now = self.now();
                        self.exchange.cancel_all(now);
                        self.collect_reports(None);
                    }
                }
            }
            std::hint::spin_loop();
        }
        MockVenueSummary {
            reports: self.retained.back().map_or(0, |report| report.sequence),
        }
    }

    /// Returns the exchange.
    #[must_use]
    pub const fn exchange(&self) -> &PaperExchange<Q> {
        &self.exchange
    }
}

fn prepare(stream: TcpStream) -> io::Result<Connection> {
    stream.set_nonblocking(true)?;
    stream.set_nodelay(true)?;
    let now = Instant::now();
    Ok(Connection {
        stream,
        reader: FrameReader::default(),
        outgoing: Vec::with_capacity(1 << 16),
        written: 0,
        last_received: now,
        last_sent: now,
        introduced: false,
    })
}

fn queue_frame(connection: &mut Connection, frame: &Frame) {
    let mut bytes = [0; FRAME_LEN];
    encode(frame, &mut bytes);
    connection.outgoing.extend_from_slice(&bytes);
}

fn flush(connection: &mut Connection) -> io::Result<()> {
    while connection.written < connection.outgoing.len() {
        match connection
            .stream
            .write(&connection.outgoing[connection.written..])
        {
            Ok(0) => return Err(ErrorKind::WriteZero.into()),
            Ok(count) => {
                connection.written += count;
                connection.last_sent = Instant::now();
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(()),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    connection.outgoing.clear();
    connection.written = 0;
    Ok(())
}
