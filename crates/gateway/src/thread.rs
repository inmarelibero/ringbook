//! The pinned, busy-polling thread that owns the socket to the venue.
//!
//! The hot thread only touches rings; every socket call happens here.

use std::io::{self, ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ringbook_clock::Clock;
use ringbook_core::{ClientOrderId, Event, EventTime, ExecReport, MeasureStamp, OrderCommand, Qty};
use ringbook_core::{InstrumentId, VenueId};
use ringbook_engine::{TelemetryRecord, VenueLink};
use ringbook_ring::{Consumer, Producer, channel};

use crate::protocol::{FRAME_LEN, Frame, FrameReader, decode, encode};

const COMMAND_RING: usize = 1 << 12;
const INBOUND_RING: usize = 1 << 14;
const HEARTBEAT_PERIOD: Duration = Duration::from_millis(500);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
const RECONNECT_DELAY: Duration = Duration::from_millis(250);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// What a reconciliation delivers to the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconcileItem {
    /// The session is open; retransmitted reports follow.
    Session(u64),
    /// An order open at the venue.
    OpenOrder {
        /// The order's ID.
        id: ClientOrderId,
        /// The unfilled quantity.
        remaining: Qty,
    },
    /// A net position at the venue.
    Position {
        /// The venue.
        venue: VenueId,
        /// The instrument.
        instrument: InstrumentId,
        /// The net quantity.
        qty: i64,
    },
    /// The venue's state has been fully delivered.
    End,
    /// The venue no longer retains reports the engine needs.
    ResumeFailed,
}

#[derive(Clone, Copy, Debug)]
enum Inbound {
    Report(ExecReport),
    Reconcile(ReconcileItem),
}

/// The engine's side of the gateway: rings and flags only.
pub struct GatewayLink {
    commands: Producer<(OrderCommand, MeasureStamp)>,
    inbound: Consumer<Inbound>,
    up: Arc<AtomicBool>,
    state_lost: Arc<AtomicBool>,
    pending_item: Option<ReconcileItem>,
}

impl VenueLink for GatewayLink {
    #[inline]
    fn on_market(&mut self, _event: &Event) {}

    #[inline]
    fn send(&mut self, command: OrderCommand, _now: EventTime, stamp: MeasureStamp) -> bool {
        self.up.load(Ordering::Acquire) && self.commands.push((command, stamp)).is_ok()
    }

    #[inline]
    fn poll(&mut self, _now: EventTime, mut deliver: impl FnMut(ExecReport)) {
        // Reconciliation items stop the drain, so they stay in order with
        // the reports around them.
        while self.pending_item.is_none() {
            match self.inbound.pop() {
                Some(Inbound::Report(report)) => deliver(report),
                Some(Inbound::Reconcile(item)) => self.pending_item = Some(item),
                None => break,
            }
        }
    }

    #[inline]
    fn is_up(&self) -> bool {
        self.up.load(Ordering::Acquire)
    }

    #[inline]
    fn state_lost(&self) -> bool {
        self.state_lost.load(Ordering::Acquire)
    }

    #[inline]
    fn next_reconcile(&mut self) -> Option<ringbook_engine::link::ReconcileEvent> {
        use ringbook_engine::link::ReconcileEvent;
        self.pending_item.take().map(|item| match item {
            ReconcileItem::Session(session) => ReconcileEvent::Session(session),
            ReconcileItem::OpenOrder { id, remaining } => {
                ReconcileEvent::OpenOrder { id, remaining }
            }
            ReconcileItem::Position {
                venue,
                instrument,
                qty,
            } => ReconcileEvent::Position {
                venue,
                instrument,
                qty,
            },
            ReconcileItem::End => ReconcileEvent::End,
            ReconcileItem::ResumeFailed => ReconcileEvent::ResumeFailed,
        })
    }
}

/// How the gateway connects.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// The mock venue's host and port, resolved on every connection attempt.
    pub address: String,
    /// The session to resume, or zero for a new run.
    pub session: u64,
    /// The last report already applied.
    pub last_sequence: u64,
    /// The core to pin the thread to.
    pub core: Option<usize>,
}

/// Controls a running gateway thread.
pub struct GatewayHandle {
    thread: JoinHandle<()>,
    stop: Arc<AtomicBool>,
}

impl GatewayHandle {
    /// Stops the thread and waits for it.
    ///
    /// # Panics
    ///
    /// Panics if the gateway thread panicked.
    pub fn stop(self) {
        self.stop.store(true, Ordering::Release);
        self.thread.join().expect("the gateway thread panicked");
    }
}

/// Starts the gateway thread and returns the engine's end of it.
///
/// # Errors
///
/// Returns an error if the thread can't be started.
///
/// # Panics
///
/// Panics only if the constant ring capacities are invalid.
pub fn start_gateway(
    config: GatewayConfig,
    clock: Clock,
    telemetry: Producer<TelemetryRecord>,
    pin: impl FnOnce(Option<usize>) + Send + 'static,
) -> io::Result<(GatewayLink, GatewayHandle)> {
    let (commands_tx, commands_rx) = channel(COMMAND_RING).expect("constant capacity is valid");
    let (inbound_tx, inbound_rx) = channel(INBOUND_RING).expect("constant capacity is valid");
    let up = Arc::new(AtomicBool::new(false));
    let state_lost = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let mut worker = Worker {
        address: config.address,
        session: config.session,
        last_sequence: config.last_sequence,
        clock,
        telemetry,
        commands: commands_rx,
        inbound: inbound_tx,
        up: Arc::clone(&up),
        state_lost: Arc::clone(&state_lost),
        stop: Arc::clone(&stop),
    };
    let core = config.core;
    let thread = std::thread::Builder::new()
        .name("gateway".into())
        .spawn(move || {
            pin(core);
            worker.run();
        })?;

    Ok((
        GatewayLink {
            commands: commands_tx,
            inbound: inbound_rx,
            up,
            state_lost,
            pending_item: None,
        },
        GatewayHandle { thread, stop },
    ))
}

struct Worker {
    address: String,
    session: u64,
    last_sequence: u64,
    clock: Clock,
    telemetry: Producer<TelemetryRecord>,
    commands: Consumer<(OrderCommand, MeasureStamp)>,
    inbound: Producer<Inbound>,
    up: Arc<AtomicBool>,
    state_lost: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

fn write_frame(stream: &mut TcpStream, frame: &Frame) -> io::Result<()> {
    let mut bytes = [0; FRAME_LEN];
    encode(frame, &mut bytes);
    write_all_spinning(stream, &bytes)
}

fn write_all_spinning(stream: &mut TcpStream, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        match stream.write(bytes) {
            Ok(0) => return Err(ErrorKind::WriteZero.into()),
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == ErrorKind::WouldBlock => std::hint::spin_loop(),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

impl Worker {
    fn run(&mut self) {
        while !self.stop.load(Ordering::Acquire) {
            match self.connect() {
                Ok(stream) => {
                    if let Err(error) = self.serve(stream) {
                        eprintln!("gateway: connection ended: {error}");
                    }
                    self.up.store(false, Ordering::Release);
                }
                Err(error) => eprintln!("gateway: connect failed: {error}"),
            }
            if self.state_lost.load(Ordering::Acquire) {
                return;
            }
            // Commands queued for a dead connection never reached the venue.
            while self.commands.pop().is_some() {}
            std::thread::sleep(RECONNECT_DELAY);
        }
    }

    fn connect(&self) -> io::Result<TcpStream> {
        let address = self
            .address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::from(ErrorKind::AddrNotAvailable))?;
        TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)
    }

    fn serve(&mut self, mut stream: TcpStream) -> io::Result<()> {
        stream.set_nodelay(true)?;
        write_frame(
            &mut stream,
            &Frame::Hello {
                session: self.session,
                last_sequence: self.last_sequence,
            },
        )?;
        stream.set_nonblocking(true)?;

        let mut reader = FrameReader::default();
        let mut last_received = Instant::now();
        let mut last_sent = Instant::now();
        let mut held: Option<Inbound> = None;

        while !self.stop.load(Ordering::Acquire) {
            if let Some(item) = held.take()
                && let Err(returned) = self.inbound.push(item)
            {
                // The ring is full: leave the socket unread so TCP pushes back.
                held = Some(returned);
            }

            if held.is_none() {
                match stream.read(reader.unfilled()) {
                    Ok(0) => return Err(ErrorKind::ConnectionReset.into()),
                    Ok(count) => {
                        reader.advance(count);
                        last_received = Instant::now();
                        if let Some(bytes) = reader.take() {
                            let frame = decode(bytes)
                                .ok_or_else(|| io::Error::from(ErrorKind::InvalidData))?;
                            held = self.on_frame(frame, &mut stream)?;
                        }
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            ErrorKind::WouldBlock | ErrorKind::Interrupted
                        ) => {}
                    Err(error) => return Err(error),
                }
            }

            while let Some((command, stamp)) = self.commands.pop() {
                write_frame(&mut stream, &Frame::Command(command))?;
                let elapsed = self.clock.elapsed_ns(stamp, self.clock.measure());
                let _ = self.telemetry.push(TelemetryRecord::TickToWire(elapsed));
                last_sent = Instant::now();
            }

            let now = Instant::now();
            if now.duration_since(last_sent) >= HEARTBEAT_PERIOD {
                write_frame(&mut stream, &Frame::Heartbeat)?;
                last_sent = now;
            }
            if now.duration_since(last_received) >= HEARTBEAT_TIMEOUT {
                return Err(ErrorKind::TimedOut.into());
            }
            std::hint::spin_loop();
        }
        Ok(())
    }

    fn on_frame(&mut self, frame: Frame, stream: &mut TcpStream) -> io::Result<Option<Inbound>> {
        Ok(match frame {
            Frame::HelloAck { session } => {
                if self.session != 0 && self.session != session {
                    self.state_lost.store(true, Ordering::Release);
                    return Err(ErrorKind::ConnectionAborted.into());
                }
                self.session = session;
                // The venue retransmits first, so the state query follows it.
                write_frame(stream, &Frame::QueryState)?;
                Some(Inbound::Reconcile(ReconcileItem::Session(session)))
            }
            Frame::ResumeFailed => Some(Inbound::Reconcile(ReconcileItem::ResumeFailed)),
            Frame::Report(report) => {
                self.last_sequence = self.last_sequence.max(report.sequence);
                Some(Inbound::Report(report))
            }
            Frame::OpenOrder { id, remaining } => {
                Some(Inbound::Reconcile(ReconcileItem::OpenOrder {
                    id,
                    remaining,
                }))
            }
            Frame::Position {
                venue,
                instrument,
                qty,
            } => Some(Inbound::Reconcile(ReconcileItem::Position {
                venue,
                instrument,
                qty,
            })),
            Frame::StateEnd => {
                self.up.store(true, Ordering::Release);
                Some(Inbound::Reconcile(ReconcileItem::End))
            }
            Frame::Heartbeat | Frame::Hello { .. } | Frame::Command(_) | Frame::QueryState => None,
        })
    }
}
