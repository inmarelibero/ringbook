//! The execution path behind the order manager.

use ringbook_core::{
    ClientOrderId, Event, EventTime, ExecReport, InstrumentId, MeasureStamp, OrderCommand, Qty,
    VenueId,
};
use ringbook_paper_exchange::{PaperExchange, QueueModel};

/// What a venue reports while the engine reconciles with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconcileEvent {
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

/// Carries orders to a venue and brings its reports back.
pub trait VenueLink: Send {
    /// Shows a market event to an in-process venue.
    fn on_market(&mut self, event: &Event);

    /// Sends a command, returning `false` if it can't be delivered right now.
    fn send(&mut self, command: OrderCommand, now: EventTime, stamp: MeasureStamp) -> bool;

    /// Delivers every report due by `now`.
    fn poll(&mut self, now: EventTime, deliver: impl FnMut(ExecReport));

    /// Returns whether the venue runs independently of market data batches.
    ///
    /// An in-process venue returns `false`, which keeps replay deterministic.
    fn runs_independently(&self) -> bool {
        true
    }

    /// Returns whether the path to the venue is up.
    fn is_up(&self) -> bool {
        true
    }

    /// Returns whether the venue has lost its session state.
    fn state_lost(&self) -> bool {
        false
    }

    /// Returns the next reconciliation event, if one has arrived.
    fn next_reconcile(&mut self) -> Option<ReconcileEvent> {
        None
    }
}

/// Runs the paper exchange directly on the hot thread, in event time.
pub struct InProcess<Q> {
    exchange: PaperExchange<Q>,
}

impl<Q> InProcess<Q> {
    /// Wraps a paper exchange.
    #[must_use]
    pub const fn new(exchange: PaperExchange<Q>) -> Self {
        Self { exchange }
    }

    /// Returns the wrapped exchange.
    #[must_use]
    pub const fn exchange(&self) -> &PaperExchange<Q> {
        &self.exchange
    }
}

impl<Q: QueueModel> VenueLink for InProcess<Q> {
    #[inline]
    fn runs_independently(&self) -> bool {
        false
    }

    #[inline]
    fn on_market(&mut self, event: &Event) {
        self.exchange.on_market(event);
    }

    #[inline]
    fn send(&mut self, command: OrderCommand, now: EventTime, _stamp: MeasureStamp) -> bool {
        self.exchange.submit(command, now);
        true
    }

    #[inline]
    fn poll(&mut self, now: EventTime, deliver: impl FnMut(ExecReport)) {
        self.exchange.advance(now);
        self.exchange.poll(now, deliver);
    }
}
