//! Fixed-size order commands and execution reports.

use crate::decimal::{Price, Qty};
use crate::event::{EventTime, InstrumentId, Side, VenueId};

/// A client order ID: a 16-bit run epoch above a 48-bit sequence.
///
/// The epoch increases on every restart, so an ID is never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientOrderId(pub u64);

impl ClientOrderId {
    const SEQUENCE_BITS: u32 = 48;
    const SEQUENCE_MASK: u64 = (1 << Self::SEQUENCE_BITS) - 1;

    /// Combines a run epoch and a per-run sequence.
    #[must_use]
    pub const fn new(epoch: u16, sequence: u64) -> Self {
        Self(((epoch as u64) << Self::SEQUENCE_BITS) | (sequence & Self::SEQUENCE_MASK))
    }

    /// Returns the run epoch.
    #[must_use]
    pub const fn epoch(self) -> u16 {
        (self.0 >> Self::SEQUENCE_BITS) as u16
    }

    /// Returns the per-run sequence.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.0 & Self::SEQUENCE_MASK
    }
}

/// How long an order may stay on the book.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    /// Rests on the book, and is rejected if it would cross.
    PostOnly,
    /// Takes available liquidity, and any remainder is cancelled.
    ImmediateOrCancel,
}

/// A request for a new order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewOrder {
    /// The order's ID.
    pub id: ClientOrderId,
    /// The target venue.
    pub venue: VenueId,
    /// The instrument.
    pub instrument: InstrumentId,
    /// Buy or sell.
    pub side: Side,
    /// The limit price.
    pub price: Price,
    /// The order quantity.
    pub qty: Qty,
    /// How long the order may stay on the book.
    pub time_in_force: TimeInForce,
}

/// A request to cancel an order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CancelOrder {
    /// The order to cancel.
    pub id: ClientOrderId,
    /// The venue holding the order.
    pub venue: VenueId,
    /// The order's instrument.
    pub instrument: InstrumentId,
}

/// A command sent to a venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderCommand {
    /// Place an order.
    New(NewOrder),
    /// Cancel an order.
    Cancel(CancelOrder),
}

/// Why a venue refused a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RejectReason {
    /// A post-only order would have crossed the book.
    PostOnlyCross,
    /// The venue's order capacity is exhausted.
    VenueFull,
    /// The order isn't open at the venue.
    UnknownOrder,
    /// The venue has no book for the instrument yet.
    NoMarket,
}

/// Whether a fill added or removed liquidity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Liquidity {
    /// The order was resting on the book.
    Maker,
    /// The order crossed the book.
    Taker,
}

/// What an [`ExecReport`] reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecKind {
    /// The order is live at the venue.
    Ack,
    /// A new order or cancel was refused.
    Reject(RejectReason),
    /// All or part of the order executed.
    Fill {
        /// The execution price.
        price: Price,
        /// The executed quantity.
        qty: Qty,
        /// The fee, in units of price times quantity; negative for a rebate.
        fee: i64,
        /// Maker or taker.
        liquidity: Liquidity,
    },
    /// The order was cancelled.
    CancelAck,
    /// The unfilled remainder of an immediate order was cancelled.
    Expired,
}

/// A venue's report about an order, echoing the order's full details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecReport {
    /// The session sequence number, starting at one.
    pub sequence: u64,
    /// When the report becomes visible to the engine.
    pub time: EventTime,
    /// The order's ID.
    pub id: ClientOrderId,
    /// The order's venue.
    pub venue: VenueId,
    /// The order's instrument.
    pub instrument: InstrumentId,
    /// The order's side.
    pub side: Side,
    /// The order's limit price.
    pub order_price: Price,
    /// The order's original quantity.
    pub order_qty: Qty,
    /// What happened.
    pub kind: ExecKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_ids_keep_epoch_and_sequence_apart() {
        let id = ClientOrderId::new(7, 42);
        assert_eq!(id.epoch(), 7);
        assert_eq!(id.sequence(), 42);
        assert_ne!(ClientOrderId::new(8, 42), id);
    }

    #[test]
    fn commands_and_reports_stay_small() {
        assert!(size_of::<OrderCommand>() <= 64);
        assert!(size_of::<ExecReport>() <= 96);
    }
}
