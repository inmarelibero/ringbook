//! The fixed-size event that every market data source normalises into.

use core::num::NonZeroU64;

use crate::decimal::{Price, Qty};

/// A venue's compact identifier, assigned at startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VenueId(pub u8);

/// An instrument's compact identifier, assigned at startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstrumentId(pub u16);

/// The deterministic clock: nanoseconds stamped when a frame was received.
///
/// Trading logic reads only this clock, so a run's decisions never depend on
/// how fast the machine is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventTime(pub u64);

/// A venue-reported timestamp in nanoseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExchangeTime(pub NonZeroU64);

/// An opaque stamp used only to measure latency.
///
/// It deliberately has no ordering, so trading logic can't compare against it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MeasureStamp(pub u64);

/// The side of a book level, or the aggressor's side of a trade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    /// A bid level, or a trade whose aggressor was buying.
    Buy,
    /// An ask level, or a trade whose aggressor was selling.
    Sell,
}

/// Whether a venue's data can currently be trusted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VenueState {
    /// Data is flowing and the book is valid.
    Live,
    /// Data has stopped arriving.
    Stale,
    /// The book is being rebuilt.
    Resyncing,
}

/// What an [`Event`] reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Payload {
    /// The levels that follow replace this instrument's book.
    BookClear,
    /// The absolute quantity at a price level.
    BookLevel {
        /// The side of the book.
        side: Side,
        /// The level's price.
        price: Price,
        /// The absolute quantity; zero removes the level.
        qty: Qty,
    },
    /// A public trade.
    Trade {
        /// The aggressor's side.
        aggressor: Side,
        /// The trade's price.
        price: Price,
        /// The trade's quantity.
        qty: Qty,
    },
    /// A change in the venue's state.
    VenueStatus(VenueState),
}

/// One normalised market data event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event {
    /// When the frame was received; the only clock trading logic reads.
    pub event_time: EventTime,
    /// The stamp used for latency measurement.
    pub measure_stamp: MeasureStamp,
    /// The venue's own timestamp, if it provides one.
    pub exchange_time: Option<ExchangeTime>,
    /// The instrument this event concerns.
    pub instrument: InstrumentId,
    /// The venue this event came from.
    pub venue: VenueId,
    /// Set on the last event of each exchange message.
    pub end_of_batch: bool,
    /// What happened.
    pub payload: Payload,
}

const _: () = assert!(
    size_of::<Event>() <= 64,
    "an event must fit in one cache line"
);
const _: () = assert!(align_of::<Event>() == 8);

#[cfg(test)]
mod tests {
    use super::*;

    fn event(payload: Payload, end_of_batch: bool) -> Event {
        Event {
            event_time: EventTime(1),
            measure_stamp: MeasureStamp(2),
            exchange_time: None,
            instrument: InstrumentId(3),
            venue: VenueId(4),
            end_of_batch,
            payload,
        }
    }

    #[test]
    fn an_absent_exchange_time_costs_no_extra_space() {
        assert_eq!(size_of::<Option<ExchangeTime>>(), size_of::<u64>());
    }

    #[test]
    fn every_payload_uses_the_same_fixed_size() {
        let clear = event(Payload::BookClear, false);
        let level = event(
            Payload::BookLevel {
                side: Side::Buy,
                price: Price(100),
                qty: Qty::ZERO,
            },
            true,
        );
        assert_eq!(size_of_val(&clear), size_of_val(&level));
    }

    #[test]
    fn events_copy_by_value() {
        let first = event(Payload::VenueStatus(VenueState::Stale), true);
        let second = first;
        assert_eq!(first, second);
    }

    #[test]
    fn event_time_is_ordered() {
        assert!(EventTime(2) > EventTime(1));
    }
}
