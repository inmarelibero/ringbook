//! The strategy interface and the built-in market maker.

use ringbook_book::Book;
use ringbook_core::{InstrumentId, ParamChange, ParamKey, Price, VenueId};

use crate::config::SlotParams;

const CENTI_BPS: i128 = 1_000_000;

/// What a strategy may look at when deciding its quotes.
#[derive(Debug)]
pub struct MarketView<'a> {
    /// The venue.
    pub venue: VenueId,
    /// The instrument.
    pub instrument: InstrumentId,
    /// The venue's current book.
    pub book: &'a Book,
    /// The net position in scaled quantity units.
    pub position: i64,
    /// The instrument's rules at this venue.
    pub params: &'a SlotParams,
}

/// The prices a strategy wants to quote; `None` means no quote on that side.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Quote {
    /// The desired bid.
    pub bid: Option<Price>,
    /// The desired ask.
    pub ask: Option<Price>,
}

/// A compiled-in trading strategy.
pub trait Strategy: Send {
    /// Decides the quotes after a complete batch.
    fn quote(&mut self, view: &MarketView<'_>) -> Quote;

    /// Applies a parameter change between batches.
    fn set_param(&mut self, change: ParamChange);

    /// The price move, as a fraction in hundredths of a basis point, that
    /// justifies replacing a working order.
    fn requote_threshold_centi_bps(&self) -> i64;
}

/// The market maker's settings, in hundredths of a basis point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StrategyParams {
    /// The full quoted spread.
    pub spread_centi_bps: i64,
    /// The price shift at maximum inventory.
    pub inventory_skew_centi_bps: i64,
    /// The move that triggers a requote.
    pub requote_threshold_centi_bps: i64,
}

/// Quotes around the venue's mid, skewed against inventory.
///
/// It is a load generator rather than a source of edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketMaker {
    params: StrategyParams,
}

impl MarketMaker {
    /// Creates a market maker with its starting settings.
    #[must_use]
    pub const fn new(params: StrategyParams) -> Self {
        Self { params }
    }

    /// Returns the current settings.
    #[must_use]
    pub const fn params(&self) -> StrategyParams {
        self.params
    }
}

fn round_down(price: i64, tick: i64) -> i64 {
    price - price.rem_euclid(tick)
}

fn round_up(price: i64, tick: i64) -> i64 {
    round_down(price + tick - 1, tick)
}

/// Returns the step size that limits `price` to `figures` significant digits.
fn significant_step(price: i64, figures: u8) -> i64 {
    let mut step = 1;
    let mut remaining = price / 10_i64.pow(u32::from(figures));
    while remaining > 0 {
        step *= 10;
        remaining /= 10;
    }
    step
}

fn valid_step(price: i64, params: &SlotParams) -> i64 {
    let step = params
        .max_significant_figures
        .map_or(1, |figures| significant_step(price, figures));
    step.max(params.tick)
}

impl Strategy for MarketMaker {
    #[inline]
    fn quote(&mut self, view: &MarketView<'_>) -> Quote {
        let Some(mid) = view.book.mid() else {
            return Quote::default();
        };
        let mid_wide = i128::from(mid.0);
        let half_spread =
            (mid_wide * i128::from(self.params.spread_centi_bps) / (2 * CENTI_BPS)) as i64;
        let full_skew = mid_wide * i128::from(self.params.inventory_skew_centi_bps) / CENTI_BPS;
        let skew = (full_skew * i128::from(view.position)
            / i128::from(view.params.max_position.max(1))) as i64;

        let raw_bid = mid.0 - half_spread - skew;
        let raw_ask = mid.0 + half_spread - skew;
        let bid = round_down(raw_bid, valid_step(raw_bid, view.params));
        let ask = round_up(raw_ask, valid_step(raw_ask, view.params));

        let order_qty = view.params.order_qty;
        Quote {
            bid: (view.position + order_qty <= view.params.max_position).then_some(Price(bid)),
            ask: (-view.position + order_qty <= view.params.max_position).then_some(Price(ask)),
        }
    }

    fn set_param(&mut self, change: ParamChange) {
        match change.key {
            ParamKey::SpreadCentiBps => self.params.spread_centi_bps = change.value,
            ParamKey::InventorySkewCentiBps => self.params.inventory_skew_centi_bps = change.value,
            ParamKey::RequoteThresholdCentiBps => {
                self.params.requote_threshold_centi_bps = change.value;
            }
        }
    }

    #[inline]
    fn requote_threshold_centi_bps(&self) -> i64 {
        self.params.requote_threshold_centi_bps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbook_core::{Qty, Side};

    const PARAMS: StrategyParams = StrategyParams {
        spread_centi_bps: 400,
        inventory_skew_centi_bps: 200,
        requote_threshold_centi_bps: 100,
    };

    fn slot(figures: Option<u8>) -> SlotParams {
        SlotParams {
            tick: 1,
            price_decimals: 1,
            qty_decimals: 3,
            max_significant_figures: figures,
            order_qty: 1,
            max_order_qty: 5,
            max_position: 10,
        }
    }

    fn book() -> Book {
        let mut book = Book::new(1, 1 << 12).unwrap();
        book.apply(Side::Buy, Price(1_000_000), Qty(1));
        book.apply(Side::Sell, Price(1_000_002), Qty(1));
        book
    }

    fn quote(position: i64, params: &SlotParams) -> Quote {
        let book = book();
        MarketMaker::new(PARAMS).quote(&MarketView {
            venue: VenueId(0),
            instrument: InstrumentId(0),
            book: &book,
            position,
            params,
        })
    }

    #[test]
    fn quotes_symmetrically_around_the_mid_when_flat() {
        let quote = quote(0, &slot(None));
        assert_eq!(quote.bid, Some(Price(1_000_001 - 200)));
        assert_eq!(quote.ask, Some(Price(1_000_001 + 200)));
    }

    #[test]
    fn inventory_shifts_both_quotes_against_the_position() {
        let flat = quote(0, &slot(None));
        let long = quote(5, &slot(None));
        assert_eq!(long.bid.unwrap().0, flat.bid.unwrap().0 - 100);
        assert_eq!(long.ask.unwrap().0, flat.ask.unwrap().0 - 100);
    }

    #[test]
    fn stops_quoting_the_side_that_would_exceed_the_limit() {
        let quote = quote(10, &slot(None));
        assert_eq!(quote.bid, None);
        assert!(quote.ask.is_some());
    }

    #[test]
    fn prices_obey_a_significant_figure_limit() {
        let quote = quote(0, &slot(Some(5)));
        assert_eq!(quote.bid.unwrap().0 % 100, 0);
        assert_eq!(quote.ask.unwrap().0 % 100, 0);
        assert!(quote.bid.unwrap().0 < 1_000_001);
        assert!(quote.ask.unwrap().0 > 1_000_001);
    }

    #[test]
    fn significant_steps_scale_with_magnitude() {
        assert_eq!(significant_step(99_999, 5), 1);
        assert_eq!(significant_step(100_000, 5), 10);
        assert_eq!(significant_step(1_234_567, 5), 100);
    }

    #[test]
    fn no_book_means_no_quotes() {
        let book = Book::new(1, 1 << 12).unwrap();
        let quote = MarketMaker::new(PARAMS).quote(&MarketView {
            venue: VenueId(0),
            instrument: InstrumentId(0),
            book: &book,
            position: 0,
            params: &slot(None),
        });
        assert_eq!(quote, Quote::default());
    }

    #[test]
    fn parameters_change_between_batches() {
        let mut maker = MarketMaker::new(PARAMS);
        maker.set_param(ParamChange {
            key: ParamKey::SpreadCentiBps,
            value: 800,
        });
        maker.set_param(ParamChange {
            key: ParamKey::RequoteThresholdCentiBps,
            value: 50,
        });
        assert_eq!(maker.params().spread_centi_bps, 800);
        assert_eq!(maker.requote_threshold_centi_bps(), 50);
    }
}
