//! Positions and PnL, updated incrementally from fills.

use ringbook_core::{Price, Qty, Side};

use crate::config::{MAX_INSTRUMENTS, MAX_VENUES, SlotParams};

const MICRO_USD: i128 = 1_000_000;
const SLOTS: usize = MAX_VENUES * MAX_INSTRUMENTS;

/// One pair's position and accounting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Position {
    /// The net quantity.
    pub qty: i64,
    /// Cash from fills, in units of price times quantity.
    pub cash: i128,
    /// Fees paid, in units of price times quantity.
    pub fees: i128,
    /// The value last counted towards equity, in micro-USD.
    pub equity_micro_usd: i64,
}

/// Every pair's position, and the run's equity and high-water mark.
#[derive(Clone, Debug)]
pub struct Positions {
    positions: [Position; SLOTS],
    total_equity_micro_usd: i64,
    high_water_mark_micro_usd: i64,
}

#[inline]
const fn slot(venue: usize, instrument: usize) -> usize {
    venue * MAX_INSTRUMENTS + instrument
}

impl Positions {
    /// Creates flat positions.
    #[must_use]
    pub fn new() -> Self {
        Self {
            positions: [Position::default(); SLOTS],
            total_equity_micro_usd: 0,
            high_water_mark_micro_usd: 0,
        }
    }

    /// Returns a pair's position.
    #[inline]
    #[must_use]
    pub fn get(&self, venue: usize, instrument: usize) -> &Position {
        &self.positions[slot(venue, instrument)]
    }

    /// Applies a fill.
    #[inline]
    pub fn apply_fill(
        &mut self,
        venue: usize,
        instrument: usize,
        side: Side,
        price: Price,
        qty: Qty,
        fee: i64,
    ) {
        let position = &mut self.positions[slot(venue, instrument)];
        let value = i128::from(price.0) * i128::from(qty.0);
        match side {
            Side::Buy => {
                position.qty += qty.0;
                position.cash -= value;
            }
            Side::Sell => {
                position.qty -= qty.0;
                position.cash += value;
            }
        }
        position.fees += i128::from(fee);
    }

    /// Re-marks a pair at `mark`, and updates total equity incrementally.
    #[inline]
    pub fn mark(&mut self, venue: usize, instrument: usize, mark: Price, params: &SlotParams) {
        let position = &mut self.positions[slot(venue, instrument)];
        let raw = position.cash - position.fees + i128::from(position.qty) * i128::from(mark.0);
        let equity = (raw * MICRO_USD / params.notional_factor()) as i64;

        self.total_equity_micro_usd += equity - position.equity_micro_usd;
        position.equity_micro_usd = equity;
        self.high_water_mark_micro_usd = self
            .high_water_mark_micro_usd
            .max(self.total_equity_micro_usd);
    }

    /// Returns the run's equity in micro-USD.
    #[inline]
    #[must_use]
    pub const fn equity_micro_usd(&self) -> i64 {
        self.total_equity_micro_usd
    }

    /// Returns the fall in equity from its high-water mark, in micro-USD.
    #[inline]
    #[must_use]
    pub const fn drawdown_micro_usd(&self) -> i64 {
        self.high_water_mark_micro_usd - self.total_equity_micro_usd
    }
}

impl Default for Positions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARAMS: SlotParams = SlotParams {
        tick: 1,
        price_decimals: 1,
        qty_decimals: 3,
        max_significant_figures: None,
        order_qty: 1,
        max_order_qty: 5,
        max_position: 10,
    };

    #[test]
    fn a_round_trip_realises_the_price_difference_less_fees() {
        let mut positions = Positions::new();
        positions.apply_fill(0, 0, Side::Buy, Price(1000), Qty(1000), 100);
        positions.apply_fill(0, 0, Side::Sell, Price(1010), Qty(1000), 100);
        positions.mark(0, 0, Price(5000), &PARAMS);

        assert_eq!(positions.get(0, 0).qty, 0);
        // 1 USD of profit, less two 0.01 USD fees.
        assert_eq!(positions.equity_micro_usd(), 980_000);
    }

    #[test]
    fn open_positions_are_marked_to_the_mid() {
        let mut positions = Positions::new();
        positions.apply_fill(0, 0, Side::Buy, Price(1000), Qty(1000), 0);
        positions.mark(0, 0, Price(1020), &PARAMS);
        assert_eq!(positions.equity_micro_usd(), 2_000_000);
    }

    #[test]
    fn drawdown_is_measured_from_the_high_water_mark() {
        let mut positions = Positions::new();
        positions.apply_fill(0, 0, Side::Buy, Price(1000), Qty(1000), 0);
        positions.mark(0, 0, Price(1020), &PARAMS);
        positions.mark(0, 0, Price(1005), &PARAMS);
        assert_eq!(positions.drawdown_micro_usd(), 1_500_000);
        positions.mark(0, 0, Price(1030), &PARAMS);
        assert_eq!(positions.drawdown_micro_usd(), 0);
    }

    #[test]
    fn equity_sums_across_venues_and_instruments() {
        let mut positions = Positions::new();
        positions.apply_fill(0, 0, Side::Buy, Price(1000), Qty(1000), 0);
        positions.apply_fill(1, 3, Side::Sell, Price(1000), Qty(1000), 0);
        positions.mark(0, 0, Price(1010), &PARAMS);
        positions.mark(1, 3, Price(1005), &PARAMS);
        assert_eq!(positions.equity_micro_usd(), 1_000_000 - 500_000);
    }
}
