//! Pre-trade checks.

use ringbook_core::{EventTime, NewOrder, Price, Side};
use ringbook_journal::RiskRejection;

use crate::config::{MAX_VENUES, SlotParams};

const SECOND_NS: u64 = 1_000_000_000;
const CENTI_BPS: i128 = 1_000_000;
const MICRO_USD: i128 = 1_000_000;

/// Limits shared by every instrument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RiskLimits {
    /// The largest total value of position plus open orders, per pair.
    pub max_notional_micro_usd: i64,
    /// The most new orders per venue in a one-second window.
    pub max_orders_per_sec: u32,
    /// The furthest an order may be from the mid.
    pub price_collar_centi_bps: i64,
    /// The largest fall in equity from its high-water mark.
    pub max_drawdown_micro_usd: i64,
}

/// What risk needs to know about a pair's current exposure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Exposure {
    /// The net position.
    pub position: i64,
    /// Unfilled quantity of open buy orders.
    pub open_buy: i64,
    /// Unfilled quantity of open sell orders.
    pub open_sell: i64,
    /// The lowest price of our open sell orders.
    pub lowest_own_ask: Option<Price>,
    /// The highest price of our open buy orders.
    pub highest_own_bid: Option<Price>,
}

#[derive(Clone, Copy, Debug, Default)]
struct RateWindow {
    started: u64,
    count: u32,
}

/// The pre-trade checks.
#[derive(Clone, Copy, Debug)]
pub struct Risk {
    limits: RiskLimits,
    windows: [RateWindow; MAX_VENUES],
}

impl Risk {
    /// Creates the checks with their limits.
    #[must_use]
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            windows: [RateWindow::default(); MAX_VENUES],
        }
    }

    /// Returns the limits.
    #[must_use]
    pub const fn limits(&self) -> &RiskLimits {
        &self.limits
    }

    /// Checks an order, consuming rate budget only if it passes every check.
    ///
    /// # Errors
    ///
    /// Returns the first limit the order would break.
    #[inline]
    pub fn check(
        &mut self,
        order: &NewOrder,
        exposure: &Exposure,
        mid: Price,
        params: &SlotParams,
        now: EventTime,
    ) -> Result<(), RiskRejection> {
        if order.qty.0 > params.max_order_qty {
            return Err(RiskRejection::OrderSize);
        }

        let distance = i128::from((order.price.0 - mid.0).abs());
        if distance * CENTI_BPS > i128::from(self.limits.price_collar_centi_bps) * i128::from(mid.0)
        {
            return Err(RiskRejection::PriceCollar);
        }

        match order.side {
            Side::Buy
                if exposure
                    .lowest_own_ask
                    .is_some_and(|ask| order.price >= ask) =>
            {
                return Err(RiskRejection::SelfTrade);
            }
            Side::Sell
                if exposure
                    .highest_own_bid
                    .is_some_and(|bid| order.price <= bid) =>
            {
                return Err(RiskRejection::SelfTrade);
            }
            _ => {}
        }

        let worst_case = match order.side {
            Side::Buy => exposure.position + exposure.open_buy + order.qty.0,
            Side::Sell => -exposure.position + exposure.open_sell + order.qty.0,
        };
        if worst_case > params.max_position {
            return Err(RiskRejection::Position);
        }

        let notional =
            i128::from(worst_case) * i128::from(mid.0) * MICRO_USD / params.notional_factor();
        if notional > i128::from(self.limits.max_notional_micro_usd) {
            return Err(RiskRejection::Notional);
        }

        let window = &mut self.windows[usize::from(order.venue.0)];
        if now.0.saturating_sub(window.started) >= SECOND_NS {
            *window = RateWindow {
                started: now.0,
                count: 0,
            };
        }
        if window.count >= self.limits.max_orders_per_sec {
            return Err(RiskRejection::OrderRate);
        }
        window.count += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use ringbook_core::{ClientOrderId, InstrumentId, Qty, TimeInForce, VenueId};

    const LIMITS: RiskLimits = RiskLimits {
        max_notional_micro_usd: 1_000_000_000,
        max_orders_per_sec: 2,
        price_collar_centi_bps: 5_000,
        max_drawdown_micro_usd: 1,
    };

    const PARAMS: SlotParams = SlotParams {
        tick: 1,
        price_decimals: 1,
        qty_decimals: 3,
        max_significant_figures: None,
        order_qty: 1,
        max_order_qty: 5,
        max_position: 10,
    };

    const FLAT: Exposure = Exposure {
        position: 0,
        open_buy: 0,
        open_sell: 0,
        lowest_own_ask: None,
        highest_own_bid: None,
    };

    const MID: Price = Price(1_000_000);

    fn order(side: Side, price: i64, qty: i64) -> NewOrder {
        NewOrder {
            id: ClientOrderId::new(1, 1),
            venue: VenueId(0),
            instrument: InstrumentId(0),
            side,
            price: Price(price),
            qty: Qty(qty),
            time_in_force: TimeInForce::PostOnly,
        }
    }

    fn check(order: &NewOrder, exposure: &Exposure) -> Result<(), RiskRejection> {
        Risk::new(LIMITS).check(order, exposure, MID, &PARAMS, EventTime(1))
    }

    #[test]
    fn accepts_an_order_within_every_limit() {
        assert_eq!(check(&order(Side::Buy, 999_000, 1), &FLAT), Ok(()));
    }

    #[test]
    fn rejects_oversized_orders() {
        assert_eq!(
            check(&order(Side::Buy, 999_000, 6), &FLAT),
            Err(RiskRejection::OrderSize)
        );
    }

    #[test]
    fn rejects_prices_outside_the_collar() {
        assert_eq!(
            check(&order(Side::Buy, 900_000, 1), &FLAT),
            Err(RiskRejection::PriceCollar)
        );
        assert_eq!(check(&order(Side::Buy, 995_000, 1), &FLAT), Ok(()));
    }

    #[test]
    fn position_limit_counts_open_orders() {
        let exposure = Exposure {
            position: 6,
            open_buy: 3,
            ..FLAT
        };
        assert_eq!(check(&order(Side::Buy, 999_000, 1), &exposure), Ok(()));
        assert_eq!(
            check(&order(Side::Buy, 999_000, 2), &exposure),
            Err(RiskRejection::Position)
        );
        assert_eq!(check(&order(Side::Sell, 1_001_000, 2), &exposure), Ok(()));
    }

    #[test]
    fn rejects_orders_that_would_breach_notional() {
        let limits = RiskLimits {
            max_notional_micro_usd: 99_000_000,
            ..LIMITS
        };
        let result = Risk::new(limits).check(
            &order(Side::Buy, 999_000, 1),
            &FLAT,
            MID,
            &PARAMS,
            EventTime(1),
        );
        assert_eq!(result, Err(RiskRejection::Notional));
    }

    #[test]
    fn rejects_orders_that_would_trade_with_ourselves() {
        let exposure = Exposure {
            lowest_own_ask: Some(Price(1_000_100)),
            highest_own_bid: Some(Price(999_900)),
            ..FLAT
        };
        assert_eq!(
            check(&order(Side::Buy, 1_000_100, 1), &exposure),
            Err(RiskRejection::SelfTrade)
        );
        assert_eq!(
            check(&order(Side::Sell, 999_900, 1), &exposure),
            Err(RiskRejection::SelfTrade)
        );
    }

    #[test]
    fn the_order_rate_resets_each_second_of_event_time() {
        let mut risk = Risk::new(LIMITS);
        let new = order(Side::Buy, 999_000, 1);
        let mut at = |time| risk.check(&new, &FLAT, MID, &PARAMS, EventTime(time));
        assert_eq!(at(SECOND_NS), Ok(()));
        assert_eq!(at(SECOND_NS + 1), Ok(()));
        assert_eq!(at(SECOND_NS + 2), Err(RiskRejection::OrderRate));
        assert_eq!(at(2 * SECOND_NS), Ok(()));
    }

    #[test]
    fn rejected_orders_do_not_consume_rate_budget() {
        let mut risk = Risk::new(LIMITS);
        for _ in 0..10 {
            let _ = risk.check(&order(Side::Buy, 1, 1), &FLAT, MID, &PARAMS, EventTime(1));
        }
        assert_eq!(
            risk.check(
                &order(Side::Buy, 999_000, 1),
                &FLAT,
                MID,
                &PARAMS,
                EventTime(1)
            ),
            Ok(())
        );
    }

    proptest! {
        #[test]
        fn an_accepted_order_never_breaks_a_limit(
            buy in any::<bool>(),
            price in 1_i64..2_000_000,
            qty in 1_i64..20,
            position in -20_i64..20,
            open in 0_i64..20,
            notional in 1_i64..2_000_000_000,
        ) {
            let side = if buy { Side::Buy } else { Side::Sell };
            let exposure = Exposure { position, open_buy: open, open_sell: open, ..FLAT };
            let limits = RiskLimits { max_notional_micro_usd: notional, ..LIMITS };
            let result = Risk::new(limits).check(
                &order(side, price, qty), &exposure, MID, &PARAMS, EventTime(1),
            );
            if result.is_ok() {
                let worst = if buy { position + open + qty } else { -position + open + qty };
                prop_assert!(qty <= PARAMS.max_order_qty);
                prop_assert!(worst <= PARAMS.max_position);
                prop_assert!((price - MID.0).abs() * 1_000_000 <= LIMITS.price_collar_centi_bps * MID.0);
                let value = i128::from(worst) * i128::from(MID.0) * 1_000_000 / PARAMS.notional_factor();
                prop_assert!(value <= i128::from(notional));
            }
        }
    }
}
