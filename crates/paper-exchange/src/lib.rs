//! Simulated matching: a pure state machine with no I/O.
//!
//! Commands arrive after a configured delay and reports leave after another.
//! Time advances only when the caller says so, which is how the same core runs
//! deterministically in replay and on wall-clock time in the mock venue.
//!
//! Passive fills follow an estimated queue position: public trades at our
//! price consume the volume ahead of us, and we fill only once it is gone.
//! Our own orders never change the book, so market impact is not modelled.

#![forbid(unsafe_code)]

use std::collections::VecDeque;

use ringbook_book::Book;
use ringbook_core::{
    CancelOrder, ClientOrderId, Event, EventTime, ExecKind, ExecReport, InstrumentId, Liquidity,
    NewOrder, OrderCommand, Payload, Price, Qty, RejectReason, Side, TimeInForce, VenueId,
};

const FEE_DENOMINATOR: i128 = 1_000_000;

/// Estimates how much of a cancelled quantity was ahead of our order.
pub trait QueueModel: Send {
    /// Returns the volume ahead after `cancelled` left a level of `level` units.
    fn ahead_after_cancel(&self, ahead: i64, level: i64, cancelled: i64) -> i64;
}

/// Assumes cancels are more likely to come from the larger part of the queue.
///
/// With `f(x) = x^power`, the share of a cancel taken from ahead of us is
/// `f(ahead) / (f(ahead) + f(behind))`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProbabilisticQueue {
    /// The exponent; one means proportional to queue position.
    pub power: i32,
}

impl QueueModel for ProbabilisticQueue {
    fn ahead_after_cancel(&self, ahead: i64, level: i64, cancelled: i64) -> i64 {
        if ahead <= 0 || cancelled <= 0 {
            return ahead.max(0);
        }
        let behind = (level - ahead).max(0);
        let weight_ahead = (ahead as f64).powi(self.power);
        let weight_behind = (behind as f64).powi(self.power);
        let share = weight_ahead / (weight_ahead + weight_behind);
        let removed = (cancelled as f64 * share).round() as i64;
        (ahead - removed).max(0)
    }
}

/// A venue's simulated fees and delays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VenueSimulation {
    /// The maker fee in hundredths of a basis point; negative for a rebate.
    pub maker_fee_centi_bps: i64,
    /// The taker fee in hundredths of a basis point.
    pub taker_fee_centi_bps: i64,
    /// The delay from decision to exchange, in nanoseconds.
    pub order_latency_ns: u64,
    /// The delay from exchange to acknowledgement, in nanoseconds.
    pub ack_latency_ns: u64,
}

/// How a paper exchange is sized.
#[derive(Clone, Debug)]
pub struct ExchangeConfig {
    /// Each venue's simulation settings, indexed by venue ID.
    pub venues: Vec<VenueSimulation>,
    /// Each instrument's book tick at each venue: `ticks[venue][instrument]`.
    pub ticks: Vec<Vec<i64>>,
    /// Prices held by each book's window.
    pub book_levels: usize,
    /// The most orders resting per venue and instrument.
    pub max_resting: usize,
    /// The most commands in flight per venue.
    pub max_in_flight: usize,
}

#[derive(Clone, Copy, Debug)]
struct Resting {
    order: NewOrder,
    remaining: i64,
    ahead: i64,
    level_seen: i64,
    traded_since: i64,
}

#[derive(Clone, Copy, Debug)]
struct InFlight {
    arrival: EventTime,
    command: OrderCommand,
}

struct Slot {
    book: Book,
    resting: Vec<Resting>,
    position: i64,
    book_changed: bool,
}

struct Lane {
    simulation: VenueSimulation,
    inbound: VecDeque<InFlight>,
    outbound: VecDeque<ExecReport>,
}

/// An order open at the simulated venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenOrder {
    /// The order.
    pub order: NewOrder,
    /// Its unfilled quantity.
    pub remaining: Qty,
}

/// The simulated exchange.
pub struct PaperExchange<Q> {
    queue_model: Q,
    instruments: usize,
    slots: Box<[Slot]>,
    lanes: Box<[Lane]>,
    max_resting: usize,
    max_in_flight: usize,
    next_sequence: u64,
}

fn notional(price: Price, qty: i64) -> i128 {
    i128::from(price.0) * i128::from(qty)
}

fn fee(price: Price, qty: i64, centi_bps: i64) -> i64 {
    (notional(price, qty) * i128::from(centi_bps) / FEE_DENOMINATOR) as i64
}

impl<Q: QueueModel> PaperExchange<Q> {
    /// Creates an exchange, allocating everything it will ever need.
    ///
    /// # Panics
    ///
    /// Panics if a tick or the window size can't form a valid book.
    #[must_use]
    pub fn new(config: &ExchangeConfig, queue_model: Q) -> Self {
        let instruments = config.ticks.first().map_or(0, Vec::len);
        let slots = config
            .ticks
            .iter()
            .flatten()
            .map(|&tick| Slot {
                book: Book::new(tick, config.book_levels).expect("config validated the book"),
                resting: Vec::with_capacity(config.max_resting),
                position: 0,
                book_changed: false,
            })
            .collect();
        let lanes = config
            .venues
            .iter()
            .map(|&simulation| Lane {
                simulation,
                inbound: VecDeque::with_capacity(config.max_in_flight),
                outbound: VecDeque::with_capacity(config.max_in_flight * 8),
            })
            .collect();
        Self {
            queue_model,
            instruments,
            slots,
            lanes,
            max_resting: config.max_resting,
            max_in_flight: config.max_in_flight,
            next_sequence: 0,
        }
    }

    #[inline]
    fn slot_index(&self, venue: VenueId, instrument: InstrumentId) -> usize {
        usize::from(venue.0) * self.instruments + usize::from(instrument.0)
    }

    /// Processes a market event at its event time.
    #[inline]
    pub fn on_market(&mut self, event: &Event) {
        self.advance(event.event_time);
        let index = self.slot_index(event.venue, event.instrument);
        match event.payload {
            Payload::BookClear => {
                self.slots[index].book.clear();
                self.slots[index].book_changed = true;
            }
            Payload::BookLevel { side, price, qty } => {
                self.slots[index].book.apply(side, price, qty);
                self.slots[index].book_changed = true;
            }
            Payload::Trade {
                aggressor,
                price,
                qty,
            } => self.on_trade(event, aggressor, price, qty.0),
            Payload::VenueStatus(_) => {}
        }
        // Trades and book updates arrive separately, so a trade-only batch
        // says nothing yet about how the level's size changed.
        if event.end_of_batch && self.slots[index].book_changed {
            self.slots[index].book_changed = false;
            self.reconcile_queues(index);
        }
    }

    fn on_trade(&mut self, event: &Event, aggressor: Side, price: Price, qty: i64) {
        let index = self.slot_index(event.venue, event.instrument);
        let lane = usize::from(event.venue.0);
        let ack_time = EventTime(event.event_time.0 + self.lanes[lane].simulation.ack_latency_ns);
        let maker_fee = self.lanes[lane].simulation.maker_fee_centi_bps;

        let Slot {
            resting, position, ..
        } = &mut self.slots[index];
        let outbound = &mut self.lanes[lane].outbound;
        let mut volume = qty;
        resting.retain_mut(|order| {
            if order.order.side == aggressor || volume == 0 {
                return true;
            }
            let reached = match order.order.side {
                Side::Buy => price <= order.order.price,
                Side::Sell => price >= order.order.price,
            };
            if !reached {
                return true;
            }

            let mut available = volume;
            if price == order.order.price {
                order.traded_since += qty;
                let consumed = order.ahead.min(available);
                order.ahead -= consumed;
                available -= consumed;
            } else {
                // The market traded through our price, so nothing is ahead.
                order.ahead = 0;
            }

            let filled = available.min(order.remaining);
            if filled == 0 {
                return true;
            }
            volume -= filled;
            order.remaining -= filled;
            *position += match order.order.side {
                Side::Buy => filled,
                Side::Sell => -filled,
            };
            outbound.push_back(report(
                &order.order,
                ack_time,
                ExecKind::Fill {
                    price: order.order.price,
                    qty: Qty(filled),
                    fee: fee(order.order.price, filled, maker_fee),
                    liquidity: Liquidity::Maker,
                },
            ));
            order.remaining > 0
        });
    }

    /// Attributes size decreases that no public trade explains to cancels.
    fn reconcile_queues(&mut self, index: usize) {
        let Self {
            slots, queue_model, ..
        } = self;
        let slot = &mut slots[index];
        let book = &slot.book;
        for order in &mut slot.resting {
            let level = book.qty_at(order.order.side, order.order.price).0;
            if level == 0 && beyond_visible_depth(book, order.order.side, order.order.price) {
                // The feed no longer shows this price, so its size is unknown.
                order.traded_since = 0;
                continue;
            }
            let decrease = order.level_seen - level;
            if decrease > 0 {
                let cancelled = (decrease - order.traded_since).max(0);
                order.ahead =
                    queue_model.ahead_after_cancel(order.ahead, order.level_seen, cancelled);
            }
            order.ahead = order.ahead.min(level);
            order.level_seen = level;
            order.traded_since = (order.traded_since - decrease.max(0)).max(0);
        }
    }

    /// Queues a command, which reaches the exchange after the order latency.
    pub fn submit(&mut self, command: OrderCommand, now: EventTime) {
        let lane_index = match command {
            OrderCommand::New(order) => usize::from(order.venue.0),
            OrderCommand::Cancel(cancel) => usize::from(cancel.venue.0),
        };
        let max_in_flight = self.max_in_flight;
        let lane = &mut self.lanes[lane_index];
        if lane.inbound.len() < max_in_flight {
            lane.inbound.push_back(InFlight {
                arrival: EventTime(now.0 + lane.simulation.order_latency_ns),
                command,
            });
            return;
        }

        let time = EventTime(now.0 + lane.simulation.ack_latency_ns);
        let reject = ExecKind::Reject(RejectReason::VenueFull);
        match command {
            OrderCommand::New(order) => lane.outbound.push_back(report(&order, time, reject)),
            OrderCommand::Cancel(cancel) => {
                lane.outbound
                    .push_back(cancel_report(&cancel, time, reject));
            }
        }
    }

    /// Processes every command that has arrived by `now`.
    #[inline]
    pub fn advance(&mut self, now: EventTime) {
        for lane_index in 0..self.lanes.len() {
            while let Some(in_flight) = self.lanes[lane_index].inbound.front().copied() {
                if in_flight.arrival > now {
                    break;
                }
                self.lanes[lane_index].inbound.pop_front();
                match in_flight.command {
                    OrderCommand::New(order) => self.on_new(&order, in_flight.arrival),
                    OrderCommand::Cancel(cancel) => self.on_cancel(&cancel, in_flight.arrival),
                }
            }
        }
    }

    fn on_new(&mut self, order: &NewOrder, arrival: EventTime) {
        let index = self.slot_index(order.venue, order.instrument);
        let lane = usize::from(order.venue.0);
        let simulation = self.lanes[lane].simulation;
        let time = EventTime(arrival.0 + simulation.ack_latency_ns);
        let reject = |lanes: &mut [Lane], reason| {
            lanes[lane]
                .outbound
                .push_back(report(order, time, ExecKind::Reject(reason)));
        };

        let (best_bid, best_ask) = {
            let book = &self.slots[index].book;
            (book.best_bid(), book.best_ask())
        };
        if best_bid.is_none() && best_ask.is_none() {
            reject(&mut self.lanes, RejectReason::NoMarket);
            return;
        }

        match order.time_in_force {
            TimeInForce::PostOnly => {
                let crosses = match order.side {
                    Side::Buy => best_ask.is_some_and(|(ask, _)| order.price >= ask),
                    Side::Sell => best_bid.is_some_and(|(bid, _)| order.price <= bid),
                };
                if crosses {
                    reject(&mut self.lanes, RejectReason::PostOnlyCross);
                    return;
                }
                if self.slots[index].resting.len() == self.max_resting {
                    reject(&mut self.lanes, RejectReason::VenueFull);
                    return;
                }
                let level = self.slots[index].book.qty_at(order.side, order.price).0;
                self.slots[index].resting.push(Resting {
                    order: *order,
                    remaining: order.qty.0,
                    ahead: level,
                    level_seen: level,
                    traded_since: 0,
                });
                self.lanes[lane]
                    .outbound
                    .push_back(report(order, time, ExecKind::Ack));
            }
            TimeInForce::ImmediateOrCancel => {
                self.take_liquidity(order, index, lane, time, simulation.taker_fee_centi_bps);
            }
        }
    }

    fn take_liquidity(
        &mut self,
        order: &NewOrder,
        index: usize,
        lane: usize,
        time: EventTime,
        taker_fee: i64,
    ) {
        let Self { slots, lanes, .. } = self;
        let outbound = &mut lanes[lane].outbound;
        let Slot { book, position, .. } = &mut slots[index];
        outbound.push_back(report(order, time, ExecKind::Ack));

        let opposite = match order.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let mut remaining = order.qty.0;
        for (price, qty) in book.levels(opposite) {
            let acceptable = match order.side {
                Side::Buy => price <= order.price,
                Side::Sell => price >= order.price,
            };
            if !acceptable || remaining == 0 {
                break;
            }
            let filled = remaining.min(qty.0);
            remaining -= filled;
            *position += match order.side {
                Side::Buy => filled,
                Side::Sell => -filled,
            };
            outbound.push_back(report(
                order,
                time,
                ExecKind::Fill {
                    price,
                    qty: Qty(filled),
                    fee: fee(price, filled, taker_fee),
                    liquidity: Liquidity::Taker,
                },
            ));
        }
        if remaining > 0 {
            outbound.push_back(report(order, time, ExecKind::Expired));
        }
    }

    fn on_cancel(&mut self, cancel: &CancelOrder, arrival: EventTime) {
        let index = self.slot_index(cancel.venue, cancel.instrument);
        let lane = usize::from(cancel.venue.0);
        let time = EventTime(arrival.0 + self.lanes[lane].simulation.ack_latency_ns);
        let resting = &mut self.slots[index].resting;
        let outcome = match resting.iter().position(|order| order.order.id == cancel.id) {
            Some(position) => report(&resting.remove(position).order, time, ExecKind::CancelAck),
            None => cancel_report(cancel, time, ExecKind::Reject(RejectReason::UnknownOrder)),
        };
        self.lanes[lane].outbound.push_back(outcome);
    }

    /// Delivers reports due by `now`, assigning their session sequence numbers.
    #[inline]
    pub fn poll(&mut self, now: EventTime, mut deliver: impl FnMut(ExecReport)) {
        for lane in &mut self.lanes {
            while lane.outbound.front().is_some_and(|next| next.time <= now) {
                let Some(mut next) = lane.outbound.pop_front() else {
                    break;
                };
                self.next_sequence += 1;
                next.sequence = self.next_sequence;
                deliver(next);
            }
        }
    }

    /// Cancels every open order immediately, as on a lost connection.
    ///
    /// The cancels are reported at `now`, with no simulated delay.
    pub fn cancel_all(&mut self, now: EventTime) {
        let instruments = self.instruments;
        for (index, slot) in self.slots.iter_mut().enumerate() {
            let lane = index.checked_div(instruments).unwrap_or(0);
            for order in slot.resting.drain(..) {
                self.lanes[lane]
                    .outbound
                    .push_back(report(&order.order, now, ExecKind::CancelAck));
            }
        }
        for lane in &mut self.lanes {
            lane.inbound.clear();
        }
    }

    /// Clears everything belonging to the previous session, keeping the books.
    pub fn reset_session(&mut self) {
        for slot in &mut self.slots {
            slot.resting.clear();
            slot.position = 0;
        }
        for lane in &mut self.lanes {
            lane.inbound.clear();
            lane.outbound.clear();
        }
        self.next_sequence = 0;
    }

    /// Calls `visit` for each open order.
    pub fn open_orders(&self, mut visit: impl FnMut(OpenOrder)) {
        for slot in &self.slots {
            for order in &slot.resting {
                visit(OpenOrder {
                    order: order.order,
                    remaining: Qty(order.remaining),
                });
            }
        }
    }

    /// Returns the net position in scaled quantity units.
    #[must_use]
    pub fn position(&self, venue: VenueId, instrument: InstrumentId) -> i64 {
        self.slots[self.slot_index(venue, instrument)].position
    }

    /// Returns the book the exchange is matching against.
    #[must_use]
    pub fn book(&self, venue: VenueId, instrument: InstrumentId) -> &Book {
        &self.slots[self.slot_index(venue, instrument)].book
    }
}

fn beyond_visible_depth(book: &Book, side: Side, price: Price) -> bool {
    book.levels(side)
        .last()
        .is_none_or(|(deepest, _)| match side {
            Side::Buy => price < deepest,
            Side::Sell => price > deepest,
        })
}

fn report(order: &NewOrder, time: EventTime, kind: ExecKind) -> ExecReport {
    ExecReport {
        sequence: 0,
        time,
        id: order.id,
        venue: order.venue,
        instrument: order.instrument,
        side: order.side,
        order_price: order.price,
        order_qty: order.qty,
        kind,
    }
}

fn cancel_report(cancel: &CancelOrder, time: EventTime, kind: ExecKind) -> ExecReport {
    ExecReport {
        sequence: 0,
        time,
        id: cancel.id,
        venue: cancel.venue,
        instrument: cancel.instrument,
        side: Side::Buy,
        order_price: Price(0),
        order_qty: Qty::ZERO,
        kind,
    }
}

/// Returns the sequence number of an order's ID, for tests and tools.
#[must_use]
pub const fn order_sequence(id: ClientOrderId) -> u64 {
    id.sequence()
}

#[cfg(test)]
mod tests;
