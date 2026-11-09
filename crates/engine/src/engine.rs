//! The hot loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ringbook_book::Book;
use ringbook_clock::Clock;
use ringbook_core::{
    CancelOrder, ClientOrderId, ControlCommand, Event, EventTime, ExecKind, ExecReport, HaltReason,
    InstrumentId, MeasureStamp, NewOrder, OrderCommand, Payload, Price, Qty, Side, TimeInForce,
    VenueId, VenueState,
};
use ringbook_journal::{JournalBody, JournalRecord, RiskRejection};
use ringbook_ring::{Consumer, Producer};

use crate::config::{EngineParams, MAX_INSTRUMENTS, MAX_VENUES, SlotParams};
use crate::link::{ReconcileEvent, VenueLink};
use crate::oms::{OrderManager, OrderState, ReportOutcome};
use crate::positions::Positions;
use crate::risk::{Exposure, Risk};
use crate::strategy::{MarketView, Strategy};

const SLOTS: usize = MAX_VENUES * MAX_INSTRUMENTS;
const ORDER_TABLE: usize = 1 << 10;
/// Journal space kept free for the halt record and its cancels.
const JOURNAL_RESERVE: usize = 256;
/// How many outbound failures in a row trigger a halt.
const MAX_BLOCKED_ORDERS: u32 = 16;
const CENTI_BPS: i128 = 1_000_000;
const COUNTER_PERIOD_NS: u64 = 1_000_000_000;
const API_PERIOD_NS: u64 = 100_000_000;
/// Book levels per side pushed to the API.
pub const API_DEPTH: usize = 10;

/// Counters the hot thread publishes to telemetry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineCounters {
    /// Market events processed.
    pub events: u64,
    /// Market data batches processed.
    pub batches: u64,
    /// Orders sent.
    pub orders_sent: u64,
    /// Cancels sent.
    pub cancels_sent: u64,
    /// Fills applied.
    pub fills: u64,
    /// Orders refused by risk.
    pub risk_rejections: u64,
    /// Orders refused by the venue.
    pub venue_rejections: u64,
    /// Telemetry records dropped because the ring was full.
    pub telemetry_drops: u64,
    /// API updates dropped because the ring was full.
    pub api_drops: u64,
    /// Book window moves across all books.
    pub book_recentres: u64,
    /// Equity in micro-USD.
    pub equity_micro_usd: i64,
    /// Fall from the high-water mark in micro-USD.
    pub drawdown_micro_usd: i64,
    /// Whether the engine is free to quote.
    pub status: EngineStatus,
}

/// A record pushed to the telemetry thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TelemetryRecord {
    /// Frame at the parser to an accepted or rejected order decision.
    TickToDecision(u64),
    /// Frame at the parser to the order's bytes reaching the socket.
    TickToWire(u64),
    /// Frame at the parser to the end of processing a batch.
    BatchLatency(u64),
    /// The latest counters.
    Counters(EngineCounters),
}

/// A book summary pushed to the API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiBook {
    /// The venue.
    pub venue: VenueId,
    /// The instrument.
    pub instrument: InstrumentId,
    /// The bids, best first; empty entries have zero quantity.
    pub bids: [(Price, Qty); API_DEPTH],
    /// The asks, best first; empty entries have zero quantity.
    pub asks: [(Price, Qty); API_DEPTH],
}

/// A position summary pushed to the API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiPosition {
    /// The venue.
    pub venue: VenueId,
    /// The instrument.
    pub instrument: InstrumentId,
    /// The net quantity.
    pub qty: i64,
    /// The pair's equity in micro-USD.
    pub equity_micro_usd: i64,
    /// The run's total equity in micro-USD.
    pub total_equity_micro_usd: i64,
}

/// A copy of hot-thread state for the API gateway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum ApiUpdate {
    /// The top of a book.
    Book(ApiBook),
    /// A position.
    Position(ApiPosition),
}

/// Whether the engine is free to quote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EngineStatus {
    /// Quoting normally.
    #[default]
    Running,
    /// Not quoting until reconciliation succeeds.
    AwaitingReconcile,
    /// Halted until restart.
    Halted(HaltReason),
}

/// What a finished run did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunSummary {
    /// The final counters.
    pub counters: EngineCounters,
    /// The engine's final status.
    pub status: EngineStatus,
}

/// The rings and flags connecting the hot thread to everything else.
pub struct EngineChannels {
    /// Market data, one ring per source.
    pub inputs: Vec<Consumer<Event>>,
    /// Operator commands.
    pub control: Consumer<ControlCommand>,
    /// The journal.
    pub journal: Producer<JournalRecord>,
    /// Telemetry; may drop.
    pub telemetry: Producer<TelemetryRecord>,
    /// The API gateway; may drop.
    pub api: Producer<ApiUpdate>,
    /// Set by the operator to halt immediately, bypassing the control ring.
    pub halt_flag: Arc<AtomicBool>,
    /// Set when the journal can't be written.
    pub journal_failed: Arc<AtomicBool>,
    /// Set when every market data source has finished.
    pub inputs_done: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, Default)]
struct Working {
    id: Option<ClientOrderId>,
    price: Price2,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Price2(i64);

#[derive(Clone, Copy, Debug)]
struct Slot {
    params: Option<SlotParams>,
    live: bool,
    bid: Working,
    ask: Working,
    open_buy: i64,
    open_sell: i64,
    next_api: u64,
    recentres: u64,
}

impl Slot {
    const fn working(&mut self, side: Side) -> &mut Working {
        match side {
            Side::Buy => &mut self.bid,
            Side::Sell => &mut self.ask,
        }
    }
}

/// The hot thread's state.
pub struct Engine<S, L> {
    strategy: S,
    link: L,
    channels: EngineChannels,
    clock: Clock,
    books: Box<[Book]>,
    slots: [Slot; SLOTS],
    instrument_halted: [bool; MAX_INSTRUMENTS],
    venues: usize,
    instruments: usize,
    orders: OrderManager,
    positions: Positions,
    risk: Risk,
    status: EngineStatus,
    time: EventTime,
    batch: u64,
    blocked_orders: u32,
    script: Box<[(u64, ControlCommand)]>,
    script_position: usize,
    next_counters: u64,
    counters: EngineCounters,
    last_stamp: Option<MeasureStamp>,
    decided: bool,
    reports: Vec<ExecReport>,
    link_was_up: bool,
    open_batches: u32,
    recovered_session: u64,
    venue_open_count: u32,
    venue_open_mismatch: bool,
    venue_positions_mismatch: bool,
}

#[inline]
const fn slot_index(venue: usize, instrument: usize) -> usize {
    venue * MAX_INSTRUMENTS + instrument
}

const fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

impl<S: Strategy, L: VenueLink> Engine<S, L> {
    /// Builds an engine, allocating everything it will ever need.
    ///
    /// # Panics
    ///
    /// Panics if `params` describes a book that can't be built; validation
    /// rejects those before this point.
    pub fn new(
        params: &EngineParams,
        strategy: S,
        link: L,
        channels: EngineChannels,
        clock: Clock,
        epoch: u16,
    ) -> Self {
        let empty = Slot {
            params: None,
            live: false,
            bid: Working::default(),
            ask: Working::default(),
            open_buy: 0,
            open_sell: 0,
            next_api: 0,
            recentres: 0,
        };
        let mut slots = [empty; SLOTS];
        let mut books = Vec::with_capacity(SLOTS);
        for venue in 0..MAX_VENUES {
            for instrument in 0..MAX_INSTRUMENTS {
                let slot_params = params.slots[venue][instrument];
                slots[slot_index(venue, instrument)].params = slot_params;
                // Pairs that aren't traded get the smallest valid book.
                let (tick, levels) = slot_params.map_or((1, 64), |p| (p.tick, params.book_levels));
                books.push(Book::new(tick, levels).expect("config validated the book"));
            }
        }

        Self {
            strategy,
            link,
            channels,
            clock,
            books: books.into_boxed_slice(),
            slots,
            instrument_halted: [false; MAX_INSTRUMENTS],
            venues: params.venues,
            instruments: params.instruments,
            orders: OrderManager::new(ORDER_TABLE, epoch),
            positions: Positions::new(),
            risk: Risk::new(params.risk),
            status: EngineStatus::Running,
            time: EventTime(0),
            batch: 0,
            blocked_orders: 0,
            script: Box::new([]),
            script_position: 0,
            next_counters: 0,
            counters: EngineCounters::default(),
            last_stamp: None,
            decided: false,
            reports: Vec::with_capacity(4096),
            link_was_up: false,
            open_batches: 0,
            recovered_session: 0,
            venue_open_count: 0,
            venue_open_mismatch: false,
            venue_positions_mismatch: false,
        }
    }

    /// Sets commands to apply at given batches, to reproduce a recorded run.
    pub fn set_script(&mut self, script: Vec<(u64, ControlCommand)>) {
        self.script = script.into_boxed_slice();
        self.script_position = 0;
    }

    /// Returns the execution path.
    pub const fn link(&self) -> &L {
        &self.link
    }

    /// Returns the execution path.
    pub const fn link_mut(&mut self) -> &mut L {
        &mut self.link
    }

    /// Returns the current status.
    pub const fn status(&self) -> EngineStatus {
        self.status
    }

    /// Returns the positions.
    pub const fn positions(&self) -> &Positions {
        &self.positions
    }

    /// Returns the order table.
    pub const fn orders(&self) -> &OrderManager {
        &self.orders
    }

    /// Returns the counters.
    pub const fn counters(&self) -> &EngineCounters {
        &self.counters
    }

    /// Runs until the inputs finish and every ring is empty.
    pub fn run(&mut self) -> RunSummary {
        loop {
            let finished = self.channels.inputs_done.load(Ordering::Acquire);
            if !self.step() && finished {
                break;
            }
        }
        self.publish_counters();
        RunSummary {
            counters: self.counters,
            status: self.status,
        }
    }

    /// Performs one pass of the loop, returning whether any work was done.
    #[inline]
    pub fn step(&mut self) -> bool {
        self.check_flags();
        let mut busy = false;
        for ring in 0..self.channels.inputs.len() {
            // One batch per ring per pass, so no source can starve another.
            let mask = 1_u32 << ring;
            loop {
                let Some(event) = self.channels.inputs[ring].pop() else {
                    break;
                };
                busy = true;
                let end_of_batch = event.end_of_batch;
                self.on_event(&event);
                if end_of_batch {
                    self.open_batches &= !mask;
                    break;
                }
                self.open_batches |= mask;
            }
        }
        // An in-process venue only advances at batch boundaries, so its reports
        // never depend on how the threads happened to interleave.
        if self.link.runs_independently() {
            busy |= self.poll_reports();
            busy |= self.poll_reconcile();
            self.watch_link();
        }
        if self.open_batches == 0 {
            self.poll_control();
        }
        busy
    }

    #[inline]
    fn stamp(&self) -> MeasureStamp {
        self.last_stamp.unwrap_or(MeasureStamp(0))
    }

    /// Stops quoting when the link drops; quoting resumes after reconciling.
    #[inline]
    fn watch_link(&mut self) {
        let up = self.link.is_up();
        if self.link_was_up && !up && self.status == EngineStatus::Running {
            self.status = EngineStatus::AwaitingReconcile;
        }
        self.link_was_up = up;
    }

    #[inline]
    fn poll_reconcile(&mut self) -> bool {
        let mut busy = false;
        while let Some(event) = self.link.next_reconcile() {
            busy = true;
            self.on_reconcile_event(event);
            // Reports held behind the item can now be delivered in order.
            self.poll_reports();
        }
        busy
    }

    #[cold]
    fn on_reconcile_event(&mut self, event: ReconcileEvent) {
        match event {
            ReconcileEvent::Session(session) => {
                self.journal(JournalBody::Session(session));
                self.venue_open_count = 0;
                self.venue_open_mismatch = false;
                self.venue_positions_mismatch = false;
            }
            ReconcileEvent::OpenOrder { id, remaining } => {
                self.venue_open_count += 1;
                let matches = self
                    .orders
                    .get(id)
                    .is_some_and(|tracked| tracked.remaining() == remaining.0);
                self.venue_open_mismatch |= !matches;
            }
            ReconcileEvent::Position {
                venue,
                instrument,
                qty,
            } => {
                let (venue, instrument) = (usize::from(venue.0), usize::from(instrument.0));
                if venue < MAX_VENUES && instrument < MAX_INSTRUMENTS {
                    self.venue_positions_mismatch |=
                        self.positions.get(venue, instrument).qty != qty;
                }
            }
            ReconcileEvent::End => self.finish_venue_state(),
            ReconcileEvent::ResumeFailed => self.halt(HaltReason::ReconcileFailed),
        }
    }

    /// Compares the venue's reported state with ours after all retransmission.
    fn finish_venue_state(&mut self) {
        // Every report has now been applied, so a tracked order the venue
        // doesn't list can never be answered: its command was lost in transit.
        let mut stranded = [None; 64];
        if self.venue_open_count == 0 {
            for (entry, tracked) in stranded.iter_mut().zip(self.orders.open_orders()) {
                *entry = Some(*tracked);
            }
        }
        for tracked in stranded.into_iter().flatten() {
            self.drop_stranded(&tracked);
        }

        let matched = !self.venue_open_mismatch && !self.venue_positions_mismatch;
        if self.status == EngineStatus::AwaitingReconcile {
            self.finish_reconcile(matched);
        } else if !matched {
            self.halt(HaltReason::ReconcileFailed);
        }
    }

    fn drop_stranded(&mut self, tracked: &crate::oms::TrackedOrder) {
        let venue = usize::from(tracked.order.venue.0);
        let instrument = usize::from(tracked.order.instrument.0);
        let slot = &mut self.slots[slot_index(venue, instrument)];
        match tracked.order.side {
            Side::Buy => slot.open_buy -= tracked.remaining(),
            Side::Sell => slot.open_sell -= tracked.remaining(),
        }
        let working = slot.working(tracked.order.side);
        if working.id == Some(tracked.order.id) {
            *working = Working::default();
        }
        self.orders.forget(tracked.order.id);
    }

    #[inline]
    fn check_flags(&mut self) {
        if self.channels.halt_flag.load(Ordering::Relaxed) {
            self.halt(HaltReason::Operator);
        }
        if self.channels.journal_failed.load(Ordering::Relaxed) {
            self.halt(HaltReason::JournalWriteFailed);
        }
        if self.link.state_lost() {
            self.halt(HaltReason::VenueStateLost);
        }
    }

    #[inline]
    fn on_event(&mut self, event: &Event) {
        self.time = self.time.max(event.event_time);
        self.counters.events += 1;
        self.last_stamp = Some(event.measure_stamp);
        self.link.on_market(event);

        let venue = usize::from(event.venue.0);
        let instrument = usize::from(event.instrument.0);
        let index = slot_index(venue, instrument);
        match event.payload {
            Payload::BookClear => self.books[index].clear(),
            Payload::BookLevel { side, price, qty } => self.books[index].apply(side, price, qty),
            Payload::VenueStatus(state) => self.slots[index].live = state == VenueState::Live,
            Payload::Trade { .. } => {}
        }

        if event.end_of_batch {
            self.on_batch(venue, instrument);
        }
    }

    #[inline]
    fn on_batch(&mut self, venue: usize, instrument: usize) {
        self.batch += 1;
        self.counters.batches += 1;
        self.decided = false;
        self.poll_reports();
        self.run_script();

        let index = slot_index(venue, instrument);
        if let (Some(params), Some(mid)) = (self.slots[index].params, self.books[index].mid()) {
            self.positions.mark(venue, instrument, mid, &params);
            self.check_drawdown();
            self.update_quotes(venue, instrument, &params, mid);
            self.push_api(venue, instrument, &params);
        }

        if let Some(stamp) = self.last_stamp {
            let elapsed = self.clock.elapsed_ns(stamp, self.clock.measure());
            self.push_telemetry(TelemetryRecord::BatchLatency(elapsed));
        }
        self.maybe_publish_counters();
    }

    #[inline]
    fn check_drawdown(&mut self) {
        if self.positions.drawdown_micro_usd() > self.risk.limits().max_drawdown_micro_usd {
            self.halt(HaltReason::Drawdown);
        }
    }

    fn may_quote(&self, instrument: usize, index: usize) -> bool {
        self.status == EngineStatus::Running
            && !self.instrument_halted[instrument]
            && self.slots[index].live
            && self.link.is_up()
    }

    #[inline]
    fn update_quotes(&mut self, venue: usize, instrument: usize, params: &SlotParams, mid: Price) {
        let index = slot_index(venue, instrument);
        if !self.may_quote(instrument, index) {
            self.cancel_slot(index);
            return;
        }

        let position = self.positions.get(venue, instrument).qty;
        let view = MarketView {
            venue: VenueId(venue as u8),
            instrument: InstrumentId(instrument as u16),
            book: &self.books[index],
            position,
            params,
        };
        let quote = self.strategy.quote(&view);
        self.work_side(venue, instrument, Side::Buy, quote.bid, params, mid);
        self.work_side(venue, instrument, Side::Sell, quote.ask, params, mid);
    }

    #[inline]
    fn work_side(
        &mut self,
        venue: usize,
        instrument: usize,
        side: Side,
        desired: Option<Price>,
        params: &SlotParams,
        mid: Price,
    ) {
        let index = slot_index(venue, instrument);
        let working = *self.slots[index].working(side);

        if let Some(id) = working.id {
            let threshold = i128::from(self.strategy.requote_threshold_centi_bps());
            let stale = desired.is_none_or(|price| {
                i128::from((price.0 - working.price.0).abs()) * CENTI_BPS
                    > threshold * i128::from(mid.0)
            });
            if stale {
                self.send_cancel(id);
            }
            return;
        }
        if let Some(price) = desired {
            self.place(venue, instrument, side, price, params, mid);
        }
    }

    fn exposure(&mut self, venue: usize, instrument: usize) -> Exposure {
        let slot = &self.slots[slot_index(venue, instrument)];
        let working_price = |working: &Working| working.id.map(|_| Price(working.price.0));
        Exposure {
            position: self.positions.get(venue, instrument).qty,
            open_buy: slot.open_buy,
            open_sell: slot.open_sell,
            lowest_own_ask: working_price(&slot.ask),
            highest_own_bid: working_price(&slot.bid),
        }
    }

    #[inline]
    fn place(
        &mut self,
        venue: usize,
        instrument: usize,
        side: Side,
        price: Price,
        params: &SlotParams,
        mid: Price,
    ) {
        let Some(id) = self.orders.next_id() else {
            self.reject_without_id(venue, instrument, side, price, params);
            return;
        };
        let order = NewOrder {
            id,
            venue: VenueId(venue as u8),
            instrument: InstrumentId(instrument as u16),
            side,
            price,
            qty: Qty(params.order_qty),
            time_in_force: TimeInForce::PostOnly,
        };

        let exposure = self.exposure(venue, instrument);
        let verdict = self.risk.check(&order, &exposure, mid, params, self.time);
        self.record_decision();
        if let Err(reason) = verdict {
            self.on_risk_rejection(order, reason);
            return;
        }

        if !self
            .link
            .send(OrderCommand::New(order), self.time, self.stamp())
        {
            self.on_order_path_blocked(order);
            return;
        }
        self.blocked_orders = 0;
        self.orders.track(order);
        let index = slot_index(venue, instrument);
        let slot = &mut self.slots[index];
        *slot.working(side) = Working {
            id: Some(id),
            price: Price2(price.0),
        };
        match side {
            Side::Buy => slot.open_buy += order.qty.0,
            Side::Sell => slot.open_sell += order.qty.0,
        }
        self.counters.orders_sent += 1;
        self.journal(JournalBody::Sent(OrderCommand::New(order)));
    }

    #[cold]
    fn reject_without_id(
        &mut self,
        venue: usize,
        instrument: usize,
        side: Side,
        price: Price,
        params: &SlotParams,
    ) {
        let order = NewOrder {
            id: ClientOrderId::new(0, 0),
            venue: VenueId(venue as u8),
            instrument: InstrumentId(instrument as u16),
            side,
            price,
            qty: Qty(params.order_qty),
            time_in_force: TimeInForce::PostOnly,
        };
        self.on_risk_rejection(order, RiskRejection::OrderTableFull);
    }

    #[inline]
    fn record_decision(&mut self) {
        if !self.decided
            && let Some(stamp) = self.last_stamp
        {
            self.decided = true;
            let elapsed = self.clock.elapsed_ns(stamp, self.clock.measure());
            self.push_telemetry(TelemetryRecord::TickToDecision(elapsed));
        }
    }

    #[cold]
    fn on_risk_rejection(&mut self, order: NewOrder, reason: RiskRejection) {
        self.counters.risk_rejections += 1;
        self.journal(JournalBody::RiskRejected { order, reason });
    }

    #[cold]
    fn on_order_path_blocked(&mut self, order: NewOrder) {
        self.blocked_orders += 1;
        self.on_risk_rejection(order, RiskRejection::OrderPathFull);
        if self.blocked_orders >= MAX_BLOCKED_ORDERS {
            self.halt(HaltReason::OrderPathBlocked);
        }
    }

    #[inline]
    fn send_cancel(&mut self, id: ClientOrderId) {
        let Some(tracked) = self.orders.get(id).copied() else {
            return;
        };
        if tracked.state == OrderState::PendingCancel || !self.orders.begin_cancel(id) {
            return;
        }
        let command = OrderCommand::Cancel(CancelOrder {
            id,
            venue: tracked.order.venue,
            instrument: tracked.order.instrument,
        });
        if self.link.send(command, self.time, self.stamp()) {
            self.counters.cancels_sent += 1;
            self.journal(JournalBody::Sent(command));
        }
    }

    fn cancel_slot(&mut self, index: usize) {
        for id in [self.slots[index].bid.id, self.slots[index].ask.id]
            .into_iter()
            .flatten()
        {
            self.send_cancel(id);
        }
    }

    fn cancel_all(&mut self) {
        for venue in 0..self.venues {
            for instrument in 0..self.instruments {
                self.cancel_slot(slot_index(venue, instrument));
            }
        }
    }

    #[inline]
    fn poll_reports(&mut self) -> bool {
        let Self { link, reports, .. } = self;
        link.poll(self.time, |report| reports.push(report));
        if self.reports.is_empty() {
            return false;
        }
        let reconciling = self.status == EngineStatus::AwaitingReconcile;
        for position in 0..self.reports.len() {
            let report = self.reports[position];
            if reconciling {
                self.apply_retransmitted(&report);
            } else {
                self.on_report(&report);
            }
        }
        self.reports.clear();
        true
    }

    #[inline]
    fn on_report(&mut self, report: &ExecReport) {
        let tracked = self.orders.get(report.id).copied();
        let outcome = self.orders.on_report(report);
        let ReportOutcome::Applied { fill, closed } = outcome else {
            return;
        };
        self.journal(JournalBody::Report(*report));
        if matches!(report.kind, ExecKind::Reject(_)) {
            self.counters.venue_rejections += 1;
        }

        let venue = usize::from(report.venue.0);
        let instrument = usize::from(report.instrument.0);
        let index = slot_index(venue, instrument);
        let Some(tracked) = tracked else {
            return;
        };
        let side = tracked.order.side;

        if let Some((_, price, qty, fee)) = fill {
            self.counters.fills += 1;
            self.positions
                .apply_fill(venue, instrument, side, price, qty, fee);
            let slot = &mut self.slots[index];
            match side {
                Side::Buy => slot.open_buy -= qty.0,
                Side::Sell => slot.open_sell -= qty.0,
            }
            if let (Some(params), Some(mid)) = (self.slots[index].params, self.books[index].mid()) {
                self.positions.mark(venue, instrument, mid, &params);
                self.check_drawdown();
            }
        }

        if closed {
            let unfilled = tracked.remaining() - fill.map_or(0, |(_, _, qty, _)| qty.0);
            let slot = &mut self.slots[index];
            match side {
                Side::Buy => slot.open_buy -= unfilled,
                Side::Sell => slot.open_sell -= unfilled,
            }
            let working = slot.working(side);
            if working.id == Some(report.id) {
                *working = Working::default();
            }
        }
    }

    #[inline]
    fn poll_control(&mut self) {
        while let Some(command) = self.channels.control.pop() {
            self.apply_control(command);
        }
    }

    #[inline]
    fn run_script(&mut self) {
        while let Some(&(batch, command)) = self.script.get(self.script_position) {
            if batch > self.batch {
                break;
            }
            self.script_position += 1;
            self.apply_control(command);
        }
    }

    fn apply_control(&mut self, command: ControlCommand) {
        self.journal(JournalBody::Control(command));
        match command {
            ControlCommand::SetParam(change) => self.strategy.set_param(change),
            ControlCommand::HaltInstrument(instrument) => {
                let instrument = usize::from(instrument.0);
                if let Some(halted) = self.instrument_halted.get_mut(instrument) {
                    *halted = true;
                }
                for venue in 0..self.venues {
                    self.cancel_slot(slot_index(venue, instrument));
                }
            }
            ControlCommand::ResumeInstrument(instrument) => {
                if let Some(halted) = self.instrument_halted.get_mut(usize::from(instrument.0)) {
                    *halted = false;
                }
            }
            ControlCommand::GlobalHalt => self.halt(HaltReason::Operator),
        }
    }

    /// Enters a global halt. The first reason wins and lasts until restart.
    #[cold]
    pub fn halt(&mut self, reason: HaltReason) {
        if matches!(self.status, EngineStatus::Halted(_)) {
            return;
        }
        self.status = EngineStatus::Halted(reason);
        self.journal(JournalBody::Halted(reason));
        self.cancel_all();
    }

    /// Stops quoting until [`Engine::finish_reconcile`] is called.
    pub fn begin_reconcile(&mut self) {
        self.status = EngineStatus::AwaitingReconcile;
    }

    /// Ends reconciliation, resuming quoting or halting.
    pub fn finish_reconcile(&mut self, succeeded: bool) {
        if succeeded {
            if self.status == EngineStatus::AwaitingReconcile {
                self.status = EngineStatus::Running;
            }
        } else {
            self.status = EngineStatus::AwaitingReconcile;
            self.halt(HaltReason::ReconcileFailed);
        }
    }

    #[inline]
    fn journal(&mut self, body: JournalBody) {
        let free = self.channels.journal.free_slots();
        if free <= JOURNAL_RESERVE && !matches!(self.status, EngineStatus::Halted(_)) {
            self.status = EngineStatus::Halted(HaltReason::JournalBackpressure);
            self.push_journal(JournalBody::Halted(HaltReason::JournalBackpressure));
            self.push_journal(body);
            self.cancel_all();
            return;
        }
        self.push_journal(body);
    }

    #[inline]
    fn push_journal(&mut self, body: JournalBody) {
        let record = JournalRecord {
            batch: self.batch,
            time: self.time,
            body,
        };
        // The reserve keeps room for a halt and its cancels; beyond that the
        // engine stops producing records.
        let _ = self.channels.journal.push(record);
    }

    #[inline]
    fn push_telemetry(&mut self, record: TelemetryRecord) {
        if self.channels.telemetry.push(record).is_err() {
            self.counters.telemetry_drops += 1;
        }
    }

    #[inline]
    fn maybe_publish_counters(&mut self) {
        if self.time.0 >= self.next_counters {
            self.next_counters = self.time.0 + COUNTER_PERIOD_NS;
            self.publish_counters();
        }
    }

    fn publish_counters(&mut self) {
        self.counters.status = self.status;
        self.counters.equity_micro_usd = self.positions.equity_micro_usd();
        self.counters.drawdown_micro_usd = self.positions.drawdown_micro_usd();
        let mut recentres = 0;
        for (slot, book) in self.slots.iter_mut().zip(self.books.iter()) {
            slot.recentres = book.stats().recentres;
            recentres += slot.recentres;
        }
        self.counters.book_recentres = recentres;
        self.push_telemetry(TelemetryRecord::Counters(self.counters));
    }

    #[inline]
    fn push_api(&mut self, venue: usize, instrument: usize, _params: &SlotParams) {
        let index = slot_index(venue, instrument);
        if self.time.0 < self.slots[index].next_api {
            return;
        }
        self.slots[index].next_api = self.time.0 + API_PERIOD_NS;

        let copy = |book: &Book, side| {
            let mut levels = [(Price(0), Qty::ZERO); API_DEPTH];
            for (level, entry) in book.levels(side).take(API_DEPTH).zip(levels.iter_mut()) {
                *entry = level;
            }
            levels
        };
        let book = &self.books[index];
        let update = ApiUpdate::Book(ApiBook {
            venue: VenueId(venue as u8),
            instrument: InstrumentId(instrument as u16),
            bids: copy(book, Side::Buy),
            asks: copy(book, Side::Sell),
        });
        if self.channels.api.push(update).is_err() {
            self.counters.api_drops += 1;
        }

        let position = self.positions.get(venue, instrument);
        let update = ApiUpdate::Position(ApiPosition {
            venue: VenueId(venue as u8),
            instrument: InstrumentId(instrument as u16),
            qty: position.qty,
            equity_micro_usd: position.equity_micro_usd,
            total_equity_micro_usd: self.positions.equity_micro_usd(),
        });
        if self.channels.api.push(update).is_err() {
            self.counters.api_drops += 1;
        }
    }

    /// Rebuilds orders and positions from journal records.
    pub fn recover(&mut self, records: &[JournalRecord]) {
        for record in records {
            self.batch = self.batch.max(record.batch);
            self.time = self.time.max(record.time);
            match record.body {
                JournalBody::Session(session) => self.recovered_session = session,
                JournalBody::Sent(OrderCommand::New(order)) => self.recover_sent(order),
                JournalBody::Report(report) => self.on_recovered_report(&report),
                JournalBody::Halted(reason) => self.status = EngineStatus::Halted(reason),
                _ => {}
            }
        }
    }

    fn recover_sent(&mut self, order: NewOrder) {
        self.orders.replay_sent(order);
        let index = slot_index(usize::from(order.venue.0), usize::from(order.instrument.0));
        let slot = &mut self.slots[index];
        *slot.working(order.side) = Working {
            id: Some(order.id),
            price: Price2(order.price.0),
        };
        match order.side {
            Side::Buy => slot.open_buy += order.qty.0,
            Side::Sell => slot.open_sell += order.qty.0,
        }
    }

    fn on_recovered_report(&mut self, report: &ExecReport) {
        self.on_report_without_journal(report);
    }

    /// Applies a venue report during recovery, restoring an order the journal
    /// lost from the details the report echoes.
    pub fn apply_retransmitted(&mut self, report: &ExecReport) {
        if self.orders.get(report.id).is_none()
            && !matches!(report.kind, ExecKind::CancelAck | ExecKind::Reject(_))
            && report.order_qty.0 > 0
        {
            self.recover_sent(NewOrder {
                id: report.id,
                venue: report.venue,
                instrument: report.instrument,
                side: report.side,
                price: report.order_price,
                qty: report.order_qty,
                time_in_force: TimeInForce::PostOnly,
            });
        }
        self.on_report(report);
    }

    fn on_report_without_journal(&mut self, report: &ExecReport) {
        let journal_status = self.status;
        // Recovered reports are already in the journal; reading them back
        // must not append them again.
        let tracked = self.orders.get(report.id).copied();
        let outcome = self.orders.on_report(report);
        let ReportOutcome::Applied { fill, closed } = outcome else {
            return;
        };
        let Some(tracked) = tracked else {
            return;
        };
        let venue = usize::from(report.venue.0);
        let instrument = usize::from(report.instrument.0);
        let index = slot_index(venue, instrument);
        let side = tracked.order.side;
        if let Some((_, price, qty, fee)) = fill {
            self.positions
                .apply_fill(venue, instrument, side, price, qty, fee);
            match side {
                Side::Buy => self.slots[index].open_buy -= qty.0,
                Side::Sell => self.slots[index].open_sell -= qty.0,
            }
        }
        if closed {
            let unfilled = tracked.remaining() - fill.map_or(0, |(_, _, qty, _)| qty.0);
            let slot = &mut self.slots[index];
            match side {
                Side::Buy => slot.open_buy -= unfilled,
                Side::Sell => slot.open_sell -= unfilled,
            }
            let working = slot.working(side);
            if working.id == Some(report.id) {
                *working = Working::default();
            }
        }
        self.status = journal_status;
    }

    /// Returns the venue session found while recovering, or zero.
    #[must_use]
    pub const fn recovered_session(&self) -> u64 {
        self.recovered_session
    }

    /// Returns the working order on a side, for reconciliation and tests.
    #[must_use]
    pub fn working_order(
        &mut self,
        venue: usize,
        instrument: usize,
        side: Side,
    ) -> Option<ClientOrderId> {
        self.slots[slot_index(venue, instrument)].working(side).id
    }

    /// Returns the opposite side; exposed for strategies and tests.
    #[must_use]
    pub const fn opposite_side(side: Side) -> Side {
        opposite(side)
    }
}

#[cfg(test)]
mod tests;
