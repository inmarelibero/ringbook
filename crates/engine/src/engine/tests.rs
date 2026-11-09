use super::*;
use crate::config::{EngineParams, SlotParams};
use crate::link::InProcess;
use crate::risk::RiskLimits;
use crate::strategy::{MarketMaker, StrategyParams};
use ringbook_core::{ParamChange, ParamKey};
use ringbook_paper_exchange::{ExchangeConfig, PaperExchange, ProbabilisticQueue, VenueSimulation};
use ringbook_ring::channel;

pub(crate) type TestEngine = Engine<MarketMaker, InProcess<ProbabilisticQueue>>;

pub(crate) struct Harness {
    pub engine: TestEngine,
    pub market: Producer<Event>,
    pub control: Producer<ControlCommand>,
    pub journal: Consumer<JournalRecord>,
    pub telemetry: Consumer<TelemetryRecord>,
    pub api: Consumer<ApiUpdate>,
    pub halt_flag: Arc<AtomicBool>,
    pub journal_failed: Arc<AtomicBool>,
}

const SLOT: SlotParams = SlotParams {
    tick: 1,
    price_decimals: 1,
    qty_decimals: 3,
    max_significant_figures: None,
    order_qty: 2,
    max_order_qty: 5,
    max_position: 4,
};

pub(crate) fn params() -> EngineParams {
    let mut slots = [[None; MAX_INSTRUMENTS]; MAX_VENUES];
    slots[0][0] = Some(SLOT);
    EngineParams {
        venues: 1,
        instruments: 1,
        book_levels: 1 << 12,
        slots,
        strategy: StrategyParams {
            spread_centi_bps: 400,
            inventory_skew_centi_bps: 0,
            requote_threshold_centi_bps: 100,
        },
        risk: RiskLimits {
            max_notional_micro_usd: 1_000_000_000_000,
            max_orders_per_sec: 1_000,
            price_collar_centi_bps: 50_000,
            max_drawdown_micro_usd: 1_000_000,
        },
    }
}

pub(crate) fn harness_with(params: &EngineParams, journal_capacity: usize) -> Harness {
    let exchange = PaperExchange::new(
        &ExchangeConfig {
            venues: vec![VenueSimulation {
                maker_fee_centi_bps: 0,
                taker_fee_centi_bps: 0,
                order_latency_ns: 10,
                ack_latency_ns: 10,
            }],
            ticks: vec![vec![1]],
            book_levels: 1 << 12,
            max_resting: 8,
            max_in_flight: 64,
        },
        ProbabilisticQueue { power: 1 },
    );

    let (market, input) = channel(1 << 12).unwrap();
    let (control, control_rx) = channel(16).unwrap();
    let (journal_tx, journal) = channel(journal_capacity).unwrap();
    let (telemetry_tx, telemetry) = channel(1 << 12).unwrap();
    let (api_tx, api) = channel(16).unwrap();
    let halt_flag = Arc::new(AtomicBool::new(false));
    let journal_failed = Arc::new(AtomicBool::new(false));

    let engine = Engine::new(
        params,
        MarketMaker::new(params.strategy),
        InProcess::new(exchange),
        EngineChannels {
            inputs: vec![input],
            control: control_rx,
            journal: journal_tx,
            telemetry: telemetry_tx,
            api: api_tx,
            halt_flag: Arc::clone(&halt_flag),
            journal_failed: Arc::clone(&journal_failed),
            inputs_done: Arc::new(AtomicBool::new(false)),
        },
        Clock::new(),
        1,
    );
    Harness {
        engine,
        market,
        control,
        journal,
        telemetry,
        api,
        halt_flag,
        journal_failed,
    }
}

pub(crate) fn harness() -> Harness {
    harness_with(&params(), 1 << 12)
}

pub(crate) fn event(time: u64, end_of_batch: bool, payload: Payload) -> Event {
    Event {
        event_time: EventTime(time),
        measure_stamp: MeasureStamp(0),
        exchange_time: None,
        instrument: InstrumentId(0),
        venue: VenueId(0),
        end_of_batch,
        payload,
    }
}

impl Harness {
    pub fn feed(&mut self, event: Event) {
        self.market.push(event).unwrap();
        while self.engine.step() {}
    }

    pub fn live_market(&mut self, time: u64, bid: i64, ask: i64) {
        self.feed(event(time, false, Payload::VenueStatus(VenueState::Live)));
        self.feed(event(time, false, Payload::BookClear));
        self.feed(event(
            time,
            false,
            Payload::BookLevel {
                side: Side::Buy,
                price: Price(bid),
                qty: Qty(10),
            },
        ));
        self.feed(event(
            time,
            true,
            Payload::BookLevel {
                side: Side::Sell,
                price: Price(ask),
                qty: Qty(10),
            },
        ));
    }

    pub fn trade(&mut self, time: u64, aggressor: Side, price: i64, qty: i64) {
        self.feed(event(
            time,
            true,
            Payload::Trade {
                aggressor,
                price: Price(price),
                qty: Qty(qty),
            },
        ));
    }

    pub fn records(&mut self) -> Vec<JournalBody> {
        let mut records = Vec::new();
        while let Some(record) = self.journal.pop() {
            records.push(record.body);
        }
        records
    }

    pub fn sent_orders(&mut self) -> Vec<NewOrder> {
        self.records()
            .into_iter()
            .filter_map(|body| match body {
                JournalBody::Sent(OrderCommand::New(order)) => Some(order),
                _ => None,
            })
            .collect()
    }
}

const BID: i64 = 1_000_000;
const ASK: i64 = 1_000_010;

#[test]
fn a_complete_batch_places_quotes_on_both_sides() {
    let mut harness = harness();
    harness.live_market(100, BID, ASK);
    let orders = harness.sent_orders();
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0].side, Side::Buy);
    assert!(orders[0].price.0 < BID);
    assert_eq!(orders[1].side, Side::Sell);
    assert!(orders[1].price.0 > ASK);
    assert_eq!(orders[0].qty, Qty(2));
}

#[test]
fn nothing_is_quoted_before_a_batch_completes() {
    let mut harness = harness();
    harness.feed(event(100, false, Payload::VenueStatus(VenueState::Live)));
    harness.feed(event(
        100,
        false,
        Payload::BookLevel {
            side: Side::Buy,
            price: Price(BID),
            qty: Qty(10),
        },
    ));
    harness.feed(event(
        100,
        false,
        Payload::BookLevel {
            side: Side::Sell,
            price: Price(ASK),
            qty: Qty(10),
        },
    ));
    assert!(harness.sent_orders().is_empty());
}

#[test]
fn a_venue_that_is_not_live_gets_no_quotes() {
    let mut harness = harness();
    harness.feed(event(
        100,
        true,
        Payload::BookLevel {
            side: Side::Buy,
            price: Price(BID),
            qty: Qty(10),
        },
    ));
    harness.feed(event(
        100,
        true,
        Payload::BookLevel {
            side: Side::Sell,
            price: Price(ASK),
            qty: Qty(10),
        },
    ));
    assert!(harness.sent_orders().is_empty());
}

#[test]
fn quotes_are_pulled_when_the_venue_goes_stale() {
    let mut harness = harness();
    harness.live_market(100, BID, ASK);
    harness.live_market(200, BID, ASK);
    harness.records();

    harness.feed(event(300, true, Payload::VenueStatus(VenueState::Stale)));
    let cancels = harness
        .records()
        .iter()
        .filter(|body| matches!(body, JournalBody::Sent(OrderCommand::Cancel(_))))
        .count();
    assert_eq!(cancels, 2);
}

#[test]
fn small_moves_keep_working_orders_and_large_moves_replace_them() {
    let mut harness = harness();
    harness.live_market(100, BID, ASK);
    harness.live_market(200, BID, ASK);
    harness.records();

    harness.live_market(300, BID + 20, ASK + 20);
    assert!(
        harness
            .records()
            .iter()
            .all(|body| !matches!(body, JournalBody::Sent(_)))
    );

    harness.live_market(400, BID + 2_000, ASK + 2_000);
    let cancels = harness
        .records()
        .iter()
        .filter(|body| matches!(body, JournalBody::Sent(OrderCommand::Cancel(_))))
        .count();
    assert_eq!(cancels, 2);
}

#[test]
fn fills_update_positions_and_clear_the_working_order() {
    let mut harness = harness();
    harness.live_market(100, BID, ASK);
    harness.live_market(200, BID, ASK);

    // Quotes rest away from the touch, so a trade through them fills fully.
    harness.trade(300, Side::Sell, BID - 1_000, 2);
    harness.live_market(400, BID, ASK);

    assert_eq!(harness.engine.positions().get(0, 0).qty, 2);
    assert_eq!(harness.engine.counters().fills, 1);
}

#[test]
fn positions_at_the_limit_stop_new_bids() {
    let mut harness = harness();
    harness.live_market(100, BID, ASK);
    harness.live_market(200, BID, ASK);
    harness.trade(300, Side::Sell, BID - 1_000, 2);
    harness.live_market(400, BID, ASK);
    harness.trade(500, Side::Sell, BID - 1_000, 2);
    harness.live_market(600, BID, ASK);
    harness.records();

    harness.live_market(700, BID, ASK);
    assert_eq!(harness.engine.positions().get(0, 0).qty, 4);
    assert!(harness.engine.working_order(0, 0, Side::Buy).is_none());
}

#[test]
fn the_halt_flag_cancels_quotes_and_stops_quoting() {
    let mut harness = harness();
    harness.live_market(100, BID, ASK);
    harness.live_market(200, BID, ASK);
    harness.records();

    harness.halt_flag.store(true, Ordering::Relaxed);
    harness.engine.step();
    assert_eq!(
        harness.engine.status(),
        EngineStatus::Halted(HaltReason::Operator)
    );

    let records = harness.records();
    assert!(records.contains(&JournalBody::Halted(HaltReason::Operator)));
    assert!(
        records
            .iter()
            .any(|body| matches!(body, JournalBody::Sent(OrderCommand::Cancel(_))))
    );

    harness.live_market(300, BID, ASK);
    assert!(harness.sent_orders().is_empty());
}

#[test]
fn a_breached_drawdown_halts_the_engine() {
    let mut params = params();
    params.risk.max_drawdown_micro_usd = 1;
    let mut harness = harness_with(&params, 1 << 12);
    harness.live_market(100, BID, ASK);
    harness.live_market(200, BID, ASK);
    harness.trade(300, Side::Sell, BID - 1_000, 2);
    harness.live_market(400, BID - 50_000, ASK - 50_000);
    assert_eq!(
        harness.engine.status(),
        EngineStatus::Halted(HaltReason::Drawdown)
    );
}

#[test]
fn a_journal_write_failure_halts_the_engine() {
    let mut harness = harness();
    harness.journal_failed.store(true, Ordering::Relaxed);
    harness.engine.step();
    assert_eq!(
        harness.engine.status(),
        EngineStatus::Halted(HaltReason::JournalWriteFailed)
    );
}

#[test]
fn a_journal_near_capacity_halts_instead_of_losing_records() {
    let mut harness = harness_with(&params(), 512);
    let mut time = 100;
    while !matches!(harness.engine.status(), EngineStatus::Halted(_)) {
        time += 1_000;
        harness
            .control
            .push(ControlCommand::SetParam(ParamChange {
                key: ParamKey::SpreadCentiBps,
                value: 400,
            }))
            .unwrap();
        harness.live_market(time, BID, ASK);
        assert!(time < 100_000_000);
    }
    assert_eq!(
        harness.engine.status(),
        EngineStatus::Halted(HaltReason::JournalBackpressure)
    );
    assert!(
        harness
            .records()
            .contains(&JournalBody::Halted(HaltReason::JournalBackpressure))
    );
}

#[test]
fn control_commands_apply_and_are_journaled() {
    let mut harness = harness();
    let change = ControlCommand::SetParam(ParamChange {
        key: ParamKey::SpreadCentiBps,
        value: 2_000,
    });
    harness.control.push(change).unwrap();
    harness.engine.step();
    assert!(harness.records().contains(&JournalBody::Control(change)));

    harness.live_market(100, BID, ASK);
    let orders = harness.sent_orders();
    assert!(orders[0].price.0 < BID - 900);
}

#[test]
fn an_instrument_halt_pulls_its_quotes_until_resumed() {
    let mut harness = harness();
    harness.live_market(100, BID, ASK);
    harness.live_market(200, BID, ASK);

    harness
        .control
        .push(ControlCommand::HaltInstrument(InstrumentId(0)))
        .unwrap();
    harness.live_market(300, BID, ASK);
    harness.live_market(400, BID, ASK);
    harness.records();
    assert!(harness.engine.working_order(0, 0, Side::Buy).is_none());

    harness.live_market(500, BID, ASK);
    assert!(harness.sent_orders().is_empty());

    harness
        .control
        .push(ControlCommand::ResumeInstrument(InstrumentId(0)))
        .unwrap();
    harness.engine.step();
    harness.live_market(600, BID, ASK);
    assert_eq!(harness.sent_orders().len(), 2);
}

#[test]
fn scripted_commands_apply_at_their_batch() {
    let mut harness = harness();
    harness
        .engine
        .set_script(vec![(2, ControlCommand::GlobalHalt)]);
    harness.live_market(100, BID, ASK);
    assert_eq!(harness.engine.status(), EngineStatus::Running);
    harness.live_market(200, BID, ASK);
    assert_eq!(
        harness.engine.status(),
        EngineStatus::Halted(HaltReason::Operator)
    );
}

#[test]
fn telemetry_receives_latency_samples_and_counters() {
    let mut harness = harness();
    harness.live_market(100, BID, ASK);
    let mut decisions = 0;
    let mut counters = 0;
    while let Some(record) = harness.telemetry.pop() {
        match record {
            TelemetryRecord::TickToDecision(_) => decisions += 1,
            TelemetryRecord::Counters(_) => counters += 1,
            TelemetryRecord::BatchLatency(_) | TelemetryRecord::TickToWire(_) => {}
        }
    }
    assert_eq!(decisions, 1);
    assert!(counters >= 1);
}

#[test]
fn a_full_api_ring_drops_updates_without_blocking() {
    let mut harness = harness();
    for step in 0..64 {
        harness.live_market(step * API_PERIOD_NS, BID, ASK);
    }
    assert!(harness.engine.counters().api_drops > 0);
    assert!(matches!(harness.api.pop(), Some(ApiUpdate::Book(_))));
}

/// Drains the API ring, returning the last book and whether a status was sent.
fn drain_api(harness: &mut Harness) -> (Option<ApiBook>, bool) {
    let mut book = None;
    let mut status = false;
    while let Some(update) = harness.api.pop() {
        match update {
            ApiUpdate::Book(update) => book = Some(update),
            ApiUpdate::Status(_) => status = true,
            ApiUpdate::Position(_) => {}
        }
    }
    (book, status)
}

#[test]
fn the_api_reports_our_quotes_and_feed_departures_from_live() {
    let mut harness = harness();
    // Connecting reports Resyncing, which isn't a departure from Live.
    harness.feed(event(
        50,
        false,
        Payload::VenueStatus(VenueState::Resyncing),
    ));
    harness.live_market(100, BID, ASK);
    let (book, status) = drain_api(&mut harness);
    let book = book.unwrap();
    assert!(status);
    assert_eq!(book.feed, Some(VenueState::Live));
    assert_eq!((book.resyncs, book.stalls), (0, 0));
    assert!(book.own_bid.is_some() && book.own_ask.is_some());

    // A stall, then reconnecting: one stall and no resync.
    harness.feed(event(200, false, Payload::VenueStatus(VenueState::Stale)));
    harness.feed(event(
        300,
        false,
        Payload::VenueStatus(VenueState::Resyncing),
    ));
    harness.live_market(100 + API_PERIOD_NS, BID, ASK);
    let book = drain_api(&mut harness).0.unwrap();
    assert_eq!((book.resyncs, book.stalls), (0, 1));

    // A sequence gap while live: one resync.
    let gap = 200 + API_PERIOD_NS;
    harness.feed(event(
        gap,
        false,
        Payload::VenueStatus(VenueState::Resyncing),
    ));
    harness.live_market(100 + 2 * API_PERIOD_NS, BID, ASK);
    let book = drain_api(&mut harness).0.unwrap();
    assert_eq!((book.resyncs, book.stalls), (1, 1));
}

#[test]
fn recovery_restores_positions_and_open_orders_from_the_journal() {
    let mut original = harness();
    original.live_market(100, BID, ASK);
    original.live_market(200, BID, ASK);
    original.trade(300, Side::Sell, BID - 1_000, 2);
    original.live_market(400, BID, ASK);
    let position = original.engine.positions().get(0, 0).qty;

    let mut records = Vec::new();
    while let Some(record) = original.journal.pop() {
        records.push(record);
    }

    let mut recovered = harness();
    recovered.engine.recover(&records);
    assert_eq!(recovered.engine.positions().get(0, 0).qty, position);
    assert_eq!(
        recovered.engine.orders().last_report(),
        original.engine.orders().last_report()
    );
    assert_eq!(
        recovered.engine.working_order(0, 0, Side::Sell),
        original.engine.working_order(0, 0, Side::Sell)
    );
    assert!(
        recovered.records().is_empty(),
        "recovery must not rewrite the journal"
    );
}

#[test]
fn retransmitted_reports_restore_orders_the_journal_lost() {
    let mut harness = harness();
    let id = ClientOrderId::new(1, 7);
    let report = ExecReport {
        sequence: 1,
        time: EventTime(5),
        id,
        venue: VenueId(0),
        instrument: InstrumentId(0),
        side: Side::Buy,
        order_price: Price(BID),
        order_qty: Qty(2),
        kind: ExecKind::Fill {
            price: Price(BID),
            qty: Qty(1),
            fee: 0,
            liquidity: ringbook_core::Liquidity::Maker,
        },
    };
    harness.engine.apply_retransmitted(&report);
    assert_eq!(harness.engine.positions().get(0, 0).qty, 1);
    assert_eq!(harness.engine.orders().get(id).unwrap().filled, 1);

    harness.engine.apply_retransmitted(&report);
    assert_eq!(harness.engine.positions().get(0, 0).qty, 1);
}

fn scripted_events() -> Vec<Event> {
    let mut events = Vec::new();
    for round in 0..200_i64 {
        let time = 1_000 + round as u64 * 1_000;
        let mid = BID + round % 40 * 100;
        events.push(event(time, false, Payload::VenueStatus(VenueState::Live)));
        events.push(event(time, false, Payload::BookClear));
        events.push(event(
            time,
            false,
            Payload::BookLevel {
                side: Side::Buy,
                price: Price(mid),
                qty: Qty(10),
            },
        ));
        events.push(event(
            time,
            true,
            Payload::BookLevel {
                side: Side::Sell,
                price: Price(mid + 10),
                qty: Qty(10),
            },
        ));
        events.push(event(
            time + 1,
            true,
            Payload::Trade {
                aggressor: if round % 2 == 0 {
                    Side::Sell
                } else {
                    Side::Buy
                },
                price: Price(if round % 2 == 0 {
                    mid - 2_000
                } else {
                    mid + 2_000
                }),
                qty: Qty(1),
            },
        ));
    }
    events
}

fn drain_journal(harness: &mut Harness) -> Vec<JournalRecord> {
    let mut records = Vec::new();
    while let Some(record) = harness.journal.pop() {
        records.push(record);
    }
    records
}

#[test]
fn delivery_timing_never_changes_the_journal() {
    // One event per pass exposes every point where a ring runs empty mid-batch.
    let mut drip = harness();
    for item in scripted_events() {
        drip.market.push(item).unwrap();
        drip.engine.step();
        drip.engine.step();
    }

    // Whole batches at once never run empty mid-batch.
    let mut bulk = harness();
    for chunk in scripted_events().chunks(16) {
        for item in chunk {
            bulk.market.push(*item).unwrap();
        }
        while bulk.engine.step() {}
    }

    let (first, second) = (drain_journal(&mut drip), drain_journal(&mut bulk));
    assert!(!first.is_empty());
    assert_eq!(first, second);
}

#[test]
fn control_commands_wait_for_the_end_of_a_batch() {
    let mut harness = harness();
    harness.feed(event(100, false, Payload::VenueStatus(VenueState::Live)));
    let command = ControlCommand::HaltInstrument(InstrumentId(0));
    harness.control.push(command).unwrap();
    harness.engine.step();
    assert!(harness.records().is_empty());

    harness.feed(event(100, true, Payload::BookClear));
    assert!(harness.records().contains(&JournalBody::Control(command)));
}

#[test]
fn reconciliation_gates_quoting() {
    let mut harness = harness();
    harness.engine.begin_reconcile();
    harness.live_market(100, BID, ASK);
    assert!(harness.sent_orders().is_empty());

    harness.engine.finish_reconcile(true);
    harness.live_market(200, BID, ASK);
    assert_eq!(harness.sent_orders().len(), 2);

    harness.engine.finish_reconcile(false);
    assert_eq!(
        harness.engine.status(),
        EngineStatus::Halted(HaltReason::ReconcileFailed)
    );
}
