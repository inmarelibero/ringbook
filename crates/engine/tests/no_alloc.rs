//! Proves that the whole hot path never touches the heap after warm-up.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ringbook_clock::Clock;
use ringbook_core::{
    Event, EventTime, InstrumentId, MeasureStamp, Payload, Price, Qty, Side, VenueId, VenueState,
};
use ringbook_engine::config::{EngineParams, MAX_INSTRUMENTS, MAX_VENUES, SlotParams};
use ringbook_engine::risk::RiskLimits;
use ringbook_engine::{Engine, EngineChannels, InProcess, MarketMaker, StrategyParams};
use ringbook_paper_exchange::{ExchangeConfig, PaperExchange, ProbabilisticQueue, VenueSimulation};
use ringbook_ring::channel;
use ringbook_testkit::{CountingAllocator, count_allocations};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn event(time: u64, end_of_batch: bool, payload: Payload) -> Event {
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

#[test]
fn quoting_fills_and_cancels_never_allocate() {
    let mut slots = [[None; MAX_INSTRUMENTS]; MAX_VENUES];
    slots[0][0] = Some(SlotParams {
        tick: 1,
        price_decimals: 1,
        qty_decimals: 3,
        max_significant_figures: None,
        order_qty: 1,
        max_order_qty: 5,
        max_position: 1_000_000,
    });
    let strategy = StrategyParams {
        spread_centi_bps: 200,
        inventory_skew_centi_bps: 50,
        requote_threshold_centi_bps: 10,
    };
    let params = EngineParams {
        venues: 1,
        instruments: 1,
        book_levels: 1 << 12,
        slots,
        strategy,
        risk: RiskLimits {
            max_notional_micro_usd: i64::MAX,
            max_orders_per_sec: u32::MAX,
            price_collar_centi_bps: 1_000_000,
            max_drawdown_micro_usd: i64::MAX,
        },
    };
    let exchange = PaperExchange::new(
        &ExchangeConfig {
            venues: vec![VenueSimulation {
                maker_fee_centi_bps: 10,
                taker_fee_centi_bps: 40,
                order_latency_ns: 10,
                ack_latency_ns: 10,
            }],
            ticks: vec![vec![1]],
            book_levels: 1 << 12,
            max_resting: 8,
            max_in_flight: 64,
        },
        ProbabilisticQueue { power: 2 },
    );

    let (mut market, input) = channel(1 << 10).unwrap();
    let (_control, control_rx) = channel(16).unwrap();
    let (journal_tx, mut journal) = channel(1 << 14).unwrap();
    let (telemetry_tx, mut telemetry) = channel(1 << 14).unwrap();
    let (api_tx, mut api) = channel(16).unwrap();
    let mut engine = Engine::new(
        &params,
        MarketMaker::new(strategy),
        InProcess::new(exchange),
        EngineChannels {
            inputs: vec![input],
            control: control_rx,
            journal: journal_tx,
            telemetry: telemetry_tx,
            api: api_tx,
            halt_flag: Arc::new(AtomicBool::new(false)),
            journal_failed: Arc::new(AtomicBool::new(false)),
            inputs_done: Arc::new(AtomicBool::new(false)),
        },
        Clock::new(),
        1,
    );

    let mut time = 1_000;
    let mut round = |engine: &mut Engine<_, _>, step: i64| {
        time += 100_000_000;
        let mid = 1_000_000 + (step % 400) * 5;
        let level = |side, price| Payload::BookLevel {
            side,
            price: Price(price),
            qty: Qty(10 + step % 3),
        };
        for item in [
            event(time, false, Payload::VenueStatus(VenueState::Live)),
            event(time, false, Payload::BookClear),
            event(time, false, level(Side::Buy, mid - 1)),
            event(time, true, level(Side::Sell, mid + 1)),
            event(
                time + 1,
                true,
                Payload::Trade {
                    aggressor: if step % 2 == 0 { Side::Sell } else { Side::Buy },
                    price: Price(if step % 2 == 0 { mid - 500 } else { mid + 500 }),
                    qty: Qty(2),
                },
            ),
        ] {
            assert!(market.push(item).is_ok());
            while engine.step() {}
        }
        while journal.pop().is_some() {}
        while telemetry.pop().is_some() {}
        while api.pop().is_some() {}
    };

    // Warm-up reaches every buffer's working size before counting starts.
    for step in 0..1_000 {
        round(&mut engine, step);
    }
    assert!(engine.counters().fills > 0);
    let fills_before = engine.counters().fills;

    let allocations = count_allocations(|| {
        for step in 1_000..20_000 {
            round(&mut engine, step);
        }
    });

    assert!(engine.counters().fills > fills_before);
    assert_eq!(allocations, 0);
}
