use super::*;
use proptest::prelude::*;
use ringbook_core::MeasureStamp;

const VENUE: VenueId = VenueId(0);
const INSTRUMENT: InstrumentId = InstrumentId(0);
const ORDER_LATENCY: u64 = 10;
const ACK_LATENCY: u64 = 5;

fn exchange() -> PaperExchange<ProbabilisticQueue> {
    PaperExchange::new(
        &ExchangeConfig {
            venues: vec![VenueSimulation {
                maker_fee_centi_bps: 100,
                taker_fee_centi_bps: 500,
                order_latency_ns: ORDER_LATENCY,
                ack_latency_ns: ACK_LATENCY,
            }],
            ticks: vec![vec![1]],
            book_levels: 4096,
            max_resting: 4,
            max_in_flight: 4,
        },
        ProbabilisticQueue { power: 1 },
    )
}

fn event(time: u64, end_of_batch: bool, payload: Payload) -> Event {
    Event {
        event_time: EventTime(time),
        measure_stamp: MeasureStamp(0),
        exchange_time: None,
        instrument: INSTRUMENT,
        venue: VENUE,
        end_of_batch,
        payload,
    }
}

fn level(time: u64, side: Side, price: i64, qty: i64) -> Event {
    event(
        time,
        true,
        Payload::BookLevel {
            side,
            price: Price(price),
            qty: Qty(qty),
        },
    )
}

fn trade(time: u64, aggressor: Side, price: i64, qty: i64) -> Event {
    event(
        time,
        true,
        Payload::Trade {
            aggressor,
            price: Price(price),
            qty: Qty(qty),
        },
    )
}

fn order(id: u64, side: Side, price: i64, qty: i64, time_in_force: TimeInForce) -> NewOrder {
    NewOrder {
        id: ClientOrderId::new(1, id),
        venue: VENUE,
        instrument: INSTRUMENT,
        side,
        price: Price(price),
        qty: Qty(qty),
        time_in_force,
    }
}

fn with_market() -> PaperExchange<ProbabilisticQueue> {
    let mut exchange = exchange();
    exchange.on_market(&level(1, Side::Buy, 1000, 10));
    exchange.on_market(&level(2, Side::Sell, 1010, 10));
    exchange
}

fn drain(exchange: &mut PaperExchange<ProbabilisticQueue>, now: u64) -> Vec<ExecReport> {
    let mut reports = Vec::new();
    exchange.poll(EventTime(now), |report| reports.push(report));
    reports
}

fn place(exchange: &mut PaperExchange<ProbabilisticQueue>, new: NewOrder, now: u64) {
    exchange.submit(OrderCommand::New(new), EventTime(now));
}

fn resting_bid(exchange: &mut PaperExchange<ProbabilisticQueue>) -> u64 {
    place(
        exchange,
        order(1, Side::Buy, 1000, 5, TimeInForce::PostOnly),
        10,
    );
    exchange.advance(EventTime(20));
    let reports = drain(exchange, 25);
    assert_eq!(reports[0].kind, ExecKind::Ack);
    30
}

fn fills(reports: &[ExecReport]) -> Vec<i64> {
    reports
        .iter()
        .filter_map(|report| match report.kind {
            ExecKind::Fill { qty, .. } => Some(qty.0),
            _ => None,
        })
        .collect()
}

#[test]
fn orders_take_effect_only_after_the_order_latency() {
    let mut exchange = with_market();
    place(
        &mut exchange,
        order(1, Side::Buy, 1000, 5, TimeInForce::PostOnly),
        10,
    );

    exchange.advance(EventTime(19));
    assert!(drain(&mut exchange, 100).is_empty());

    exchange.advance(EventTime(20));
    assert!(drain(&mut exchange, 24).is_empty());
    let reports = drain(&mut exchange, 25);
    assert_eq!(reports[0].kind, ExecKind::Ack);
    assert_eq!(reports[0].time, EventTime(25));
    assert_eq!(reports[0].order_qty, Qty(5));
}

#[test]
fn report_sequences_are_contiguous_from_one() {
    let mut exchange = with_market();
    place(
        &mut exchange,
        order(1, Side::Buy, 1000, 5, TimeInForce::PostOnly),
        10,
    );
    place(
        &mut exchange,
        order(2, Side::Buy, 999, 5, TimeInForce::PostOnly),
        10,
    );
    exchange.advance(EventTime(20));
    let sequences: Vec<_> = drain(&mut exchange, 25)
        .iter()
        .map(|r| r.sequence)
        .collect();
    assert_eq!(sequences, [1, 2]);
}

#[test]
fn post_only_orders_that_would_cross_are_rejected() {
    let mut exchange = with_market();
    place(
        &mut exchange,
        order(1, Side::Buy, 1010, 5, TimeInForce::PostOnly),
        10,
    );
    exchange.advance(EventTime(20));
    assert_eq!(
        drain(&mut exchange, 25)[0].kind,
        ExecKind::Reject(RejectReason::PostOnlyCross)
    );
}

#[test]
fn an_empty_market_rejects_orders() {
    let mut exchange = exchange();
    place(
        &mut exchange,
        order(1, Side::Buy, 1000, 5, TimeInForce::PostOnly),
        10,
    );
    exchange.advance(EventTime(20));
    assert_eq!(
        drain(&mut exchange, 25)[0].kind,
        ExecKind::Reject(RejectReason::NoMarket)
    );
}

#[test]
fn passive_orders_fill_only_after_the_volume_ahead_trades() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);

    exchange.on_market(&trade(now, Side::Sell, 1000, 8));
    assert!(fills(&drain(&mut exchange, now + 100)).is_empty());

    exchange.on_market(&trade(now + 1, Side::Sell, 1000, 4));
    let reports = drain(&mut exchange, now + 100);
    assert_eq!(fills(&reports), [2]);
    assert!(matches!(
        reports[0].kind,
        ExecKind::Fill {
            liquidity: Liquidity::Maker,
            ..
        }
    ));
}

#[test]
fn touching_the_price_without_volume_does_not_fill() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    exchange.on_market(&level(now, Side::Sell, 1001, 1));
    assert!(fills(&drain(&mut exchange, now + 100)).is_empty());
}

#[test]
fn trades_on_our_own_side_do_not_fill_us() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    exchange.on_market(&trade(now, Side::Buy, 1000, 100));
    assert!(fills(&drain(&mut exchange, now + 100)).is_empty());
}

#[test]
fn trading_through_our_price_ignores_the_queue() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    exchange.on_market(&trade(now, Side::Sell, 999, 3));
    assert_eq!(fills(&drain(&mut exchange, now + 100)), [3]);
}

#[test]
fn unexplained_decreases_advance_the_queue_by_the_model() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    // Nothing is behind us, so the whole cancel must have been ahead.
    exchange.on_market(&level(now, Side::Buy, 1000, 4));
    exchange.on_market(&trade(now + 1, Side::Sell, 1000, 6));
    assert_eq!(fills(&drain(&mut exchange, now + 100)), [2]);
}

#[test]
fn decreases_explained_by_trades_are_not_counted_twice() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    exchange.on_market(&trade(now, Side::Sell, 1000, 6));
    exchange.on_market(&level(now + 1, Side::Buy, 1000, 4));
    // Six of the original ten traded, so four remain ahead.
    exchange.on_market(&trade(now + 2, Side::Sell, 1000, 4));
    assert!(fills(&drain(&mut exchange, now + 100)).is_empty());
    exchange.on_market(&trade(now + 3, Side::Sell, 1000, 1));
    assert_eq!(fills(&drain(&mut exchange, now + 200)), [1]);
}

#[test]
fn immediate_orders_walk_the_book_and_expire_the_remainder() {
    let mut exchange = exchange();
    exchange.on_market(&level(1, Side::Sell, 1010, 2));
    exchange.on_market(&level(2, Side::Sell, 1020, 3));
    place(
        &mut exchange,
        order(1, Side::Buy, 1020, 9, TimeInForce::ImmediateOrCancel),
        10,
    );
    exchange.advance(EventTime(20));

    let reports = drain(&mut exchange, 25);
    let kinds: Vec<_> = reports.iter().map(|report| report.kind).collect();
    assert_eq!(kinds[0], ExecKind::Ack);
    assert_eq!(
        kinds[1],
        ExecKind::Fill {
            price: Price(1010),
            qty: Qty(2),
            fee: 1,
            liquidity: Liquidity::Taker,
        }
    );
    assert!(matches!(
        kinds[2],
        ExecKind::Fill {
            price: Price(1020),
            qty: Qty(3),
            ..
        }
    ));
    assert_eq!(kinds[3], ExecKind::Expired);
    assert_eq!(exchange.position(VENUE, INSTRUMENT), 5);
}

#[test]
fn immediate_orders_respect_their_limit() {
    let mut exchange = exchange();
    exchange.on_market(&level(1, Side::Sell, 1010, 2));
    exchange.on_market(&level(2, Side::Sell, 1020, 3));
    place(
        &mut exchange,
        order(1, Side::Buy, 1010, 9, TimeInForce::ImmediateOrCancel),
        10,
    );
    exchange.advance(EventTime(20));
    assert_eq!(fills(&drain(&mut exchange, 25)), [2]);
}

#[test]
fn maker_fees_can_be_rebates() {
    assert_eq!(fee(Price(1_000_000), 1_000, 100), 100_000);
    assert_eq!(fee(Price(1_000_000), 1_000, -50), -50_000);
}

#[test]
fn cancels_are_delayed_so_fills_can_still_land() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    exchange.submit(
        OrderCommand::Cancel(CancelOrder {
            id: ClientOrderId::new(1, 1),
            venue: VENUE,
            instrument: INSTRUMENT,
        }),
        EventTime(now),
    );

    exchange.on_market(&trade(now + 1, Side::Sell, 1000, 12));
    exchange.advance(EventTime(now + ORDER_LATENCY));
    let reports = drain(&mut exchange, now + 100);
    assert_eq!(fills(&reports), [2]);
    assert_eq!(reports[1].kind, ExecKind::CancelAck);
}

#[test]
fn cancelling_an_unknown_order_is_rejected() {
    let mut exchange = with_market();
    exchange.submit(
        OrderCommand::Cancel(CancelOrder {
            id: ClientOrderId::new(1, 9),
            venue: VENUE,
            instrument: INSTRUMENT,
        }),
        EventTime(10),
    );
    exchange.advance(EventTime(20));
    assert_eq!(
        drain(&mut exchange, 25)[0].kind,
        ExecKind::Reject(RejectReason::UnknownOrder)
    );
}

#[test]
fn excess_commands_are_rejected_rather_than_queued() {
    let mut exchange = with_market();
    for id in 1..=5 {
        place(
            &mut exchange,
            order(id, Side::Buy, 900, 1, TimeInForce::PostOnly),
            10,
        );
    }
    assert_eq!(
        drain(&mut exchange, 15)[0].kind,
        ExecKind::Reject(RejectReason::VenueFull)
    );
}

#[test]
fn cancel_all_clears_resting_orders_and_commands_in_flight() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    place(
        &mut exchange,
        order(2, Side::Buy, 999, 1, TimeInForce::PostOnly),
        now,
    );
    exchange.cancel_all(EventTime(now));

    let mut open = 0;
    exchange.open_orders(|_| open += 1);
    assert_eq!(open, 0);
    assert_eq!(drain(&mut exchange, now)[0].kind, ExecKind::CancelAck);
    exchange.advance(EventTime(now + 100));
    assert!(drain(&mut exchange, now + 100).is_empty());
}

#[test]
fn a_new_session_starts_flat_but_keeps_the_market() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    exchange.on_market(&trade(now, Side::Sell, 999, 3));
    assert_eq!(exchange.position(VENUE, INSTRUMENT), 3);

    exchange.reset_session();
    assert_eq!(exchange.position(VENUE, INSTRUMENT), 0);
    let mut open = 0;
    exchange.open_orders(|_| open += 1);
    assert_eq!(open, 0);
    assert!(drain(&mut exchange, now + 1_000).is_empty());
    assert!(exchange.book(VENUE, INSTRUMENT).best_bid().is_some());

    place(
        &mut exchange,
        order(2, Side::Buy, 1000, 1, TimeInForce::PostOnly),
        now + 1_000,
    );
    exchange.advance(EventTime(now + 2_000));
    assert_eq!(drain(&mut exchange, now + 2_000)[0].sequence, 1);
}

#[test]
fn a_price_that_leaves_the_visible_book_keeps_its_queue() {
    let mut exchange = with_market();
    let now = resting_bid(&mut exchange);
    exchange.on_market(&event(now, false, Payload::BookClear));
    exchange.on_market(&level(now, Side::Buy, 1005, 3));
    exchange.on_market(&trade(now + 1, Side::Sell, 1000, 10));
    assert!(fills(&drain(&mut exchange, now + 100)).is_empty());
}

#[derive(Clone, Debug)]
enum Step {
    Level(bool, i64, i64),
    Trade(bool, i64, i64),
    Place(bool, i64, i64),
    Advance(u64),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        (any::<bool>(), 990_i64..1010, 0_i64..20).prop_map(|(b, p, q)| Step::Level(b, p, q)),
        (any::<bool>(), 990_i64..1010, 1_i64..20).prop_map(|(b, p, q)| Step::Trade(b, p, q)),
        (any::<bool>(), 990_i64..1010, 1_i64..20).prop_map(|(b, p, q)| Step::Place(b, p, q)),
        (1_u64..30).prop_map(Step::Advance),
    ]
}

fn side(buy: bool) -> Side {
    if buy { Side::Buy } else { Side::Sell }
}

proptest! {
    #[test]
    fn fills_conserve_quantity_and_queues_stay_valid(
        steps in proptest::collection::vec(step(), 0..300),
    ) {
        let mut exchange = exchange();
        let mut now = 1;
        let mut next_id = 0;
        let mut ordered = std::collections::HashMap::new();
        let mut filled: std::collections::HashMap<ClientOrderId, i64> = Default::default();
        let mut position = 0;

        for step in steps {
            match step {
                Step::Level(buy, price, qty) => exchange.on_market(&level(now, side(buy), price, qty)),
                Step::Trade(buy, price, qty) => exchange.on_market(&trade(now, side(buy), price, qty)),
                Step::Place(buy, price, qty) => {
                    next_id += 1;
                    let new = order(next_id, side(buy), price, qty, TimeInForce::PostOnly);
                    ordered.insert(new.id, qty);
                    place(&mut exchange, new, now);
                }
                Step::Advance(by) => now += by,
            }
            exchange.advance(EventTime(now));
            for report in drain(&mut exchange, now) {
                if let ExecKind::Fill { qty, .. } = report.kind {
                    prop_assert!(qty.0 > 0);
                    let total = filled.entry(report.id).or_default();
                    *total += qty.0;
                    prop_assert!(*total <= ordered[&report.id]);
                    position += if report.side == Side::Buy { qty.0 } else { -qty.0 };
                }
            }
            for slot in &exchange.slots {
                for resting in &slot.resting {
                    prop_assert!(resting.ahead >= 0);
                    prop_assert!(resting.remaining > 0);
                }
            }
        }
        // Fills not yet delivered can't exist once everything has been drained.
        exchange.advance(EventTime(now + 1_000));
        for report in drain(&mut exchange, now + 1_000) {
            if let ExecKind::Fill { qty, .. } = report.kind {
                position += if report.side == Side::Buy { qty.0 } else { -qty.0 };
            }
        }
        prop_assert_eq!(position, exchange.position(VENUE, INSTRUMENT));
    }

    #[test]
    fn the_queue_model_stays_in_range(
        ahead in 0_i64..1_000_000,
        behind in 0_i64..1_000_000,
        cancelled in 0_i64..2_000_000,
        power in 1_i32..4,
    ) {
        let model = ProbabilisticQueue { power };
        let result = model.ahead_after_cancel(ahead, ahead + behind, cancelled);
        prop_assert!(result >= 0);
        prop_assert!(result <= ahead);
    }
}
