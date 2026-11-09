//! Order lifecycle tracking in a fixed-capacity table.

use ringbook_core::{ClientOrderId, ExecKind, ExecReport, NewOrder, Price, Qty, Side};

/// Where an order is in its lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderState {
    /// Sent, not yet acknowledged.
    PendingNew,
    /// Live at the venue with nothing filled.
    Acked,
    /// Live at the venue and partly filled.
    PartiallyFilled,
    /// A cancel was sent and hasn't been answered.
    PendingCancel,
}

/// A tracked order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackedOrder {
    /// The order as sent.
    pub order: NewOrder,
    /// Where it is in its lifecycle.
    pub state: OrderState,
    /// How much has executed.
    pub filled: i64,
}

impl TrackedOrder {
    /// Returns the quantity still open.
    #[inline]
    #[must_use]
    pub const fn remaining(&self) -> i64 {
        self.order.qty.0 - self.filled
    }
}

/// What a report did to the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReportOutcome {
    /// The sequence number was already applied.
    Duplicate,
    /// The report concerned an order no longer tracked, and was ignored.
    Unknown,
    /// The report changed a tracked order, which may now be closed.
    Applied {
        /// A fill to book, as side, price, quantity and fee.
        fill: Option<(Side, Price, Qty, i64)>,
        /// Whether the order reached a terminal state.
        closed: bool,
    },
}

/// The table of open orders.
#[derive(Debug)]
pub struct OrderManager {
    table: Box<[Option<TrackedOrder>]>,
    mask: usize,
    epoch: u16,
    next_sequence: u64,
    last_report: u64,
}

impl OrderManager {
    /// Creates a table holding up to `capacity` orders.
    ///
    /// # Panics
    ///
    /// Panics unless `capacity` is a non-zero power of two.
    #[must_use]
    pub fn new(capacity: usize, epoch: u16) -> Self {
        assert!(capacity.is_power_of_two());
        Self {
            table: vec![None; capacity].into_boxed_slice(),
            mask: capacity - 1,
            epoch,
            next_sequence: 0,
            last_report: 0,
        }
    }

    #[inline]
    fn index(&self, id: ClientOrderId) -> usize {
        id.sequence() as usize & self.mask
    }

    /// Returns the next order ID, or `None` if its table slot is still in use.
    #[inline]
    #[must_use]
    pub fn next_id(&self) -> Option<ClientOrderId> {
        let id = ClientOrderId::new(self.epoch, self.next_sequence + 1);
        self.table[self.index(id)].is_none().then_some(id)
    }

    /// Starts tracking an order using the ID from [`OrderManager::next_id`].
    #[inline]
    pub fn track(&mut self, order: NewOrder) {
        self.next_sequence = order.id.sequence();
        let index = self.index(order.id);
        self.table[index] = Some(TrackedOrder {
            order,
            state: OrderState::PendingNew,
            filled: 0,
        });
    }

    /// Returns a tracked order.
    #[inline]
    #[must_use]
    pub fn get(&self, id: ClientOrderId) -> Option<&TrackedOrder> {
        self.table[self.index(id)]
            .as_ref()
            .filter(|tracked| tracked.order.id == id)
    }

    /// Marks an order as having a cancel in flight.
    ///
    /// Returns whether a cancel should actually be sent.
    #[inline]
    pub fn begin_cancel(&mut self, id: ClientOrderId) -> bool {
        let index = self.index(id);
        match &mut self.table[index] {
            Some(tracked)
                if tracked.order.id == id && tracked.state != OrderState::PendingCancel =>
            {
                tracked.state = OrderState::PendingCancel;
                true
            }
            _ => false,
        }
    }

    /// Applies a venue report exactly once.
    #[inline]
    pub fn on_report(&mut self, report: &ExecReport) -> ReportOutcome {
        if report.sequence <= self.last_report {
            return ReportOutcome::Duplicate;
        }
        self.last_report = report.sequence;

        let index = self.index(report.id);
        let Some(tracked) = self.table[index]
            .as_mut()
            .filter(|tracked| tracked.order.id == report.id)
        else {
            return ReportOutcome::Unknown;
        };

        let mut fill = None;
        let closed = match report.kind {
            ExecKind::Ack => {
                if tracked.state == OrderState::PendingNew {
                    tracked.state = OrderState::Acked;
                }
                false
            }
            ExecKind::Fill {
                price, qty, fee, ..
            } => {
                let applied = qty.0.min(tracked.remaining());
                tracked.filled += applied;
                fill = Some((tracked.order.side, price, Qty(applied), fee));
                if tracked.state != OrderState::PendingCancel {
                    tracked.state = OrderState::PartiallyFilled;
                }
                tracked.remaining() == 0
            }
            ExecKind::Reject(_) | ExecKind::CancelAck | ExecKind::Expired => true,
        };
        if closed {
            self.table[index] = None;
        }
        ReportOutcome::Applied { fill, closed }
    }

    /// Returns the sequence number of the last applied report.
    #[inline]
    #[must_use]
    pub const fn last_report(&self) -> u64 {
        self.last_report
    }

    /// Restores an order during recovery, using details echoed by the venue.
    pub fn restore(&mut self, order: NewOrder) {
        let index = self.index(order.id);
        if self.get(order.id).is_none() {
            self.next_sequence = self.next_sequence.max(order.id.sequence());
            self.table[index] = Some(TrackedOrder {
                order,
                state: OrderState::Acked,
                filled: 0,
            });
        }
    }

    /// Stops tracking an order that can never be answered.
    pub fn forget(&mut self, id: ClientOrderId) {
        let index = self.index(id);
        if self.get(id).is_some() {
            self.table[index] = None;
        }
    }

    /// Iterates the open orders.
    pub fn open_orders(&self) -> impl Iterator<Item = &TrackedOrder> {
        self.table.iter().flatten()
    }

    /// Replays a recorded send during recovery.
    pub fn replay_sent(&mut self, order: NewOrder) {
        self.next_sequence = self.next_sequence.max(order.id.sequence());
        self.track_keeping_sequence(order);
    }

    fn track_keeping_sequence(&mut self, order: NewOrder) {
        let index = self.index(order.id);
        self.table[index] = Some(TrackedOrder {
            order,
            state: OrderState::PendingNew,
            filled: 0,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use ringbook_core::{EventTime, InstrumentId, Liquidity, RejectReason, TimeInForce, VenueId};

    fn new_order(manager: &OrderManager, qty: i64) -> NewOrder {
        NewOrder {
            id: manager.next_id().unwrap(),
            venue: VenueId(0),
            instrument: InstrumentId(0),
            side: Side::Buy,
            price: Price(100),
            qty: Qty(qty),
            time_in_force: TimeInForce::PostOnly,
        }
    }

    fn report(order: &NewOrder, sequence: u64, kind: ExecKind) -> ExecReport {
        ExecReport {
            sequence,
            time: EventTime(1),
            id: order.id,
            venue: order.venue,
            instrument: order.instrument,
            side: order.side,
            order_price: order.price,
            order_qty: order.qty,
            kind,
        }
    }

    fn fill(qty: i64) -> ExecKind {
        ExecKind::Fill {
            price: Price(100),
            qty: Qty(qty),
            fee: 1,
            liquidity: Liquidity::Maker,
        }
    }

    fn tracked() -> (OrderManager, NewOrder) {
        let mut manager = OrderManager::new(8, 1);
        let order = new_order(&manager, 10);
        manager.track(order);
        (manager, order)
    }

    #[test]
    fn walks_through_ack_partial_and_full_fill() {
        let (mut manager, order) = tracked();
        assert_eq!(manager.get(order.id).unwrap().state, OrderState::PendingNew);

        manager.on_report(&report(&order, 1, ExecKind::Ack));
        assert_eq!(manager.get(order.id).unwrap().state, OrderState::Acked);

        manager.on_report(&report(&order, 2, fill(4)));
        let current = manager.get(order.id).unwrap();
        assert_eq!(current.state, OrderState::PartiallyFilled);
        assert_eq!(current.remaining(), 6);

        let outcome = manager.on_report(&report(&order, 3, fill(6)));
        assert!(matches!(
            outcome,
            ReportOutcome::Applied { closed: true, .. }
        ));
        assert!(manager.get(order.id).is_none());
    }

    #[test]
    fn rejects_close_the_order() {
        let (mut manager, order) = tracked();
        let kind = ExecKind::Reject(RejectReason::PostOnlyCross);
        manager.on_report(&report(&order, 1, kind));
        assert!(manager.get(order.id).is_none());
    }

    #[test]
    fn a_fill_during_a_pending_cancel_still_counts() {
        let (mut manager, order) = tracked();
        manager.on_report(&report(&order, 1, ExecKind::Ack));
        assert!(manager.begin_cancel(order.id));

        let outcome = manager.on_report(&report(&order, 2, fill(3)));
        assert!(matches!(
            outcome,
            ReportOutcome::Applied {
                fill: Some(_),
                closed: false
            }
        ));
        assert_eq!(
            manager.get(order.id).unwrap().state,
            OrderState::PendingCancel
        );

        manager.on_report(&report(&order, 3, ExecKind::CancelAck));
        assert!(manager.get(order.id).is_none());
    }

    #[test]
    fn a_cancel_is_sent_only_once() {
        let (mut manager, order) = tracked();
        assert!(manager.begin_cancel(order.id));
        assert!(!manager.begin_cancel(order.id));
    }

    #[test]
    fn a_late_report_for_a_closed_order_is_ignored() {
        let (mut manager, order) = tracked();
        manager.on_report(&report(&order, 1, fill(10)));
        let late = ExecKind::Reject(RejectReason::UnknownOrder);
        assert_eq!(
            manager.on_report(&report(&order, 2, late)),
            ReportOutcome::Unknown
        );
    }

    #[test]
    fn duplicate_sequence_numbers_apply_once() {
        let (mut manager, order) = tracked();
        manager.on_report(&report(&order, 1, fill(4)));
        assert_eq!(
            manager.on_report(&report(&order, 1, fill(4))),
            ReportOutcome::Duplicate
        );
        assert_eq!(manager.get(order.id).unwrap().filled, 4);
    }

    #[test]
    fn a_full_table_slot_refuses_a_new_id() {
        let mut manager = OrderManager::new(2, 1);
        let first = new_order(&manager, 1);
        manager.track(first);
        let second = new_order(&manager, 1);
        manager.track(second);
        assert!(manager.next_id().is_none());

        manager.on_report(&report(&first, 1, ExecKind::CancelAck));
        assert!(manager.next_id().is_some());
    }

    #[test]
    fn ids_carry_the_run_epoch() {
        let manager = OrderManager::new(8, 9);
        assert_eq!(manager.next_id().unwrap().epoch(), 9);
    }

    #[derive(Clone, Debug)]
    enum Step {
        Ack,
        Fill(i64),
        CancelRequest,
        CancelAck,
        Reject,
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            Just(Step::Ack),
            (1_i64..6).prop_map(Step::Fill),
            Just(Step::CancelRequest),
            Just(Step::CancelAck),
            Just(Step::Reject),
        ]
    }

    proptest! {
        #[test]
        fn any_message_order_keeps_the_order_consistent(
            steps in proptest::collection::vec(step(), 0..40),
            duplicate_every in 1_usize..5,
        ) {
            let (mut manager, order) = tracked();
            let mut sequence = 0;
            let mut total = 0;
            let mut open = true;

            for (position, step) in steps.into_iter().enumerate() {
                let kind = match step {
                    Step::CancelRequest => {
                        let sent = manager.begin_cancel(order.id);
                        prop_assert!(!sent || open);
                        continue;
                    }
                    Step::Ack => ExecKind::Ack,
                    Step::Fill(qty) => fill(qty),
                    Step::CancelAck => ExecKind::CancelAck,
                    Step::Reject => ExecKind::Reject(RejectReason::UnknownOrder),
                };

                sequence += 1;
                let current = report(&order, sequence, kind);
                let outcome = manager.on_report(&current);
                if position % duplicate_every == 0 {
                    prop_assert_eq!(manager.on_report(&current), ReportOutcome::Duplicate);
                }

                match outcome {
                    ReportOutcome::Applied { fill, closed } => {
                        prop_assert!(open);
                        if let Some((_, _, qty, _)) = fill {
                            total += qty.0;
                        }
                        open = !closed;
                    }
                    ReportOutcome::Unknown => prop_assert!(!open),
                    ReportOutcome::Duplicate => prop_assert!(false),
                }
                prop_assert!(total <= order.qty.0);
                prop_assert_eq!(manager.get(order.id).is_some(), open);
            }
        }
    }
}
