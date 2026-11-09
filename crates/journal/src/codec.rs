//! A stable binary encoding for journal records.

use ringbook_core::{
    CancelOrder, ClientOrderId, ControlCommand, EventTime, ExecKind, ExecReport, HaltReason,
    InstrumentId, Liquidity, NewOrder, OrderCommand, ParamChange, ParamKey, Price, Qty,
    RejectReason, Side, TimeInForce, VenueId,
};

use crate::{JournalBody, JournalRecord, RiskRejection};

struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn new_order(&mut self, order: &NewOrder) {
        self.u64(order.id.0);
        self.u8(order.venue.0);
        self.u16(order.instrument.0);
        self.u8(side_code(order.side));
        self.i64(order.price.0);
        self.i64(order.qty.0);
        self.u8(match order.time_in_force {
            TimeInForce::PostOnly => 0,
            TimeInForce::ImmediateOrCancel => 1,
        });
    }

    fn report(&mut self, report: &ExecReport) {
        self.u64(report.sequence);
        self.u64(report.time.0);
        self.u64(report.id.0);
        self.u8(report.venue.0);
        self.u16(report.instrument.0);
        self.u8(side_code(report.side));
        self.i64(report.order_price.0);
        self.i64(report.order_qty.0);
        match report.kind {
            ExecKind::Ack => self.u8(0),
            ExecKind::Reject(reason) => {
                self.u8(1);
                self.u8(match reason {
                    RejectReason::PostOnlyCross => 0,
                    RejectReason::VenueFull => 1,
                    RejectReason::UnknownOrder => 2,
                    RejectReason::NoMarket => 3,
                });
            }
            ExecKind::Fill {
                price,
                qty,
                fee,
                liquidity,
            } => {
                self.u8(2);
                self.i64(price.0);
                self.i64(qty.0);
                self.i64(fee);
                self.u8(match liquidity {
                    Liquidity::Maker => 0,
                    Liquidity::Taker => 1,
                });
            }
            ExecKind::CancelAck => self.u8(3),
            ExecKind::Expired => self.u8(4),
        }
    }

    fn control(&mut self, command: &ControlCommand) {
        match command {
            ControlCommand::SetParam(change) => {
                self.u8(0);
                self.u8(match change.key {
                    ParamKey::SpreadCentiBps => 0,
                    ParamKey::InventorySkewCentiBps => 1,
                    ParamKey::RequoteThresholdCentiBps => 2,
                });
                self.i64(change.value);
            }
            ControlCommand::HaltInstrument(id) => {
                self.u8(1);
                self.u16(id.0);
            }
            ControlCommand::ResumeInstrument(id) => {
                self.u8(2);
                self.u16(id.0);
            }
            ControlCommand::GlobalHalt => self.u8(3),
        }
    }
}

const fn side_code(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

const fn halt_code(reason: HaltReason) -> u8 {
    match reason {
        HaltReason::Operator => 0,
        HaltReason::Drawdown => 1,
        HaltReason::JournalBackpressure => 2,
        HaltReason::JournalWriteFailed => 3,
        HaltReason::VenueStateLost => 4,
        HaltReason::OrderPathBlocked => 5,
        HaltReason::ReconcileFailed => 6,
    }
}

const fn rejection_code(reason: RiskRejection) -> u8 {
    match reason {
        RiskRejection::Position => 0,
        RiskRejection::Notional => 1,
        RiskRejection::OrderRate => 2,
        RiskRejection::OrderSize => 3,
        RiskRejection::PriceCollar => 4,
        RiskRejection::SelfTrade => 5,
        RiskRejection::OrderTableFull => 6,
        RiskRejection::OrderPathFull => 7,
    }
}

pub(crate) fn encode(record: &JournalRecord) -> Vec<u8> {
    let mut writer = Writer {
        bytes: Vec::with_capacity(128),
    };
    writer.u64(record.batch);
    writer.u64(record.time.0);
    match &record.body {
        JournalBody::Sent(OrderCommand::New(order)) => {
            writer.u8(0);
            writer.new_order(order);
        }
        JournalBody::Sent(OrderCommand::Cancel(cancel)) => {
            writer.u8(1);
            writer.u64(cancel.id.0);
            writer.u8(cancel.venue.0);
            writer.u16(cancel.instrument.0);
        }
        JournalBody::RiskRejected { order, reason } => {
            writer.u8(2);
            writer.new_order(order);
            writer.u8(rejection_code(*reason));
        }
        JournalBody::Report(report) => {
            writer.u8(3);
            writer.report(report);
        }
        JournalBody::Control(command) => {
            writer.u8(4);
            writer.control(command);
        }
        JournalBody::Halted(reason) => {
            writer.u8(5);
            writer.u8(halt_code(*reason));
        }
        JournalBody::Session(session) => {
            writer.u8(6);
            writer.u64(*session);
        }
    }
    writer.bytes
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, tail) = self.bytes.split_first_chunk::<N>()?;
        self.bytes = tail;
        Some(*head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|[value]| value)
    }

    fn u16(&mut self) -> Option<u16> {
        self.take().map(u16::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_le_bytes)
    }

    fn i64(&mut self) -> Option<i64> {
        self.take().map(i64::from_le_bytes)
    }

    fn side(&mut self) -> Option<Side> {
        match self.u8()? {
            0 => Some(Side::Buy),
            1 => Some(Side::Sell),
            _ => None,
        }
    }

    fn new_order(&mut self) -> Option<NewOrder> {
        Some(NewOrder {
            id: ClientOrderId(self.u64()?),
            venue: VenueId(self.u8()?),
            instrument: InstrumentId(self.u16()?),
            side: self.side()?,
            price: Price(self.i64()?),
            qty: Qty(self.i64()?),
            time_in_force: match self.u8()? {
                0 => TimeInForce::PostOnly,
                1 => TimeInForce::ImmediateOrCancel,
                _ => return None,
            },
        })
    }

    fn report(&mut self) -> Option<ExecReport> {
        Some(ExecReport {
            sequence: self.u64()?,
            time: EventTime(self.u64()?),
            id: ClientOrderId(self.u64()?),
            venue: VenueId(self.u8()?),
            instrument: InstrumentId(self.u16()?),
            side: self.side()?,
            order_price: Price(self.i64()?),
            order_qty: Qty(self.i64()?),
            kind: match self.u8()? {
                0 => ExecKind::Ack,
                1 => ExecKind::Reject(match self.u8()? {
                    0 => RejectReason::PostOnlyCross,
                    1 => RejectReason::VenueFull,
                    2 => RejectReason::UnknownOrder,
                    3 => RejectReason::NoMarket,
                    _ => return None,
                }),
                2 => ExecKind::Fill {
                    price: Price(self.i64()?),
                    qty: Qty(self.i64()?),
                    fee: self.i64()?,
                    liquidity: match self.u8()? {
                        0 => Liquidity::Maker,
                        1 => Liquidity::Taker,
                        _ => return None,
                    },
                },
                3 => ExecKind::CancelAck,
                4 => ExecKind::Expired,
                _ => return None,
            },
        })
    }

    fn control(&mut self) -> Option<ControlCommand> {
        Some(match self.u8()? {
            0 => ControlCommand::SetParam(ParamChange {
                key: match self.u8()? {
                    0 => ParamKey::SpreadCentiBps,
                    1 => ParamKey::InventorySkewCentiBps,
                    2 => ParamKey::RequoteThresholdCentiBps,
                    _ => return None,
                },
                value: self.i64()?,
            }),
            1 => ControlCommand::HaltInstrument(InstrumentId(self.u16()?)),
            2 => ControlCommand::ResumeInstrument(InstrumentId(self.u16()?)),
            3 => ControlCommand::GlobalHalt,
            _ => return None,
        })
    }
}

fn decode_halt(code: u8) -> Option<HaltReason> {
    Some(match code {
        0 => HaltReason::Operator,
        1 => HaltReason::Drawdown,
        2 => HaltReason::JournalBackpressure,
        3 => HaltReason::JournalWriteFailed,
        4 => HaltReason::VenueStateLost,
        5 => HaltReason::OrderPathBlocked,
        6 => HaltReason::ReconcileFailed,
        _ => return None,
    })
}

fn decode_rejection(code: u8) -> Option<RiskRejection> {
    Some(match code {
        0 => RiskRejection::Position,
        1 => RiskRejection::Notional,
        2 => RiskRejection::OrderRate,
        3 => RiskRejection::OrderSize,
        4 => RiskRejection::PriceCollar,
        5 => RiskRejection::SelfTrade,
        6 => RiskRejection::OrderTableFull,
        7 => RiskRejection::OrderPathFull,
        _ => return None,
    })
}

pub(crate) fn decode(bytes: &[u8]) -> Option<JournalRecord> {
    let mut reader = Reader { bytes };
    let batch = reader.u64()?;
    let time = EventTime(reader.u64()?);
    let body = match reader.u8()? {
        0 => JournalBody::Sent(OrderCommand::New(reader.new_order()?)),
        1 => JournalBody::Sent(OrderCommand::Cancel(CancelOrder {
            id: ClientOrderId(reader.u64()?),
            venue: VenueId(reader.u8()?),
            instrument: InstrumentId(reader.u16()?),
        })),
        2 => JournalBody::RiskRejected {
            order: reader.new_order()?,
            reason: decode_rejection(reader.u8()?)?,
        },
        3 => JournalBody::Report(reader.report()?),
        4 => JournalBody::Control(reader.control()?),
        5 => JournalBody::Halted(decode_halt(reader.u8()?)?),
        6 => JournalBody::Session(reader.u64()?),
        _ => return None,
    };
    reader
        .bytes
        .is_empty()
        .then_some(JournalRecord { batch, time, body })
}
