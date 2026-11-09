//! A fixed-layout binary protocol between the gateway and the mock venue.
//!
//! Every message is exactly [`FRAME_LEN`] bytes, so encoding and decoding need
//! no allocation or length handling. Integers are little-endian.

use ringbook_core::{
    CancelOrder, ClientOrderId, EventTime, ExecKind, ExecReport, InstrumentId, Liquidity, NewOrder,
    OrderCommand, Price, Qty, RejectReason, Side, TimeInForce, VenueId,
};

/// The size of every frame.
pub const FRAME_LEN: usize = 96;

/// A protocol message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Frame {
    /// Client: start or resume a session after the given sequence number.
    Hello {
        /// The session to resume, or zero for whichever session is current.
        session: u64,
        /// The last report the client has applied.
        last_sequence: u64,
    },
    /// Venue: the session is open and retransmission follows.
    HelloAck {
        /// The current session.
        session: u64,
    },
    /// Venue: the requested reports are no longer retained.
    ResumeFailed,
    /// Client: place or cancel an order.
    Command(OrderCommand),
    /// Venue: an execution report.
    Report(ExecReport),
    /// Either side: the connection is alive.
    Heartbeat,
    /// Client: send open orders and positions.
    QueryState,
    /// Venue: an open order.
    OpenOrder {
        /// The order's ID.
        id: ClientOrderId,
        /// The unfilled quantity.
        remaining: Qty,
    },
    /// Venue: a net position.
    Position {
        /// The venue.
        venue: VenueId,
        /// The instrument.
        instrument: InstrumentId,
        /// The net quantity.
        qty: i64,
    },
    /// Venue: the state query is complete.
    StateEnd,
}

struct Cursor<'a> {
    bytes: &'a mut [u8; FRAME_LEN],
    offset: usize,
}

impl Cursor<'_> {
    fn u8(&mut self, value: u8) {
        self.bytes[self.offset] = value;
        self.offset += 1;
    }

    fn u16(&mut self, value: u16) {
        self.bytes[self.offset..self.offset + 2].copy_from_slice(&value.to_le_bytes());
        self.offset += 2;
    }

    fn u64(&mut self, value: u64) {
        self.bytes[self.offset..self.offset + 8].copy_from_slice(&value.to_le_bytes());
        self.offset += 8;
    }

    fn i64(&mut self, value: i64) {
        self.u64(value as u64);
    }
}

const fn side_code(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

/// Encodes a frame without allocating.
#[inline]
pub fn encode(frame: &Frame, bytes: &mut [u8; FRAME_LEN]) {
    *bytes = [0; FRAME_LEN];
    let mut cursor = Cursor { bytes, offset: 0 };
    match frame {
        Frame::Hello {
            session,
            last_sequence,
        } => {
            cursor.u8(1);
            cursor.u64(*session);
            cursor.u64(*last_sequence);
        }
        Frame::HelloAck { session } => {
            cursor.u8(2);
            cursor.u64(*session);
        }
        Frame::ResumeFailed => cursor.u8(3),
        Frame::Command(OrderCommand::New(order)) => {
            cursor.u8(4);
            cursor.u64(order.id.0);
            cursor.u8(order.venue.0);
            cursor.u16(order.instrument.0);
            cursor.u8(side_code(order.side));
            cursor.i64(order.price.0);
            cursor.i64(order.qty.0);
            cursor.u8(match order.time_in_force {
                TimeInForce::PostOnly => 0,
                TimeInForce::ImmediateOrCancel => 1,
            });
        }
        Frame::Command(OrderCommand::Cancel(cancel)) => {
            cursor.u8(5);
            cursor.u64(cancel.id.0);
            cursor.u8(cancel.venue.0);
            cursor.u16(cancel.instrument.0);
        }
        Frame::Report(report) => {
            cursor.u8(6);
            cursor.u64(report.sequence);
            cursor.u64(report.time.0);
            cursor.u64(report.id.0);
            cursor.u8(report.venue.0);
            cursor.u16(report.instrument.0);
            cursor.u8(side_code(report.side));
            cursor.i64(report.order_price.0);
            cursor.i64(report.order_qty.0);
            match report.kind {
                ExecKind::Ack => cursor.u8(0),
                ExecKind::Reject(reason) => {
                    cursor.u8(1);
                    cursor.u8(match reason {
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
                    cursor.u8(2);
                    cursor.u8(match liquidity {
                        Liquidity::Maker => 0,
                        Liquidity::Taker => 1,
                    });
                    cursor.i64(price.0);
                    cursor.i64(qty.0);
                    cursor.i64(fee);
                }
                ExecKind::CancelAck => cursor.u8(3),
                ExecKind::Expired => cursor.u8(4),
            }
        }
        Frame::Heartbeat => cursor.u8(7),
        Frame::QueryState => cursor.u8(8),
        Frame::OpenOrder { id, remaining } => {
            cursor.u8(9);
            cursor.u64(id.0);
            cursor.i64(remaining.0);
        }
        Frame::Position {
            venue,
            instrument,
            qty,
        } => {
            cursor.u8(10);
            cursor.u8(venue.0);
            cursor.u16(instrument.0);
            cursor.i64(*qty);
        }
        Frame::StateEnd => cursor.u8(11),
    }
}

struct Reader<'a> {
    bytes: &'a [u8; FRAME_LEN],
    offset: usize,
}

impl Reader<'_> {
    fn u8(&mut self) -> u8 {
        let value = self.bytes[self.offset];
        self.offset += 1;
        value
    }

    fn u16(&mut self) -> u16 {
        let value = u16::from_le_bytes([self.bytes[self.offset], self.bytes[self.offset + 1]]);
        self.offset += 2;
        value
    }

    fn u64(&mut self) -> u64 {
        let mut value = [0; 8];
        value.copy_from_slice(&self.bytes[self.offset..self.offset + 8]);
        self.offset += 8;
        u64::from_le_bytes(value)
    }

    fn i64(&mut self) -> i64 {
        self.u64() as i64
    }

    fn side(&mut self) -> Option<Side> {
        match self.u8() {
            0 => Some(Side::Buy),
            1 => Some(Side::Sell),
            _ => None,
        }
    }
}

/// Decodes a frame, or returns `None` if it isn't valid.
#[inline]
#[must_use]
pub fn decode(bytes: &[u8; FRAME_LEN]) -> Option<Frame> {
    let mut reader = Reader { bytes, offset: 0 };
    Some(match reader.u8() {
        1 => Frame::Hello {
            session: reader.u64(),
            last_sequence: reader.u64(),
        },
        2 => Frame::HelloAck {
            session: reader.u64(),
        },
        3 => Frame::ResumeFailed,
        4 => Frame::Command(OrderCommand::New(NewOrder {
            id: ClientOrderId(reader.u64()),
            venue: VenueId(reader.u8()),
            instrument: InstrumentId(reader.u16()),
            side: reader.side()?,
            price: Price(reader.i64()),
            qty: Qty(reader.i64()),
            time_in_force: match reader.u8() {
                0 => TimeInForce::PostOnly,
                1 => TimeInForce::ImmediateOrCancel,
                _ => return None,
            },
        })),
        5 => Frame::Command(OrderCommand::Cancel(CancelOrder {
            id: ClientOrderId(reader.u64()),
            venue: VenueId(reader.u8()),
            instrument: InstrumentId(reader.u16()),
        })),
        6 => {
            let sequence = reader.u64();
            let time = EventTime(reader.u64());
            let id = ClientOrderId(reader.u64());
            let venue = VenueId(reader.u8());
            let instrument = InstrumentId(reader.u16());
            let side = reader.side()?;
            let order_price = Price(reader.i64());
            let order_qty = Qty(reader.i64());
            let kind = match reader.u8() {
                0 => ExecKind::Ack,
                1 => ExecKind::Reject(match reader.u8() {
                    0 => RejectReason::PostOnlyCross,
                    1 => RejectReason::VenueFull,
                    2 => RejectReason::UnknownOrder,
                    3 => RejectReason::NoMarket,
                    _ => return None,
                }),
                2 => {
                    let liquidity = match reader.u8() {
                        0 => Liquidity::Maker,
                        1 => Liquidity::Taker,
                        _ => return None,
                    };
                    ExecKind::Fill {
                        price: Price(reader.i64()),
                        qty: Qty(reader.i64()),
                        fee: reader.i64(),
                        liquidity,
                    }
                }
                3 => ExecKind::CancelAck,
                4 => ExecKind::Expired,
                _ => return None,
            };
            Frame::Report(ExecReport {
                sequence,
                time,
                id,
                venue,
                instrument,
                side,
                order_price,
                order_qty,
                kind,
            })
        }
        7 => Frame::Heartbeat,
        8 => Frame::QueryState,
        9 => Frame::OpenOrder {
            id: ClientOrderId(reader.u64()),
            remaining: Qty(reader.i64()),
        },
        10 => Frame::Position {
            venue: VenueId(reader.u8()),
            instrument: InstrumentId(reader.u16()),
            qty: reader.i64(),
        },
        11 => Frame::StateEnd,
        _ => return None,
    })
}

/// Reassembles frames from a byte stream that may split them anywhere.
#[derive(Clone, Copy, Debug)]
pub struct FrameReader {
    buffer: [u8; FRAME_LEN],
    filled: usize,
}

impl Default for FrameReader {
    fn default() -> Self {
        Self {
            buffer: [0; FRAME_LEN],
            filled: 0,
        }
    }
}

impl FrameReader {
    /// Returns the part of the buffer still to be filled.
    #[inline]
    pub fn unfilled(&mut self) -> &mut [u8] {
        &mut self.buffer[self.filled..]
    }

    /// Records that `count` bytes were read into [`FrameReader::unfilled`].
    #[inline]
    pub const fn advance(&mut self, count: usize) {
        self.filled += count;
    }

    /// Takes a complete frame's bytes, if one has arrived.
    #[inline]
    pub const fn take(&mut self) -> Option<&[u8; FRAME_LEN]> {
        if self.filled == FRAME_LEN {
            self.filled = 0;
            Some(&self.buffer)
        } else {
            None
        }
    }

    /// Discards a partial frame after a reconnect.
    #[inline]
    pub const fn reset(&mut self) {
        self.filled = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn round_trip(frame: Frame) {
        let mut bytes = [0xFF; FRAME_LEN];
        encode(&frame, &mut bytes);
        assert_eq!(decode(&bytes), Some(frame));
    }

    fn order() -> NewOrder {
        NewOrder {
            id: ClientOrderId::new(1, 2),
            venue: VenueId(1),
            instrument: InstrumentId(300),
            side: Side::Sell,
            price: Price(i64::MAX),
            qty: Qty(5),
            time_in_force: TimeInForce::ImmediateOrCancel,
        }
    }

    fn report(kind: ExecKind) -> ExecReport {
        ExecReport {
            sequence: u64::MAX,
            time: EventTime(1),
            id: ClientOrderId::new(1, 2),
            venue: VenueId(255),
            instrument: InstrumentId(65535),
            side: Side::Buy,
            order_price: Price(-1),
            order_qty: Qty(9),
            kind,
        }
    }

    #[test]
    fn every_frame_round_trips() {
        for frame in [
            Frame::Hello {
                session: 7,
                last_sequence: 9,
            },
            Frame::HelloAck { session: 7 },
            Frame::ResumeFailed,
            Frame::Command(OrderCommand::New(order())),
            Frame::Command(OrderCommand::Cancel(CancelOrder {
                id: ClientOrderId::new(1, 2),
                venue: VenueId(0),
                instrument: InstrumentId(1),
            })),
            Frame::Report(report(ExecKind::Ack)),
            Frame::Report(report(ExecKind::Reject(RejectReason::NoMarket))),
            Frame::Report(report(ExecKind::Fill {
                price: Price(5),
                qty: Qty(6),
                fee: -7,
                liquidity: Liquidity::Taker,
            })),
            Frame::Report(report(ExecKind::CancelAck)),
            Frame::Report(report(ExecKind::Expired)),
            Frame::Heartbeat,
            Frame::QueryState,
            Frame::OpenOrder {
                id: ClientOrderId::new(1, 2),
                remaining: Qty(3),
            },
            Frame::Position {
                venue: VenueId(1),
                instrument: InstrumentId(2),
                qty: -3,
            },
            Frame::StateEnd,
        ] {
            round_trip(frame);
        }
    }

    #[test]
    fn unknown_types_are_rejected() {
        assert_eq!(decode(&[0; FRAME_LEN]), None);
        assert_eq!(decode(&[200; FRAME_LEN]), None);
    }

    #[test]
    fn the_reader_reassembles_split_frames() {
        let mut bytes = [0; FRAME_LEN];
        encode(&Frame::Heartbeat, &mut bytes);

        let mut reader = FrameReader::default();
        reader.unfilled()[..10].copy_from_slice(&bytes[..10]);
        reader.advance(10);
        assert!(reader.take().is_none());

        let remaining = reader.unfilled().len();
        reader
            .unfilled()
            .copy_from_slice(&bytes[FRAME_LEN - remaining..]);
        reader.advance(remaining);
        assert_eq!(decode(reader.take().unwrap()), Some(Frame::Heartbeat));
        assert!(reader.take().is_none());
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic_and_valid_frames_are_stable(
            bytes in proptest::array::uniform32(any::<u8>()),
            tail in proptest::array::uniform32(any::<u8>()),
            more in proptest::array::uniform32(any::<u8>()),
        ) {
            let mut frame_bytes = [0; FRAME_LEN];
            frame_bytes[..32].copy_from_slice(&bytes);
            frame_bytes[32..64].copy_from_slice(&tail);
            frame_bytes[64..96].copy_from_slice(&more);
            if let Some(frame) = decode(&frame_bytes) {
                let mut encoded = [0; FRAME_LEN];
                encode(&frame, &mut encoded);
                prop_assert_eq!(decode(&encoded), Some(frame));
            }
        }
    }
}
