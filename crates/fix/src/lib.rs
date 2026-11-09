//! Zero-copy parsing and serialisation for a subset of FIX 4.4.
//!
//! This is a message codec, not a FIX engine: it has no sessions, sequence
//! recovery or persistence. Parsing borrows from the input and serialising
//! writes into a caller's buffer, so neither allocates.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

use core::fmt;

const SOH: u8 = 0x01;
const BEGIN_STRING: &[u8] = b"FIX.4.4";
const CHECKSUM_FIELD_LEN: usize = 7;

/// `BeginString`.
pub const TAG_BEGIN_STRING: u32 = 8;
/// `BodyLength`.
pub const TAG_BODY_LENGTH: u32 = 9;
/// `CheckSum`.
pub const TAG_CHECKSUM: u32 = 10;
/// `ClOrdID`.
pub const TAG_CL_ORD_ID: u32 = 11;
/// `ExecID`.
pub const TAG_EXEC_ID: u32 = 17;
/// `MsgSeqNum`.
pub const TAG_MSG_SEQ_NUM: u32 = 34;
/// `MsgType`.
pub const TAG_MSG_TYPE: u32 = 35;
/// `OrderID`.
pub const TAG_ORDER_ID: u32 = 37;
/// `OrderQty`.
pub const TAG_ORDER_QTY: u32 = 38;
/// `OrdStatus`.
pub const TAG_ORD_STATUS: u32 = 39;
/// `OrigClOrdID`.
pub const TAG_ORIG_CL_ORD_ID: u32 = 41;
/// `Price`.
pub const TAG_PRICE: u32 = 44;
/// `SenderCompID`.
pub const TAG_SENDER_COMP_ID: u32 = 49;
/// `SendingTime`.
pub const TAG_SENDING_TIME: u32 = 52;
/// `Side`.
pub const TAG_SIDE: u32 = 54;
/// `Symbol`.
pub const TAG_SYMBOL: u32 = 55;
/// `TargetCompID`.
pub const TAG_TARGET_COMP_ID: u32 = 56;
/// `ExecType`.
pub const TAG_EXEC_TYPE: u32 = 150;
/// `LeavesQty`.
pub const TAG_LEAVES_QTY: u32 = 151;
/// `CumQty`.
pub const TAG_CUM_QTY: u32 = 14;

/// Why a message could not be parsed or written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FixError {
    /// More bytes are needed to complete the message.
    Incomplete,
    /// The message isn't structured as FIX.
    Malformed,
    /// The message isn't FIX 4.4.
    UnsupportedVersion,
    /// `BodyLength` doesn't match the message.
    BadBodyLength,
    /// `CheckSum` doesn't match the message.
    BadChecksum,
    /// A required field is absent.
    MissingField(u32),
    /// The output buffer is too small.
    BufferTooSmall,
}

impl fmt::Display for FixError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => formatter.write_str("incomplete message"),
            Self::Malformed => formatter.write_str("malformed message"),
            Self::UnsupportedVersion => formatter.write_str("unsupported FIX version"),
            Self::BadBodyLength => formatter.write_str("incorrect BodyLength"),
            Self::BadChecksum => formatter.write_str("incorrect CheckSum"),
            Self::MissingField(tag) => write!(formatter, "missing field {tag}"),
            Self::BufferTooSmall => formatter.write_str("buffer too small"),
        }
    }
}

impl core::error::Error for FixError {}

/// One tag and its value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Field<'a> {
    /// The tag number.
    pub tag: u32,
    /// The raw value.
    pub value: &'a [u8],
}

fn parse_tag(bytes: &[u8]) -> Result<u32, FixError> {
    if bytes.is_empty() || bytes.len() > 9 {
        return Err(FixError::Malformed);
    }
    bytes.iter().try_fold(0_u32, |tag, byte| {
        if byte.is_ascii_digit() {
            Ok(tag * 10 + u32::from(byte - b'0'))
        } else {
            Err(FixError::Malformed)
        }
    })
}

/// Reads one field from the front of `bytes`.
fn next_field(bytes: &[u8]) -> Result<(Field<'_>, &[u8]), FixError> {
    let separator = bytes
        .iter()
        .position(|&byte| byte == b'=')
        .ok_or(FixError::Incomplete)?;
    let tag = parse_tag(&bytes[..separator])?;
    let rest = &bytes[separator + 1..];
    let end = rest
        .iter()
        .position(|&byte| byte == SOH)
        .ok_or(FixError::Incomplete)?;
    Ok((
        Field {
            tag,
            value: &rest[..end],
        },
        &rest[end + 1..],
    ))
}

fn checksum(bytes: &[u8]) -> u8 {
    bytes
        .iter()
        .fold(0_u8, |total, byte| total.wrapping_add(*byte))
}

/// A parsed message, borrowing from its input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Message<'a> {
    msg_type: &'a [u8],
    fields: &'a [u8],
}

impl<'a> Message<'a> {
    /// Returns the `MsgType`.
    #[must_use]
    pub const fn msg_type(&self) -> &'a [u8] {
        self.msg_type
    }

    /// Iterates the body fields in order, including `MsgType`.
    #[must_use]
    pub const fn fields(&self) -> Fields<'a> {
        Fields {
            remaining: self.fields,
        }
    }

    /// Returns the first value for a tag.
    #[must_use]
    pub fn get(&self, tag: u32) -> Option<&'a [u8]> {
        self.fields()
            .find(|field| field.tag == tag)
            .map(|field| field.value)
    }

    fn required(&self, tag: u32) -> Result<&'a [u8], FixError> {
        self.get(tag).ok_or(FixError::MissingField(tag))
    }
}

/// An iterator over a message's body fields.
#[derive(Clone, Copy, Debug)]
pub struct Fields<'a> {
    remaining: &'a [u8],
}

impl<'a> Iterator for Fields<'a> {
    type Item = Field<'a>;

    fn next(&mut self) -> Option<Field<'a>> {
        if self.remaining.is_empty() {
            return None;
        }
        // `parse` validated the structure, so this can't fail.
        let (field, rest) = next_field(self.remaining).ok()?;
        self.remaining = rest;
        Some(field)
    }
}

/// Parses one message from the front of `input`.
///
/// Returns the message and the number of bytes it occupied, so a caller can
/// parse a stream.
///
/// # Errors
///
/// Returns [`FixError::Incomplete`] if more bytes are needed, or another
/// [`FixError`] if the message is invalid.
pub fn parse(input: &[u8]) -> Result<(Message<'_>, usize), FixError> {
    let (begin, rest) = next_field(input)?;
    if begin.tag != TAG_BEGIN_STRING {
        return Err(FixError::Malformed);
    }
    if begin.value != BEGIN_STRING {
        return Err(FixError::UnsupportedVersion);
    }

    let (length, body_and_trailer) = next_field(rest)?;
    if length.tag != TAG_BODY_LENGTH {
        return Err(FixError::Malformed);
    }
    if length.value.is_empty() || !length.value.iter().all(u8::is_ascii_digit) {
        return Err(FixError::Malformed);
    }
    let body_length =
        usize::try_from(parse_tag(length.value)?).map_err(|_| FixError::BadBodyLength)?;

    let body_start = input.len() - body_and_trailer.len();
    let body_end = body_start
        .checked_add(body_length)
        .ok_or(FixError::BadBodyLength)?;
    let total = body_end
        .checked_add(CHECKSUM_FIELD_LEN)
        .ok_or(FixError::BadBodyLength)?;
    if input.len() < total {
        return Err(FixError::Incomplete);
    }
    if body_length == 0 || input[body_end - 1] != SOH {
        return Err(FixError::BadBodyLength);
    }

    let (trailer, _) = next_field(&input[body_end..total]).map_err(|_| FixError::BadBodyLength)?;
    if trailer.tag != TAG_CHECKSUM || trailer.value.len() != 3 {
        return Err(FixError::BadBodyLength);
    }
    let expected = u32::from(checksum(&input[..body_end]));
    if parse_tag(trailer.value)? != expected {
        return Err(FixError::BadChecksum);
    }

    let fields = &input[body_start..body_end];
    let mut remaining = fields;
    let mut msg_type = None;
    while !remaining.is_empty() {
        let (field, rest) = next_field(remaining).map_err(|_| FixError::Malformed)?;
        if field.tag == TAG_MSG_TYPE && msg_type.is_none() {
            msg_type = Some(field.value);
        }
        remaining = rest;
    }
    let msg_type = msg_type.ok_or(FixError::MissingField(TAG_MSG_TYPE))?;
    Ok((Message { msg_type, fields }, total))
}

/// A `NewOrderSingle` (35=D).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewOrderSingle<'a> {
    /// `ClOrdID`.
    pub cl_ord_id: &'a [u8],
    /// `Symbol`.
    pub symbol: &'a [u8],
    /// `Side`.
    pub side: &'a [u8],
    /// `OrderQty`.
    pub order_qty: &'a [u8],
    /// `Price`.
    pub price: &'a [u8],
}

impl<'a> NewOrderSingle<'a> {
    /// Reads the fields from a parsed message.
    ///
    /// # Errors
    ///
    /// Returns [`FixError::MissingField`] if a required field is absent.
    pub fn from_message(message: &Message<'a>) -> Result<Self, FixError> {
        Ok(Self {
            cl_ord_id: message.required(TAG_CL_ORD_ID)?,
            symbol: message.required(TAG_SYMBOL)?,
            side: message.required(TAG_SIDE)?,
            order_qty: message.required(TAG_ORDER_QTY)?,
            price: message.required(TAG_PRICE)?,
        })
    }
}

/// An `OrderCancelRequest` (35=F).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderCancelRequest<'a> {
    /// `ClOrdID`.
    pub cl_ord_id: &'a [u8],
    /// `OrigClOrdID`.
    pub orig_cl_ord_id: &'a [u8],
    /// `Symbol`.
    pub symbol: &'a [u8],
    /// `Side`.
    pub side: &'a [u8],
}

impl<'a> OrderCancelRequest<'a> {
    /// Reads the fields from a parsed message.
    ///
    /// # Errors
    ///
    /// Returns [`FixError::MissingField`] if a required field is absent.
    pub fn from_message(message: &Message<'a>) -> Result<Self, FixError> {
        Ok(Self {
            cl_ord_id: message.required(TAG_CL_ORD_ID)?,
            orig_cl_ord_id: message.required(TAG_ORIG_CL_ORD_ID)?,
            symbol: message.required(TAG_SYMBOL)?,
            side: message.required(TAG_SIDE)?,
        })
    }
}

/// An `ExecutionReport` (35=8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionReport<'a> {
    /// `OrderID`.
    pub order_id: &'a [u8],
    /// `ExecID`.
    pub exec_id: &'a [u8],
    /// `ExecType`.
    pub exec_type: &'a [u8],
    /// `OrdStatus`.
    pub ord_status: &'a [u8],
    /// `Symbol`.
    pub symbol: &'a [u8],
    /// `Side`.
    pub side: &'a [u8],
    /// `LeavesQty`.
    pub leaves_qty: &'a [u8],
    /// `CumQty`.
    pub cum_qty: &'a [u8],
}

impl<'a> ExecutionReport<'a> {
    /// Reads the fields from a parsed message.
    ///
    /// # Errors
    ///
    /// Returns [`FixError::MissingField`] if a required field is absent.
    pub fn from_message(message: &Message<'a>) -> Result<Self, FixError> {
        Ok(Self {
            order_id: message.required(TAG_ORDER_ID)?,
            exec_id: message.required(TAG_EXEC_ID)?,
            exec_type: message.required(TAG_EXEC_TYPE)?,
            ord_status: message.required(TAG_ORD_STATUS)?,
            symbol: message.required(TAG_SYMBOL)?,
            side: message.required(TAG_SIDE)?,
            leaves_qty: message.required(TAG_LEAVES_QTY)?,
            cum_qty: message.required(TAG_CUM_QTY)?,
        })
    }
}

/// The fields every message carries in its header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header<'a> {
    /// `SenderCompID`.
    pub sender: &'a [u8],
    /// `TargetCompID`.
    pub target: &'a [u8],
    /// `MsgSeqNum`.
    pub sequence: u64,
    /// `SendingTime`.
    pub sending_time: &'a [u8],
}

struct Output<'a> {
    bytes: &'a mut [u8],
    length: usize,
}

impl Output<'_> {
    fn push(&mut self, data: &[u8]) -> Result<(), FixError> {
        let end = self.length + data.len();
        self.bytes
            .get_mut(self.length..end)
            .ok_or(FixError::BufferTooSmall)?
            .copy_from_slice(data);
        self.length = end;
        Ok(())
    }

    fn number(&mut self, mut value: u64) -> Result<(), FixError> {
        let mut digits = [0; 20];
        let mut start = digits.len();
        loop {
            start -= 1;
            digits[start] = b'0' + (value % 10) as u8;
            value /= 10;
            if value == 0 {
                break;
            }
        }
        self.push(&digits[start..])
    }

    fn field(&mut self, tag: u32, value: &[u8]) -> Result<(), FixError> {
        self.number(u64::from(tag))?;
        self.push(b"=")?;
        self.push(value)?;
        self.push(&[SOH])
    }
}

/// The widest `BodyLength` written, zero-padded so it can be filled in later.
const BODY_LENGTH_WIDTH: usize = 6;

/// Writes a message into `buffer`, returning its length.
///
/// `body` lists the fields after the standard header.
///
/// # Errors
///
/// Returns [`FixError::BufferTooSmall`] if the message doesn't fit.
pub fn write_message(
    buffer: &mut [u8],
    msg_type: &[u8],
    header: &Header<'_>,
    body: &[Field<'_>],
) -> Result<usize, FixError> {
    let mut output = Output {
        bytes: buffer,
        length: 0,
    };
    output.field(TAG_BEGIN_STRING, BEGIN_STRING)?;
    output.push(b"9=")?;
    let length_position = output.length;
    output.push(&[b'0'; BODY_LENGTH_WIDTH])?;
    output.push(&[SOH])?;
    let body_start = output.length;

    output.field(TAG_MSG_TYPE, msg_type)?;
    output.field(TAG_SENDER_COMP_ID, header.sender)?;
    output.field(TAG_TARGET_COMP_ID, header.target)?;
    output.push(b"34=")?;
    output.number(header.sequence)?;
    output.push(&[SOH])?;
    output.field(TAG_SENDING_TIME, header.sending_time)?;
    for field in body {
        output.field(field.tag, field.value)?;
    }

    let mut remaining = output.length - body_start;
    if remaining >= 10_usize.pow(BODY_LENGTH_WIDTH as u32) {
        return Err(FixError::BufferTooSmall);
    }
    for position in (length_position..length_position + BODY_LENGTH_WIDTH).rev() {
        output.bytes[position] = b'0' + (remaining % 10) as u8;
        remaining /= 10;
    }

    let sum = checksum(&output.bytes[..output.length]);
    output.push(b"10=")?;
    output.push(&[b'0' + sum / 100, b'0' + sum / 10 % 10, b'0' + sum % 10, SOH])?;
    Ok(output.length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const HEADER: Header<'static> = Header {
        sender: b"RINGBOOK",
        target: b"VENUE",
        sequence: 42,
        sending_time: b"20260101-00:00:00.000",
    };

    fn new_order(buffer: &mut [u8]) -> usize {
        write_message(
            buffer,
            b"D",
            &HEADER,
            &[
                Field {
                    tag: TAG_CL_ORD_ID,
                    value: b"ORDER1",
                },
                Field {
                    tag: TAG_SYMBOL,
                    value: b"BTC-PERP",
                },
                Field {
                    tag: TAG_SIDE,
                    value: b"1",
                },
                Field {
                    tag: TAG_ORDER_QTY,
                    value: b"0.5",
                },
                Field {
                    tag: TAG_PRICE,
                    value: b"73000.1",
                },
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_written_order_parses_back() {
        let mut buffer = [0; 256];
        let length = new_order(&mut buffer);
        let (message, consumed) = parse(&buffer[..length]).unwrap();
        assert_eq!(consumed, length);
        assert_eq!(message.msg_type(), b"D");
        assert_eq!(message.get(TAG_MSG_SEQ_NUM), Some(&b"42"[..]));

        let order = NewOrderSingle::from_message(&message).unwrap();
        assert_eq!(order.cl_ord_id, b"ORDER1");
        assert_eq!(order.price, b"73000.1");
    }

    #[test]
    fn parses_a_message_written_by_hand() {
        let body = b"35=0\x0149=A\x0156=B\x0134=1\x0152=T\x01";
        let mut message = alloc_message(body);
        let (parsed, _) = parse(&message).unwrap();
        assert_eq!(parsed.msg_type(), b"0");
        message.pop();
        assert_eq!(parse(&message), Err(FixError::Incomplete));
    }

    fn alloc_message(body: &[u8]) -> Vec<u8> {
        let mut message = format!("8=FIX.4.4\x019={}\x01", body.len()).into_bytes();
        message.extend_from_slice(body);
        let sum = checksum(&message);
        message.extend_from_slice(format!("10={sum:03}\x01").as_bytes());
        message
    }

    #[test]
    fn parses_cancels_and_execution_reports() {
        let body = b"35=F\x0111=B\x0141=A\x0155=X\x0154=2\x01";
        let message = alloc_message(body);
        let (parsed, _) = parse(&message).unwrap();
        let cancel = OrderCancelRequest::from_message(&parsed).unwrap();
        assert_eq!(cancel.orig_cl_ord_id, b"A");

        let body = b"35=8\x0137=1\x0117=2\x01150=F\x0139=2\x0155=X\x0154=1\x01151=0\x0114=5\x01";
        let message = alloc_message(body);
        let (parsed, _) = parse(&message).unwrap();
        let report = ExecutionReport::from_message(&parsed).unwrap();
        assert_eq!(report.exec_type, b"F");
        assert_eq!(report.cum_qty, b"5");
    }

    #[test]
    fn missing_required_fields_are_named() {
        let message = alloc_message(b"35=D\x0111=A\x01");
        let (parsed, _) = parse(&message).unwrap();
        assert_eq!(
            NewOrderSingle::from_message(&parsed),
            Err(FixError::MissingField(TAG_SYMBOL))
        );
    }

    #[test]
    fn rejects_a_wrong_checksum() {
        let mut buffer = [0; 256];
        let length = new_order(&mut buffer);
        buffer[length - 2] = if buffer[length - 2] == b'9' {
            b'8'
        } else {
            b'9'
        };
        assert_eq!(parse(&buffer[..length]), Err(FixError::BadChecksum));
    }

    #[test]
    fn rejects_a_flipped_body_byte() {
        let mut buffer = [0; 256];
        let length = new_order(&mut buffer);
        buffer[length - 20] ^= 1;
        assert!(parse(&buffer[..length]).is_err());
    }

    #[test]
    fn rejects_other_versions_and_wrong_lengths() {
        assert_eq!(
            parse(b"8=FIX.4.2\x019=5\x0135=0\x0110=000\x01"),
            Err(FixError::UnsupportedVersion)
        );
        let mut message = alloc_message(b"35=0\x01");
        message[12] = b'9';
        assert!(parse(&message).is_err());
    }

    #[test]
    fn reports_incomplete_input_at_every_cut() {
        let mut buffer = [0; 256];
        let length = new_order(&mut buffer);
        for cut in 0..length {
            assert_eq!(
                parse(&buffer[..cut]),
                Err(FixError::Incomplete),
                "cut {cut}"
            );
        }
    }

    #[test]
    fn parses_a_stream_message_by_message() {
        let mut buffer = [0; 512];
        let first = new_order(&mut buffer);
        let (head, tail) = buffer.split_at_mut(first);
        let second = new_order(tail);
        assert_eq!(parse(head).unwrap().1, first);
        let mut stream = head.to_vec();
        stream.extend_from_slice(&tail[..second]);
        let (_, consumed) = parse(&stream).unwrap();
        let (_, next) = parse(&stream[consumed..]).unwrap();
        assert_eq!(consumed + next, stream.len());
    }

    #[test]
    fn a_small_buffer_is_reported() {
        let mut buffer = [0; 16];
        assert_eq!(
            write_message(&mut buffer, b"0", &HEADER, &[]),
            Err(FixError::BufferTooSmall)
        );
    }

    proptest! {
        #[test]
        fn arbitrary_input_never_panics(input in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = parse(&input);
        }

        #[test]
        fn written_values_round_trip(
            symbol in "[A-Z0-9-]{1,16}",
            quantity in "[0-9.]{1,12}",
            sequence in any::<u64>(),
        ) {
            let mut buffer = [0; 512];
            let header = Header { sequence, ..HEADER };
            let length = write_message(&mut buffer, b"D", &header, &[
                Field { tag: TAG_SYMBOL, value: symbol.as_bytes() },
                Field { tag: TAG_ORDER_QTY, value: quantity.as_bytes() },
            ]).unwrap();
            let (message, consumed) = parse(&buffer[..length]).unwrap();
            prop_assert_eq!(consumed, length);
            prop_assert_eq!(message.get(TAG_SYMBOL), Some(symbol.as_bytes()));
            prop_assert_eq!(message.get(TAG_ORDER_QTY), Some(quantity.as_bytes()));
            let sequence_text = sequence.to_string();
            prop_assert_eq!(message.get(TAG_MSG_SEQ_NUM), Some(sequence_text.as_bytes()));
        }
    }
}
