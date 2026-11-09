//! The interface every market data venue implements.

use std::fmt;
use std::time::Duration;

use ringbook_core::{Event, EventTime, ExchangeTime, InstrumentId, MeasureStamp, Payload, VenueId};

/// The kind of contract an instrument is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContractType {
    /// A linear perpetual, the only type the engine supports.
    LinearPerpetual,
    /// Anything else.
    Unsupported,
}

/// An instrument's trading rules, as published by its venue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstrumentMeta {
    /// The venue's symbol.
    pub symbol: String,
    /// Decimal places in the instrument's prices.
    pub price_decimals: u8,
    /// The finest price increment, in scaled units.
    pub tick: i64,
    /// Decimal places in the instrument's quantities.
    pub qty_decimals: u8,
    /// A significant-figure limit on order prices, if the venue has one.
    pub max_significant_figures: Option<u8>,
    /// The contract type.
    pub contract: ContractType,
}

/// An instrument bound to its runtime identifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstrumentSpec {
    /// The identifier carried on events.
    pub id: InstrumentId,
    /// The venue's published rules.
    pub meta: InstrumentMeta,
}

/// The HTTP method of a [`RestRequest`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestMethod {
    /// `GET`.
    Get,
    /// `POST` with a JSON body.
    Post,
}

/// A REST request an adapter wants made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestRequest {
    /// The method.
    pub method: RestMethod,
    /// The full URL.
    pub url: String,
    /// The JSON body for a `POST`.
    pub body: Option<String>,
}

/// A WebSocket connection an adapter needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// The WebSocket URL.
    pub url: String,
    /// Text messages sent after connecting.
    pub subscriptions: Vec<String>,
    /// A text message sent periodically to keep the connection open.
    pub keepalive: Option<String>,
    /// Silence longer than this is reported as a stall.
    pub stall_timeout: Duration,
}

/// A change in connection state, recorded so that replay reproduces outages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnEvent {
    /// A connection opened.
    Connected = 1,
    /// A connection closed or failed.
    Disconnected = 2,
    /// No message arrived within the stall timeout.
    StallTimeout = 3,
    /// A REST request failed.
    RestFailed = 4,
    /// A live ring was full, so a batch was discarded.
    RingOverflow = 5,
}

impl ConnEvent {
    /// Decodes a recorded connection event.
    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Connected),
            2 => Some(Self::Disconnected),
            3 => Some(Self::StallTimeout),
            4 => Some(Self::RestFailed),
            5 => Some(Self::RingOverflow),
            _ => None,
        }
    }
}

/// One input to a parser, live or recorded.
#[derive(Clone, Copy, Debug)]
pub enum Input<'a> {
    /// A WebSocket text frame.
    Frame {
        /// The endpoint's index.
        stream: u8,
        /// The frame's bytes.
        data: &'a [u8],
    },
    /// A successful REST response.
    Rest {
        /// The tag from the request's [`Action::Rest`].
        tag: u32,
        /// The response body.
        body: &'a [u8],
    },
    /// A connection event.
    Conn {
        /// The endpoint's index.
        stream: u8,
        /// The tag of the failed request, for [`ConnEvent::RestFailed`].
        tag: u32,
        /// What happened.
        event: ConnEvent,
    },
}

/// Follow-up work a parser requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Make a REST request and return the response as [`Input::Rest`].
    Rest {
        /// Identifies the request when its response arrives.
        tag: u32,
        /// The request.
        request: RestRequest,
    },
}

/// Collects a parser's events and actions, reusing its buffers.
#[derive(Debug, Default)]
pub struct ParseOutput {
    /// Events produced so far.
    pub events: Vec<Event>,
    /// Actions requested so far.
    pub actions: Vec<Action>,
    event_time: Option<EventTime>,
    measure_stamp: Option<MeasureStamp>,
}

impl ParseOutput {
    /// Clears the output and sets the stamps for the next input.
    pub fn begin(&mut self, event_time: EventTime, measure_stamp: MeasureStamp) {
        self.events.clear();
        self.actions.clear();
        self.event_time = Some(event_time);
        self.measure_stamp = Some(measure_stamp);
    }

    /// Appends an event.
    ///
    /// # Panics
    ///
    /// Panics if called before [`ParseOutput::begin`].
    pub fn push(
        &mut self,
        venue: VenueId,
        instrument: InstrumentId,
        exchange_time: Option<ExchangeTime>,
        payload: Payload,
    ) {
        self.events.push(Event {
            event_time: self.event_time.expect("begin must be called first"),
            measure_stamp: self.measure_stamp.expect("begin must be called first"),
            exchange_time,
            instrument,
            venue,
            end_of_batch: false,
            payload,
        });
    }

    /// Marks the most recent event as the end of its batch.
    pub fn end_batch(&mut self) {
        if let Some(last) = self.events.last_mut() {
            last.end_of_batch = true;
        }
    }
}

/// Counters a parser keeps about its input.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ParserStats {
    /// Frames that could not be understood.
    pub malformed: u64,
    /// Book rebuilds started.
    pub resyncs: u64,
    /// Sequence gaps detected.
    pub gaps: u64,
    /// Duplicate trades suppressed.
    pub duplicate_trades: u64,
}

/// A venue's state machine: inputs in, normalised events out, with no I/O.
pub trait FrameParser: Send {
    /// Processes one input, appending to `output`.
    fn on_input(&mut self, input: Input<'_>, output: &mut ParseOutput);

    /// Returns the parser's counters.
    fn stats(&self) -> ParserStats;
}

impl FrameParser for Box<dyn FrameParser> {
    fn on_input(&mut self, input: Input<'_>, output: &mut ParseOutput) {
        (**self).on_input(input, output);
    }

    fn stats(&self) -> ParserStats {
        (**self).stats()
    }
}

/// A plugin that connects ringbook to one market data venue.
pub trait VenueAdapter: Send + Sync + 'static {
    /// The venue's state machine.
    type Parser: FrameParser + 'static;

    /// The name the venue is selected by in the config.
    fn name(&self) -> &'static str;

    /// The request that fetches instrument metadata.
    fn metadata_request(&self) -> RestRequest;

    /// Extracts the rules for the requested symbols from a metadata response.
    ///
    /// # Errors
    ///
    /// Returns an [`AdapterError`] if the response is unreadable or a symbol
    /// isn't listed.
    fn parse_metadata(
        &self,
        body: &[u8],
        symbols: &[String],
    ) -> Result<Vec<InstrumentMeta>, AdapterError>;

    /// The WebSocket connections needed for the given symbols.
    fn endpoints(&self, symbols: &[String]) -> Vec<Endpoint>;

    /// Creates a parser for the given instruments.
    fn parser(&self, venue: VenueId, instruments: Vec<InstrumentSpec>) -> Self::Parser;
}

/// Why an adapter could not use a venue's metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdapterError {
    /// The metadata response could not be read.
    BadMetadata(String),
    /// The venue doesn't list this symbol.
    UnknownSymbol(String),
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMetadata(detail) => write!(formatter, "unreadable metadata: {detail}"),
            Self::UnknownSymbol(symbol) => write!(formatter, "venue does not list {symbol}"),
        }
    }
}

impl std::error::Error for AdapterError {}
