//! Binance USD-M perpetuals market data adapter.
//!
//! The book follows Binance's local order book procedure: buffer the diff
//! stream, apply a REST snapshot, bridge it to the stream, then require every
//! event's `pu` to equal the previous event's `u`.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::time::Duration;

use ringbook_core::{
    ExchangeTime, InstrumentId, Payload, Price, Qty, Scale, Side, VenueId, VenueState,
};
use ringbook_feeds::{
    Action, AdapterError, ConnEvent, ContractType, Endpoint, FrameParser, Input, InstrumentMeta,
    InstrumentSpec, ParseOutput, ParserStats, RestMethod, RestRequest, VenueAdapter,
};
use serde::Deserialize;

const REST_ROOT: &str = "https://fapi.binance.com";
const PUBLIC_WS: &str = "wss://fstream.binance.com/public/stream";
const MARKET_WS: &str = "wss://fstream.binance.com/market/stream";
const STALL_TIMEOUT: Duration = Duration::from_secs(10);
const SNAPSHOT_DEPTH: u32 = 1000;
const MAX_BUFFERED: usize = 4096;
/// The endpoint carrying the depth stream.
const DEPTH_STREAM: u8 = 0;

/// The Binance USD-M adapter.
#[derive(Clone, Copy, Debug, Default)]
pub struct Binance;

#[derive(Deserialize)]
struct ExchangeInfo<'a> {
    #[serde(borrow)]
    symbols: Vec<SymbolInfo<'a>>,
}

#[derive(Deserialize)]
struct SymbolInfo<'a> {
    symbol: &'a str,
    #[serde(rename = "contractType")]
    contract_type: &'a str,
    #[serde(rename = "marginAsset")]
    margin_asset: &'a str,
    #[serde(borrow)]
    filters: Vec<Filter<'a>>,
}

#[derive(Deserialize)]
struct Filter<'a> {
    #[serde(rename = "filterType")]
    filter_type: &'a str,
    #[serde(rename = "tickSize", default, borrow)]
    tick_size: Option<&'a str>,
    #[serde(rename = "stepSize", default, borrow)]
    step_size: Option<&'a str>,
}

/// Returns the decimal places a step like `0.10` needs, and its scaled value.
fn step_scale(step: &str) -> Option<(u8, i64)> {
    let trimmed = if step.contains('.') {
        step.trim_end_matches('0').trim_end_matches('.')
    } else {
        step
    };
    let decimals = trimmed
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let decimals = u8::try_from(decimals).ok()?;
    let scaled = Scale::new(decimals)?.parse(trimmed.as_bytes()).ok()?;
    (scaled > 0).then_some((decimals, scaled))
}

impl VenueAdapter for Binance {
    type Parser = Parser;

    fn name(&self) -> &'static str {
        "binance"
    }

    fn metadata_request(&self) -> RestRequest {
        RestRequest {
            method: RestMethod::Get,
            url: format!("{REST_ROOT}/fapi/v1/exchangeInfo"),
            body: None,
        }
    }

    fn parse_metadata(
        &self,
        body: &[u8],
        symbols: &[String],
    ) -> Result<Vec<InstrumentMeta>, AdapterError> {
        let info: ExchangeInfo<'_> = sonic_rs::from_slice(body)
            .map_err(|error| AdapterError::BadMetadata(error.to_string()))?;
        symbols
            .iter()
            .map(|symbol| {
                let found = info
                    .symbols
                    .iter()
                    .find(|candidate| candidate.symbol == symbol)
                    .ok_or_else(|| AdapterError::UnknownSymbol(symbol.clone()))?;
                let filter = |name: &str| {
                    found
                        .filters
                        .iter()
                        .find(|filter| filter.filter_type == name)
                };
                let missing =
                    |name: &str| AdapterError::BadMetadata(format!("{symbol}: missing {name}"));
                let (price_decimals, tick) = filter("PRICE_FILTER")
                    .and_then(|filter| filter.tick_size)
                    .and_then(step_scale)
                    .ok_or_else(|| missing("PRICE_FILTER"))?;
                let (qty_decimals, _) = filter("LOT_SIZE")
                    .and_then(|filter| filter.step_size)
                    .and_then(step_scale)
                    .ok_or_else(|| missing("LOT_SIZE"))?;
                let linear = found.contract_type == "PERPETUAL" && found.margin_asset != "BTC";
                Ok(InstrumentMeta {
                    symbol: symbol.clone(),
                    price_decimals,
                    tick,
                    qty_decimals,
                    max_significant_figures: None,
                    contract: if linear {
                        ContractType::LinearPerpetual
                    } else {
                        ContractType::Unsupported
                    },
                })
            })
            .collect()
    }

    fn endpoints(&self, symbols: &[String]) -> Vec<Endpoint> {
        let streams = |suffix: &str| {
            symbols
                .iter()
                .map(|symbol| format!("{}@{suffix}", symbol.to_lowercase()))
                .collect::<Vec<_>>()
                .join("/")
        };
        [
            format!("{PUBLIC_WS}?streams={}", streams("depth@100ms")),
            format!("{MARKET_WS}?streams={}", streams("aggTrade")),
        ]
        .map(|url| Endpoint {
            url,
            subscriptions: Vec::new(),
            keepalive: None,
            stall_timeout: STALL_TIMEOUT,
        })
        .into()
    }

    fn parser(&self, venue: VenueId, instruments: Vec<InstrumentSpec>) -> Parser {
        Parser::new(venue, instruments)
    }
}

#[derive(Debug)]
struct Depth {
    first: u64,
    last: u64,
    previous: u64,
    time: Option<ExchangeTime>,
    levels: Vec<(Side, Price, Qty)>,
}

#[derive(Debug)]
enum Sync {
    /// Disconnected; waiting for the depth stream to return.
    Down,
    /// Waiting for a REST snapshot while buffering the stream.
    AwaitingSnapshot { buffer: Vec<Depth> },
    /// A snapshot is applied; the next event must bridge to it.
    Bridging { snapshot: u64 },
    /// In sequence.
    Live { last: u64 },
}

struct Instrument {
    id: InstrumentId,
    symbol: String,
    price_scale: Scale,
    qty_scale: Scale,
    sync: Sync,
}

/// Binance's state machine.
pub struct Parser {
    venue: VenueId,
    instruments: Vec<Instrument>,
    by_symbol: HashMap<String, usize>,
    stats: ParserStats,
}

#[derive(Deserialize)]
struct Combined<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: sonic_rs::LazyValue<'a>,
}

#[derive(Deserialize)]
struct DepthMessage<'a> {
    #[serde(rename = "E")]
    event_time: u64,
    s: &'a str,
    #[serde(rename = "U")]
    first: u64,
    u: u64,
    pu: u64,
    #[serde(borrow)]
    b: Vec<(&'a str, &'a str)>,
    #[serde(borrow)]
    a: Vec<(&'a str, &'a str)>,
}

#[derive(Deserialize)]
struct AggTrade<'a> {
    s: &'a str,
    #[serde(rename = "p")]
    price: &'a str,
    #[serde(rename = "q")]
    qty: &'a str,
    #[serde(rename = "T")]
    trade_time: u64,
    /// True when the buyer was the maker, meaning the seller was the aggressor.
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

#[derive(Deserialize)]
struct Snapshot<'a> {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "E", default)]
    event_time: u64,
    #[serde(borrow)]
    bids: Vec<(&'a str, &'a str)>,
    #[serde(borrow)]
    asks: Vec<(&'a str, &'a str)>,
}

fn exchange_time(milliseconds: u64) -> Option<ExchangeTime> {
    NonZeroU64::new(milliseconds.saturating_mul(1_000_000)).map(ExchangeTime)
}

fn parse_levels(
    instrument: &Instrument,
    bids: &[(&str, &str)],
    asks: &[(&str, &str)],
) -> Result<Vec<(Side, Price, Qty)>, ()> {
    let mut levels = Vec::with_capacity(bids.len() + asks.len());
    for (side, entries) in [(Side::Buy, bids), (Side::Sell, asks)] {
        for (price, qty) in entries {
            levels.push((
                side,
                Price::parse(instrument.price_scale, price.as_bytes()).map_err(|_| ())?,
                Qty::parse(instrument.qty_scale, qty.as_bytes()).map_err(|_| ())?,
            ));
        }
    }
    Ok(levels)
}

impl Parser {
    fn new(venue: VenueId, specs: Vec<InstrumentSpec>) -> Self {
        let mut by_symbol = HashMap::new();
        let mut instruments = Vec::with_capacity(specs.len());
        for (index, spec) in specs.into_iter().enumerate() {
            by_symbol.insert(spec.meta.symbol.clone(), index);
            instruments.push(Instrument {
                id: spec.id,
                price_scale: Scale::new(spec.meta.price_decimals).expect("adapter checked scale"),
                qty_scale: Scale::new(spec.meta.qty_decimals).expect("adapter checked scale"),
                symbol: spec.meta.symbol,
                sync: Sync::Down,
            });
        }
        Self {
            venue,
            instruments,
            by_symbol,
            stats: ParserStats::default(),
        }
    }

    fn snapshot_request(symbol: &str) -> RestRequest {
        RestRequest {
            method: RestMethod::Get,
            url: format!("{REST_ROOT}/fapi/v1/depth?symbol={symbol}&limit={SNAPSHOT_DEPTH}"),
            body: None,
        }
    }

    fn start_resync(&mut self, index: usize, output: &mut ParseOutput) {
        self.stats.resyncs += 1;
        let instrument = &mut self.instruments[index];
        instrument.sync = Sync::AwaitingSnapshot { buffer: Vec::new() };
        output.push(
            self.venue,
            instrument.id,
            None,
            Payload::VenueStatus(VenueState::Resyncing),
        );
        output.end_batch();
        output.actions.push(Action::Rest {
            tag: index as u32,
            request: Self::snapshot_request(&instrument.symbol),
        });
    }

    fn on_frame(&mut self, data: &[u8], output: &mut ParseOutput) -> Result<(), ()> {
        let combined: Combined<'_> = sonic_rs::from_slice(data).map_err(|_| ())?;
        let raw = combined.data.as_raw_str();
        if combined.stream.ends_with("@aggTrade") {
            let trade: AggTrade<'_> = sonic_rs::from_str(raw).map_err(|_| ())?;
            self.on_trade(&trade, output)
        } else if combined.stream.contains("@depth") {
            let depth: DepthMessage<'_> = sonic_rs::from_str(raw).map_err(|_| ())?;
            self.on_depth(&depth, output)
        } else {
            Ok(())
        }
    }

    fn on_trade(&mut self, trade: &AggTrade<'_>, output: &mut ParseOutput) -> Result<(), ()> {
        let Some(&index) = self.by_symbol.get(trade.s) else {
            return Ok(());
        };
        let instrument = &self.instruments[index];
        let price = Price::parse(instrument.price_scale, trade.price.as_bytes()).map_err(|_| ())?;
        let qty = Qty::parse(instrument.qty_scale, trade.qty.as_bytes()).map_err(|_| ())?;
        output.push(
            self.venue,
            instrument.id,
            exchange_time(trade.trade_time),
            Payload::Trade {
                aggressor: if trade.buyer_is_maker {
                    Side::Sell
                } else {
                    Side::Buy
                },
                price,
                qty,
            },
        );
        output.end_batch();
        Ok(())
    }

    fn on_depth(&mut self, message: &DepthMessage<'_>, output: &mut ParseOutput) -> Result<(), ()> {
        let Some(&index) = self.by_symbol.get(message.s) else {
            return Ok(());
        };
        let depth = Depth {
            first: message.first,
            last: message.u,
            previous: message.pu,
            time: exchange_time(message.event_time),
            levels: parse_levels(&self.instruments[index], &message.b, &message.a)?,
        };

        match &mut self.instruments[index].sync {
            Sync::Down => {}
            Sync::AwaitingSnapshot { buffer } => {
                if buffer.len() == MAX_BUFFERED {
                    self.stats.gaps += 1;
                    self.start_resync(index, output);
                } else {
                    buffer.push(depth);
                }
            }
            Sync::Bridging { .. } | Sync::Live { .. } => {
                self.advance(index, &depth, output);
                output.end_batch();
            }
        }
        Ok(())
    }

    /// Applies one in-order event, or starts a resync if it doesn't follow.
    fn advance(&mut self, index: usize, depth: &Depth, output: &mut ParseOutput) {
        let venue = self.venue;
        let instrument = &mut self.instruments[index];
        let became_live = match instrument.sync {
            Sync::Bridging { snapshot } => {
                if depth.last < snapshot {
                    return;
                }
                if depth.first > snapshot {
                    self.stats.gaps += 1;
                    self.start_resync(index, output);
                    return;
                }
                true
            }
            Sync::Live { last } => {
                if depth.previous != last {
                    self.stats.gaps += 1;
                    self.start_resync(index, output);
                    return;
                }
                false
            }
            Sync::Down | Sync::AwaitingSnapshot { .. } => return,
        };

        for &(side, price, qty) in &depth.levels {
            output.push(
                venue,
                instrument.id,
                depth.time,
                Payload::BookLevel { side, price, qty },
            );
        }
        if became_live {
            output.push(
                venue,
                instrument.id,
                depth.time,
                Payload::VenueStatus(VenueState::Live),
            );
        }
        instrument.sync = Sync::Live { last: depth.last };
    }

    fn on_snapshot(&mut self, tag: u32, body: &[u8], output: &mut ParseOutput) -> Result<(), ()> {
        let index = tag as usize;
        let Some(instrument) = self.instruments.get_mut(index) else {
            return Ok(());
        };
        let Sync::AwaitingSnapshot { buffer } = &mut instrument.sync else {
            return Ok(());
        };
        let buffered = std::mem::take(buffer);

        let snapshot: Snapshot<'_> = sonic_rs::from_slice(body).map_err(|_| ())?;
        let levels = parse_levels(instrument, &snapshot.bids, &snapshot.asks)?;
        let (venue, id) = (self.venue, instrument.id);
        let time = exchange_time(snapshot.event_time);
        output.push(venue, id, time, Payload::BookClear);
        for (side, price, qty) in levels {
            if !qty.is_zero() {
                output.push(venue, id, time, Payload::BookLevel { side, price, qty });
            }
        }
        instrument.sync = Sync::Bridging {
            snapshot: snapshot.last_update_id,
        };

        for depth in &buffered {
            self.advance(index, depth, output);
            if matches!(self.instruments[index].sync, Sync::AwaitingSnapshot { .. }) {
                return Ok(());
            }
        }
        output.end_batch();
        Ok(())
    }

    fn on_conn(&mut self, stream: u8, tag: u32, event: ConnEvent, output: &mut ParseOutput) {
        match event {
            ConnEvent::Connected if stream == DEPTH_STREAM => {
                for index in 0..self.instruments.len() {
                    self.start_resync(index, output);
                }
            }
            ConnEvent::Disconnected | ConnEvent::StallTimeout if stream == DEPTH_STREAM => {
                for instrument in &mut self.instruments {
                    instrument.sync = Sync::Down;
                    output.push(
                        self.venue,
                        instrument.id,
                        None,
                        Payload::VenueStatus(VenueState::Stale),
                    );
                }
                output.end_batch();
            }
            ConnEvent::RingOverflow => {
                for index in 0..self.instruments.len() {
                    if !matches!(self.instruments[index].sync, Sync::Down) {
                        self.start_resync(index, output);
                    }
                }
            }
            ConnEvent::RestFailed => {
                let index = tag as usize;
                if let Some(instrument) = self.instruments.get(index)
                    && matches!(instrument.sync, Sync::AwaitingSnapshot { .. })
                {
                    output.actions.push(Action::Rest {
                        tag,
                        request: Self::snapshot_request(&instrument.symbol),
                    });
                }
            }
            ConnEvent::Connected | ConnEvent::Disconnected | ConnEvent::StallTimeout => {}
        }
    }
}

impl FrameParser for Parser {
    fn on_input(&mut self, input: Input<'_>, output: &mut ParseOutput) {
        let result = match input {
            Input::Frame { data, .. } => self.on_frame(data, output),
            Input::Rest { tag, body } => self.on_snapshot(tag, body, output),
            Input::Conn { stream, tag, event } => {
                self.on_conn(stream, tag, event, output);
                Ok(())
            }
        };
        if result.is_err() {
            self.stats.malformed += 1;
        }
    }

    fn stats(&self) -> ParserStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbook_feeds::conformance::{
        assert_batched, assert_disconnect_reports_status, assert_ignores_junk, feed,
    };

    const INFO: &[u8] = br#"{"symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","marginAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10","minPrice":"1"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}"#;

    fn parser() -> Parser {
        let meta = Binance
            .parse_metadata(INFO, &["BTCUSDT".to_owned()])
            .unwrap();
        Binance.parser(
            VenueId(2),
            vec![InstrumentSpec {
                id: InstrumentId(0),
                meta: meta[0].clone(),
            }],
        )
    }

    fn connect(parser: &mut Parser) -> ParseOutput {
        feed(
            parser,
            Input::Conn {
                stream: 0,
                tag: 0,
                event: ConnEvent::Connected,
            },
        )
    }

    fn depth(first: u64, last: u64, previous: u64) -> String {
        format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1700000000000,"T":1,"s":"BTCUSDT","U":{first},"u":{last},"pu":{previous},"b":[["73000.10","1.500"]],"a":[]}}}}"#
        )
    }

    fn frame(parser: &mut Parser, data: &str) -> ParseOutput {
        feed(
            parser,
            Input::Frame {
                stream: 0,
                data: data.as_bytes(),
            },
        )
    }

    fn snapshot(parser: &mut Parser, last_update_id: u64) -> ParseOutput {
        let body = format!(
            r#"{{"lastUpdateId":{last_update_id},"E":1700000000000,"T":1,"bids":[["73000.00","2.000"]],"asks":[["73000.20","1.000"]]}}"#
        );
        feed(
            parser,
            Input::Rest {
                tag: 0,
                body: body.as_bytes(),
            },
        )
    }

    fn status(output: &ParseOutput) -> Vec<VenueState> {
        output
            .events
            .iter()
            .filter_map(|event| match event.payload {
                Payload::VenueStatus(state) => Some(state),
                _ => None,
            })
            .collect()
    }

    fn synced() -> Parser {
        let mut parser = parser();
        connect(&mut parser);
        frame(&mut parser, &depth(95, 105, 94));
        snapshot(&mut parser, 100);
        parser
    }

    #[test]
    fn metadata_handles_trailing_zeros_in_steps() {
        let meta = Binance
            .parse_metadata(INFO, &["BTCUSDT".to_owned()])
            .unwrap();
        assert_eq!(meta[0].price_decimals, 1);
        assert_eq!(meta[0].tick, 1);
        assert_eq!(meta[0].qty_decimals, 3);
    }

    #[test]
    fn step_scale_reads_whole_and_fractional_steps() {
        assert_eq!(step_scale("0.10"), Some((1, 1)));
        assert_eq!(step_scale("0.005"), Some((3, 5)));
        assert_eq!(step_scale("10"), Some((0, 10)));
        assert_eq!(step_scale("1.000"), Some((0, 1)));
        assert_eq!(step_scale("0"), None);
    }

    #[test]
    fn connecting_requests_a_snapshot_and_reports_resyncing() {
        let mut parser = parser();
        let out = connect(&mut parser);
        assert_eq!(status(&out), [VenueState::Resyncing]);
        assert_eq!(out.actions.len(), 1);
    }

    #[test]
    fn a_snapshot_bridges_to_the_buffered_stream() {
        let mut parser = parser();
        connect(&mut parser);
        assert!(frame(&mut parser, &depth(90, 95, 89)).events.is_empty());
        assert!(frame(&mut parser, &depth(96, 105, 95)).events.is_empty());

        let out = snapshot(&mut parser, 100);
        assert_batched(&out);
        assert_eq!(out.events[0].payload, Payload::BookClear);
        assert_eq!(status(&out), [VenueState::Live]);
        // Clear, two snapshot levels, one bridged level and the status.
        assert_eq!(out.events.len(), 5);
    }

    #[test]
    fn events_in_sequence_apply_and_end_their_batch() {
        let mut parser = synced();
        let out = frame(&mut parser, &depth(106, 110, 105));
        assert_batched(&out);
        assert_eq!(
            out.events[0].payload,
            Payload::BookLevel {
                side: Side::Buy,
                price: Price(730_001),
                qty: Qty(1500),
            }
        );
    }

    #[test]
    fn a_sequence_gap_triggers_a_resync() {
        let mut parser = synced();
        let out = frame(&mut parser, &depth(120, 125, 119));
        assert_eq!(status(&out), [VenueState::Resyncing]);
        assert_eq!(out.actions.len(), 1);
        assert_eq!(parser.stats().gaps, 1);
        assert!(
            !out.events
                .iter()
                .any(|event| matches!(event.payload, Payload::BookLevel { .. }))
        );
    }

    #[test]
    fn a_snapshot_newer_than_the_first_event_is_a_gap_only_if_nothing_bridges() {
        let mut parser = parser();
        connect(&mut parser);
        frame(&mut parser, &depth(150, 160, 149));
        let out = snapshot(&mut parser, 100);
        assert_eq!(parser.stats().gaps, 1);
        assert_eq!(status(&out), [VenueState::Resyncing]);
    }

    #[test]
    fn a_failed_snapshot_is_requested_again() {
        let mut parser = parser();
        connect(&mut parser);
        let out = feed(
            &mut parser,
            Input::Conn {
                stream: 0,
                tag: 0,
                event: ConnEvent::RestFailed,
            },
        );
        assert_eq!(out.actions.len(), 1);
    }

    #[test]
    fn a_trade_stream_disconnect_leaves_the_book_alone() {
        let mut parser = synced();
        let out = feed(
            &mut parser,
            Input::Conn {
                stream: 1,
                tag: 0,
                event: ConnEvent::Disconnected,
            },
        );
        assert!(out.events.is_empty());
    }

    #[test]
    fn aggregate_trades_map_the_maker_flag_to_the_aggressor() {
        let mut parser = parser();
        let trade = |maker: bool| {
            format!(
                r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"73000.10","q":"0.500","f":1,"l":1,"T":2,"m":{maker}}}}}"#
            )
        };
        let sold = frame(&mut parser, &trade(true));
        assert_batched(&sold);
        assert!(matches!(
            sold.events[0].payload,
            Payload::Trade {
                aggressor: Side::Sell,
                ..
            }
        ));
        assert!(matches!(
            frame(&mut parser, &trade(false)).events[0].payload,
            Payload::Trade {
                aggressor: Side::Buy,
                ..
            }
        ));
    }

    #[test]
    fn meets_the_adapter_contract() {
        let mut parser = synced();
        assert_ignores_junk(&mut parser);
        assert_disconnect_reports_status(&mut parser);
    }
}
