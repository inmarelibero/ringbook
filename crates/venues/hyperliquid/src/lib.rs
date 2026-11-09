//! Hyperliquid market data adapter.
//!
//! `l2Book` messages are full snapshots with no sequence number, so there are
//! no gaps to detect. The failure to watch for is a stall.

#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU64;
use std::time::Duration;

use ringbook_core::{
    ExchangeTime, InstrumentId, Payload, Price, Qty, Scale, Side, VenueId, VenueState,
};
use ringbook_feeds::{
    AdapterError, ConnEvent, ContractType, Endpoint, FrameParser, Input, InstrumentMeta,
    InstrumentSpec, ParseOutput, ParserStats, RestMethod, RestRequest, VenueAdapter,
};
use serde::Deserialize;

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const PERP_MAX_DECIMALS: u8 = 6;
const PRICE_SIGNIFICANT_FIGURES: u8 = 5;
const STALL_TIMEOUT: Duration = Duration::from_secs(10);
const SEEN_TRADES: usize = 1024;

/// The Hyperliquid adapter.
#[derive(Clone, Copy, Debug, Default)]
pub struct Hyperliquid;

#[derive(Deserialize)]
struct Meta<'a> {
    #[serde(borrow)]
    universe: Vec<Asset<'a>>,
}

#[derive(Deserialize)]
struct Asset<'a> {
    name: &'a str,
    #[serde(rename = "szDecimals")]
    sz_decimals: u8,
}

impl VenueAdapter for Hyperliquid {
    type Parser = Parser;

    fn name(&self) -> &'static str {
        "hyperliquid"
    }

    fn metadata_request(&self) -> RestRequest {
        RestRequest {
            method: RestMethod::Post,
            url: INFO_URL.to_owned(),
            body: Some(r#"{"type":"meta"}"#.to_owned()),
        }
    }

    fn parse_metadata(
        &self,
        body: &[u8],
        symbols: &[String],
    ) -> Result<Vec<InstrumentMeta>, AdapterError> {
        let meta: Meta<'_> = sonic_rs::from_slice(body)
            .map_err(|error| AdapterError::BadMetadata(error.to_string()))?;
        symbols
            .iter()
            .map(|symbol| {
                let asset = meta
                    .universe
                    .iter()
                    .find(|asset| asset.name == symbol)
                    .ok_or_else(|| AdapterError::UnknownSymbol(symbol.clone()))?;
                let price_decimals = PERP_MAX_DECIMALS
                    .checked_sub(asset.sz_decimals)
                    .ok_or_else(|| {
                        AdapterError::BadMetadata(format!("{symbol}: szDecimals out of range"))
                    })?;
                Ok(InstrumentMeta {
                    symbol: symbol.clone(),
                    price_decimals,
                    tick: 1,
                    qty_decimals: asset.sz_decimals,
                    max_significant_figures: Some(PRICE_SIGNIFICANT_FIGURES),
                    contract: ContractType::LinearPerpetual,
                })
            })
            .collect()
    }

    fn endpoints(&self, symbols: &[String]) -> Vec<Endpoint> {
        let subscriptions = symbols
            .iter()
            .flat_map(|symbol| {
                ["l2Book", "trades"].map(|channel| {
                    format!(
                        r#"{{"method":"subscribe","subscription":{{"type":"{channel}","coin":"{symbol}"}}}}"#
                    )
                })
            })
            .collect();
        vec![Endpoint {
            url: WS_URL.to_owned(),
            subscriptions,
            keepalive: Some(r#"{"method":"ping"}"#.to_owned()),
            stall_timeout: STALL_TIMEOUT,
        }]
    }

    fn parser(&self, venue: VenueId, instruments: Vec<InstrumentSpec>) -> Parser {
        Parser::new(venue, instruments)
    }
}

struct Instrument {
    id: InstrumentId,
    price_scale: Scale,
    qty_scale: Scale,
    live: bool,
    seen_order: VecDeque<(u64, u64)>,
}

/// Hyperliquid's state machine.
pub struct Parser {
    venue: VenueId,
    instruments: Vec<Instrument>,
    by_symbol: HashMap<String, usize>,
    stats: ParserStats,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    channel: &'a str,
    #[serde(borrow, default)]
    data: Option<sonic_rs::LazyValue<'a>>,
}

#[derive(Deserialize)]
struct WsBook<'a> {
    coin: &'a str,
    time: u64,
    #[serde(borrow)]
    levels: (Vec<WsLevel<'a>>, Vec<WsLevel<'a>>),
}

#[derive(Deserialize)]
struct WsLevel<'a> {
    px: &'a str,
    sz: &'a str,
}

#[derive(Deserialize)]
struct WsTrade<'a> {
    coin: &'a str,
    side: &'a str,
    px: &'a str,
    sz: &'a str,
    time: u64,
    tid: u64,
}

fn exchange_time(milliseconds: u64) -> Option<ExchangeTime> {
    NonZeroU64::new(milliseconds.saturating_mul(1_000_000)).map(ExchangeTime)
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
                live: false,
                seen_order: VecDeque::with_capacity(SEEN_TRADES),
            });
        }
        Self {
            venue,
            instruments,
            by_symbol,
            stats: ParserStats::default(),
        }
    }

    fn on_frame(&mut self, data: &[u8], output: &mut ParseOutput) -> Result<(), ()> {
        let envelope: Envelope<'_> = sonic_rs::from_slice(data).map_err(|_| ())?;
        match (envelope.channel, &envelope.data) {
            ("l2Book", Some(data)) => {
                let book: WsBook<'_> = sonic_rs::from_str(data.as_raw_str()).map_err(|_| ())?;
                self.on_book(&book, output)
            }
            ("trades", Some(data)) => {
                let trades: Vec<WsTrade<'_>> =
                    sonic_rs::from_str(data.as_raw_str()).map_err(|_| ())?;
                self.on_trades(&trades, output)
            }
            ("l2Book" | "trades", None) => Err(()),
            _ => Ok(()),
        }
    }

    fn on_book(&mut self, book: &WsBook<'_>, output: &mut ParseOutput) -> Result<(), ()> {
        let Some(&index) = self.by_symbol.get(book.coin) else {
            return Ok(());
        };
        let start = output.events.len();
        let instrument = &mut self.instruments[index];
        let time = exchange_time(book.time);
        let (venue, id, price_scale, qty_scale) = (
            self.venue,
            instrument.id,
            instrument.price_scale,
            instrument.qty_scale,
        );

        let became_live = !instrument.live;
        if became_live {
            output.push(venue, id, time, Payload::VenueStatus(VenueState::Resyncing));
        }
        output.push(venue, id, time, Payload::BookClear);

        for (side, levels) in [(Side::Buy, &book.levels.0), (Side::Sell, &book.levels.1)] {
            for level in levels {
                let parsed = (
                    Price::parse(price_scale, level.px.as_bytes()),
                    Qty::parse(qty_scale, level.sz.as_bytes()),
                );
                let (Ok(price), Ok(qty)) = parsed else {
                    output.events.truncate(start);
                    return Err(());
                };
                output.push(venue, id, time, Payload::BookLevel { side, price, qty });
            }
        }

        if became_live {
            instrument.live = true;
            output.push(venue, id, time, Payload::VenueStatus(VenueState::Live));
        }
        output.end_batch();
        Ok(())
    }

    fn on_trades(&mut self, trades: &[WsTrade<'_>], output: &mut ParseOutput) -> Result<(), ()> {
        let start = output.events.len();
        for trade in trades {
            let Some(&index) = self.by_symbol.get(trade.coin) else {
                continue;
            };
            let instrument = &mut self.instruments[index];
            let key = (trade.time, trade.tid);
            if instrument.seen_order.contains(&key) {
                self.stats.duplicate_trades += 1;
                continue;
            }

            let aggressor = match trade.side {
                "B" => Side::Buy,
                "A" => Side::Sell,
                _ => {
                    output.events.truncate(start);
                    return Err(());
                }
            };
            let parsed = (
                Price::parse(instrument.price_scale, trade.px.as_bytes()),
                Qty::parse(instrument.qty_scale, trade.sz.as_bytes()),
            );
            let (Ok(price), Ok(qty)) = parsed else {
                output.events.truncate(start);
                return Err(());
            };

            if instrument.seen_order.len() == SEEN_TRADES {
                instrument.seen_order.pop_front();
            }
            instrument.seen_order.push_back(key);
            output.push(
                self.venue,
                instrument.id,
                exchange_time(trade.time),
                Payload::Trade {
                    aggressor,
                    price,
                    qty,
                },
            );
        }
        output.end_batch();
        Ok(())
    }

    fn mark_all_stale(&mut self, output: &mut ParseOutput) {
        for instrument in &mut self.instruments {
            instrument.live = false;
            output.push(
                self.venue,
                instrument.id,
                None,
                Payload::VenueStatus(VenueState::Stale),
            );
        }
        output.end_batch();
    }
}

impl FrameParser for Parser {
    fn on_input(&mut self, input: Input<'_>, output: &mut ParseOutput) {
        match input {
            Input::Frame { data, .. } => {
                if self.on_frame(data, output).is_err() {
                    self.stats.malformed += 1;
                }
            }
            Input::Rest { .. } => {}
            Input::Conn { event, .. } => match event {
                ConnEvent::Disconnected | ConnEvent::StallTimeout => self.mark_all_stale(output),
                ConnEvent::RingOverflow => {
                    // The next snapshot heals the book by itself.
                    self.stats.resyncs += 1;
                    self.mark_all_stale(output);
                }
                ConnEvent::Connected | ConnEvent::RestFailed => {}
            },
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

    const META: &[u8] =
        br#"{"universe":[{"szDecimals":5,"name":"BTC","maxLeverage":40},{"szDecimals":4,"name":"ETH"}]}"#;

    fn parser() -> Parser {
        let symbols = ["BTC".to_owned()];
        let meta = Hyperliquid.parse_metadata(META, &symbols).unwrap();
        Hyperliquid.parser(
            VenueId(1),
            vec![InstrumentSpec {
                id: InstrumentId(0),
                meta: meta[0].clone(),
            }],
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

    const BOOK: &str = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1700000000000,"levels":[[{"px":"73000.0","sz":"1.5","n":3}],[{"px":"73001.0","sz":"0.25","n":1}]]}}"#;

    #[test]
    fn metadata_derives_scales_from_sz_decimals() {
        let meta = Hyperliquid
            .parse_metadata(META, &["ETH".to_owned()])
            .unwrap();
        assert_eq!(meta[0].price_decimals, 2);
        assert_eq!(meta[0].qty_decimals, 4);
        assert_eq!(meta[0].tick, 1);
        assert_eq!(meta[0].max_significant_figures, Some(5));
    }

    #[test]
    fn metadata_rejects_unlisted_symbols() {
        assert_eq!(
            Hyperliquid.parse_metadata(META, &["DOGE".to_owned()]),
            Err(AdapterError::UnknownSymbol("DOGE".to_owned()))
        );
    }

    #[test]
    fn a_book_replaces_all_levels_in_one_batch() {
        let mut parser = parser();
        let out = frame(&mut parser, BOOK);
        assert_batched(&out);
        let payloads: Vec<_> = out.events.iter().map(|event| event.payload).collect();
        assert_eq!(
            payloads,
            [
                Payload::VenueStatus(VenueState::Resyncing),
                Payload::BookClear,
                Payload::BookLevel {
                    side: Side::Buy,
                    price: Price(730_000),
                    qty: Qty(150_000),
                },
                Payload::BookLevel {
                    side: Side::Sell,
                    price: Price(730_010),
                    qty: Qty(25_000),
                },
                Payload::VenueStatus(VenueState::Live),
            ]
        );
        assert!(
            out.events[..out.events.len() - 1]
                .iter()
                .all(|event| !event.end_of_batch)
        );

        let again = frame(&mut parser, BOOK);
        assert_eq!(again.events.len(), 3);
    }

    #[test]
    fn trades_are_normalised_and_deduplicated_across_reconnects() {
        const TRADES: &str = r#"{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"73000.0","sz":"0.1","hash":"0x0","time":1700000000001,"tid":9,"users":["a","b"]}]}"#;
        let mut parser = parser();
        let out = frame(&mut parser, TRADES);
        assert_batched(&out);
        assert_eq!(
            out.events[0].payload,
            Payload::Trade {
                aggressor: Side::Sell,
                price: Price(730_000),
                qty: Qty(10_000),
            }
        );

        assert!(frame(&mut parser, TRADES).events.is_empty());
        assert_eq!(parser.stats().duplicate_trades, 1);
    }

    #[test]
    fn a_stall_marks_the_venue_stale_until_the_next_book() {
        let mut parser = parser();
        frame(&mut parser, BOOK);
        let stale = feed(
            &mut parser,
            Input::Conn {
                stream: 0,
                tag: 0,
                event: ConnEvent::StallTimeout,
            },
        );
        assert_batched(&stale);
        assert_eq!(
            stale.events[0].payload,
            Payload::VenueStatus(VenueState::Stale)
        );

        let live = frame(&mut parser, BOOK);
        assert_eq!(
            live.events.last().unwrap().payload,
            Payload::VenueStatus(VenueState::Live)
        );
    }

    #[test]
    fn keepalive_replies_are_not_malformed() {
        let mut parser = parser();
        assert!(
            frame(&mut parser, r#"{"channel":"pong"}"#)
                .events
                .is_empty()
        );
        assert_eq!(parser.stats().malformed, 0);
    }

    #[test]
    fn a_book_with_bad_numbers_emits_nothing() {
        let mut parser = parser();
        let bad = BOOK.replace("73001.0", "oops");
        assert!(frame(&mut parser, &bad).events.is_empty());
        assert_eq!(parser.stats().malformed, 1);
    }

    #[test]
    fn meets_the_adapter_contract() {
        let mut parser = parser();
        assert_ignores_junk(&mut parser);
        assert_disconnect_reports_status(&mut parser);
    }

    #[test]
    fn endpoints_subscribe_to_books_and_trades() {
        let endpoints = Hyperliquid.endpoints(&["BTC".to_owned()]);
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].subscriptions.len(), 2);
        assert!(endpoints[0].subscriptions[0].contains("l2Book"));
    }
}
