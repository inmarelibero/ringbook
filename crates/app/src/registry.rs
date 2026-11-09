//! The one place that knows which venues exist.

use std::sync::Arc;

use anyhow::{Result, bail};
use ringbook_core::{Event, VenueId};
use ringbook_feeds::{
    FeedConfig, FrameParser, InstrumentMeta, InstrumentSpec, VenueAdapter, fetch_metadata, run_feed,
};
use ringbook_ring::Producer;
use ringbook_venue_binance::Binance;
use ringbook_venue_hyperliquid::Hyperliquid;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Every venue compiled into this binary.
pub const KNOWN_VENUES: &[&str] = &["binance", "hyperliquid"];

macro_rules! with_adapter {
    ($name:expr, $adapter:ident => $body:expr) => {
        match $name {
            "binance" => {
                let $adapter = Binance;
                $body
            }
            "hyperliquid" => {
                let $adapter = Hyperliquid;
                $body
            }
            other => bail!("unknown venue {other}"),
        }
    };
}

/// Fetches instrument metadata, returning the rules and the raw response.
pub async fn metadata(venue: &str, symbols: &[String]) -> Result<(Vec<InstrumentMeta>, Vec<u8>)> {
    with_adapter!(venue, adapter => {
        fetch_metadata(&adapter, symbols).await.map_err(anyhow::Error::msg)
    })
}

/// Reads instrument metadata from a recorded response.
pub fn parse_metadata(venue: &str, body: &[u8], symbols: &[String]) -> Result<Vec<InstrumentMeta>> {
    with_adapter!(venue, adapter => {
        adapter.parse_metadata(body, symbols).map_err(anyhow::Error::from)
    })
}

/// Creates a parser for replaying a venue's recording.
pub fn parser(
    venue_name: &str,
    venue: VenueId,
    instruments: Vec<InstrumentSpec>,
) -> Result<Box<dyn FrameParser>> {
    with_adapter!(venue_name, adapter => {
        Ok(Box::new(adapter.parser(venue, instruments)) as Box<dyn FrameParser>)
    })
}

/// Starts a venue's live feed.
pub fn spawn_feed(
    venue_name: &str,
    venue: VenueId,
    config: FeedConfig,
    publisher: Producer<Event>,
    shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    with_adapter!(venue_name, adapter => {
        Ok(tokio::spawn(run_feed(Arc::new(adapter), venue, config, publisher, shutdown)))
    })
}

/// Starts a venue's feed that only records, publishing to nothing.
pub fn spawn_recording_feed(
    venue_name: &str,
    venue: VenueId,
    config: FeedConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    with_adapter!(venue_name, adapter => {
        Ok(tokio::spawn(run_feed(Arc::new(adapter), venue, config, Discard, shutdown)))
    })
}

struct Discard;

impl ringbook_feeds::Publish for Discard {
    fn free_slots(&mut self) -> usize {
        usize::MAX
    }

    fn publish(&mut self, _event: Event) {}
}
