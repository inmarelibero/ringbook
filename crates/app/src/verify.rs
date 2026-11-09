//! Checks a recording against what its parsers claim about it.
//!
//! Hyperliquid's docs don't define a trade's `side`, so this confirms each
//! venue's aggressor mapping empirically. Buyers lift the ask and sellers hit
//! the bid, so when trades on opposite sides print moments apart at different
//! prices, the buyer-initiated one should be the higher. This doesn't depend
//! on a book, which on Hyperliquid can be half a second old.

use std::path::Path;

use anyhow::{Context, Result, bail};
use ringbook_clock::Clock;
use ringbook_core::{Payload, Price, Side, VenueId};
use ringbook_feeds::{ConnEvent, Input, ParseOutput, RecordKind, RecordingReader};
use ringbook_replay::{read_metadata, venue_file};

use crate::{registry, setup};

const MINIMUM_AGREEMENT: f64 = 0.9;

const NEARBY_TRADE_NS: u64 = 250_000_000;

#[derive(Default)]
struct Tally {
    records: u64,
    trades: u64,
    pairs: u64,
    agreed: u64,
    recorder_gaps: u64,
    disconnects: u64,
}

/// Verifies every venue in a recording.
pub fn verify_recording(config_path: &Path, recording: &Path) -> Result<()> {
    let config = setup::load_config(config_path)?;
    let clock = Clock::new();
    let mut failed = false;

    for (index, name) in config.venue_names.iter().enumerate() {
        let path = venue_file(recording, name);
        let body = read_metadata(&path).with_context(|| format!("reading {}", path.display()))?;
        let metas = registry::parse_metadata(name, &body, &setup::venue_symbols(&config, index))?;
        let specs = setup::instrument_specs(&config, index, &metas);
        let mut last_trade = vec![None; config.instruments.len()];

        let mut parser = registry::parser(name, VenueId(index as u8), specs)?;
        let mut reader = RecordingReader::open(&path)?;
        let mut output = ParseOutput::default();
        let mut tally = Tally::default();

        while let Some(record) = reader.next_record()? {
            tally.records += 1;
            let input = match record.kind {
                RecordKind::Frame => Input::Frame {
                    stream: record.stream,
                    data: &record.payload,
                },
                RecordKind::Rest => Input::Rest {
                    tag: record.tag,
                    body: &record.payload,
                },
                RecordKind::Conn => {
                    let Some(event) = record
                        .payload
                        .first()
                        .copied()
                        .and_then(ConnEvent::from_code)
                    else {
                        continue;
                    };
                    if event == ConnEvent::Disconnected {
                        tally.disconnects += 1;
                    }
                    Input::Conn {
                        stream: record.stream,
                        tag: record.tag,
                        event,
                    }
                }
                RecordKind::Gap => {
                    tally.recorder_gaps += 1;
                    continue;
                }
                RecordKind::Metadata => continue,
            };

            output.begin(record.received, clock.measure());
            parser.on_input(input, &mut output);
            for event in &output.events {
                let Payload::Trade {
                    aggressor, price, ..
                } = event.payload
                else {
                    continue;
                };
                tally.trades += 1;
                let time = event.exchange_time.map_or(0, |time| time.0.get());
                let slot: &mut Option<(u64, Side, Price)> =
                    &mut last_trade[usize::from(event.instrument.0)];
                if let Some((previous_time, previous_side, previous_price)) = *slot
                    && previous_side != aggressor
                    && previous_price != price
                    && time.abs_diff(previous_time) <= NEARBY_TRADE_NS
                {
                    tally.pairs += 1;
                    let buy_is_higher = match aggressor {
                        Side::Buy => price > previous_price,
                        Side::Sell => previous_price > price,
                    };
                    tally.agreed += u64::from(buy_is_higher);
                }
                *slot = Some((time, aggressor, price));
            }
        }

        let stats = parser.stats();
        let agreement = tally.agreed as f64 / tally.pairs.max(1) as f64;
        println!(
            "{name}: records={} trades={} opposite-pairs={} side-agreement={:.2}% malformed={} gaps={} resyncs={} \
             duplicates={} disconnects={} recorder-gaps={}",
            tally.records,
            tally.trades,
            tally.pairs,
            agreement * 100.0,
            stats.malformed,
            stats.gaps,
            stats.resyncs,
            stats.duplicate_trades,
            tally.disconnects,
            tally.recorder_gaps,
        );
        failed |= stats.malformed > 0 || (tally.pairs > 0 && agreement < MINIMUM_AGREEMENT);
    }

    if failed {
        bail!("the recording failed verification");
    }
    Ok(())
}
