//! Turns a config and venue metadata into runnable parts.

use std::path::Path;

use anyhow::{Context, Result, bail};
use ringbook_core::VenueId;
use ringbook_engine::config::{self, VenueRules};
use ringbook_engine::{Config, ConfigError, EngineParams};
use ringbook_feeds::{ContractType, InstrumentMeta, InstrumentSpec};
use ringbook_paper_exchange::{ExchangeConfig, PaperExchange, ProbabilisticQueue};

use crate::registry::KNOWN_VENUES;

const MAX_RESTING: usize = 64;
const MAX_IN_FLIGHT: usize = 1024;

fn format_errors(errors: &[ConfigError]) -> String {
    errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n  ")
}

/// Loads and validates a config file.
pub fn load_config(path: &Path) -> Result<Config> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config {}", path.display()))?;
    match Config::parse(&text, KNOWN_VENUES) {
        Ok(config) => Ok(config),
        Err(errors) => bail!("invalid config:\n  {}", format_errors(&errors)),
    }
}

/// Returns the symbols a venue trades, in instrument order.
pub fn venue_symbols(config: &Config, venue: usize) -> Vec<String> {
    config
        .instruments
        .iter()
        .filter_map(|instrument| instrument.symbols[venue].clone())
        .collect()
}

/// Binds a venue's published rules to its instruments' runtime identifiers.
pub fn instrument_specs(
    config: &Config,
    venue: usize,
    metas: &[InstrumentMeta],
) -> Vec<InstrumentSpec> {
    config
        .instruments
        .iter()
        .filter_map(|instrument| {
            let symbol = instrument.symbols[venue].as_ref()?;
            let meta = metas.iter().find(|meta| &meta.symbol == symbol)?;
            Some(InstrumentSpec {
                id: instrument.id,
                meta: meta.clone(),
            })
        })
        .collect()
}

/// Compiles a config against every venue's published rules.
pub fn compile(config: &Config, metas: &[Vec<InstrumentMeta>]) -> Result<EngineParams> {
    let rules = |venue: VenueId, symbol: &str| {
        let meta = metas[usize::from(venue.0)]
            .iter()
            .find(|meta| meta.symbol == symbol)?;
        Some(VenueRules {
            tick: meta.tick,
            price_decimals: meta.price_decimals,
            qty_decimals: meta.qty_decimals,
            max_significant_figures: meta.max_significant_figures,
            supported: meta.contract == ContractType::LinearPerpetual,
        })
    };
    match config::compile(config, rules) {
        Ok(params) => Ok(params),
        Err(errors) => bail!("invalid config:\n  {}", format_errors(&errors)),
    }
}

/// Builds the paper exchange a run matches against.
pub fn exchange(config: &Config, params: &EngineParams) -> PaperExchange<ProbabilisticQueue> {
    let ticks = (0..params.venues)
        .map(|venue| {
            (0..params.instruments)
                .map(|instrument| params.slots[venue][instrument].map_or(1, |slot| slot.tick))
                .collect()
        })
        .collect();
    PaperExchange::new(
        &ExchangeConfig {
            venues: config.simulation.clone(),
            ticks,
            book_levels: params.book_levels,
            max_resting: MAX_RESTING,
            max_in_flight: MAX_IN_FLIGHT,
        },
        ProbabilisticQueue {
            power: config.queue_model_power,
        },
    )
}

/// Pins the current thread to a core, if the platform allows it.
pub fn pin_current_thread(core: Option<usize>) {
    let Some(core) = core else {
        return;
    };
    let pinned = core_affinity::set_for_current(core_affinity::CoreId { id: core });
    if !pinned {
        tracing::warn!(core, "could not pin thread");
    }
}
