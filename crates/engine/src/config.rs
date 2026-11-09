//! Loading, validating and compiling the TOML config.
//!
//! Text is parsed and checked into a [`Config`], then combined with venue
//! metadata into [`EngineParams`], a fixed-size `Copy` value of fixed-point
//! integers. Only that value reaches the hot thread, so it can't see an
//! unvalidated setting.

use std::collections::BTreeMap;
use std::fmt;

use ringbook_core::{DecimalError, InstrumentId, Scale, VenueId};
use ringbook_paper_exchange::VenueSimulation;
use serde::Deserialize;

use crate::risk::RiskLimits;
use crate::strategy::StrategyParams;

/// The most venues a run can use.
pub const MAX_VENUES: usize = 4;
/// The most instruments a run can use.
pub const MAX_INSTRUMENTS: usize = 8;

const BASE_DECIMALS: u8 = 8;
const USD_DECIMALS: u8 = 6;
const BPS_DECIMALS: u8 = 2;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    engine: RawEngine,
    instruments: Vec<RawInstrument>,
    strategy: RawStrategy,
    risk: RawRisk,
    venues: BTreeMap<String, RawVenue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawEngine {
    book_levels: usize,
    queue_model_power: i32,
}

impl Default for RawEngine {
    fn default() -> Self {
        Self {
            book_levels: 1 << 17,
            queue_model_power: 2,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInstrument {
    name: String,
    order_size: String,
    max_order_size: String,
    max_position: String,
    venues: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStrategy {
    spread_bps: String,
    inventory_skew_bps: String,
    requote_threshold_bps: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRisk {
    max_notional_usd: String,
    max_orders_per_sec: u32,
    price_collar_bps: String,
    max_drawdown_usd: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVenue {
    maker_fee_bps: String,
    taker_fee_bps: String,
    order_latency_us: u64,
    ack_latency_us: u64,
}

/// One problem found in a config, named by its field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError {
    /// The field's path.
    pub path: String,
    /// What is wrong with it.
    pub message: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for ConfigError {}

#[derive(Default)]
struct Problems(Vec<ConfigError>);

impl Problems {
    fn add(&mut self, path: impl Into<String>, message: impl Into<String>) {
        self.0.push(ConfigError {
            path: path.into(),
            message: message.into(),
        });
    }

    fn decimal(&mut self, path: &str, text: &str, decimals: u8) -> i64 {
        let scale = Scale::new(decimals).expect("constant scales are valid");
        match scale.parse(text.as_bytes()) {
            Ok(value) => value,
            Err(error) => {
                self.add(path, describe(text, error));
                0
            }
        }
    }

    fn positive(&mut self, path: &str, text: &str, decimals: u8) -> i64 {
        let value = self.decimal(path, text, decimals);
        if value == 0
            && Scale::new(decimals)
                .expect("valid")
                .parse(text.as_bytes())
                .is_ok()
        {
            self.add(path, "must be greater than zero");
        }
        value
    }

    fn signed(&mut self, path: &str, text: &str, decimals: u8) -> i64 {
        match text.strip_prefix('-') {
            Some(rest) => -self.decimal(path, rest, decimals),
            None => self.decimal(path, text, decimals),
        }
    }
}

fn describe(text: &str, error: DecimalError) -> String {
    format!("{text:?}: {error}")
}

/// One instrument's validated rules, in base units with eight decimals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstrumentRules {
    /// The instrument's name.
    pub name: String,
    /// The runtime identifier.
    pub id: InstrumentId,
    /// The size of each quote.
    pub order_size: i64,
    /// The largest single order.
    pub max_order_size: i64,
    /// The largest absolute position per venue.
    pub max_position: i64,
    /// The venue symbol at each venue that trades it, by venue index.
    pub symbols: Vec<Option<String>>,
}

/// A config that has passed every check not needing venue metadata.
#[derive(Clone, Debug)]
pub struct Config {
    /// The exact text the config was loaded from.
    pub text: String,
    /// Prices held by each book's window.
    pub book_levels: usize,
    /// The queue model's exponent.
    pub queue_model_power: i32,
    /// Venue names; a name's position is its [`VenueId`].
    pub venue_names: Vec<String>,
    /// Each venue's simulation settings.
    pub simulation: Vec<VenueSimulation>,
    /// The instruments, in [`InstrumentId`] order.
    pub instruments: Vec<InstrumentRules>,
    /// The strategy's settings.
    pub strategy: StrategyParams,
    /// The risk limits shared by every instrument.
    pub risk: RiskLimits,
}

impl Config {
    /// Parses and validates a config, reporting every problem found.
    ///
    /// # Errors
    ///
    /// Returns all the problems with the config.
    pub fn parse(text: &str, known_venues: &[&str]) -> Result<Self, Vec<ConfigError>> {
        let raw: RawConfig = toml::from_str(text).map_err(|error| {
            vec![ConfigError {
                path: "config".to_owned(),
                message: error.message().to_owned(),
            }]
        })?;
        let mut problems = Problems::default();

        if raw.engine.book_levels < 1024 {
            problems.add("engine.book_levels", "must be at least 1024");
        }
        if !(1..=8).contains(&raw.engine.queue_model_power) {
            problems.add("engine.queue_model_power", "must be between 1 and 8");
        }

        if raw.venues.is_empty() {
            problems.add("venues", "at least one venue is required");
        }
        if raw.venues.len() > MAX_VENUES {
            problems.add(
                "venues",
                format!("at most {MAX_VENUES} venues are supported"),
            );
        }
        let venue_names: Vec<String> = raw.venues.keys().cloned().collect();
        let mut simulation = Vec::with_capacity(raw.venues.len());
        for (name, venue) in &raw.venues {
            let path = format!("venues.{name}");
            if !known_venues.contains(&name.as_str()) {
                problems.add(
                    &path,
                    format!("unknown venue; available: {}", known_venues.join(", ")),
                );
            }
            simulation.push(VenueSimulation {
                maker_fee_centi_bps: problems.signed(
                    &format!("{path}.maker_fee_bps"),
                    &venue.maker_fee_bps,
                    BPS_DECIMALS,
                ),
                taker_fee_centi_bps: problems.decimal(
                    &format!("{path}.taker_fee_bps"),
                    &venue.taker_fee_bps,
                    BPS_DECIMALS,
                ),
                order_latency_ns: venue.order_latency_us.saturating_mul(1_000),
                ack_latency_ns: venue.ack_latency_us.saturating_mul(1_000),
            });
        }

        if raw.instruments.is_empty() {
            problems.add("instruments", "at least one instrument is required");
        }
        if raw.instruments.len() > MAX_INSTRUMENTS {
            problems.add(
                "instruments",
                format!("at most {MAX_INSTRUMENTS} instruments are supported"),
            );
        }
        let mut instruments = Vec::with_capacity(raw.instruments.len());
        for (index, instrument) in raw.instruments.iter().enumerate() {
            let path = format!("instruments[{index}]");
            let order_size = problems.positive(
                &format!("{path}.order_size"),
                &instrument.order_size,
                BASE_DECIMALS,
            );
            let max_order_size = problems.positive(
                &format!("{path}.max_order_size"),
                &instrument.max_order_size,
                BASE_DECIMALS,
            );
            let max_position = problems.positive(
                &format!("{path}.max_position"),
                &instrument.max_position,
                BASE_DECIMALS,
            );
            if order_size > max_order_size {
                problems.add(
                    format!("{path}.order_size"),
                    "must not exceed max_order_size",
                );
            }
            if max_order_size > max_position {
                problems.add(
                    format!("{path}.max_order_size"),
                    "must not exceed max_position",
                );
            }
            if instrument.venues.is_empty() {
                problems.add(format!("{path}.venues"), "at least one venue is required");
            }
            for venue in instrument.venues.keys() {
                if !raw.venues.contains_key(venue) {
                    problems.add(format!("{path}.venues.{venue}"), "has no [venues] section");
                }
            }
            if raw.instruments[..index]
                .iter()
                .any(|earlier| earlier.name == instrument.name)
            {
                problems.add(format!("{path}.name"), "is used more than once");
            }
            instruments.push(InstrumentRules {
                name: instrument.name.clone(),
                id: InstrumentId(index as u16),
                order_size,
                max_order_size,
                max_position,
                symbols: venue_names
                    .iter()
                    .map(|venue| instrument.venues.get(venue).cloned())
                    .collect(),
            });
        }

        let strategy = StrategyParams {
            spread_centi_bps: problems.positive(
                "strategy.spread_bps",
                &raw.strategy.spread_bps,
                BPS_DECIMALS,
            ),
            inventory_skew_centi_bps: problems.decimal(
                "strategy.inventory_skew_bps",
                &raw.strategy.inventory_skew_bps,
                BPS_DECIMALS,
            ),
            requote_threshold_centi_bps: problems.decimal(
                "strategy.requote_threshold_bps",
                &raw.strategy.requote_threshold_bps,
                BPS_DECIMALS,
            ),
        };

        let risk = RiskLimits {
            max_notional_micro_usd: problems.positive(
                "risk.max_notional_usd",
                &raw.risk.max_notional_usd,
                USD_DECIMALS,
            ),
            max_orders_per_sec: raw.risk.max_orders_per_sec,
            price_collar_centi_bps: problems.positive(
                "risk.price_collar_bps",
                &raw.risk.price_collar_bps,
                BPS_DECIMALS,
            ),
            max_drawdown_micro_usd: problems.positive(
                "risk.max_drawdown_usd",
                &raw.risk.max_drawdown_usd,
                USD_DECIMALS,
            ),
        };
        if raw.risk.max_orders_per_sec == 0 {
            problems.add("risk.max_orders_per_sec", "must be greater than zero");
        }

        if !problems.0.is_empty() {
            return Err(problems.0);
        }
        Ok(Self {
            text: text.to_owned(),
            book_levels: raw.engine.book_levels,
            queue_model_power: raw.engine.queue_model_power,
            venue_names,
            simulation,
            instruments,
            strategy,
            risk,
        })
    }

    /// Returns a venue's identifier from its name.
    #[must_use]
    pub fn venue_id(&self, name: &str) -> Option<VenueId> {
        self.venue_names
            .iter()
            .position(|candidate| candidate == name)
            .map(|index| VenueId(index as u8))
    }
}

/// Rules for one instrument at one venue, in that venue's scaled units.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotParams {
    /// The finest price increment.
    pub tick: i64,
    /// Decimal places in prices.
    pub price_decimals: u8,
    /// Decimal places in quantities.
    pub qty_decimals: u8,
    /// A significant-figure limit on order prices.
    pub max_significant_figures: Option<u8>,
    /// The size of each quote.
    pub order_qty: i64,
    /// The largest single order.
    pub max_order_qty: i64,
    /// The largest absolute position.
    pub max_position: i64,
}

impl SlotParams {
    /// Returns ten to the power of the combined price and quantity decimals.
    #[must_use]
    pub const fn notional_factor(&self) -> i128 {
        10_i128.pow(self.price_decimals as u32 + self.qty_decimals as u32)
    }
}

/// An instrument's published rules, as the engine needs them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VenueRules {
    /// The finest price increment.
    pub tick: i64,
    /// Decimal places in prices.
    pub price_decimals: u8,
    /// Decimal places in quantities.
    pub qty_decimals: u8,
    /// A significant-figure limit on order prices.
    pub max_significant_figures: Option<u8>,
    /// Whether the engine supports this contract.
    pub supported: bool,
}

/// The compiled config: fixed-size, `Copy`, and fully validated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineParams {
    /// The number of venues.
    pub venues: usize,
    /// The number of instruments.
    pub instruments: usize,
    /// Prices held by each book's window.
    pub book_levels: usize,
    /// Each pair's rules: `slots[venue][instrument]`, `None` if it isn't traded.
    pub slots: [[Option<SlotParams>; MAX_INSTRUMENTS]; MAX_VENUES],
    /// The strategy's starting settings.
    pub strategy: StrategyParams,
    /// The risk limits.
    pub risk: RiskLimits,
}

fn to_venue_units(value: i64, decimals: u8) -> i64 {
    value / 10_i64.pow(u32::from(BASE_DECIMALS.saturating_sub(decimals)))
}

/// Combines a config with venue rules into the engine's fixed-size parameters.
///
/// `rules` returns the published rules for a venue and symbol, or `None` if
/// the venue doesn't list the symbol.
///
/// # Errors
///
/// Returns every instrument that is unlisted, unsupported, or whose sizes
/// round to zero at the venue's precision.
pub fn compile(
    config: &Config,
    rules: impl Fn(VenueId, &str) -> Option<VenueRules>,
) -> Result<EngineParams, Vec<ConfigError>> {
    let mut problems = Problems::default();
    let mut slots = [[None; MAX_INSTRUMENTS]; MAX_VENUES];

    for instrument in &config.instruments {
        for (venue_index, symbol) in instrument.symbols.iter().enumerate() {
            let Some(symbol) = symbol else {
                continue;
            };
            let path = format!(
                "instruments.{}.venues.{}",
                instrument.name, config.venue_names[venue_index]
            );
            let Some(published) = rules(VenueId(venue_index as u8), symbol) else {
                problems.add(&path, format!("the venue does not list {symbol}"));
                continue;
            };
            if !published.supported {
                problems.add(&path, format!("{symbol} is not a linear perpetual"));
                continue;
            }
            let order_qty = to_venue_units(instrument.order_size, published.qty_decimals);
            if order_qty == 0 {
                problems.add(&path, "order_size rounds to zero at this venue's precision");
                continue;
            }
            slots[venue_index][usize::from(instrument.id.0)] = Some(SlotParams {
                tick: published.tick,
                price_decimals: published.price_decimals,
                qty_decimals: published.qty_decimals,
                max_significant_figures: published.max_significant_figures,
                order_qty,
                max_order_qty: to_venue_units(instrument.max_order_size, published.qty_decimals),
                max_position: to_venue_units(instrument.max_position, published.qty_decimals),
            });
        }
    }

    if !problems.0.is_empty() {
        return Err(problems.0);
    }
    Ok(EngineParams {
        venues: config.venue_names.len(),
        instruments: config.instruments.len(),
        book_levels: config.book_levels,
        slots,
        strategy: config.strategy,
        risk: config.risk,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
        [[instruments]]
        name = "BTC-PERP"
        order_size = "0.001"
        max_order_size = "0.01"
        max_position = "0.05"
        venues = { hyperliquid = "BTC", binance = "BTCUSDT" }

        [strategy]
        spread_bps = "4"
        inventory_skew_bps = "2"
        requote_threshold_bps = "1"

        [risk]
        max_notional_usd = "5000"
        max_orders_per_sec = 10
        price_collar_bps = "50"
        max_drawdown_usd = "200"

        [venues.hyperliquid]
        maker_fee_bps = "1.5"
        taker_fee_bps = "4.5"
        order_latency_us = 50000
        ack_latency_us = 50000

        [venues.binance]
        maker_fee_bps = "-0.5"
        taker_fee_bps = "4"
        order_latency_us = 30000
        ack_latency_us = 30000
    "#;

    const KNOWN: &[&str] = &["binance", "hyperliquid"];

    fn parse(text: &str) -> Result<Config, Vec<ConfigError>> {
        Config::parse(text, KNOWN)
    }

    fn paths(text: &str) -> Vec<String> {
        parse(text)
            .unwrap_err()
            .into_iter()
            .map(|error| error.path)
            .collect()
    }

    fn rules() -> impl Fn(VenueId, &str) -> Option<VenueRules> {
        |venue, _| {
            Some(VenueRules {
                tick: 1,
                price_decimals: 1,
                qty_decimals: if venue == VenueId(0) { 3 } else { 5 },
                max_significant_figures: None,
                supported: true,
            })
        }
    }

    #[test]
    fn a_valid_config_parses_into_fixed_point() {
        let config = parse(VALID).unwrap();
        assert_eq!(config.venue_names, ["binance", "hyperliquid"]);
        assert_eq!(config.strategy.spread_centi_bps, 400);
        assert_eq!(config.simulation[0].maker_fee_centi_bps, -50);
        assert_eq!(config.simulation[1].order_latency_ns, 50_000_000);
        assert_eq!(config.instruments[0].order_size, 100_000);
        assert_eq!(
            config.instruments[0].symbols,
            [Some("BTCUSDT".to_owned()), Some("BTC".to_owned())]
        );
    }

    #[test]
    fn compiling_converts_to_each_venues_precision() {
        let params = compile(&parse(VALID).unwrap(), rules()).unwrap();
        assert_eq!(params.slots[0][0].unwrap().order_qty, 1);
        assert_eq!(params.slots[1][0].unwrap().order_qty, 100);
        assert_eq!(params.slots[1][0].unwrap().max_position, 5_000);
        assert_eq!(params.slots[0][1], None);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let text = VALID.replace("spread_bps", "spred_bps");
        let errors = parse(&text).unwrap_err();
        assert!(errors[0].message.contains("spred_bps"));
    }

    #[test]
    fn risk_limits_have_no_defaults() {
        let text = VALID.replace("max_drawdown_usd = \"200\"", "");
        assert!(
            parse(&text).unwrap_err()[0]
                .message
                .contains("max_drawdown_usd")
        );
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let text = VALID
            .replace(r#"spread_bps = "4""#, r#"spread_bps = "0""#)
            .replace(
                r#"max_notional_usd = "5000""#,
                r#"max_notional_usd = "abc""#,
            )
            .replace("max_orders_per_sec = 10", "max_orders_per_sec = 0");
        assert_eq!(
            paths(&text),
            [
                "strategy.spread_bps",
                "risk.max_notional_usd",
                "risk.max_orders_per_sec"
            ]
        );
    }

    #[test]
    fn sizes_must_nest() {
        let text = VALID.replace(r#"max_order_size = "0.01""#, r#"max_order_size = "0.1""#);
        assert_eq!(paths(&text), ["instruments[0].max_order_size"]);
        let text = VALID.replace(r#"order_size = "0.001""#, r#"order_size = "0.02""#);
        assert_eq!(paths(&text), ["instruments[0].order_size"]);
    }

    #[test]
    fn venues_must_be_known_and_configured() {
        let text = VALID.replace("venues.binance", "venues.kraken");
        let found = paths(&text);
        assert!(found.contains(&"venues.kraken".to_owned()));
        assert!(found.contains(&"instruments[0].venues.binance".to_owned()));
    }

    #[test]
    fn unlisted_symbols_stop_compilation() {
        let errors = compile(&parse(VALID).unwrap(), |_, _| None).unwrap_err();
        assert_eq!(errors.len(), 2);
        assert!(errors[0].message.contains("does not list"));
    }

    #[test]
    fn unsupported_contracts_stop_compilation() {
        let unsupported = |_: VenueId, _: &str| {
            Some(VenueRules {
                tick: 1,
                price_decimals: 1,
                qty_decimals: 3,
                max_significant_figures: None,
                supported: false,
            })
        };
        let errors = compile(&parse(VALID).unwrap(), unsupported).unwrap_err();
        assert!(errors[0].message.contains("linear perpetual"));
    }

    #[test]
    fn sizes_rounding_to_zero_stop_compilation() {
        let coarse = |_: VenueId, _: &str| {
            Some(VenueRules {
                tick: 1,
                price_decimals: 1,
                qty_decimals: 2,
                max_significant_figures: None,
                supported: true,
            })
        };
        let errors = compile(&parse(VALID).unwrap(), coarse).unwrap_err();
        assert!(errors[0].message.contains("rounds to zero"));
    }
}
