//! The hot thread: strategy, risk, positions and order management.
//!
//! [`engine::Engine`] is generic over its strategy and execution path, so both
//! are statically dispatched. Everything is allocated before the thread is
//! pinned; the per-event path doesn't allocate, lock or perform I/O.

#![forbid(unsafe_code)]

pub mod config;
pub mod engine;
pub mod link;
pub mod oms;
pub mod positions;
pub mod risk;
pub mod strategy;

pub use config::{Config, ConfigError, EngineParams, InstrumentRules, SlotParams};
pub use engine::{
    ApiBook, ApiPosition, ApiUpdate, Engine, EngineChannels, EngineCounters, EngineStatus,
    RunSummary, TelemetryRecord,
};
pub use link::{InProcess, VenueLink};
pub use oms::{OrderManager, OrderState, ReportOutcome};
pub use positions::Positions;
pub use risk::{Risk, RiskLimits};
pub use strategy::{MarketMaker, MarketView, Quote, Strategy, StrategyParams};
