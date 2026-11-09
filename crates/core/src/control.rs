//! Operator commands and the reasons trading can halt.

use crate::event::InstrumentId;

/// A strategy parameter that can change while running.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ParamKey {
    /// The full quoted spread, in hundredths of a basis point.
    SpreadCentiBps,
    /// The price shift at maximum inventory, in hundredths of a basis point.
    InventorySkewCentiBps,
    /// The price move that triggers a requote, in hundredths of a basis point.
    RequoteThresholdCentiBps,
}

/// A new value for one strategy parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParamChange {
    /// The parameter.
    pub key: ParamKey,
    /// The new value.
    pub value: i64,
}

/// A command applied by the hot thread at a batch boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlCommand {
    /// Change a strategy parameter.
    SetParam(ParamChange),
    /// Stop quoting one instrument.
    HaltInstrument(InstrumentId),
    /// Resume quoting one instrument.
    ResumeInstrument(InstrumentId),
    /// Stop all quoting until restart.
    GlobalHalt,
}

/// Why the engine entered a global halt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HaltReason {
    /// An operator asked for it.
    Operator,
    /// The drawdown limit was breached.
    Drawdown,
    /// The journal could not keep up.
    JournalBackpressure,
    /// The journal could not be written.
    JournalWriteFailed,
    /// The mock venue lost its session state.
    VenueStateLost,
    /// Orders could not be delivered to the venue.
    OrderPathBlocked,
    /// Recovery could not reconcile with the venue.
    ReconcileFailed,
}

impl HaltReason {
    /// Returns the reason's name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Operator => "Operator",
            Self::Drawdown => "Drawdown",
            Self::JournalBackpressure => "JournalBackpressure",
            Self::JournalWriteFailed => "JournalWriteFailed",
            Self::VenueStateLost => "VenueStateLost",
            Self::OrderPathBlocked => "OrderPathBlocked",
            Self::ReconcileFailed => "ReconcileFailed",
        }
    }
}
