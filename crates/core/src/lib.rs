//! Event types and fixed-point values shared by every ringbook component.
//!
//! This crate depends on no other workspace crate and is `no_std` without
//! `alloc`, so nothing defined here can touch the heap.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

pub mod control;
pub mod decimal;
pub mod event;
pub mod order;

pub use control::{ControlCommand, HaltReason, ParamChange, ParamKey};
pub use decimal::{DecimalError, Price, Qty, Scale};
pub use event::{
    Event, EventTime, ExchangeTime, InstrumentId, MeasureStamp, Payload, Side, VenueId, VenueState,
};
pub use order::{
    CancelOrder, ClientOrderId, ExecKind, ExecReport, Liquidity, NewOrder, OrderCommand,
    RejectReason, TimeInForce,
};
