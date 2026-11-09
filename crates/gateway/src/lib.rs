//! The binary order protocol and the order gateway thread.

#![forbid(unsafe_code)]

pub mod protocol;
pub mod thread;

pub use protocol::{FRAME_LEN, Frame, FrameReader, decode, encode};
pub use thread::{GatewayConfig, GatewayHandle, GatewayLink, ReconcileItem, start_gateway};
