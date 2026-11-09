//! The venue adapter interface, generic feed handler and recorder.

#![forbid(unsafe_code)]

pub mod adapter;
pub mod conformance;
pub mod handler;
pub mod recording;

pub use adapter::{
    Action, AdapterError, ConnEvent, ContractType, Endpoint, FrameParser, Input, InstrumentMeta,
    InstrumentSpec, ParseOutput, ParserStats, RestMethod, RestRequest, VenueAdapter,
};
pub use handler::{FeedConfig, Publish, fetch_metadata, run_feed};
pub use recording::{
    RecordKind, RecorderHandle, RecorderStats, Recording, RecordingError, RecordingReader,
    start_recorder,
};
