//! The contract every venue parser must meet.
//!
//! Each adapter runs these checks from its own tests, using frames it
//! generates in code.

use ringbook_core::{EventTime, MeasureStamp, Payload, VenueState};

use crate::adapter::{ConnEvent, FrameParser, Input, ParseOutput};

fn output() -> ParseOutput {
    let mut output = ParseOutput::default();
    output.begin(EventTime(1), MeasureStamp(1));
    output
}

/// Feeds an input and returns what the parser produced.
pub fn feed(parser: &mut impl FrameParser, input: Input<'_>) -> ParseOutput {
    let mut out = output();
    parser.on_input(input, &mut out);
    out
}

/// Checks that any non-empty output ends with a completed batch.
///
/// # Panics
///
/// Panics if the last event isn't flagged as the end of a batch.
pub fn assert_batched(output: &ParseOutput) {
    if let Some(last) = output.events.last() {
        assert!(last.end_of_batch, "the last event must end its batch");
    }
}

/// Checks that a parser survives junk input without emitting events.
///
/// # Panics
///
/// Panics if junk input produces events.
pub fn assert_ignores_junk(parser: &mut impl FrameParser) {
    for junk in [
        &b""[..],
        b"{",
        b"null",
        b"[]",
        b"{\"channel\":5}",
        b"not json",
        b"{\"stream\":\"x\",\"data\":{}}",
    ] {
        let out = feed(
            parser,
            Input::Frame {
                stream: 0,
                data: junk,
            },
        );
        assert!(out.events.is_empty(), "junk must not produce events");
    }
}

/// Checks that a disconnect marks the venue as no longer live.
///
/// # Panics
///
/// Panics if a disconnect doesn't report a non-live state in a complete batch.
pub fn assert_disconnect_reports_status(parser: &mut impl FrameParser) {
    let out = feed(
        parser,
        Input::Conn {
            stream: 0,
            tag: 0,
            event: ConnEvent::Disconnected,
        },
    );
    assert!(!out.events.is_empty(), "a disconnect must report status");
    assert_batched(&out);
    assert!(out.events.iter().all(|event| matches!(
        event.payload,
        Payload::VenueStatus(VenueState::Stale | VenueState::Resyncing)
    )));
}
