#![no_main]

use libfuzzer_sys::fuzz_target;
use ringbook_core::{EventTime, InstrumentId, MeasureStamp, VenueId};
use ringbook_feeds::{FrameParser, Input, InstrumentSpec, ParseOutput, VenueAdapter};
use ringbook_venue_hyperliquid::Hyperliquid;

const META: &[u8] = br#"{"universe":[{"szDecimals":5,"name":"BTC"}]}"#;

fuzz_target!(|data: &[u8]| {
    let symbols = ["BTC".to_owned()];
    let meta = Hyperliquid.parse_metadata(META, &symbols).unwrap();
    let mut parser = Hyperliquid.parser(
        VenueId(0),
        vec![InstrumentSpec {
            id: InstrumentId(0),
            meta: meta[0].clone(),
        }],
    );
    let mut output = ParseOutput::default();
    output.begin(EventTime(1), MeasureStamp(1));
    parser.on_input(Input::Frame { stream: 0, data }, &mut output);
    if let Some(last) = output.events.last() {
        assert!(last.end_of_batch);
    }
});
