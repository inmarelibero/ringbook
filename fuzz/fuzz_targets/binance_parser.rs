#![no_main]

use libfuzzer_sys::fuzz_target;
use ringbook_core::{EventTime, InstrumentId, MeasureStamp, VenueId};
use ringbook_feeds::{ConnEvent, FrameParser, Input, InstrumentSpec, ParseOutput, VenueAdapter};
use ringbook_venue_binance::Binance;

const INFO: &[u8] = br#"{"symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","marginAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}"#;

fuzz_target!(|data: &[u8]| {
    let symbols = ["BTCUSDT".to_owned()];
    let meta = Binance.parse_metadata(INFO, &symbols).unwrap();
    let mut parser = Binance.parser(
        VenueId(0),
        vec![InstrumentSpec {
            id: InstrumentId(0),
            meta: meta[0].clone(),
        }],
    );
    let mut output = ParseOutput::default();
    output.begin(EventTime(1), MeasureStamp(1));
    parser.on_input(
        Input::Conn {
            stream: 0,
            tag: 0,
            event: ConnEvent::Connected,
        },
        &mut output,
    );
    parser.on_input(Input::Rest { tag: 0, body: data }, &mut output);
    output.begin(EventTime(2), MeasureStamp(2));
    parser.on_input(Input::Frame { stream: 0, data }, &mut output);
});
