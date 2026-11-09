#![no_main]

use libfuzzer_sys::fuzz_target;
use ringbook_fix::parse;

fuzz_target!(|data: &[u8]| {
    if let Ok((message, consumed)) = parse(data) {
        assert!(consumed <= data.len());
        assert!(message.fields().count() > 0);
    }
});
