#![no_main]

use libfuzzer_sys::fuzz_target;
use ringbook_core::Scale;

fuzz_target!(|data: &[u8]| {
    for decimals in [0, 2, 8, 18] {
        let scale = Scale::new(decimals).unwrap();
        if let Ok(value) = scale.parse(data) {
            assert!(value >= 0);
            let text = scale.display(value).to_string();
            assert_eq!(scale.parse(text.as_bytes()), Ok(value));
        }
    }
});
