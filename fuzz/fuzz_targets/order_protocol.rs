#![no_main]

use libfuzzer_sys::fuzz_target;
use ringbook_gateway::{FRAME_LEN, decode, encode};

fuzz_target!(|data: &[u8]| {
    let mut frame = [0; FRAME_LEN];
    let length = data.len().min(FRAME_LEN);
    frame[..length].copy_from_slice(&data[..length]);
    if let Some(decoded) = decode(&frame) {
        let mut encoded = [0; FRAME_LEN];
        encode(&decoded, &mut encoded);
        assert_eq!(decode(&encoded), Some(decoded));
    }
});
