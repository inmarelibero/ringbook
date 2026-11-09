use super::*;
use ringbook_core::{CancelOrder, ClientOrderId, ExecKind, InstrumentId, Liquidity, ParamChange};
use ringbook_core::{ParamKey, Price, Qty, RejectReason, Side, TimeInForce, VenueId};
use ringbook_ring::channel;

fn path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ringbook-journal-{}-{name}", std::process::id()))
}

fn header() -> JournalHeader {
    JournalHeader {
        config: "spread = 4".to_owned(),
        binary_version: "0.1.0".to_owned(),
        epoch: 3,
        run_id: 42,
    }
}

fn new_order() -> NewOrder {
    NewOrder {
        id: ClientOrderId::new(3, 9),
        venue: VenueId(1),
        instrument: InstrumentId(2),
        side: Side::Sell,
        price: Price(1234),
        qty: Qty(5),
        time_in_force: TimeInForce::PostOnly,
    }
}

fn report(kind: ExecKind) -> ExecReport {
    ExecReport {
        sequence: 11,
        time: EventTime(50),
        id: ClientOrderId::new(3, 9),
        venue: VenueId(1),
        instrument: InstrumentId(2),
        side: Side::Buy,
        order_price: Price(1234),
        order_qty: Qty(5),
        kind,
    }
}

fn every_body() -> Vec<JournalBody> {
    vec![
        JournalBody::Sent(OrderCommand::New(new_order())),
        JournalBody::Sent(OrderCommand::Cancel(CancelOrder {
            id: ClientOrderId::new(3, 9),
            venue: VenueId(1),
            instrument: InstrumentId(2),
        })),
        JournalBody::RiskRejected {
            order: new_order(),
            reason: RiskRejection::PriceCollar,
        },
        JournalBody::Report(report(ExecKind::Ack)),
        JournalBody::Report(report(ExecKind::Reject(RejectReason::PostOnlyCross))),
        JournalBody::Report(report(ExecKind::Fill {
            price: Price(1230),
            qty: Qty(2),
            fee: -7,
            liquidity: Liquidity::Maker,
        })),
        JournalBody::Report(report(ExecKind::CancelAck)),
        JournalBody::Report(report(ExecKind::Expired)),
        JournalBody::Control(ControlCommand::SetParam(ParamChange {
            key: ParamKey::SpreadCentiBps,
            value: 500,
        })),
        JournalBody::Control(ControlCommand::HaltInstrument(InstrumentId(1))),
        JournalBody::Control(ControlCommand::ResumeInstrument(InstrumentId(1))),
        JournalBody::Control(ControlCommand::GlobalHalt),
        JournalBody::Halted(HaltReason::Drawdown),
        JournalBody::Session(42),
    ]
}

fn records() -> Vec<JournalRecord> {
    every_body()
        .into_iter()
        .enumerate()
        .map(|(index, body)| JournalRecord {
            batch: index as u64,
            time: EventTime(100 + index as u64),
            body,
        })
        .collect()
}

fn write_journal(path: &Path, records: &[JournalRecord]) -> u64 {
    let (mut producer, consumer) = channel(1024).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let writer =
        JournalWriter::start(path, &header(), consumer, Arc::clone(&stop), failed).unwrap();
    for record in records {
        producer.push(*record).unwrap();
    }
    stop.store(true, Ordering::Release);
    writer.finish().unwrap()
}

#[test]
fn every_record_round_trips() {
    for record in records() {
        assert_eq!(codec::decode(&codec::encode(&record)), Some(record));
    }
}

#[test]
fn a_journal_round_trips_through_the_writer_thread() {
    let path = path("round-trip");
    let records = records();
    assert_eq!(write_journal(&path, &records), records.len() as u64);

    let contents = read_journal(&path).unwrap();
    assert_eq!(contents.header, header());
    assert_eq!(contents.records, records);
    assert!(!contents.truncated_tail);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn identical_records_produce_identical_files() {
    let (first, second) = (path("same-a"), path("same-b"));
    write_journal(&first, &records());
    write_journal(&second, &records());
    assert_eq!(
        std::fs::read(&first).unwrap(),
        std::fs::read(&second).unwrap()
    );
    std::fs::remove_file(first).unwrap();
    std::fs::remove_file(second).unwrap();
}

#[test]
fn a_cut_at_any_point_in_the_records_recovers_a_prefix() {
    let path = path("cut");
    let records = records();
    write_journal(&path, &records);
    let bytes = std::fs::read(&path).unwrap();
    let header_length = encode_header(&header()).len();

    for cut in header_length..bytes.len() {
        let contents = decode_journal(&bytes[..cut]).unwrap();
        assert!(contents.records.len() < records.len());
        assert_eq!(contents.records, records[..contents.records.len()]);
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn damage_to_the_last_record_marks_the_end_of_the_journal() {
    let path = path("tail");
    let records = records();
    write_journal(&path, &records);
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;

    let contents = decode_journal(&bytes).unwrap();
    assert!(contents.truncated_tail);
    assert_eq!(contents.records, records[..records.len() - 1]);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn damage_before_the_end_stops_recovery() {
    let path = path("middle");
    write_journal(&path, &records());
    let mut bytes = std::fs::read(&path).unwrap();
    let target = encode_header(&header()).len() + FRAME_HEADER + 1;
    bytes[target] ^= 1;
    assert!(matches!(
        decode_journal(&bytes),
        Err(JournalError::Corrupt { .. })
    ));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn a_damaged_length_before_the_end_stops_recovery() {
    let path = path("length");
    let records: Vec<_> = records().into_iter().cycle().take(200).collect();
    write_journal(&path, &records);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[encode_header(&header()).len() + 1] = 0xFF;
    assert!(matches!(
        decode_journal(&bytes),
        Err(JournalError::Corrupt { .. })
    ));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn a_damaged_header_is_rejected() {
    let path = path("header");
    write_journal(&path, &records());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[MAGIC.len() + 10] ^= 1;
    assert!(matches!(
        decode_journal(&bytes),
        Err(JournalError::BadHeader)
    ));
    assert!(matches!(
        decode_journal(b"nope"),
        Err(JournalError::BadHeader)
    ));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn usable_length_excludes_a_truncated_tail() {
    let path = path("usable");
    write_journal(&path, &records());
    let full = std::fs::metadata(&path).unwrap().len();
    assert_eq!(usable_length(&path).unwrap(), full);

    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(full - 1)
        .unwrap();
    assert!(usable_length(&path).unwrap() < full - 1);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn a_journal_resumes_after_a_truncated_tail() {
    let path = path("resume");
    let records = records();
    write_journal(&path, &records);
    let full = std::fs::metadata(&path).unwrap().len();
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(full - 1)
        .unwrap();

    let usable = usable_length(&path).unwrap();
    let (mut producer, consumer) = channel(16).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let writer = JournalWriter::resume(&path, usable, consumer, Arc::clone(&stop), failed).unwrap();
    producer.push(records[0]).unwrap();
    stop.store(true, Ordering::Release);
    writer.finish().unwrap();

    let contents = read_journal(&path).unwrap();
    assert!(!contents.truncated_tail);
    assert_eq!(contents.records.len(), records.len());
    assert_eq!(contents.records.last(), Some(&records[0]));
    std::fs::remove_file(path).unwrap();
}
