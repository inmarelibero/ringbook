//! Runs the real binaries against a generated recording, injecting failures.
//!
//! The recording uses each venue's real message formats, so these tests need
//! no network access and no market data in the repository.

#![allow(missing_docs)]

use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use ringbook_core::EventTime;
use ringbook_ctl::send_command;
use ringbook_feeds::{RecordKind, Recording, start_recorder};

const BINARY: &str = env!("CARGO_BIN_EXE_ringbook");
static SERIAL: Mutex<()> = Mutex::new(());

const CONFIG: &str = r#"
[engine]
book_levels = 65536
queue_model_power = 2

[[instruments]]
name = "BTC-PERP"
order_size = "0.001"
max_order_size = "0.01"
max_position = "0.5"
venues = { hyperliquid = "BTC", binance = "BTCUSDT" }

[strategy]
spread_bps = "2"
inventory_skew_bps = "1"
requote_threshold_bps = "1"

[risk]
max_notional_usd = "100000"
max_orders_per_sec = 200
price_collar_bps = "200"
max_drawdown_usd = "100000"

[venues.hyperliquid]
maker_fee_bps = "1.5"
taker_fee_bps = "4.5"
order_latency_us = 2000
ack_latency_us = 2000

[venues.binance]
maker_fee_bps = "2"
taker_fee_bps = "5"
order_latency_us = 1000
ack_latency_us = 1000
"#;

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Workspace {
    directory: PathBuf,
    config: PathBuf,
    recording: PathBuf,
    socket: PathBuf,
    port: u16,
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
        let _ = std::fs::remove_file(&self.socket);
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn workspace(name: &str, seconds: u64) -> Workspace {
    let tag = format!("{}-{name}", std::process::id());
    let directory = std::env::temp_dir().join(format!("ringbook-e2e-{tag}"));
    let recording = directory.join("recording");
    std::fs::create_dir_all(&recording).unwrap();
    let config = directory.join("config.toml");
    std::fs::write(&config, CONFIG).unwrap();
    generate(&recording, seconds);
    Workspace {
        directory,
        config,
        recording,
        // Unix socket paths are limited to about 100 bytes.
        socket: PathBuf::from(format!("/tmp/rb-{tag}.sock")),
        port: free_port(),
    }
}

struct Writer {
    records: Vec<Recording>,
}

impl Writer {
    fn add(&mut self, received: u64, kind: RecordKind, stream: u8, tag: u32, payload: Vec<u8>) {
        let sequence = self.records.len() as u64 + 1;
        self.records.push(Recording {
            received: EventTime(received),
            sequence,
            kind,
            stream,
            tag,
            payload,
        });
    }

    fn save(self, path: &Path) {
        let (handle, thread) = start_recorder(path).unwrap();
        for record in self.records {
            handle.record(record);
        }
        drop(handle);
        thread.join().unwrap();
    }
}

const START_NS: u64 = 1_700_000_000_000_000_000;
const STEP_NS: u64 = 25_000_000;

/// Oscillates, and prints trades far enough away to fill resting quotes.
fn generated_mid(step: u64) -> f64 {
    73_000.0 + ((step % 80) as f64 - 40.0).abs()
}

fn generate(directory: &Path, seconds: u64) {
    let steps = seconds * 1_000_000_000 / STEP_NS;
    generate_binance(&directory.join("binance.rbr"), steps);
    generate_hyperliquid(&directory.join("hyperliquid.rbr"), steps);
}

fn generate_binance(path: &Path, steps: u64) {
    let mut writer = Writer {
        records: Vec::new(),
    };
    let metadata = br#"{"symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","marginAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}"#;
    writer.add(START_NS, RecordKind::Metadata, 0, 0, metadata.to_vec());
    writer.add(START_NS + 1, RecordKind::Conn, 0, 0, vec![1]);
    let snapshot = format!(
        r#"{{"lastUpdateId":1000,"E":1,"T":1,"bids":[["{:.1}","5.000"]],"asks":[["{:.1}","5.000"]]}}"#,
        generated_mid(0) - 0.5,
        generated_mid(0) + 0.5
    );
    writer.add(START_NS + 2, RecordKind::Rest, 0, 0, snapshot.into_bytes());

    let mut previous = 1000;
    for step in 0..steps {
        let time = START_NS + 10 + step * STEP_NS;
        let mid = generated_mid(step);
        let last = previous + 5;
        let depth = format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1700000000000,"T":1,"s":"BTCUSDT","U":{},"u":{last},"pu":{previous},"b":[["{:.1}","5.000"],["{:.1}","0"]],"a":[["{:.1}","5.000"],["{:.1}","0"]]}}}}"#,
            previous + 1,
            mid - 0.5,
            generated_mid(step.wrapping_sub(1).min(steps)) - 0.5,
            mid + 0.5,
            generated_mid(step.wrapping_sub(1).min(steps)) + 0.5,
        );
        previous = last;
        writer.add(time, RecordKind::Frame, 0, 0, depth.into_bytes());

        if step % 8 == 0 {
            let selling = step % 16 == 0;
            let price = if selling { mid - 40.0 } else { mid + 40.0 };
            let trade = format!(
                r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"{price:.1}","q":"0.500","f":1,"l":1,"T":1700000000000,"m":{selling}}}}}"#
            );
            writer.add(time + 1, RecordKind::Frame, 1, 0, trade.into_bytes());
        }
    }
    writer.save(path);
}

fn generate_hyperliquid(path: &Path, steps: u64) {
    let mut writer = Writer {
        records: Vec::new(),
    };
    let metadata = br#"{"universe":[{"szDecimals":5,"name":"BTC","maxLeverage":40}]}"#;
    writer.add(START_NS, RecordKind::Metadata, 0, 0, metadata.to_vec());
    writer.add(START_NS + 1, RecordKind::Conn, 0, 0, vec![1]);

    let mut trade_id = 0;
    for step in 0..steps {
        let time = START_NS + 20 + step * STEP_NS;
        let mid = generated_mid(step);
        let book = format!(
            r#"{{"channel":"l2Book","data":{{"coin":"BTC","time":{},"levels":[[{{"px":"{:.0}","sz":"3.0","n":1}}],[{{"px":"{:.0}","sz":"3.0","n":1}}]]}}}}"#,
            1_700_000_000_000 + step,
            mid - 1.0,
            mid + 1.0
        );
        writer.add(time, RecordKind::Frame, 0, 0, book.into_bytes());

        if step % 8 == 4 {
            trade_id += 1;
            let selling = step % 16 == 4;
            let (side, price) = if selling {
                ("A", mid - 40.0)
            } else {
                ("B", mid + 40.0)
            };
            let trades = format!(
                r#"{{"channel":"trades","data":[{{"coin":"BTC","side":"{side}","px":"{price:.0}","sz":"0.5","hash":"0x","time":{},"tid":{trade_id},"users":["a","b"]}}]}}"#,
                1_700_000_000_000 + step
            );
            writer.add(time + 1, RecordKind::Frame, 0, 0, trades.into_bytes());
        }
    }
    writer.save(path);
}

fn run_to_end(arguments: &[&str]) -> (bool, String) {
    let output = Command::new(BINARY).args(arguments).output().unwrap();
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    (output.status.success(), text)
}

fn path_text(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// A running process whose output is collected in the background.
struct Process {
    child: Child,
    output: Arc<Mutex<String>>,
}

impl Process {
    fn spawn(arguments: &[&str]) -> Self {
        let mut child = Command::new(BINARY)
            .args(arguments)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = Arc::new(Mutex::new(String::new()));
        let pipes: [Box<dyn Read + Send>; 2] = [
            Box::new(child.stdout.take().unwrap()),
            Box::new(child.stderr.take().unwrap()),
        ];
        for pipe in pipes {
            let collected = Arc::clone(&output);
            std::thread::spawn(move || {
                for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                    let mut text = collected.lock().unwrap();
                    text.push_str(&line);
                    text.push('\n');
                }
            });
        }
        Self { child, output }
    }

    fn text(&self) -> String {
        self.output.lock().unwrap().clone()
    }

    fn wait_for_output(&self, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !self.text().contains(needle) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {needle:?} in:\n{}",
                self.text()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn signal_term(&self) {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn finish(mut self) -> String {
        self.signal_term();
        self.child.wait().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        self.text()
    }

    fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_mock(workspace: &Workspace, extra: &[&str]) -> Process {
    let address = format!("127.0.0.1:{}", workspace.port);
    let mut arguments = vec![
        "mock-venue",
        "--config",
        path_text(&workspace.config),
        "--listen",
        &address,
        "--replay",
        path_text(&workspace.recording),
    ];
    arguments.extend_from_slice(extra);
    let process = Process::spawn(&arguments);
    process.wait_for_output("listening");
    process
}

fn start_engine(workspace: &Workspace, journal: &Path, extra: &[&str]) -> Process {
    let address = format!("127.0.0.1:{}", workspace.port);
    let mut arguments = vec![
        "live",
        "--config",
        path_text(&workspace.config),
        "--journal",
        path_text(journal),
        "--venue",
        &address,
        "--replay",
        path_text(&workspace.recording),
        "--ctl-socket",
        path_text(&workspace.socket),
    ];
    arguments.extend_from_slice(extra);
    Process::spawn(&arguments)
}

fn status(workspace: &Workspace) -> String {
    send_command(&workspace.socket, "status").unwrap_or_default()
}

fn field(status: &str, name: &str) -> u64 {
    status
        .split_whitespace()
        .find_map(|word| word.strip_prefix(&format!("{name}=")))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn wait_for_status(workspace: &Workspace, describe: &str, check: impl Fn(&str) -> bool) -> String {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let current = status(workspace);
        if check(&current) {
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {describe}; last status: {current}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_replay_is_identical_at_any_speed() {
    let _guard = serial();
    let workspace = workspace("determinism", 4);
    let run = |name: &str, pacing: &str| {
        let journal = workspace.directory.join(name);
        let (succeeded, text) = run_to_end(&[
            "replay",
            "--config",
            path_text(&workspace.config),
            "--recording",
            path_text(&workspace.recording),
            "--journal",
            path_text(&journal),
            "--pacing",
            pacing,
        ]);
        assert!(succeeded, "{text}");
        (std::fs::read(journal).unwrap(), text)
    };

    let (first, text) = run("first.rbj", "max");
    let (second, _) = run("second.rbj", "max");
    let (paced, _) = run("paced.rbj", "recorded");
    assert!(text.contains("fills"), "{text}");
    assert!(first.len() > 1_000);
    assert_eq!(first, second);
    assert_eq!(first, paced);
}

#[test]
fn a_journal_reproduces_operator_commands() {
    let _guard = serial();
    let workspace = workspace("commands", 3);
    let journal = workspace.directory.join("live.rbj");
    let mock = start_mock(&workspace, &[]);
    let engine = start_engine(&workspace, &journal, &[]);
    wait_for_status(&workspace, "orders", |text| field(text, "orders") > 0);
    assert_eq!(
        send_command(&workspace.socket, "set spread_bps 3")
            .unwrap()
            .trim(),
        "ok"
    );
    std::thread::sleep(Duration::from_millis(200));
    engine.finish();
    mock.finish();

    let (first, second) = (
        workspace.directory.join("a.rbj"),
        workspace.directory.join("b.rbj"),
    );
    for target in [&first, &second] {
        let (succeeded, text) = run_to_end(&[
            "replay",
            "--config",
            path_text(&workspace.config),
            "--recording",
            path_text(&workspace.recording),
            "--journal",
            path_text(target),
            "--commands-from",
            path_text(&journal),
        ]);
        assert!(succeeded, "{text}");
    }
    assert_eq!(
        std::fs::read(first).unwrap(),
        std::fs::read(second).unwrap()
    );
}

#[test]
fn quotes_flow_and_the_operator_can_halt() {
    let _guard = serial();
    let workspace = workspace("halt", 8);
    let journal = workspace.directory.join("live.rbj");
    let mock = start_mock(&workspace, &[]);
    let engine = start_engine(&workspace, &journal, &[]);

    wait_for_status(&workspace, "fills", |text| field(text, "fills") > 0);
    assert_eq!(
        send_command(&workspace.socket, "set spread_bps 8")
            .unwrap()
            .trim(),
        "ok"
    );
    assert_eq!(
        send_command(&workspace.socket, "halt").unwrap().trim(),
        "halting"
    );
    wait_for_status(&workspace, "halt", |text| {
        text.contains("halted (Operator)")
    });

    let text = engine.finish();
    mock.finish();
    assert!(text.contains("tick-to-wire"), "{text}");
    let wire = text
        .lines()
        .find(|line| line.contains("tick-to-wire"))
        .unwrap();
    assert!(!wire.contains("count=0 "), "{text}");
}

#[test]
fn a_dropped_connection_reconciles_and_quoting_resumes() {
    let _guard = serial();
    let workspace = workspace("reconnect", 10);
    let journal = workspace.directory.join("live.rbj");
    let mock = start_mock(&workspace, &["--drop-after", "20"]);
    let engine = start_engine(&workspace, &journal, &[]);

    mock.wait_for_output("connection lost");
    let before = field(&status(&workspace), "orders");
    wait_for_status(&workspace, "quoting to resume", |text| {
        text.contains("status=running") && field(text, "orders") > before + 5
    });
    engine.finish();
    mock.finish();
}

#[test]
fn a_killed_engine_recovers_and_reconciles() {
    let _guard = serial();
    let workspace = workspace("crash", 14);
    let journal = workspace.directory.join("live.rbj");
    let mock = start_mock(&workspace, &[]);
    let first = start_engine(&workspace, &journal, &[]);

    wait_for_status(&workspace, "fills", |text| field(text, "fills") > 2);
    first.kill();
    std::thread::sleep(Duration::from_millis(300));

    let second = start_engine(&workspace, &journal, &["--recover"]);
    second.wait_for_output("recovery:");
    wait_for_status(&workspace, "reconciliation", |text| {
        text.contains("status=running") && field(text, "orders") > 0
    });
    let text = second.finish();
    mock.finish();
    assert!(text.contains("status             running"), "{text}");
    assert!(!text.contains("ReconcileFailed"), "{text}");
}

#[test]
fn a_new_run_on_a_used_venue_starts_flat() {
    let _guard = serial();
    let workspace = workspace("new-run", 14);
    let mock = start_mock(&workspace, &[]);

    let first = start_engine(&workspace, &workspace.directory.join("first.rbj"), &[]);
    wait_for_status(&workspace, "fills", |text| field(text, "fills") > 2);
    first.finish();

    let second = start_engine(&workspace, &workspace.directory.join("second.rbj"), &[]);
    wait_for_status(&workspace, "a fresh run to quote", |text| {
        text.contains("status=running") && field(text, "orders") > 2
    });
    let text = second.finish();
    mock.finish();
    assert!(text.contains("status             running"), "{text}");
}

#[test]
fn a_restarted_venue_is_detected_as_lost_state() {
    let _guard = serial();
    let workspace = workspace("venue-lost", 12);
    let journal = workspace.directory.join("live.rbj");
    let mock = start_mock(&workspace, &[]);
    let engine = start_engine(&workspace, &journal, &[]);

    wait_for_status(&workspace, "orders", |text| field(text, "orders") > 0);
    mock.kill();
    std::thread::sleep(Duration::from_millis(200));
    let replacement = start_mock(&workspace, &[]);

    wait_for_status(&workspace, "the halt", |text| {
        text.contains("halted (VenueStateLost)")
    });
    engine.finish();
    replacement.finish();
}

#[test]
fn a_damaged_journal_stops_recovery() {
    let _guard = serial();
    let workspace = workspace("damaged", 2);
    let journal = workspace.directory.join("live.rbj");
    let (succeeded, text) = run_to_end(&[
        "replay",
        "--config",
        path_text(&workspace.config),
        "--recording",
        path_text(&workspace.recording),
        "--journal",
        path_text(&journal),
    ]);
    assert!(succeeded, "{text}");

    let mut bytes = std::fs::read(&journal).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xFF;
    std::fs::write(&journal, bytes).unwrap();

    let (succeeded, text) = run_to_end(&[
        "live",
        "--config",
        path_text(&workspace.config),
        "--journal",
        path_text(&journal),
        "--replay",
        path_text(&workspace.recording),
        "--recover",
        "--seconds",
        "1",
    ]);
    assert!(!succeeded);
    assert!(text.contains("recovery stopped"), "{text}");
}

#[test]
fn an_invalid_config_stops_startup_with_every_problem() {
    let _guard = serial();
    let workspace = workspace("invalid", 1);
    std::fs::write(
        &workspace.config,
        CONFIG
            .replace(r#"spread_bps = "2""#, r#"spread_bps = "0""#)
            .replace("max_orders_per_sec = 200", "max_orders_per_sec = 0"),
    )
    .unwrap();
    let (succeeded, text) = run_to_end(&["validate", path_text(&workspace.config)]);
    assert!(!succeeded);
    assert!(text.contains("strategy.spread_bps"), "{text}");
    assert!(text.contains("risk.max_orders_per_sec"), "{text}");
}

#[test]
fn a_recording_verifies_its_trade_sides() {
    let _guard = serial();
    let workspace = workspace("verify", 3);
    let (succeeded, text) = run_to_end(&[
        "verify-recording",
        "--config",
        path_text(&workspace.config),
        "--recording",
        path_text(&workspace.recording),
    ]);
    assert!(succeeded, "{text}");
    assert!(text.contains("malformed=0"), "{text}");
}
