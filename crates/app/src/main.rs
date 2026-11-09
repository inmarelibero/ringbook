//! The ringbook command line.

mod live;
mod mock;
mod registry;
mod setup;
mod verify;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use ringbook_clock::Clock;
use ringbook_core::{ControlCommand, Event, VenueId};
use ringbook_engine::{Config, Engine, EngineChannels, InProcess, MarketMaker};
use ringbook_feeds::{FeedConfig, InstrumentMeta, RecordingReader, start_recorder};
use ringbook_journal::{JournalBody, JournalHeader, JournalWriter, read_journal};
use ringbook_replay::{Pacing, ReplayVenue, read_metadata, run_replay, venue_file};
use ringbook_ring::channel;
use ringbook_telemetry::{Percentiles, TelemetryReport};
use tokio::sync::watch;

pub(crate) const EVENT_RING: usize = 1 << 17;
pub(crate) const JOURNAL_RING: usize = 1 << 16;
pub(crate) const TELEMETRY_RING: usize = 1 << 18;
pub(crate) const API_RING: usize = 1 << 12;
pub(crate) const CONTROL_RING: usize = 1 << 8;

#[derive(Parser)]
#[command(
    version,
    about = "A low-latency crypto trading engine with simulated execution"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Waits for an interrupt or termination signal.
pub(crate) async fn wait_for_shutdown() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("installing a signal handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum PacingArg {
    /// As fast as possible; measures throughput.
    Max,
    /// At the original pace; measures latency.
    Recorded,
}

#[derive(Subcommand)]
enum Command {
    /// Checks a config file without running anything.
    Validate {
        /// The config file.
        config: PathBuf,
    },
    /// Records raw market data from every configured venue.
    Record {
        /// The config file.
        #[arg(long)]
        config: PathBuf,
        /// The directory to create.
        #[arg(long)]
        out: PathBuf,
        /// How long to record.
        #[arg(long)]
        seconds: u64,
    },
    /// Replays a recording through the engine.
    Replay {
        /// The config file.
        #[arg(long)]
        config: PathBuf,
        /// The recording directory.
        #[arg(long)]
        recording: PathBuf,
        /// How fast to play the recording.
        #[arg(long, value_enum, default_value = "max")]
        pacing: PacingArg,
        /// Where to write the journal.
        #[arg(long)]
        journal: PathBuf,
        /// A journal whose operator commands should be reapplied.
        #[arg(long)]
        commands_from: Option<PathBuf>,
        /// The core to pin the hot thread to.
        #[arg(long)]
        hot_core: Option<usize>,
    },
    /// Runs the engine against the mock venue.
    Live {
        /// The config file.
        #[arg(long)]
        config: PathBuf,
        /// Where the journal lives.
        #[arg(long)]
        journal: PathBuf,
        /// The mock venue's host and port.
        #[arg(long, default_value = "127.0.0.1:9100")]
        venue: String,
        /// Plays a recording at its original pace instead of connecting to venues.
        #[arg(long)]
        replay: Option<PathBuf>,
        /// Records raw market data to this directory.
        #[arg(long)]
        record_to: Option<PathBuf>,
        /// Recovers from an existing journal.
        #[arg(long)]
        recover: bool,
        /// The API gateway's address.
        #[arg(long)]
        api: Option<std::net::SocketAddr>,
        /// The Prometheus endpoint's address.
        #[arg(long)]
        metrics: Option<std::net::SocketAddr>,
        /// The operator socket.
        #[arg(long, default_value = ringbook_ctl::DEFAULT_SOCKET)]
        ctl_socket: PathBuf,
        /// Stops after this many seconds.
        #[arg(long)]
        seconds: Option<u64>,
        /// The hot thread's core.
        #[arg(long)]
        hot_core: Option<usize>,
        /// The gateway thread's core.
        #[arg(long)]
        gateway_core: Option<usize>,
        /// Takes client addresses from the reverse proxy's header.
        #[arg(long)]
        behind_proxy: bool,
    },
    /// Runs the mock venue process.
    MockVenue {
        /// The config file.
        #[arg(long)]
        config: PathBuf,
        /// The address to listen on.
        #[arg(long, default_value = "127.0.0.1:9100")]
        listen: std::net::SocketAddr,
        /// Plays a recording at its original pace instead of connecting to venues.
        #[arg(long)]
        replay: Option<PathBuf>,
        /// Injected fault: drops the connection after this many reports.
        #[arg(long)]
        drop_after: Option<u64>,
        /// The core to pin the venue thread to.
        #[arg(long)]
        core: Option<usize>,
    },
    /// Checks a recording, including each venue's trade-side mapping.
    VerifyRecording {
        /// The config file.
        #[arg(long)]
        config: PathBuf,
        /// The recording directory.
        #[arg(long)]
        recording: PathBuf,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().command {
        Command::Validate { config } => validate(&config),
        Command::Record {
            config,
            out,
            seconds,
        } => record(&config, &out, seconds),
        Command::Replay {
            config,
            recording,
            pacing,
            journal,
            commands_from,
            hot_core,
        } => replay(
            &config,
            &recording,
            match pacing {
                PacingArg::Max => Pacing::Max,
                PacingArg::Recorded => Pacing::Recorded,
            },
            &journal,
            commands_from.as_deref(),
            hot_core,
        ),
        Command::Live {
            config,
            journal,
            venue,
            replay,
            record_to,
            recover,
            api,
            metrics,
            ctl_socket,
            seconds,
            hot_core,
            gateway_core,
            behind_proxy,
        } => live::run(&live::LiveArgs {
            config,
            journal,
            venue,
            replay,
            record_to,
            recover,
            api,
            metrics,
            ctl_socket,
            seconds,
            hot_core,
            gateway_core,
            behind_proxy,
        }),
        Command::MockVenue {
            config,
            listen,
            replay,
            drop_after,
            core,
        } => mock::run(&mock::MockArgs {
            config,
            listen,
            replay,
            drop_after,
            core,
        }),
        Command::VerifyRecording { config, recording } => {
            verify::verify_recording(&config, &recording)
        }
    }
}

pub(crate) fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the async runtime")
}

fn validate(path: &Path) -> Result<()> {
    let config = setup::load_config(path)?;
    println!(
        "{}: ok ({} instruments on {} venues)",
        path.display(),
        config.instruments.len(),
        config.venue_names.len()
    );
    Ok(())
}

pub(crate) async fn fetch_all_metadata(
    config: &Config,
) -> Result<Vec<(Vec<InstrumentMeta>, Vec<u8>)>> {
    let mut all = Vec::with_capacity(config.venue_names.len());
    for (index, name) in config.venue_names.iter().enumerate() {
        all.push(registry::metadata(name, &setup::venue_symbols(config, index)).await?);
    }
    Ok(all)
}

fn record(config_path: &Path, out: &Path, seconds: u64) -> Result<()> {
    let config = setup::load_config(config_path)?;
    std::fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;
    let clock = Clock::new();

    runtime()?.block_on(async {
        let metadata = fetch_all_metadata(&config).await?;
        let metas: Vec<_> = metadata.iter().map(|(metas, _)| metas.clone()).collect();
        setup::compile(&config, &metas)?;

        let (shutdown_tx, shutdown) = watch::channel(false);
        let mut feeds = Vec::new();
        let mut writers = Vec::new();
        for (index, name) in config.venue_names.iter().enumerate() {
            let (recorder, writer) = start_recorder(&venue_file(out, name))
                .with_context(|| format!("creating the {name} recording"))?;
            writers.push(writer);
            feeds.push(registry::spawn_recording_feed(
                name,
                VenueId(index as u8),
                FeedConfig {
                    instruments: setup::instrument_specs(&config, index, &metadata[index].0),
                    metadata: Arc::new(metadata[index].1.clone()),
                    clock: clock.clone(),
                    recorder: Some(recorder),
                },
                shutdown.clone(),
            )?);
        }

        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(seconds)) => {}
            _ = tokio::signal::ctrl_c() => tracing::info!("stopping early"),
        }
        shutdown_tx.send(true).ok();
        for feed in feeds {
            feed.await.ok();
        }
        for writer in writers {
            tokio::task::spawn_blocking(move || writer.join().ok())
                .await
                .ok();
        }
        println!("recorded to {}", out.display());
        Ok(())
    })
}

fn load_script(path: Option<&Path>) -> Result<Vec<(u64, ControlCommand)>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let contents = read_journal(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(contents
        .records
        .iter()
        .filter_map(|record| match record.body {
            JournalBody::Control(command) => Some((record.batch, command)),
            _ => None,
        })
        .collect())
}

fn replay(
    config_path: &Path,
    recording: &Path,
    pacing: Pacing,
    journal_path: &Path,
    commands_from: Option<&Path>,
    hot_core: Option<usize>,
) -> Result<()> {
    let config = setup::load_config(config_path)?;
    let clock = Clock::new();
    let script = load_script(commands_from)?;

    let mut metas = Vec::with_capacity(config.venue_names.len());
    for (index, name) in config.venue_names.iter().enumerate() {
        let path = venue_file(recording, name);
        let body = read_metadata(&path).with_context(|| format!("reading {}", path.display()))?;
        metas.push(registry::parse_metadata(
            name,
            &body,
            &setup::venue_symbols(&config, index),
        )?);
    }
    let params = setup::compile(&config, &metas)?;

    let mut venues = Vec::with_capacity(config.venue_names.len());
    for (index, name) in config.venue_names.iter().enumerate() {
        let path = venue_file(recording, name);
        venues.push(ReplayVenue {
            venue: VenueId(index as u8),
            parser: registry::parser(
                name,
                VenueId(index as u8),
                setup::instrument_specs(&config, index, &metas[index]),
            )?,
            reader: RecordingReader::open(&path)
                .with_context(|| format!("opening {}", path.display()))?,
        });
    }

    let (mut producer, input) = channel::<Event>(EVENT_RING)?;
    let (_control_tx, control) = channel(CONTROL_RING)?;
    let (journal_tx, journal_rx) = channel(JOURNAL_RING)?;
    let (telemetry_tx, telemetry_rx) = channel(TELEMETRY_RING)?;
    let (api_tx, mut api_rx) = channel(API_RING)?;
    let stop = Arc::new(AtomicBool::new(false));
    let journal_stop = Arc::new(AtomicBool::new(false));
    let telemetry_stop = Arc::new(AtomicBool::new(false));
    let journal_failed = Arc::new(AtomicBool::new(false));
    let inputs_done = Arc::new(AtomicBool::new(false));

    let journal = JournalWriter::start(
        journal_path,
        &JournalHeader {
            config: config.text.clone(),
            binary_version: env!("CARGO_PKG_VERSION").to_owned(),
            epoch: 1,
        },
        journal_rx,
        Arc::clone(&journal_stop),
        Arc::clone(&journal_failed),
    )
    .with_context(|| format!("creating {}", journal_path.display()))?;
    let telemetry =
        ringbook_telemetry::start(vec![telemetry_rx], |_| {}, Arc::clone(&telemetry_stop))?;

    let mut engine = Engine::new(
        &params,
        MarketMaker::new(params.strategy),
        InProcess::new(setup::exchange(&config, &params)),
        EngineChannels {
            inputs: vec![input],
            control,
            journal: journal_tx,
            telemetry: telemetry_tx,
            api: api_tx,
            halt_flag: Arc::new(AtomicBool::new(false)),
            journal_failed,
            inputs_done: Arc::clone(&inputs_done),
        },
        clock.clone(),
        1,
    );
    engine.set_script(script);
    let hot = std::thread::Builder::new()
        .name("hot".into())
        .spawn(move || {
            setup::pin_current_thread(hot_core);
            engine.run()
        })?;

    let started = std::time::Instant::now();
    let replayed = std::thread::scope(|scope| {
        let drain_stop = Arc::clone(&inputs_done);
        scope.spawn(move || {
            while !drain_stop.load(Ordering::Acquire) {
                while api_rx.pop().is_some() {}
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let replayed = run_replay(venues, pacing, &clock, &mut producer, &stop);
        inputs_done.store(true, Ordering::Release);
        replayed
    });
    let summary = replayed?;

    let run = hot
        .join()
        .map_err(|_| anyhow::anyhow!("the hot thread panicked"))?;
    let elapsed = started.elapsed();
    journal_stop.store(true, Ordering::Release);
    let records = journal.finish().context("writing the journal")?;
    telemetry_stop.store(true, Ordering::Release);
    let report = telemetry
        .join()
        .map_err(|_| anyhow::anyhow!("the telemetry thread panicked"))?;

    print_report(&summary, &run, records, elapsed, &report);
    if summary.recording_gaps > 0 {
        bail!("the recording has {} gaps", summary.recording_gaps);
    }
    Ok(())
}

pub(crate) fn print_percentiles(name: &str, value: &Percentiles) {
    println!(
        "{name:<18} count={:<10} p50={:>9}ns p99={:>9}ns p99.9={:>9}ns max={:>11}ns",
        value.count, value.p50, value.p99, value.p999, value.max
    );
}

fn print_report(
    summary: &ringbook_replay::ReplaySummary,
    run: &ringbook_engine::RunSummary,
    journal_records: u64,
    elapsed: Duration,
    report: &TelemetryReport,
) {
    let counters = &run.counters;
    println!("status             {:?}", run.status);
    println!("records            {}", summary.records);
    println!("events             {}", counters.events);
    println!("batches            {}", counters.batches);
    println!("elapsed            {elapsed:.3?}");
    println!(
        "throughput         {:.0} events/s",
        counters.events as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
    );
    println!("orders sent        {}", counters.orders_sent);
    println!("cancels sent       {}", counters.cancels_sent);
    println!("fills              {}", counters.fills);
    println!("risk rejections    {}", counters.risk_rejections);
    println!("venue rejections   {}", counters.venue_rejections);
    println!("book recentres     {}", counters.book_recentres);
    println!("telemetry drops    {}", counters.telemetry_drops);
    println!("journal records    {journal_records}");
    println!(
        "equity             {:.2} USD (excludes funding)",
        counters.equity_micro_usd as f64 / 1e6
    );
    println!(
        "drawdown           {:.2} USD",
        counters.drawdown_micro_usd as f64 / 1e6
    );
    print_percentiles("tick-to-decision", &report.total.tick_to_decision);
    print_percentiles("batch latency", &report.total.batch);
}
