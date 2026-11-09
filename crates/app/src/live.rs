//! Runs the engine against the mock venue, on live or recorded market data.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use ringbook_api::{ApiConfig, Limits, Names, PairNames};
use ringbook_clock::Clock;
use ringbook_core::{Event, OrderCommand, VenueId};
use ringbook_ctl::start_control_server;
use ringbook_engine::{
    Config, Engine, EngineChannels, EngineParams, EngineStatus, MarketMaker, TelemetryRecord,
};
use ringbook_feeds::{FeedConfig, InstrumentMeta, RecorderHandle, RecordingReader, start_recorder};
use ringbook_gateway::{GatewayConfig, start_gateway};
use ringbook_journal::{
    JournalBody, JournalContents, JournalHeader, JournalWriter, read_journal, usable_length,
};
use ringbook_replay::{Pacing, ReplayVenue, read_metadata, run_replay, venue_file};
use ringbook_ring::{Consumer, channel};
use ringbook_telemetry::storage::{self, RunInfo, StorageHandle};
use tokio::sync::watch;

use crate::{
    API_RING, CONTROL_RING, EVENT_RING, JOURNAL_RING, TELEMETRY_RING, fetch_all_metadata,
    print_percentiles, registry, runtime, setup, wait_for_shutdown,
};

/// The live command's options.
pub struct LiveArgs {
    /// The config file.
    pub config: PathBuf,
    /// Where the journal lives.
    pub journal: PathBuf,
    /// The mock venue's host and port.
    pub venue: String,
    /// Plays a recording at its original pace instead of connecting to venues.
    pub replay: Option<PathBuf>,
    /// Records raw market data to this directory.
    pub record_to: Option<PathBuf>,
    /// Recovers from an existing journal.
    pub recover: bool,
    /// The API gateway's address.
    pub api: Option<SocketAddr>,
    /// The Prometheus endpoint's address.
    pub metrics: Option<SocketAddr>,
    /// The operator socket.
    pub ctl_socket: PathBuf,
    /// Stops after this long.
    pub seconds: Option<u64>,
    /// The hot thread's core.
    pub hot_core: Option<usize>,
    /// The gateway thread's core.
    pub gateway_core: Option<usize>,
    /// Takes client addresses from the reverse proxy's header.
    pub behind_proxy: bool,
}

struct Recovery {
    contents: JournalContents,
    usable: u64,
    session: u64,
    last_sequence: u64,
    epoch: u16,
}

fn load_recovery(path: &Path, config: &Config) -> Result<Recovery> {
    let contents = read_journal(path)
        .with_context(|| format!("recovery stopped: {} is damaged", path.display()))?;
    if contents.header.config != config.text {
        bail!("recovery stopped: the journal was written with a different config");
    }
    let usable = usable_length(path)?;
    let mut session = 0;
    let mut last_sequence = 0;
    let mut epoch = contents.header.epoch;
    for record in &contents.records {
        match record.body {
            JournalBody::Session(value) => session = value,
            JournalBody::Report(report) => last_sequence = last_sequence.max(report.sequence),
            JournalBody::Sent(OrderCommand::New(order)) => epoch = epoch.max(order.id.epoch()),
            _ => {}
        }
    }
    if contents.truncated_tail {
        eprintln!("recovery: ignoring a damaged final journal record");
    }
    Ok(Recovery {
        contents,
        usable,
        session,
        last_sequence,
        epoch: epoch.saturating_add(1),
    })
}

fn pair_names(config: &Config, params: &EngineParams) -> Names {
    let mut names = Names::default();
    for venue in 0..params.venues {
        for (instrument, rules) in config.instruments.iter().enumerate() {
            if let Some(slot) = params.slots[venue][instrument] {
                names.pairs.insert(
                    (venue as u8, instrument as u16),
                    PairNames {
                        venue: config.venue_names[venue].clone(),
                        instrument: rules.name.clone(),
                        price_decimals: slot.price_decimals,
                        qty_decimals: slot.qty_decimals,
                    },
                );
            }
        }
    }
    names
}

fn status_name(status: EngineStatus) -> String {
    match status {
        EngineStatus::Running => "running".to_owned(),
        EngineStatus::AwaitingReconcile => "awaiting-reconcile".to_owned(),
        EngineStatus::Halted(reason) => format!("halted ({})", reason.name()),
    }
}

/// A source of market data for one run.
struct Sources {
    inputs: Vec<Consumer<Event>>,
    feeds: Vec<tokio::task::JoinHandle<()>>,
    replay: Option<std::thread::JoinHandle<()>>,
    recorders: Vec<RecorderHandle>,
    writers: Vec<std::thread::JoinHandle<()>>,
    params: EngineParams,
}

fn replay_sources(
    config: &Config,
    recording: &Path,
    clock: &Clock,
    stop: &Arc<AtomicBool>,
) -> Result<Sources> {
    let mut metas: Vec<Vec<InstrumentMeta>> = Vec::new();
    let mut venues = Vec::new();
    for (index, name) in config.venue_names.iter().enumerate() {
        let path = venue_file(recording, name);
        let body = read_metadata(&path).with_context(|| format!("reading {}", path.display()))?;
        let venue_metas =
            registry::parse_metadata(name, &body, &setup::venue_symbols(config, index))?;
        venues.push(ReplayVenue {
            venue: VenueId(index as u8),
            parser: registry::parser(
                name,
                VenueId(index as u8),
                setup::instrument_specs(config, index, &venue_metas),
            )?,
            reader: RecordingReader::open(&path)?,
        });
        metas.push(venue_metas);
    }
    let params = setup::compile(config, &metas)?;

    let (mut producer, input) = channel::<Event>(EVENT_RING)?;
    let (replay_clock, replay_stop) = (clock.clone(), Arc::clone(stop));
    let replay = std::thread::spawn(move || {
        match run_replay(
            venues,
            Pacing::Recorded,
            &replay_clock,
            &mut producer,
            &replay_stop,
        ) {
            Ok(summary) => eprintln!("replay finished: {} events", summary.events),
            Err(error) => eprintln!("replay failed: {error}"),
        }
    });
    Ok(Sources {
        inputs: vec![input],
        feeds: Vec::new(),
        replay: Some(replay),
        recorders: Vec::new(),
        writers: Vec::new(),
        params,
    })
}

async fn live_sources(
    config: &Config,
    record_to: Option<&Path>,
    clock: &Clock,
    shutdown: &watch::Receiver<bool>,
) -> Result<Sources> {
    let metadata = fetch_all_metadata(config).await?;
    let metas: Vec<_> = metadata.iter().map(|(metas, _)| metas.clone()).collect();
    let params = setup::compile(config, &metas)?;

    let mut sources = Sources {
        inputs: Vec::new(),
        feeds: Vec::new(),
        replay: None,
        recorders: Vec::new(),
        writers: Vec::new(),
        params,
    };
    if let Some(directory) = record_to {
        std::fs::create_dir_all(directory)?;
    }
    for (index, name) in config.venue_names.iter().enumerate() {
        let recorder = match record_to {
            Some(directory) => {
                let (handle, writer) = start_recorder(&venue_file(directory, name))?;
                sources.writers.push(writer);
                sources.recorders.push(handle.clone());
                Some(handle)
            }
            None => None,
        };
        let (producer, input) = channel::<Event>(EVENT_RING)?;
        sources.inputs.push(input);
        sources.feeds.push(registry::spawn_feed(
            name,
            VenueId(index as u8),
            FeedConfig {
                instruments: setup::instrument_specs(config, index, &metadata[index].0),
                metadata: Arc::new(metadata[index].1.clone()),
                clock: clock.clone(),
                recorder,
            },
            producer,
            shutdown.clone(),
        )?);
    }
    Ok(sources)
}

/// Runs until interrupted, the time limit passes, or a recording ends.
#[allow(clippy::too_many_lines)]
pub fn run(args: &LiveArgs) -> Result<()> {
    let config = setup::load_config(&args.config)?;
    let clock = Clock::new();
    let recovery = if args.recover && args.journal.exists() {
        Some(load_recovery(&args.journal, &config)?)
    } else {
        None
    };
    if recovery.is_none() && args.journal.exists() {
        bail!(
            "{} already exists; pass --recover to continue it",
            args.journal.display()
        );
    }

    runtime()?.block_on(async {
        let replay_stop = Arc::new(AtomicBool::new(false));
        let (shutdown_tx, shutdown) = watch::channel(false);
        let mut sources = match &args.replay {
            Some(recording) => replay_sources(&config, recording, &clock, &replay_stop)?,
            None => live_sources(&config, args.record_to.as_deref(), &clock, &shutdown).await?,
        };
        let params = sources.params;

        if let Some(address) = args.metrics {
            ringbook_telemetry::metrics_export::install_exporter(address)
                .context("starting the metrics endpoint")?;
        }

        let database_url = std::env::var("DATABASE_URL").ok();
        let run_info = RunInfo {
            run_id: clock.event_time().0 as i64,
            mode: if args.replay.is_some() {
                "replay"
            } else {
                "live"
            }
            .to_owned(),
            config: config.text.clone(),
            binary_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let mut storage: Option<(StorageHandle, tokio::task::JoinHandle<()>)> = None;
        if let Some(url) = &database_url {
            storage = Some(storage::start_writer(url, run_info).context("invalid DATABASE_URL")?);
        }

        let (control_tx, control) = channel(CONTROL_RING)?;
        let (journal_tx, journal_rx) = channel(JOURNAL_RING)?;
        let (telemetry_tx, telemetry_rx) = channel::<TelemetryRecord>(TELEMETRY_RING)?;
        let (gateway_telemetry_tx, gateway_telemetry_rx) = channel(TELEMETRY_RING)?;
        let (api_tx, api_rx) = channel(API_RING)?;
        let halt_flag = Arc::new(AtomicBool::new(false));
        let journal_failed = Arc::new(AtomicBool::new(false));
        let inputs_done = Arc::new(AtomicBool::new(false));
        let journal_stop = Arc::new(AtomicBool::new(false));
        let telemetry_stop = Arc::new(AtomicBool::new(false));
        let api_stop = Arc::new(AtomicBool::new(false));
        let status_text = Arc::new(Mutex::new("starting".to_owned()));

        let journal = match &recovery {
            Some(found) => JournalWriter::resume(
                &args.journal,
                found.usable,
                journal_rx,
                Arc::clone(&journal_stop),
                Arc::clone(&journal_failed),
            ),
            None => JournalWriter::start(
                &args.journal,
                &JournalHeader {
                    config: config.text.clone(),
                    binary_version: env!("CARGO_PKG_VERSION").to_owned(),
                    epoch: 1,
                },
                journal_rx,
                Arc::clone(&journal_stop),
                Arc::clone(&journal_failed),
            ),
        }
        .with_context(|| format!("opening {}", args.journal.display()))?;

        let (link, gateway) = start_gateway(
            GatewayConfig {
                address: args.venue.clone(),
                session: recovery.as_ref().map_or(0, |found| found.session),
                last_sequence: recovery.as_ref().map_or(0, |found| found.last_sequence),
                core: args.gateway_core,
            },
            clock.clone(),
            gateway_telemetry_tx,
            setup::pin_current_thread,
        )?;

        let storage_for_sink = storage.as_ref().map(|(handle, _)| handle.clone());
        let status_for_sink = Arc::clone(&status_text);
        let export_metrics = args.metrics.is_some();
        let telemetry = ringbook_telemetry::start(
            vec![telemetry_rx, gateway_telemetry_rx],
            move |snapshot| {
                if export_metrics {
                    ringbook_telemetry::metrics_export::publish(snapshot);
                }
                if let Some(handle) = &storage_for_sink {
                    handle.snapshot(*snapshot);
                }
                let counters = &snapshot.counters;
                if let Ok(mut text) = status_for_sink.lock() {
                    *text = format!(
                        "status={} events={} orders={} cancels={} fills={} risk_rejections={} \
                         equity_usd={:.2} drawdown_usd={:.2}",
                        status_name(counters.status),
                        counters.events,
                        counters.orders_sent,
                        counters.cancels_sent,
                        counters.fills,
                        counters.risk_rejections,
                        counters.equity_micro_usd as f64 / 1e6,
                        counters.drawdown_micro_usd as f64 / 1e6,
                    );
                }
            },
            Arc::clone(&telemetry_stop),
        )?;

        let instrument_names = config
            .instruments
            .iter()
            .map(|instrument| (instrument.name.clone(), instrument.id))
            .collect();
        let control_server = start_control_server(
            &args.ctl_socket,
            control_tx,
            Arc::clone(&halt_flag),
            Arc::clone(&status_text),
            instrument_names,
        )
        .with_context(|| format!("creating {}", args.ctl_socket.display()))?;

        let database = match &database_url {
            Some(url) => Some(
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(3)
                    .connect_lazy(url)?,
            ),
            None => None,
        };
        let (api_state, api_bridge) = ringbook_api::start_bridge(
            api_rx,
            ApiConfig {
                rules: config.text.clone(),
                database,
                names: pair_names(&config, &params),
                limits: Limits::default(),
                trust_forwarded_for: args.behind_proxy,
            },
            Arc::clone(&api_stop),
        )?;
        let api_server = match args.api {
            Some(address) => {
                let listener = tokio::net::TcpListener::bind(address)
                    .await
                    .with_context(|| format!("listening on {address}"))?;
                let mut stopped = shutdown.clone();
                let service = ringbook_api::router(api_state)
                    .into_make_service_with_connect_info::<SocketAddr>();
                Some(tokio::spawn(async move {
                    let _ = axum::serve(listener, service)
                        .with_graceful_shutdown(async move {
                            let _ = stopped.wait_for(|value| *value).await;
                        })
                        .await;
                }))
            }
            None => None,
        };

        let mut engine = Engine::new(
            &params,
            MarketMaker::new(params.strategy),
            link,
            EngineChannels {
                inputs: std::mem::take(&mut sources.inputs),
                control,
                journal: journal_tx,
                telemetry: telemetry_tx,
                api: api_tx,
                halt_flag,
                journal_failed,
                inputs_done: Arc::clone(&inputs_done),
            },
            clock.clone(),
            recovery.as_ref().map_or(1, |found| found.epoch),
        );
        if let Some(found) = &recovery {
            engine.recover(&found.contents.records);
            engine.begin_reconcile();
            eprintln!(
                "recovery: {} records, session {}, last report {}",
                found.contents.records.len(),
                found.session,
                found.last_sequence
            );
        }
        let hot_core = args.hot_core;
        let hot = std::thread::Builder::new()
            .name("hot".into())
            .spawn(move || {
                setup::pin_current_thread(hot_core);
                engine.run()
            })?;

        let replay_thread = sources.replay.take();
        let replay_watch = replay_thread.as_ref().map(|_| ());
        let wait = async {
            match args.seconds {
                Some(seconds) => {
                    tokio::select! {
                        () = tokio::time::sleep(Duration::from_secs(seconds)) => {}
                        () = wait_for_shutdown() => {}
                    }
                }
                None => wait_for_shutdown().await,
            }
        };
        let replay_ended = async {
            if replay_watch.is_some() {
                if let Some(thread) = replay_thread {
                    let _ = tokio::task::spawn_blocking(move || thread.join()).await;
                }
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            () = wait => {}
            () = replay_ended => {}
        }

        // Shut down from the sources inwards, so nothing is left in flight.
        replay_stop.store(true, Ordering::Release);
        shutdown_tx.send(true).ok();
        for feed in sources.feeds.drain(..) {
            feed.await.ok();
        }
        drop(sources.recorders);
        for writer in sources.writers.drain(..) {
            tokio::task::spawn_blocking(move || writer.join().ok())
                .await
                .ok();
        }
        inputs_done.store(true, Ordering::Release);
        let run = tokio::task::spawn_blocking(move || hot.join())
            .await?
            .map_err(|_| anyhow::anyhow!("the hot thread panicked"))?;
        gateway.stop();
        journal_stop.store(true, Ordering::Release);
        let records = tokio::task::spawn_blocking(move || journal.finish())
            .await?
            .context("writing the journal")?;
        control_server.stop();
        api_stop.store(true, Ordering::Release);
        api_bridge.join().ok();
        if let Some(server) = api_server {
            server.await.ok();
        }
        telemetry_stop.store(true, Ordering::Release);
        let report = tokio::task::spawn_blocking(move || telemetry.join())
            .await?
            .map_err(|_| anyhow::anyhow!("the telemetry thread panicked"))?;

        if let Some((handle, task)) = storage {
            let status: &'static str = match run.status {
                EngineStatus::Running => "finished",
                EngineStatus::AwaitingReconcile => "awaiting-reconcile",
                EngineStatus::Halted(reason) => reason.name(),
            };
            handle.finished(report, status).await;
            drop(handle);
            task.await.ok();
        }

        println!("status             {}", status_name(run.status));
        println!("events             {}", run.counters.events);
        println!("orders sent        {}", run.counters.orders_sent);
        println!("fills              {}", run.counters.fills);
        println!("journal records    {records}");
        println!(
            "equity             {:.2} USD (excludes funding)",
            run.counters.equity_micro_usd as f64 / 1e6
        );
        print_percentiles("tick-to-decision", &report.total.tick_to_decision);
        print_percentiles("tick-to-wire", &report.total.tick_to_wire);
        print_percentiles("batch latency", &report.total.batch);
        Ok(())
    })
}
