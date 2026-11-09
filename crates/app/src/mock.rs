//! Runs the mock venue process.

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use ringbook_clock::Clock;
use ringbook_core::{Event, VenueId};
use ringbook_feeds::{FeedConfig, RecordingReader};
use ringbook_mock_venue::{MockVenue, MockVenueConfig};
use ringbook_replay::{Pacing, ReplayVenue, read_metadata, run_replay, venue_file};
use ringbook_ring::{Consumer, channel};
use tokio::sync::watch;

use crate::{EVENT_RING, fetch_all_metadata, registry, runtime, setup, wait_for_shutdown};

const RETAINED_REPORTS: usize = 1 << 18;

/// The mock venue's options.
pub struct MockArgs {
    /// The config file.
    pub config: PathBuf,
    /// The address to listen on.
    pub listen: SocketAddr,
    /// Plays a recording instead of connecting to the venues.
    pub replay: Option<PathBuf>,
    /// Injected fault: drop the connection after this many reports.
    pub drop_after: Option<u64>,
    /// The core to pin the venue thread to.
    pub core: Option<usize>,
}

/// Runs a mock venue until interrupted.
pub fn run(args: &MockArgs) -> Result<()> {
    let config = setup::load_config(&args.config)?;
    let clock = Clock::new();
    let listener =
        TcpListener::bind(args.listen).with_context(|| format!("listening on {}", args.listen))?;
    let stop = Arc::new(AtomicBool::new(false));
    let session = clock.event_time().0;
    eprintln!("mock venue: session {session} listening on {}", args.listen);

    runtime()?.block_on(async {
        let (shutdown_tx, shutdown) = watch::channel(false);
        let mut feeds = Vec::new();
        let mut inputs: Vec<Consumer<Event>> = Vec::new();
        let mut replay_thread = None;

        let params = if let Some(recording) = &args.replay {
            let mut metas = Vec::new();
            let mut venues = Vec::new();
            for (index, name) in config.venue_names.iter().enumerate() {
                let path = venue_file(recording, name);
                let body = read_metadata(&path)?;
                let venue_metas =
                    registry::parse_metadata(name, &body, &setup::venue_symbols(&config, index))?;
                venues.push(ReplayVenue {
                    venue: VenueId(index as u8),
                    parser: registry::parser(
                        name,
                        VenueId(index as u8),
                        setup::instrument_specs(&config, index, &venue_metas),
                    )?,
                    reader: RecordingReader::open(&path)?,
                });
                metas.push(venue_metas);
            }
            let (mut producer, input) = channel::<Event>(EVENT_RING)?;
            inputs.push(input);
            let (replay_clock, replay_stop) = (clock.clone(), Arc::clone(&stop));
            replay_thread = Some(std::thread::spawn(move || {
                run_replay(
                    venues,
                    Pacing::Recorded,
                    &replay_clock,
                    &mut producer,
                    &replay_stop,
                )
            }));
            setup::compile(&config, &metas)?
        } else {
            let metadata = fetch_all_metadata(&config).await?;
            let metas: Vec<_> = metadata.iter().map(|(metas, _)| metas.clone()).collect();
            let params = setup::compile(&config, &metas)?;
            for (index, name) in config.venue_names.iter().enumerate() {
                let (producer, input) = channel::<Event>(EVENT_RING)?;
                inputs.push(input);
                feeds.push(registry::spawn_feed(
                    name,
                    VenueId(index as u8),
                    FeedConfig {
                        instruments: setup::instrument_specs(&config, index, &metadata[index].0),
                        metadata: Arc::new(metadata[index].1.clone()),
                        clock: clock.clone(),
                        recorder: None,
                    },
                    producer,
                    shutdown.clone(),
                )?);
            }
            params
        };

        let mut venue = MockVenue::new(
            setup::exchange(&config, &params),
            MockVenueConfig {
                session,
                retained_reports: RETAINED_REPORTS,
                venues: params.venues as u8,
                instruments: params.instruments as u16,
                drop_connection_after: args.drop_after,
            },
            inputs,
            clock.clone(),
            listener,
        )?;
        let (venue_stop, core) = (Arc::clone(&stop), args.core);
        let venue_thread = std::thread::Builder::new()
            .name("mock-venue".into())
            .spawn(move || {
                setup::pin_current_thread(core);
                venue.run(&venue_stop)
            })?;

        wait_for_shutdown().await;
        shutdown_tx.send(true).ok();
        stop.store(true, Ordering::Release);
        for feed in feeds {
            feed.await.ok();
        }
        if let Some(thread) = replay_thread {
            thread.join().ok();
        }
        let summary = venue_thread
            .join()
            .map_err(|_| anyhow::anyhow!("the venue thread panicked"))?;
        println!("mock venue: {} reports", summary.reports);
        Ok(())
    })
}
