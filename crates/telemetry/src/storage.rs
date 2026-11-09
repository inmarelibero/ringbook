//! Writes telemetry snapshots to TimescaleDB.

use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use tokio::sync::mpsc;

use crate::{Percentiles, Snapshot, TelemetryReport};

const SUMMARY_RETRIES: u32 = 30;
const RETRY_DELAY: Duration = Duration::from_secs(2);
const QUEUE_DEPTH: usize = 256;

/// What identifies a run in storage.
#[derive(Clone, Debug)]
pub struct RunInfo {
    /// A unique run identifier.
    pub run_id: i64,
    /// `live`, or `replay`.
    pub mode: String,
    /// The full config text.
    pub config: String,
    /// The binary's version.
    pub binary_version: String,
}

/// A message for the storage writer.
#[derive(Clone, Copy, Debug)]
pub enum StorageMessage {
    /// A once-a-second snapshot, which may be dropped under load.
    Snapshot(Snapshot),
    /// The whole-run summary, which must not be lost.
    Finished {
        /// The summary.
        report: TelemetryReport,
        /// The run's final status.
        status: &'static str,
    },
}

/// The non-blocking sender used by the telemetry thread.
#[derive(Clone, Debug)]
pub struct StorageHandle {
    sender: mpsc::Sender<StorageMessage>,
}

impl StorageHandle {
    /// Queues a snapshot, dropping it if the writer has fallen behind.
    pub fn snapshot(&self, snapshot: Snapshot) {
        let _ = self.sender.try_send(StorageMessage::Snapshot(snapshot));
    }

    /// Queues the final summary, waiting for space because it can't be lost.
    pub async fn finished(&self, report: TelemetryReport, status: &'static str) {
        let _ = self
            .sender
            .send(StorageMessage::Finished { report, status })
            .await;
    }
}

fn latency_columns(value: &Percentiles) -> [i64; 4] {
    [
        value.p50 as i64,
        value.p99 as i64,
        value.p999 as i64,
        value.max as i64,
    ]
}

async fn insert_run(pool: &PgPool, info: &RunInfo) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO runs (run_id, mode, config, binary_version) VALUES ($1, $2, $3, $4)
         ON CONFLICT (run_id) DO NOTHING",
    )
    .bind(info.run_id)
    .bind(&info.mode)
    .bind(&info.config)
    .bind(&info.binary_version)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_snapshot(
    pool: &PgPool,
    run_id: i64,
    snapshot: &Snapshot,
) -> Result<(), sqlx::Error> {
    let decision = latency_columns(&snapshot.interval.tick_to_decision);
    let wire = latency_columns(&snapshot.interval.tick_to_wire);
    let counters = &snapshot.counters;
    sqlx::query(
        "INSERT INTO run_stats (
            time, run_id, decision_p50, decision_p99, decision_p999, decision_max,
            wire_p50, wire_p99, wire_p999, wire_max,
            events, batches, orders_sent, fills, risk_rejections, equity_usd, drawdown_usd
         ) VALUES (now(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
    )
    .bind(run_id)
    .bind(decision[0])
    .bind(decision[1])
    .bind(decision[2])
    .bind(decision[3])
    .bind(wire[0])
    .bind(wire[1])
    .bind(wire[2])
    .bind(wire[3])
    .bind(counters.events as i64)
    .bind(counters.batches as i64)
    .bind(counters.orders_sent as i64)
    .bind(counters.fills as i64)
    .bind(counters.risk_rejections as i64)
    .bind(counters.equity_micro_usd as f64 / 1e6)
    .bind(counters.drawdown_micro_usd as f64 / 1e6)
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_summary(
    pool: &PgPool,
    info: &RunInfo,
    report: &TelemetryReport,
    status: &str,
) -> Result<(), sqlx::Error> {
    insert_run(pool, info).await?;
    let total = &report.total.tick_to_decision;
    let row = sqlx::query(
        "UPDATE runs SET ended_at = now(), status = $2, events = $3, orders_sent = $4, fills = $5,
         equity_usd = $6, drawdown_usd = $7, decision_p50 = $8, decision_p99 = $9,
         decision_p999 = $10, decision_max = $11
         WHERE run_id = $1 RETURNING run_id",
    )
    .bind(info.run_id)
    .bind(status)
    .bind(report.counters.events as i64)
    .bind(report.counters.orders_sent as i64)
    .bind(report.counters.fills as i64)
    .bind(report.counters.equity_micro_usd as f64 / 1e6)
    .bind(report.counters.drawdown_micro_usd as f64 / 1e6)
    .bind(total.p50 as i64)
    .bind(total.p99 as i64)
    .bind(total.p999 as i64)
    .bind(total.max as i64)
    .fetch_one(pool)
    .await?;
    debug_assert_eq!(row.get::<i64, _>(0), info.run_id);
    Ok(())
}

/// Starts the storage writer.
///
/// Snapshot failures are logged and skipped; the final summary is retried.
///
/// # Errors
///
/// Returns an error if the database URL is invalid.
pub fn start_writer(
    database_url: &str,
    info: RunInfo,
) -> Result<(StorageHandle, tokio::task::JoinHandle<()>), sqlx::Error> {
    let options: PgConnectOptions = database_url.parse()?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect_lazy_with(options);
    let (sender, mut receiver) = mpsc::channel(QUEUE_DEPTH);

    let task = tokio::spawn(async move {
        if let Err(error) = insert_run(&pool, &info).await {
            eprintln!("storage: could not create the run: {error}");
        }
        while let Some(message) = receiver.recv().await {
            match message {
                StorageMessage::Snapshot(snapshot) => {
                    if let Err(error) = insert_snapshot(&pool, info.run_id, &snapshot).await {
                        eprintln!("storage: dropped a snapshot: {error}");
                    }
                }
                StorageMessage::Finished { report, status } => {
                    for attempt in 1..=SUMMARY_RETRIES {
                        match update_summary(&pool, &info, &report, status).await {
                            Ok(()) => break,
                            Err(error) => {
                                eprintln!("storage: summary attempt {attempt} failed: {error}");
                                tokio::time::sleep(RETRY_DELAY).await;
                            }
                        }
                    }
                }
            }
        }
    });
    Ok((StorageHandle { sender }, task))
}
