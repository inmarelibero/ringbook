//! Publishes telemetry snapshots as Prometheus metrics.

use std::net::SocketAddr;

use metrics::gauge;
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};

use crate::{Percentiles, Snapshot};

/// Starts the Prometheus endpoint; call from inside the async runtime.
///
/// # Errors
///
/// Returns an error if the endpoint can't be started.
pub fn install_exporter(address: SocketAddr) -> Result<(), BuildError> {
    PrometheusBuilder::new()
        .with_http_listener(address)
        .install()
}

fn publish_latency(metric: &'static str, value: &Percentiles) {
    if value.count == 0 {
        return;
    }
    for (quantile, number) in [
        ("0.5", value.p50),
        ("0.99", value.p99),
        ("0.999", value.p999),
        ("1", value.max),
    ] {
        gauge!("ringbook_latency_ns", "metric" => metric, "quantile" => quantile)
            .set(number as f64);
    }
}

/// Publishes one snapshot.
pub fn publish(snapshot: &Snapshot) {
    publish_latency("tick_to_decision", &snapshot.interval.tick_to_decision);
    publish_latency("batch", &snapshot.interval.batch);
    publish_latency("tick_to_wire", &snapshot.interval.tick_to_wire);

    let counters = &snapshot.counters;
    for (name, value) in [
        ("events", counters.events),
        ("batches", counters.batches),
        ("orders_sent", counters.orders_sent),
        ("cancels_sent", counters.cancels_sent),
        ("fills", counters.fills),
        ("risk_rejections", counters.risk_rejections),
        ("venue_rejections", counters.venue_rejections),
        ("telemetry_drops", counters.telemetry_drops),
        ("api_drops", counters.api_drops),
        ("book_recentres", counters.book_recentres),
    ] {
        gauge!("ringbook_engine_total", "counter" => name).set(value as f64);
    }
    gauge!("ringbook_equity_usd").set(counters.equity_micro_usd as f64 / 1e6);
    gauge!("ringbook_drawdown_usd").set(counters.drawdown_micro_usd as f64 / 1e6);
}
