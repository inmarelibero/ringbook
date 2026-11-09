//! Reads a run from TimescaleDB, summarises it and renders report charts.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use plotters::prelude::*;
use polars::prelude::*;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

#[derive(Parser)]
#[command(version, about = "Summarises a ringbook run and renders its charts")]
struct Cli {
    /// The run to analyse; defaults to the most recent.
    #[arg(long)]
    run_id: Option<i64>,
    /// The directory for the charts.
    #[arg(long, default_value = "report")]
    out: PathBuf,
    /// The database; defaults to `DATABASE_URL`.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
}

const CHART_SIZE: (u32, u32) = (1000, 420);

async fn load(url: &str, requested: Option<i64>) -> Result<(i64, DataFrame)> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .context("connecting to the database")?;
    let run_id = match requested {
        Some(run_id) => run_id,
        None => sqlx::query("SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1")
            .fetch_optional(&pool)
            .await?
            .context("the database has no runs")?
            .get(0),
    };

    let rows = sqlx::query(
        "SELECT extract(epoch FROM time)::float8 AS seconds, decision_p50, decision_p99,
                decision_p999, equity_usd, drawdown_usd, fills
         FROM run_stats WHERE run_id = $1 ORDER BY time",
    )
    .bind(run_id)
    .fetch_all(&pool)
    .await?;
    if rows.is_empty() {
        bail!("run {run_id} has no statistics");
    }

    let start: f64 = rows[0].get("seconds");
    let column_f64 = |name: &str| -> Vec<f64> { rows.iter().map(|row| row.get(name)).collect() };
    let column_i64 = |name: &str| -> Vec<i64> { rows.iter().map(|row| row.get(name)).collect() };
    let frame = df!(
        "elapsed_seconds" => rows
            .iter()
            .map(|row| row.get::<f64, _>("seconds") - start)
            .collect::<Vec<_>>(),
        "decision_p50" => column_i64("decision_p50"),
        "decision_p99" => column_i64("decision_p99"),
        "decision_p999" => column_i64("decision_p999"),
        "equity_usd" => column_f64("equity_usd"),
        "drawdown_usd" => column_f64("drawdown_usd"),
        "fills" => column_i64("fills"),
    )?;
    Ok((run_id, frame))
}

fn summarise(frame: &DataFrame) -> Result<DataFrame> {
    Ok(frame
        .clone()
        .lazy()
        // A second with no decisions records zero, which isn't a latency.
        .filter(col("decision_p50").gt(lit(0)))
        .select([
            len().alias("seconds_with_decisions"),
            col("decision_p50").median().alias("median_p50_ns"),
            col("decision_p99").median().alias("median_p99_ns"),
            col("decision_p999").max().alias("worst_p999_ns"),
        ])
        .collect()?)
}

fn numbers(frame: &DataFrame, name: &str) -> Result<Vec<f64>> {
    let series = frame.column(name)?.cast(&DataType::Float64)?;
    Ok(series.f64()?.into_no_null_iter().collect())
}

fn bounds(values: &[&[f64]]) -> (f64, f64) {
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    for value in values.iter().flat_map(|series| series.iter()) {
        low = low.min(*value);
        high = high.max(*value);
    }
    if low >= high {
        (low - 1.0, low + 1.0)
    } else {
        (low, high)
    }
}

fn chart(
    path: &Path,
    title: &str,
    y_label: &str,
    x: &[f64],
    series: &[(&str, &[f64], RGBColor)],
) -> Result<()> {
    let root = SVGBackend::new(path, CHART_SIZE).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let values: Vec<&[f64]> = series.iter().map(|(_, values, _)| *values).collect();
    let (low, high) = bounds(&values);
    let end = x.last().copied().unwrap_or(1.0).max(1.0);

    let mut figure = ChartBuilder::on(&root)
        .caption(title, ("sans-serif", 20))
        .margin(16)
        .x_label_area_size(36)
        .y_label_area_size(72)
        .build_cartesian_2d(0.0..end, low..high)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    figure
        .configure_mesh()
        .x_desc("elapsed seconds")
        .y_desc(y_label)
        .draw()
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    for (name, values, color) in series {
        let color = *color;
        figure
            .draw_series(LineSeries::new(
                x.iter().copied().zip(values.iter().copied()),
                color.stroke_width(2),
            ))
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .label(*name)
            .legend(move |(x, y)| PathElement::new([(x, y), (x + 20, y)], color.stroke_width(2)));
    }
    figure
        .configure_series_labels()
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .draw()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    root.present().map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(())
}

fn render(frame: &DataFrame, directory: &Path) -> Result<()> {
    std::fs::create_dir_all(directory)
        .with_context(|| format!("creating {}", directory.display()))?;
    let elapsed = numbers(frame, "elapsed_seconds")?;
    let p50 = numbers(frame, "decision_p50")?;
    let p99 = numbers(frame, "decision_p99")?;
    let equity = numbers(frame, "equity_usd")?;
    let drawdown = numbers(frame, "drawdown_usd")?;

    chart(
        &directory.join("latency.svg"),
        "Tick-to-decision latency",
        "nanoseconds",
        &elapsed,
        &[("p50", &p50, BLUE), ("p99", &p99, RED)],
    )?;
    chart(
        &directory.join("equity.svg"),
        "Equity (excludes funding)",
        "USD",
        &elapsed,
        &[("equity", &equity, BLUE), ("drawdown", &drawdown, RED)],
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let (run_id, frame) = load(&cli.database_url, cli.run_id).await?;
    println!("run {run_id}: {} seconds of statistics", frame.height());
    println!("{}", summarise(&frame)?);
    render(&frame, &cli.out)?;
    println!("charts written to {}", cli.out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame() -> DataFrame {
        df!(
            "elapsed_seconds" => [0.0, 1.0, 2.0, 3.0],
            "decision_p50" => [100_i64, 0, 300, 200],
            "decision_p99" => [900_i64, 0, 1_100, 1_000],
            "decision_p999" => [1_000_i64, 0, 5_000, 2_000],
            "equity_usd" => [0.0, 1.5, -0.5, 2.0],
            "drawdown_usd" => [0.0, 0.0, 2.0, 0.0],
            "fills" => [0_i64, 1, 2, 3],
        )
        .unwrap()
    }

    #[test]
    fn the_summary_ignores_seconds_without_decisions() {
        let summary = summarise(&frame()).unwrap();
        let cell = |name: &str| {
            summary
                .column(name)
                .unwrap()
                .get(0)
                .unwrap()
                .try_extract::<f64>()
                .unwrap()
        };
        assert_eq!(cell("seconds_with_decisions"), 3.0);
        assert_eq!(cell("median_p50_ns"), 200.0);
        assert_eq!(cell("worst_p999_ns"), 5_000.0);
    }

    #[test]
    fn charts_are_rendered() {
        let directory =
            std::env::temp_dir().join(format!("ringbook-analysis-{}", std::process::id()));
        render(&frame(), &directory).unwrap();
        for name in ["latency.svg", "equity.svg"] {
            let chart = std::fs::read_to_string(directory.join(name)).unwrap();
            assert!(chart.contains("<svg"));
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn flat_series_still_have_a_visible_range() {
        let flat = [5.0, 5.0];
        let (low, high) = bounds(&[&flat]);
        assert!(low < 5.0 && high > 5.0);
    }
}
