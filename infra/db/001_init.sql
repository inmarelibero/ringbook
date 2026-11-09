CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE runs (
    run_id         BIGINT PRIMARY KEY,
    mode           TEXT NOT NULL,
    config         TEXT NOT NULL,
    binary_version TEXT NOT NULL,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at       TIMESTAMPTZ,
    status         TEXT,
    events         BIGINT,
    orders_sent    BIGINT,
    fills          BIGINT,
    equity_usd     DOUBLE PRECISION,
    drawdown_usd   DOUBLE PRECISION,
    decision_p50   BIGINT,
    decision_p99   BIGINT,
    decision_p999  BIGINT,
    decision_max   BIGINT
);

CREATE TABLE run_stats (
    time            TIMESTAMPTZ NOT NULL,
    run_id          BIGINT NOT NULL REFERENCES runs (run_id),
    decision_p50    BIGINT NOT NULL,
    decision_p99    BIGINT NOT NULL,
    decision_p999   BIGINT NOT NULL,
    decision_max    BIGINT NOT NULL,
    wire_p50        BIGINT NOT NULL,
    wire_p99        BIGINT NOT NULL,
    wire_p999       BIGINT NOT NULL,
    wire_max        BIGINT NOT NULL,
    events          BIGINT NOT NULL,
    batches         BIGINT NOT NULL,
    orders_sent     BIGINT NOT NULL,
    fills           BIGINT NOT NULL,
    risk_rejections BIGINT NOT NULL,
    equity_usd      DOUBLE PRECISION NOT NULL,
    drawdown_usd    DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('run_stats', 'time');
CREATE INDEX run_stats_run ON run_stats (run_id, time DESC);

CREATE MATERIALIZED VIEW run_stats_minute
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket,
       run_id,
       max(decision_p99)  AS decision_p99,
       max(decision_p999) AS decision_p999,
       last(equity_usd, time) AS equity_usd,
       max(drawdown_usd)  AS drawdown_usd
FROM run_stats
GROUP BY bucket, run_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('run_stats_minute',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

SELECT add_retention_policy('run_stats', INTERVAL '30 days');
