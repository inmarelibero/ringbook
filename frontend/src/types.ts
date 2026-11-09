export type Level = [price: string, quantity: string];

export type FeedState = "connecting" | "live" | "stale" | "resyncing";

export interface BookUpdate {
  type: "book";
  venue: string;
  instrument: string;
  bids: Level[];
  asks: Level[];
  own_bid: string | null;
  own_ask: string | null;
  feed: FeedState;
  resyncs: number;
  stalls: number;
}

export interface PositionUpdate {
  type: "position";
  venue: string;
  instrument: string;
  qty: string;
  equity_usd: number;
  total_equity_usd: number;
}

export interface StatusUpdate {
  type: "status";
  engine: "running" | "reconciling" | "halted";
  halt_reason: string | null;
  started_at_ms: number;
  events: number;
  batches: number;
  orders_sent: number;
  cancels_sent: number;
  fills: number;
  risk_rejections: number;
  venue_rejections: number;
}

export type Update = BookUpdate | PositionUpdate | StatusUpdate;

export interface DayRow {
  day: string;
  pnl_usd: number;
  close_usd: number;
}

export interface DailyPnl {
  today: string;
  baseline_usd: number;
  days: DayRow[];
}

export interface Run {
  run_id: string;
  mode: string;
  status: string | null;
  started_at: string;
  ended_at: string | null;
  events: number | null;
  orders_sent: number | null;
  fills: number | null;
  equity_usd: number | null;
  drawdown_usd: number | null;
}

export interface Percentiles {
  p50_ns: number;
  p99_ns: number;
  p999_ns: number;
  max_ns: number;
}

/** Written by `scripts/baseline.sh` when `BENCH_JSON` is set. */
export interface Benchmarks {
  measured_at: string;
  environment: { kernel: string; cpus: number; pinning: string; recording_hash: string };
  hot_path_allocations: number;
  throughput_events_per_sec: number;
  tick_to_decision: Percentiles;
  replays_identical: boolean;
}
