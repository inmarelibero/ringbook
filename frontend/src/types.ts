export type Level = [price: string, quantity: string];

export interface BookUpdate {
  type: "book";
  venue: string;
  instrument: string;
  bids: Level[];
  asks: Level[];
}

export interface PositionUpdate {
  type: "position";
  venue: string;
  instrument: string;
  qty: string;
  equity_usd: number;
  total_equity_usd: number;
}

export type Update = BookUpdate | PositionUpdate;

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
