# Running ringbook locally

All trading is simulated: no orders reach any exchange. You need network access to Hyperliquid and Binance, and some regions can't reach Binance's API.

## With Docker

Runs the full stack on live market data.

```sh
docker compose -f infra/docker-compose.yml up --build
```

| Service | Address | Notes |
| --- | --- | --- |
| Demo site | <http://localhost:8088> | Set `WEB_PORT` to change the port |
| Grafana | <http://localhost:3000> | Log in as `admin` / `admin`, or set `GRAFANA_PASSWORD` |

Press Ctrl-C to stop. To delete the containers and their stored data as well:

```sh
docker compose -f infra/docker-compose.yml down -v
```

## Without Docker

Needs Rust 1.93 or newer. Run these from the repository root.

```sh
cargo build --release
target/release/ringbook validate configs/demo.toml
```

### Live

Start the mock venue in one terminal and the engine in another:

```sh
# Terminal 1
target/release/ringbook mock-venue --config configs/demo.toml

# Terminal 2
mkdir -p journals
target/release/ringbook live --config configs/demo.toml --journal journals/live.rbj --seconds 300
```

Leave out `--seconds` to run until you press Ctrl-C. The engine prints a summary when it stops.

### Record and replay

```sh
mkdir -p journals
target/release/ringbook record --config configs/demo.toml --out recordings/first --seconds 120
target/release/ringbook verify-recording --config configs/demo.toml --recording recordings/first
target/release/ringbook replay --config configs/demo.toml --recording recordings/first --journal journals/first.rbj
```

Replay runs at maximum speed and measures throughput. Add `--pacing recorded` to play at the original pace and measure latency. Replay overwrites the journal if the file already exists.

## Controlling a running engine

These work with the native live run above:

```sh
target/release/ringbookctl status
target/release/ringbookctl halt
target/release/ringbookctl halt-symbol NAME
target/release/ringbookctl resume-symbol NAME
target/release/ringbookctl set spread_bps 5
```

`set` also accepts `inventory_skew_bps` and `requote_threshold_bps`. `NAME` is an instrument name from the config file. `halt` stops all trading for that run.

The trading rules are in [`configs/demo.toml`](configs/demo.toml).
