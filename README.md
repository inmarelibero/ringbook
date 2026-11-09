# ringbook

A low-latency crypto trading engine in Rust. It reads live public market data from Hyperliquid and Binance USD-M perpetuals, runs a market maker on a single pinned thread, and executes against a simulated exchange.

**No order ever reaches a real or test exchange.** This is an educational project: simulated trading only, not financial advice, and provided with no warranty.

## What it demonstrates

- **A hot path that never allocates, locks or performs I/O.** A counting allocator proves zero allocations across quoting, fills and cancels.
- **A hand-written lock-free ring,** checked exhaustively with `loom` and for undefined behaviour with Miri. Weakening any of its four memory orderings makes `loom` fail.
- **Deterministic replay.** Two replays of a recording produce byte-identical journals, at any speed.
- **An honest simulator** with estimated queue positions, fees, latency and cancel races. See [the assumptions](docs/SIMULATION.md).
- **Real failure handling:** sequence-gap recovery, stall detection, a lossless journal and crash recovery that reconciles with the venue.

[The architecture document](docs/ARCHITECTURE.md) explains how the parts fit together.

## Quick start

With Docker running, from a fresh clone:

```sh
docker compose -f infra/docker-compose.yml up --build
```

Then open <http://localhost:8088> for the demo and <http://localhost:3000> for Grafana. You need network access to both venues, and some regions can't reach Binance's API.

To build and run without Docker, using Rust 1.93 or newer:

```sh
cargo build --release
target/release/ringbook record --config configs/demo.toml --out recordings/first --seconds 120
target/release/ringbook replay --config configs/demo.toml --recording recordings/first --journal first.rbj
```

The repository ships no market data, because exchange terms may restrict redistribution. Replay needs a recording you've made yourself.

## Measuring it

```sh
scripts/baseline.sh recordings/first
```

This prints the allocation proof, microbenchmarks, throughput, latency and a determinism check, along with the environment it ran on.

Set `BENCH_JSON=frontend/public/benchmarks.json` to also save the headline numbers for the demo site, which shows them with their environment once the frontend is rebuilt.

**Tick-to-decision** runs from a frame reaching the parser to risk accepting or rejecting the order. **Tick-to-wire** runs to the order's bytes reaching a loopback socket. Latency comes from replay at the recorded pace, because max-speed numbers include ring queueing and only measure throughput.

Internal latency says nothing about reaction speed on live data, which is limited by each venue's update cadence and the network.

## Design boundaries

ringbook is a low-latency trading architecture built on commodity infrastructure and public market data. It isn't a competitive high-frequency system, and its latencies aren't comparable with production colocated systems.

| Area | This release | Way forward |
| --- | --- | --- |
| Performance evidence | Baseline benchmarks | A full study of layouts, profiling and assembly |
| Hardware | An affordable shared-vCore VPS, with results labelled as VM measurements | Bare metal would tighten tail latency and expose CPU counters |
| Network and data | Kernel sockets, JSON WebSockets and batched L2 data, which is what public crypto venues offer | Parsers take raw frames and do no I/O, so a faster transport can replace them without touching the engine |

## Simulation assumptions

Modelled: fill prices, maker and taker fees, order and acknowledgement latency, estimated queue position, rejects and cancel races.

Not modelled: market impact, funding, margin, liquidation, and liquidity beyond the visible book. Reported PnL excludes funding, and it isn't the point of the project. [The details are here](docs/SIMULATION.md).

## How this was built

AI tools did much of the implementation. Claude Code wrote most of the code and tests in this repository, working from a specification developed in conversation with the author.

The author set the goals and made the design decisions, including what to simulate, what to leave out and where to draw the plugin boundaries. The AI proposed designs and flagged problems; the author chose among them.

The tests are what establish that the code works, and several caught real bugs during development: double-counted queue volume, a timing-dependent replay, and a damaged journal length mistaken for a clean cut-off.

`SPECS.md` records the plan the project was built to.

## Layout

| Path | Contents |
| --- | --- |
| `crates/core` | Event and order types, fixed-point prices |
| `crates/ring` | The SPSC ring buffer |
| `crates/book` | The flat price-indexed order book |
| `crates/feeds`, `crates/venues` | Venue adapter interface, handlers, recorder, adapters |
| `crates/replay` | Deterministic replay |
| `crates/engine` | The hot thread: strategy, risk, positions, order manager |
| `crates/paper-exchange` | Simulated matching |
| `crates/gateway`, `crates/mock-venue` | Binary order protocol and the local fake exchange |
| `crates/journal`, `crates/ctl` | The journal and operator control |
| `crates/telemetry`, `crates/api` | Metrics, storage and the read-only gateway |
| `crates/fix` | A small standalone FIX 4.4 codec |
| `analysis` | Reads a run from TimescaleDB and renders charts |
| `frontend` | The one-page Svelte site |
| `infra` | Docker Compose, Grafana, database schema, host tuning |

Hyperliquid and Binance are named only to describe connectivity. This project isn't affiliated with either.

## Licence

Licensed under either the [MIT licence](LICENSE-MIT) or the [Apache License 2.0](LICENSE-APACHE), at your option.
