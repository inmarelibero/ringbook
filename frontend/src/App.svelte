<script lang="ts">
  import Architecture from "./Architecture.svelte";
  import EquityChart from "./EquityChart.svelte";
  import { REPO, source } from "./links";
  import type {
    Benchmarks,
    BookUpdate,
    DailyPnl,
    FeedState,
    PositionUpdate,
    Run,
    StatusUpdate,
    Update,
  } from "./types";

  const MAX_POINTS = 600;
  const RECONNECT_MS = 2000;
  const RUNS_REFRESH_MS = 30000;

  let books = $state<Record<string, BookUpdate>>({});
  let positions = $state<Record<string, PositionUpdate>>({});
  let status = $state<StatusUpdate | null>(null);
  let eventRate = $state<number | null>(null);
  let connected = $state(false);
  let now = $state(Date.now());
  let totalEquity = $state(0);
  let equityTimes = $state<number[]>([]);
  let equityValues = $state<number[]>([]);
  let runs = $state<Run[]>([]);
  let runsAvailable = $state(true);
  let rules = $state("");
  let daily = $state<DailyPnl | null>(null);
  let benchmarks = $state<Benchmarks | null>(null);

  let lastStatus: { events: number; at: number } | null = null;

  const orderBooks = $derived(Object.entries(books).sort(([a], [b]) => a.localeCompare(b)));
  const baseline = $derived(daily?.baseline_usd ?? 0);
  const shownPnl = $derived(totalEquity - baseline);
  const chartValues = $derived(equityValues.map((value) => value - baseline));
  const liveFeeds = $derived(orderBooks.filter(([, book]) => book.feed === "live").length);

  interface Proof {
    value: string;
    label: string;
    detail: string;
    href: string;
  }

  const proofs = $derived.by((): Proof[] => {
    const fixed: Proof[] = [
      {
        value: "0",
        label: "heap allocations on the hot path",
        detail:
          "A counting allocator fails the test if quoting, fills or cancels touch the heap after warm-up.",
        href: source("crates/engine/tests/no_alloc.rs"),
      },
      {
        value: "Byte-identical",
        label: "journals across replays",
        detail:
          "Replaying a recording at full speed and at its original pace writes the same journal, byte for byte.",
        href: source("crates/app/tests/e2e.rs"),
      },
    ];
    if (benchmarks) {
      const latency = benchmarks.tick_to_decision;
      const env = benchmarks.environment;
      return [
        ...fixed,
        {
          value: duration(latency.p50_ns),
          label: "median tick-to-decision",
          detail: `p99 ${duration(latency.p99_ns)}, p99.9 ${duration(latency.p999_ns)}. Paced replay on ${env.cpus} shared vCPUs, ${benchmarks.measured_at.slice(0, 10)}.`,
          href: source("scripts/baseline.sh"),
        },
        {
          value: `${compact(benchmarks.throughput_events_per_sec)}/s`,
          label: "market events through one thread",
          detail: "Maximum-speed replay of a recorded session on a single pinned hot thread.",
          href: source("scripts/baseline.sh"),
        },
      ];
    }
    return [
      ...fixed,
      {
        value: "loom",
        label: "checks every interleaving of the ring",
        detail:
          "Weakening any of the lock-free ring's four memory orderings makes the model check fail. Miri checks it for undefined behaviour.",
        href: source("crates/ring/tests/loom.rs"),
      },
      {
        value: "Crash-safe",
        label: "recovery from the journal",
        detail:
          "A killed engine rebuilds its state, receives the venue messages it missed and resumes only if it reconciles.",
        href: source("crates/app/tests/e2e.rs"),
      },
    ];
  });

  const highlights = [
    {
      title: "A lock-free ring, model-checked",
      text: "A hand-written single-producer, single-consumer ring, checked exhaustively with loom and for undefined behaviour with Miri.",
      href: source("crates/ring/"),
    },
    {
      title: "A hot path that never allocates",
      text: "Book, strategy, risk and order management run on one pinned thread with no allocation, locks or I/O after start-up.",
      href: source("crates/engine/tests/no_alloc.rs"),
    },
    {
      title: "Deterministic replay",
      text: "Parsers are I/O-free state machines and venues merge in one total order, so a recording always produces the same decisions.",
      href: source("docs/ARCHITECTURE.md"),
    },
    {
      title: "Crash recovery that reconciles",
      text: "After a crash the engine rebuilds from its journal, fetches missed reports and compares open orders and positions with the venue before quoting.",
      href: source("crates/app/tests/e2e.rs"),
    },
    {
      title: "Fuzzed parsers",
      text: "The Binance, Hyperliquid, FIX and order protocol parsers have fuzz targets, alongside property-based tests.",
      href: source("fuzz/fuzz_targets/"),
    },
    {
      title: "An honest simulator",
      text: "Fills model queue position, maker and taker fees, order and acknowledgement latency, rejects and cancel races.",
      href: source("docs/SIMULATION.md"),
    },
  ];

  function pairKey(update: BookUpdate | PositionUpdate): string {
    return `${update.instrument} · ${update.venue}`;
  }

  function recordEquity(value: number) {
    totalEquity = value;
    const seconds = Date.now() / 1000;
    const last = equityTimes[equityTimes.length - 1] ?? 0;
    if (seconds - last < 1) {
      return;
    }
    equityTimes = [...equityTimes, seconds].slice(-MAX_POINTS);
    equityValues = [...equityValues, value].slice(-MAX_POINTS);
  }

  function recordStatus(update: StatusUpdate) {
    const at = Date.now();
    if (lastStatus && at > lastStatus.at && update.events >= lastStatus.events) {
      eventRate = ((update.events - lastStatus.events) * 1000) / (at - lastStatus.at);
    }
    lastStatus = { events: update.events, at };
    status = update;
  }

  function apply(update: Update) {
    if (update.type === "status") {
      recordStatus(update);
    } else if (update.type === "book") {
      books[pairKey(update)] = update;
    } else {
      positions[pairKey(update)] = update;
      recordEquity(update.total_equity_usd);
    }
  }

  $effect(() => {
    let socket: WebSocket | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;

    function connect() {
      const scheme = location.protocol === "https:" ? "wss" : "ws";
      socket = new WebSocket(`${scheme}://${location.host}/ws`);
      socket.onopen = () => (connected = true);
      socket.onmessage = (message) => apply(JSON.parse(message.data) as Update);
      socket.onclose = () => {
        connected = false;
        if (!stopped) {
          timer = setTimeout(connect, RECONNECT_MS);
        }
      };
    }

    connect();
    return () => {
      stopped = true;
      clearTimeout(timer);
      socket?.close();
    };
  });

  $effect(() => {
    const timer = setInterval(() => (now = Date.now()), 1000);
    return () => clearInterval(timer);
  });

  async function loadRuns() {
    try {
      const response = await fetch("/api/runs");
      runsAvailable = response.ok;
      if (response.ok) {
        runs = (await response.json()) as Run[];
      }
    } catch {
      runsAvailable = false;
    }
  }

  async function loadDaily() {
    try {
      const response = await fetch("/api/daily");
      if (response.ok) {
        daily = (await response.json()) as DailyPnl;
      }
    } catch {
      // Keeps the last answer; the next refresh tries again.
    }
  }

  async function loadRules() {
    try {
      const response = await fetch("/api/rules");
      if (response.ok) {
        rules = ((await response.json()) as { config: string }).config;
      }
    } catch {
      rules = "";
    }
  }

  async function loadBenchmarks() {
    try {
      const response = await fetch("/benchmarks.json");
      // Without the file, the server answers with the page itself.
      if (response.ok && response.headers.get("content-type")?.includes("json")) {
        benchmarks = (await response.json()) as Benchmarks;
      }
    } catch {
      benchmarks = null;
    }
  }

  $effect(() => {
    loadBenchmarks();
    loadRules();
    loadRuns();
    loadDaily();
    const timer = setInterval(() => {
      loadRuns();
      loadDaily();
    }, RUNS_REFRESH_MS);
    return () => clearInterval(timer);
  });

  function usd(value: number | null): string {
    return value === null ? "—" : `${value < 0 ? "-" : ""}$${Math.abs(value).toFixed(2)}`;
  }

  function compact(value: number): string {
    return new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(
      value,
    );
  }

  function count(value: number): string {
    return new Intl.NumberFormat("en").format(value);
  }

  function duration(nanos: number): string {
    if (nanos < 1_000) return `${nanos} ns`;
    if (nanos < 1_000_000) return `${(nanos / 1_000).toFixed(1)} µs`;
    return `${(nanos / 1_000_000).toFixed(1)} ms`;
  }

  function uptime(startedMs: number): string {
    const total = Math.max(0, Math.floor((now - startedMs) / 1000));
    const days = Math.floor(total / 86_400);
    const hours = Math.floor((total % 86_400) / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    if (days > 0) return `${days}d ${hours}h ${minutes}m`;
    if (hours > 0) return `${hours}h ${minutes}m`;
    return `${minutes}m ${total % 60}s`;
  }

  function engineLabel(update: StatusUpdate): string {
    if (update.engine === "running") return "Quoting";
    if (update.engine === "reconciling") return "Reconciling with the venue";
    const reason = (update.halt_reason ?? "").replace(/([a-z])([A-Z])/g, "$1 $2").toLowerCase();
    return reason ? `Halted: ${reason}` : "Halted";
  }

  const feedLabels: Record<FeedState, string> = {
    connecting: "Connecting",
    live: "Live",
    stale: "Stalled",
    resyncing: "Resyncing",
  };

  function depthShare(quantity: string, levels: [string, string][]): number {
    const largest = Math.max(...levels.map((level) => Number(level[1])));
    return largest > 0 ? (Number(quantity) / largest) * 100 : 0;
  }
</script>

<div class="page">
  <header class="top">
    <a class="brand" href="/">ringbook</a>
    <nav aria-label="Project links">
      <a href={source("docs/ARCHITECTURE.md")}>Architecture</a>
      <a href={source("docs/SIMULATION.md")}>Simulation</a>
      <a class="button" href={REPO}>
        <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true">
          <path
            fill="currentColor"
            d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8Z"
          />
        </svg>
        Source on GitHub
      </a>
    </nav>
  </header>

  <main>
    <section class="hero" aria-labelledby="hero-heading">
      <h1 id="hero-heading">A low-latency trading engine in Rust</h1>
      <p class="lead">
        ringbook reads live market data from Binance and Hyperliquid, runs a market maker on a
        single pinned thread and trades against a simulated exchange. This page is connected to the
        running engine.
      </p>
      <div class="actions">
        <a class="button primary" href={REPO}>Read the code</a>
        <a class="button" href={source("README.md")}>How it's measured</a>
      </div>
      <p class="notice">
        All trading is simulated: no order reaches any exchange. Educational project, not financial
        advice.
      </p>
    </section>

    <section class="proof" aria-label="Key results">
      {#each proofs as proof (proof.label)}
        <a class="card" href={proof.href}>
          <span class="value">{proof.value}</span>
          <span class="label">{proof.label}</span>
          <span class="detail">{proof.detail}</span>
          <span class="more">See the test</span>
        </a>
      {/each}
    </section>

    <section class="panel" aria-labelledby="live-heading">
      <div class="section-head">
        <h2 id="live-heading">Running now</h2>
        <span class="pill" class:ok={connected && status?.engine === "running"} class:warn={status?.engine === "halted"}>
          {#if !connected}
            Reconnecting to the engine
          {:else if status}
            {engineLabel(status)}
          {:else}
            Connected
          {/if}
        </span>
      </div>
      <dl class="stats">
        <div>
          <dt>Uptime</dt>
          <dd>{status ? uptime(status.started_at_ms) : "—"}</dd>
        </div>
        <div>
          <dt>Market events processed</dt>
          <dd>{status ? count(status.events) : "—"}</dd>
        </div>
        <div>
          <dt>Events per second</dt>
          <dd>{eventRate === null ? "—" : count(Math.round(eventRate))}</dd>
        </div>
        <div>
          <dt>Orders sent</dt>
          <dd>{status ? count(status.orders_sent) : "—"}</dd>
        </div>
        <div>
          <dt>Fills</dt>
          <dd>{status ? count(status.fills) : "—"}</dd>
        </div>
        <div>
          <dt>Feeds live</dt>
          <dd>{orderBooks.length ? `${liveFeeds} of ${orderBooks.length}` : "—"}</dd>
        </div>
      </dl>
      <div class="scroll">
        <table>
          <caption class="visually-hidden">Feed health per market</caption>
          <thead>
            <tr>
              <th scope="col">Market</th>
              <th scope="col">Feed</th>
              <th scope="col">Gap resyncs</th>
              <th scope="col">Stalls</th>
            </tr>
          </thead>
          <tbody>
            {#each orderBooks as [key, book] (key)}
              <tr>
                <td>{key}</td>
                <td><span class="feed {book.feed}">{feedLabels[book.feed]}</span></td>
                <td>{book.resyncs}</td>
                <td>{book.stalls}</td>
              </tr>
            {:else}
              <tr><td colspan="4" class="empty">Waiting for the engine.</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
      <p class="description">
        Resyncs count sequence gaps that forced a book rebuild from a snapshot; stalls count feeds
        that stopped sending data. Each market stops quoting until its feed is live again.
      </p>
    </section>

    <section class="panel" aria-labelledby="book-heading">
      <div class="section-head">
        <h2 id="book-heading">Order books and our quotes</h2>
        <span class="note">About 10 updates a second per market</span>
      </div>
      {#if orderBooks.length > 0}
        <div class="books">
          {#each orderBooks as [key, book] (key)}
            <article>
              <h3>{key}</h3>
              <p class="quotes">
                {#if book.own_bid || book.own_ask}
                  Our quotes: <span class="bid-text">bid {book.own_bid ?? "—"}</span> ·
                  <span class="ask-text">ask {book.own_ask ?? "—"}</span>
                {:else}
                  Not quoting
                {/if}
              </p>
              <div class="ladders">
                {#each [{ title: "Bids", levels: book.bids, side: "bid", own: book.own_bid }, { title: "Asks", levels: book.asks, side: "ask", own: book.own_ask }] as ladder (ladder.side)}
                  <table class={ladder.side}>
                    <caption>{ladder.title}</caption>
                    <thead>
                      <tr><th scope="col">Price</th><th scope="col">Size</th></tr>
                    </thead>
                    <tbody>
                      {#each ladder.levels as level, index (index)}
                        <tr
                          class:own={level[0] === ladder.own}
                          style={`--depth: ${depthShare(level[1], ladder.levels)}%`}
                        >
                          <td>{level[0]}{#if level[0] === ladder.own}<span class="tag">ours</span>{/if}</td>
                          <td>{level[1]}</td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                {/each}
              </div>
            </article>
          {/each}
        </div>
      {:else}
        <p class="empty">Waiting for market data.</p>
      {/if}
    </section>

    <section class="panel" aria-labelledby="arch-heading">
      <div class="section-head">
        <h2 id="arch-heading">How it works</h2>
        <a class="note" href={source("docs/ARCHITECTURE.md")}>Architecture document</a>
      </div>
      <Architecture />
    </section>

    <section class="panel" aria-labelledby="highlights-heading">
      <h2 id="highlights-heading">Engineering highlights</h2>
      <ul class="highlights">
        {#each highlights as item (item.title)}
          <li>
            <a href={item.href}>{item.title}</a>
            <p>{item.text}</p>
          </li>
        {/each}
      </ul>
    </section>

    <h2 class="group">Simulated trading</h2>

    <section class="panel" aria-labelledby="pnl-heading">
      <div class="section-head">
        <h2 id="pnl-heading">{daily ? "Simulated PnL today" : "Simulated PnL"}</h2>
        <strong class:negative={shownPnl < 0}>{usd(shownPnl)}</strong>
      </div>
      <p class="description">
        {#if daily}
          Profit or loss since 00:00 UTC across all markets, including fees and the change in value
          of open positions, which carry over from day to day. The run's total so far is
          {usd(totalEquity)}. PnL isn't the point of the project; the chart covers the time this
          page has been open.
        {:else}
          Total profit or loss for the current run across all markets, including fees and the
          current value of open positions. PnL isn't the point of the project.
        {/if}
      </p>
      {#if equityTimes.length > 1}
        <EquityChart times={equityTimes} values={chartValues} />
      {:else}
        <p class="empty">Collecting data.</p>
      {/if}
    </section>

    <section class="panel" aria-labelledby="positions-heading">
      <h2 id="positions-heading">Positions</h2>
      <div class="scroll">
        <table>
          <thead>
            <tr>
              <th scope="col">Market</th>
              <th scope="col">Quantity</th>
              <th scope="col">Run PnL</th>
            </tr>
          </thead>
          <tbody>
            {#each Object.entries(positions).sort() as [key, position] (key)}
              <tr>
                <td>{key}</td>
                <td>{position.qty}</td>
                <td>{usd(position.equity_usd)}</td>
              </tr>
            {:else}
              <tr><td colspan="3" class="empty">No position data yet.</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    </section>

    {#if daily}
      <section class="panel" aria-labelledby="daily-heading">
        <h2 id="daily-heading">Daily PnL</h2>
        <div class="scroll">
          <table>
            <thead>
              <tr>
                <th scope="col">Day (UTC)</th>
                <th scope="col">PnL</th>
                <th scope="col">Run total</th>
              </tr>
            </thead>
            <tbody>
              {#each daily.days as row (row.day)}
                {@const isToday = row.day === daily.today}
                <tr>
                  <td>{row.day}{isToday ? " (today)" : ""}</td>
                  <td>{usd(isToday ? shownPnl : row.pnl_usd)}</td>
                  <td>{usd(isToday ? totalEquity : row.close_usd)}</td>
                </tr>
              {:else}
                <tr><td colspan="3" class="empty">No daily data yet.</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}

    <section class="panel" aria-labelledby="runs-heading">
      <h2 id="runs-heading">Run history</h2>
      {#if runsAvailable}
        <div class="scroll">
          <table>
            <thead>
              <tr>
                <th scope="col">Started</th>
                <th scope="col">Mode</th>
                <th scope="col">Status</th>
                <th scope="col">Orders</th>
                <th scope="col">Fills</th>
                <th scope="col">PnL</th>
              </tr>
            </thead>
            <tbody>
              {#each runs as run, index (run.run_id)}
                <tr>
                  <td>{run.started_at.slice(0, 19)}</td>
                  <td>{run.mode}</td>
                  <td>{run.status ?? (index === 0 ? "running" : "interrupted")}</td>
                  <td>{run.orders_sent ?? "—"}</td>
                  <td>{run.fills ?? "—"}</td>
                  <td>{usd(run.equity_usd)}</td>
                </tr>
              {:else}
                <tr><td colspan="6" class="empty">No completed runs yet.</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else}
        <p class="empty">Run history isn't available on this deployment.</p>
      {/if}
    </section>

    <section class="panel">
      <details>
        <summary>Current trading rules (read-only)</summary>
        <pre>{rules}</pre>
      </details>
    </section>

    <section class="panel" aria-labelledby="limits-heading">
      <h2 id="limits-heading">Design boundaries</h2>
      <ul class="limits">
        <li>
          A low-latency architecture on commodity infrastructure and public market data. It isn't a
          colocated high-frequency system, and its latencies aren't comparable with one.
        </li>
        <li>
          It runs on an affordable shared-vCPU server, so every measurement is labelled as coming
          from a virtual machine.
        </li>
        <li>
          Public crypto venues offer JSON over WebSockets. Parsers take raw frames and do no I/O, so
          a faster transport could replace them without touching the engine.
        </li>
      </ul>
    </section>
  </main>

  <footer>
    <a href={REPO}>GitHub</a>
    <a href={source("README.md")}>README</a>
    <a href={source("docs/ARCHITECTURE.md")}>Architecture</a>
    <a href={source("docs/SIMULATION.md")}>Simulation</a>
    <span>MIT or Apache-2.0</span>
  </footer>
</div>
