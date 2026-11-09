<script lang="ts">
  import EquityChart from "./EquityChart.svelte";
  import type { BookUpdate, DailyPnl, PositionUpdate, Run, Update } from "./types";

  const MAX_POINTS = 600;
  const RECONNECT_MS = 2000;
  const RUNS_REFRESH_MS = 30000;

  let books = $state<Record<string, BookUpdate>>({});
  let positions = $state<Record<string, PositionUpdate>>({});
  let connected = $state(false);
  let totalEquity = $state(0);
  let equityTimes = $state<number[]>([]);
  let equityValues = $state<number[]>([]);
  let runs = $state<Run[]>([]);
  let runsAvailable = $state(true);
  let rules = $state("");
  let daily = $state<DailyPnl | null>(null);

  const orderBooks = $derived(Object.entries(books).sort(([a], [b]) => a.localeCompare(b)));

  const baseline = $derived(daily?.baseline_usd ?? 0);
  const shownPnl = $derived(totalEquity - baseline);
  const chartValues = $derived(equityValues.map((value) => value - baseline));

  function pairKey(update: Update): string {
    return `${update.instrument} · ${update.venue}`;
  }

  function recordEquity(value: number) {
    totalEquity = value;
    const now = Date.now() / 1000;
    const last = equityTimes[equityTimes.length - 1] ?? 0;
    if (now - last < 1) {
      return;
    }
    equityTimes = [...equityTimes, now].slice(-MAX_POINTS);
    equityValues = [...equityValues, value].slice(-MAX_POINTS);
  }

  function apply(update: Update) {
    const key = pairKey(update);
    if (update.type === "book") {
      books[key] = update;
    } else {
      positions[key] = update;
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

  $effect(() => {
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

  function depthShare(quantity: string, levels: [string, string][]): number {
    const largest = Math.max(...levels.map((level) => Number(level[1])));
    return largest > 0 ? (Number(quantity) / largest) * 100 : 0;
  }
</script>

<main>
  <header>
    <div>
      <h1>ringbook</h1>
      <p>A low-latency trading engine running on live public market data.</p>
    </div>
    <span class="status" class:live={connected}>{connected ? "Connected" : "Reconnecting"}</span>
  </header>

  <p class="notice">
    All trading here is simulated. No orders reach any exchange, and PnL excludes funding. This is
    an educational project, not financial advice.
  </p>

  <section class="book" aria-labelledby="book-heading">
    <div class="section-head">
      <h2 id="book-heading">Order books</h2>
      <span class="note">Updated about 10 times a second per market</span>
    </div>
    {#if orderBooks.length > 0}
      <div class="books">
        {#each orderBooks as [key, book] (key)}
          <article>
            <h3>{key}</h3>
            <div class="ladders">
              {#each [{ title: "Bids", levels: book.bids, side: "bid" }, { title: "Asks", levels: book.asks, side: "ask" }] as ladder (ladder.side)}
                <table class={ladder.side}>
                  <caption>{ladder.title}</caption>
                  <thead>
                    <tr><th scope="col">Price</th><th scope="col">Size</th></tr>
                  </thead>
                  <tbody>
                    {#each ladder.levels as level, index (index)}
                      <tr style={`--depth: ${depthShare(level[1], ladder.levels)}%`}>
                        <td>{level[0]}</td>
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

  <section aria-labelledby="pnl-heading">
    <div class="section-head">
      <h2 id="pnl-heading">{daily ? "Simulated PnL today" : "Simulated PnL"}</h2>
      <strong class:negative={shownPnl < 0}>{usd(shownPnl)}</strong>
    </div>
    <p class="description">
      {#if daily}
        Profit or loss since 00:00 UTC, across all four markets, including fees and the change in
        value of open positions, which carry over from day to day. Shown in red when negative. The
        run's total so far is {usd(totalEquity)}. The chart covers the time this page has been open.
      {:else}
        Total profit or loss for the current run, across all four markets. It starts at $0 and
        includes fees and the current value of open positions. Shown in red when negative.
      {/if}
    </p>
    {#if equityTimes.length > 1}
      <EquityChart times={equityTimes} values={chartValues} />
    {:else}
      <p class="empty">Collecting data.</p>
    {/if}
  </section>

  <section aria-labelledby="positions-heading">
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
    <section aria-labelledby="daily-heading">
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

  <section aria-labelledby="runs-heading">
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

  <section>
    <details>
      <summary>Current trading rules (read-only)</summary>
      <pre>{rules}</pre>
    </details>
  </section>
</main>
