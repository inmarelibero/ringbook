<script lang="ts">
  import EquityChart from "./EquityChart.svelte";
  import type { BookUpdate, PositionUpdate, Run, Update } from "./types";

  const MAX_POINTS = 600;
  const RECONNECT_MS = 2000;
  const RUNS_REFRESH_MS = 30000;

  let books = $state<Record<string, BookUpdate>>({});
  let positions = $state<Record<string, PositionUpdate>>({});
  let selected = $state<string | null>(null);
  let connected = $state(false);
  let totalEquity = $state(0);
  let equityTimes = $state<number[]>([]);
  let equityValues = $state<number[]>([]);
  let runs = $state<Run[]>([]);
  let runsAvailable = $state(true);
  let rules = $state("");

  const pairKeys = $derived(Object.keys(books).sort());
  const book = $derived(selected ? books[selected] : undefined);

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
      selected ??= key;
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
    const timer = setInterval(loadRuns, RUNS_REFRESH_MS);
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
      <h2 id="book-heading">Order book</h2>
      <label>
        <span class="visually-hidden">Market</span>
        <select bind:value={selected}>
          {#each pairKeys as key (key)}
            <option value={key}>{key}</option>
          {/each}
        </select>
      </label>
    </div>
    {#if book}
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
    {:else}
      <p class="empty">Waiting for market data.</p>
    {/if}
  </section>

  <section aria-labelledby="pnl-heading">
    <div class="section-head">
      <h2 id="pnl-heading">Simulated PnL</h2>
      <strong class:negative={totalEquity < 0}>{usd(totalEquity)}</strong>
    </div>
    {#if equityTimes.length > 1}
      <EquityChart times={equityTimes} values={equityValues} />
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
            <th scope="col">PnL</th>
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
