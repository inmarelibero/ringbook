<script lang="ts">
  const inputs = [
    { name: "Binance feed", detail: "WebSocket, REST snapshots on sequence gaps" },
    { name: "Hyperliquid feed", detail: "WebSocket, stall detection" },
  ];
  const outputs = [
    { name: "Order gateway", detail: "Busy-polling, loopback TCP to the mock venue" },
    { name: "Journal", detail: "Lossless: the engine halts rather than drop a record" },
    { name: "Telemetry", detail: "Latency histograms and Prometheus metrics" },
    { name: "API gateway", detail: "Read-only; feeds this page and may drop updates" },
  ];
</script>

<div class="arch" role="img" aria-label="Data flow: two venue feeds push events through single-producer rings into one pinned hot thread, which pushes through more rings to the order gateway, journal, telemetry and API gateway.">
  <div class="column">
    {#each inputs as node (node.name)}
      <div class="node"><strong>{node.name}</strong><span>{node.detail}</span></div>
    {/each}
  </div>
  <div class="arrow" aria-hidden="true"><span>SPSC rings</span></div>
  <div class="node hot">
    <strong>Hot thread</strong>
    <span>One pinned core: order book, strategy, risk and order management</span>
    <em>No allocation, locks or I/O</em>
  </div>
  <div class="arrow" aria-hidden="true"><span>SPSC rings</span></div>
  <div class="column">
    {#each outputs as node (node.name)}
      <div class="node"><strong>{node.name}</strong><span>{node.detail}</span></div>
    {/each}
  </div>
</div>
<p class="caption">
  Every arrow is a bounded single-producer, single-consumer ring, so a slow consumer can't add
  latency to trading. The operator tool reaches the hot thread through its own control ring and a
  halt flag.
</p>

<style>
  .arch {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto minmax(0, 1.1fr) auto minmax(0, 1fr);
    align-items: center;
    gap: 12px;
  }

  .column {
    display: grid;
    gap: 8px;
  }

  .node {
    display: grid;
    gap: 2px;
    padding: 10px 12px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--background);
    font-size: 0.8125rem;
  }

  .node span {
    color: var(--muted);
  }

  .hot {
    padding: 16px;
    border: 2px solid var(--accent);
    background: var(--accent-tint);
    font-size: 0.875rem;
  }

  .hot strong {
    font-size: 1rem;
  }

  .hot em {
    font-style: normal;
    font-weight: 600;
    color: var(--accent);
  }

  .arrow {
    display: grid;
    justify-items: center;
    color: var(--muted);
    font-size: 0.6875rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  .arrow::after {
    content: "→";
    font-size: 1.5rem;
    line-height: 1;
  }

  .caption {
    margin: 12px 0 0;
    color: var(--muted);
    font-size: 0.8125rem;
  }

  @media (max-width: 760px) {
    .arch {
      grid-template-columns: minmax(0, 1fr);
    }

    .arrow::after {
      content: "↓";
    }
  }
</style>
