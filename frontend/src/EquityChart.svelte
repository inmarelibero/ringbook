<script lang="ts">
  import uPlot from "uplot";
  import "uplot/dist/uPlot.min.css";

  interface Props {
    times: number[];
    values: number[];
  }

  let { times, values }: Props = $props();
  let container: HTMLDivElement;
  let chart: uPlot | undefined;

  function size() {
    return { width: container.clientWidth, height: 180 };
  }

  $effect(() => {
    chart = new uPlot(
      {
        ...size(),
        scales: { x: { time: true } },
        legend: { show: false },
        cursor: { show: false },
        axes: [
          { stroke: "#8a93a3", grid: { stroke: "rgba(138,147,163,0.15)" } },
          { stroke: "#8a93a3", grid: { stroke: "rgba(138,147,163,0.15)" }, size: 64 },
        ],
        series: [{}, { stroke: "#3987d9", width: 2 }],
      },
      [[], []],
      container,
    );
    const observer = new ResizeObserver(() => chart?.setSize(size()));
    observer.observe(container);
    return () => {
      observer.disconnect();
      chart?.destroy();
    };
  });

  $effect(() => {
    chart?.setData([times, values]);
  });
</script>

<div bind:this={container}></div>
