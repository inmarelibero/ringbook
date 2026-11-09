# Simulation assumptions

No order in ringbook reaches a real or test exchange. What matters is how realistically the simulation behaves, and being clear about where it stops.

## What is modelled

| Aspect | How it works |
| --- | --- |
| Fill price | Passive orders fill at their limit. Aggressive orders walk the visible book level by level |
| Fees | Configurable maker and taker fees per venue; a negative maker fee is a rebate |
| Latency | Configurable delays from decision to exchange and from exchange to acknowledgement |
| Passive fills | Estimated queue position: public trades at our price consume the volume ahead of us, and we fill only once it is gone |
| Cancels ahead of us | A size decrease that no public trade explains is treated as a cancel. A probabilistic model decides how much of it was ahead of us |
| Trades through our price | We fill up to the traded quantity, with nothing ahead |
| Rejects | Post-only crosses, limit breaches and venue capacity |
| Cancel races | Cancels take the simulated delay, so a fill can arrive first |

The queue model weights a cancel by `ahead^n / (ahead^n + behind^n)`, with `n` set by `queue_model_power`.

## What is not modelled

- **Market impact.** Our orders never change the book.
- **Funding, margin and liquidation.** Reported PnL excludes funding.
- **Liquidity beyond the visible book.** Any remainder of an aggressive order is cancelled and counted.
- **Binance Retail Price Improvement orders**, which its diff stream excludes.

## Where the estimate is coarse

Book data is batched rather than per-order, so queue position is always an estimate.

- Hyperliquid's `l2Book` carries 20 levels per side and arrives at most about every 0.5 seconds. Several changes can land in one update, so its estimate is coarser than Binance's.
- Trades and book updates arrive as separate messages. If a book update showing a decrease arrives before the trade that caused it, part of that trade can briefly count as a cancel.
- A resting order whose price leaves the visible book keeps its last known queue position until the price returns.

## Reading the results

PnL is reported to show the accounting works, and it shouldn't be read as a measure of edge. The strategy is a simple market maker that exists to generate load.
