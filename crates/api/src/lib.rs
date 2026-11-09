//! The read-only gateway behind the public site.
//!
//! The hot thread pushes copies of its state onto a ring. A bridge thread turns
//! those into JSON and broadcasts them to WebSocket clients. Nothing here can
//! change the engine: there are no routes that write.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use ringbook_core::{Qty, Scale};
use ringbook_engine::{ApiBook, ApiPosition, ApiUpdate};
use ringbook_ring::Consumer;
use serde::Serialize;
use sqlx::{PgPool, Row};
use tokio::sync::broadcast;

const BROADCAST_DEPTH: usize = 256;
const SEND_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_SLEEP: Duration = Duration::from_millis(5);
const RATE_WINDOW: Duration = Duration::from_secs(1);

/// How a client's connections and requests are limited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// WebSocket connections across all clients.
    pub max_connections: usize,
    /// WebSocket connections per IP address.
    pub max_connections_per_ip: u32,
    /// HTTP requests per IP address per second.
    pub max_requests_per_second: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 200,
            max_connections_per_ip: 5,
            max_requests_per_second: 20,
        }
    }
}

/// How one venue and instrument pair is displayed.
#[derive(Clone, Debug)]
pub struct PairNames {
    /// The venue's name.
    pub venue: String,
    /// The instrument's name.
    pub instrument: String,
    /// Decimal places in prices.
    pub price_decimals: u8,
    /// Decimal places in quantities.
    pub qty_decimals: u8,
}

/// Maps runtime identifiers to display names.
#[derive(Clone, Debug, Default)]
pub struct Names {
    /// Pairs keyed by venue ID and instrument ID.
    pub pairs: HashMap<(u8, u16), PairNames>,
}

#[derive(Default)]
struct RateEntry {
    started: Option<Instant>,
    count: u32,
}

struct Shared {
    updates: broadcast::Sender<Arc<str>>,
    rules: String,
    database: Option<PgPool>,
    limits: Limits,
    connections: AtomicUsize,
    per_ip: Mutex<HashMap<IpAddr, u32>>,
    rates: Mutex<HashMap<IpAddr, RateEntry>>,
    trust_forwarded_for: bool,
}

/// The gateway's running state.
#[derive(Clone)]
pub struct ApiState {
    shared: Arc<Shared>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Outbound<'a> {
    Book {
        venue: &'a str,
        instrument: &'a str,
        bids: Vec<[String; 2]>,
        asks: Vec<[String; 2]>,
    },
    Position {
        venue: &'a str,
        instrument: &'a str,
        qty: String,
        equity_usd: f64,
        total_equity_usd: f64,
    },
}

fn levels(
    entries: &[(ringbook_core::Price, Qty)],
    price_scale: Scale,
    qty_scale: Scale,
) -> Vec<[String; 2]> {
    entries
        .iter()
        .take_while(|(_, qty)| !qty.is_zero())
        .map(|(price, qty)| {
            [
                price_scale.display(price.0).to_string(),
                qty_scale.display(qty.0).to_string(),
            ]
        })
        .collect()
}

fn book_json(book: &ApiBook, pair: &PairNames) -> Option<String> {
    let price_scale = Scale::new(pair.price_decimals)?;
    let qty_scale = Scale::new(pair.qty_decimals)?;
    serde_json::to_string(&Outbound::Book {
        venue: &pair.venue,
        instrument: &pair.instrument,
        bids: levels(&book.bids, price_scale, qty_scale),
        asks: levels(&book.asks, price_scale, qty_scale),
    })
    .ok()
}

fn position_json(position: &ApiPosition, pair: &PairNames) -> Option<String> {
    let qty_scale = Scale::new(pair.qty_decimals)?;
    serde_json::to_string(&Outbound::Position {
        venue: &pair.venue,
        instrument: &pair.instrument,
        qty: qty_scale.display(position.qty).to_string(),
        equity_usd: position.equity_micro_usd as f64 / 1e6,
        total_equity_usd: position.total_equity_micro_usd as f64 / 1e6,
    })
    .ok()
}

fn update_json(update: &ApiUpdate, names: &Names) -> Option<String> {
    match update {
        ApiUpdate::Book(book) => {
            book_json(book, names.pairs.get(&(book.venue.0, book.instrument.0))?)
        }
        ApiUpdate::Position(position) => position_json(
            position,
            names
                .pairs
                .get(&(position.venue.0, position.instrument.0))?,
        ),
    }
}

/// What the gateway needs to start.
pub struct ApiConfig {
    /// The rules to display, as the config's text.
    pub rules: String,
    /// The run history database, if there is one.
    pub database: Option<PgPool>,
    /// Display names.
    pub names: Names,
    /// Connection and request limits.
    pub limits: Limits,
    /// Whether to take the client address from `X-Forwarded-For`, as set by
    /// the reverse proxy in front of the gateway.
    pub trust_forwarded_for: bool,
}

/// Creates the gateway and the thread bridging the hot thread's ring to it.
///
/// # Errors
///
/// Returns an error if the thread can't be started.
pub fn start_bridge(
    mut updates: Consumer<ApiUpdate>,
    config: ApiConfig,
    stop: Arc<AtomicBool>,
) -> std::io::Result<(ApiState, JoinHandle<()>)> {
    let (sender, _) = broadcast::channel(BROADCAST_DEPTH);
    let state = ApiState {
        shared: Arc::new(Shared {
            updates: sender.clone(),
            rules: config.rules,
            database: config.database,
            limits: config.limits,
            connections: AtomicUsize::new(0),
            per_ip: Mutex::new(HashMap::new()),
            rates: Mutex::new(HashMap::new()),
            trust_forwarded_for: config.trust_forwarded_for,
        }),
    };
    let names = config.names;
    let thread = std::thread::Builder::new()
        .name("api-bridge".into())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let mut busy = false;
                while let Some(update) = updates.pop() {
                    busy = true;
                    if let Some(json) = update_json(&update, &names) {
                        let _ = sender.send(Arc::from(json));
                    }
                }
                if !busy {
                    std::thread::sleep(IDLE_SLEEP);
                }
            }
        })?;
    Ok((state, thread))
}

fn client_ip(shared: &Shared, peer: SocketAddr, headers: &HeaderMap) -> IpAddr {
    if shared.trust_forwarded_for
        && let Some(forwarded) = headers
            .get("x-forwarded-for")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(',').next_back())
            .and_then(|value| value.trim().parse().ok())
    {
        return forwarded;
    }
    peer.ip()
}

fn allow_request(shared: &Shared, ip: IpAddr) -> bool {
    let Ok(mut rates) = shared.rates.lock() else {
        return false;
    };
    let now = Instant::now();
    if rates.len() > 100_000 {
        rates.retain(|_, entry| {
            entry
                .started
                .is_some_and(|started| now.duration_since(started) < RATE_WINDOW)
        });
    }
    let entry = rates.entry(ip).or_default();
    if entry
        .started
        .is_none_or(|started| now.duration_since(started) >= RATE_WINDOW)
    {
        *entry = RateEntry {
            started: Some(now),
            count: 0,
        };
    }
    entry.count += 1;
    entry.count <= shared.limits.max_requests_per_second
}

async fn limit_rate(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let ip = client_ip(&state.shared, peer, request.headers());
    if allow_request(&state.shared, ip) {
        next.run(request).await
    } else {
        StatusCode::TOO_MANY_REQUESTS.into_response()
    }
}

/// Holds one WebSocket connection's place in the limits.
struct ConnectionSlot {
    shared: Arc<Shared>,
    ip: IpAddr,
}

impl ConnectionSlot {
    fn acquire(shared: &Arc<Shared>, ip: IpAddr) -> Option<Self> {
        let mut per_ip = shared.per_ip.lock().ok()?;
        let count = per_ip.entry(ip).or_insert(0);
        if *count >= shared.limits.max_connections_per_ip {
            return None;
        }
        let total = shared.connections.fetch_add(1, Ordering::AcqRel);
        if total >= shared.limits.max_connections {
            shared.connections.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        *count += 1;
        Some(Self {
            shared: Arc::clone(shared),
            ip,
        })
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.shared.connections.fetch_sub(1, Ordering::AcqRel);
        if let Ok(mut per_ip) = self.shared.per_ip.lock()
            && let Some(count) = per_ip.get_mut(&self.ip)
        {
            *count -= 1;
            if *count == 0 {
                per_ip.remove(&self.ip);
            }
        }
    }
}

async fn websocket(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let ip = client_ip(&state.shared, peer, &headers);
    let Some(slot) = ConnectionSlot::acquire(&state.shared, ip) else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let updates = state.shared.updates.subscribe();
    upgrade.on_upgrade(move |socket| stream_updates(socket, updates, slot))
}

async fn stream_updates(
    mut socket: WebSocket,
    mut updates: broadcast::Receiver<Arc<str>>,
    _slot: ConnectionSlot,
) {
    loop {
        tokio::select! {
            update = updates.recv() => {
                // A client that falls behind is disconnected rather than buffered.
                let Ok(update) = update else { return };
                let sent = tokio::time::timeout(
                    SEND_TIMEOUT,
                    socket.send(Message::text(update.to_string())),
                ).await;
                if !matches!(sent, Ok(Ok(()))) {
                    return;
                }
            }
            incoming = socket.recv() => match incoming {
                None | Some(Err(_) | Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {}
            },
        }
    }
}

async fn rules(State(state): State<ApiState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "config": state.shared.rules }))
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct RunRow {
    run_id: String,
    mode: String,
    status: Option<String>,
    started_at: String,
    ended_at: Option<String>,
    events: Option<i64>,
    orders_sent: Option<i64>,
    fills: Option<i64>,
    equity_usd: Option<f64>,
    drawdown_usd: Option<f64>,
    decision_p50: Option<i64>,
    decision_p99: Option<i64>,
}

async fn runs(State(state): State<ApiState>) -> Result<Json<Vec<RunRow>>, StatusCode> {
    let pool = state
        .shared
        .database
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let rows = sqlx::query(
        "SELECT run_id, mode, status, started_at::text AS started, ended_at::text AS ended,
                events, orders_sent, fills, equity_usd, drawdown_usd, decision_p50, decision_p99
         FROM runs ORDER BY started_at DESC LIMIT 50",
    )
    .fetch_all(pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        rows.iter()
            .map(|row| RunRow {
                run_id: row.get::<i64, _>("run_id").to_string(),
                mode: row.get("mode"),
                status: row.get("status"),
                started_at: row.get("started"),
                ended_at: row.get("ended"),
                events: row.get("events"),
                orders_sent: row.get("orders_sent"),
                fills: row.get("fills"),
                equity_usd: row.get("equity_usd"),
                drawdown_usd: row.get("drawdown_usd"),
                decision_p50: row.get("decision_p50"),
                decision_p99: row.get("decision_p99"),
            })
            .collect(),
    ))
}

#[derive(Serialize)]
struct StatRow {
    time: String,
    equity_usd: f64,
    drawdown_usd: f64,
    fills: i64,
}

async fn run_stats(
    State(state): State<ApiState>,
    Path(run_id): Path<i64>,
) -> Result<Json<Vec<StatRow>>, StatusCode> {
    let pool = state
        .shared
        .database
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let rows = sqlx::query(
        "SELECT time::text AS at, equity_usd, drawdown_usd, fills
         FROM run_stats WHERE run_id = $1 ORDER BY time DESC LIMIT 3600",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        rows.iter()
            .rev()
            .map(|row| StatRow {
                time: row.get("at"),
                equity_usd: row.get("equity_usd"),
                drawdown_usd: row.get("drawdown_usd"),
                fills: row.get("fills"),
            })
            .collect(),
    ))
}

/// Builds the gateway's routes. Every route is read-only.
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/ws", get(websocket))
        .route("/api/rules", get(rules))
        .route("/api/runs", get(runs))
        .route("/api/runs/{run_id}/stats", get(run_stats))
        .route_layer(middleware::from_fn_with_state(state.clone(), limit_rate))
        .route("/healthz", get(health))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbook_core::{InstrumentId, Price, VenueId};

    fn shared(limits: Limits) -> Arc<Shared> {
        Arc::new(Shared {
            updates: broadcast::channel(4).0,
            rules: String::new(),
            database: None,
            limits,
            connections: AtomicUsize::new(0),
            per_ip: Mutex::new(HashMap::new()),
            rates: Mutex::new(HashMap::new()),
            trust_forwarded_for: true,
        })
    }

    fn names() -> Names {
        let mut names = Names::default();
        names.pairs.insert(
            (1, 2),
            PairNames {
                venue: "binance".to_owned(),
                instrument: "BTC-PERP".to_owned(),
                price_decimals: 1,
                qty_decimals: 3,
            },
        );
        names
    }

    const IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
    const OTHER: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2));

    #[test]
    fn books_serialise_with_decimal_strings_and_stop_at_empty_levels() {
        let mut book = ApiBook {
            venue: VenueId(1),
            instrument: InstrumentId(2),
            bids: [(Price(0), Qty::ZERO); ringbook_engine::engine::API_DEPTH],
            asks: [(Price(0), Qty::ZERO); ringbook_engine::engine::API_DEPTH],
        };
        book.bids[0] = (Price(730_005), Qty(1_500));
        let json = update_json(&ApiUpdate::Book(book), &names()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["type"], "book");
        assert_eq!(value["bids"][0][0], "73000.5");
        assert_eq!(value["bids"][0][1], "1.5");
        assert_eq!(value["bids"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn positions_serialise_in_usd() {
        let json = update_json(
            &ApiUpdate::Position(ApiPosition {
                venue: VenueId(1),
                instrument: InstrumentId(2),
                qty: -250,
                equity_micro_usd: 1_500_000,
                total_equity_micro_usd: 2_000_000,
            }),
            &names(),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["qty"], "-0.25");
        assert_eq!(value["total_equity_usd"], 2.0);
    }

    #[test]
    fn unknown_pairs_are_skipped() {
        let book = ApiBook {
            venue: VenueId(9),
            instrument: InstrumentId(9),
            bids: [(Price(0), Qty::ZERO); ringbook_engine::engine::API_DEPTH],
            asks: [(Price(0), Qty::ZERO); ringbook_engine::engine::API_DEPTH],
        };
        assert!(update_json(&ApiUpdate::Book(book), &names()).is_none());
    }

    #[test]
    fn connections_are_capped_per_ip_and_overall() {
        let shared = shared(Limits {
            max_connections: 3,
            max_connections_per_ip: 2,
            max_requests_per_second: 1,
        });
        let first = ConnectionSlot::acquire(&shared, IP).unwrap();
        let _second = ConnectionSlot::acquire(&shared, IP).unwrap();
        assert!(ConnectionSlot::acquire(&shared, IP).is_none());

        let _third = ConnectionSlot::acquire(&shared, OTHER).unwrap();
        assert!(ConnectionSlot::acquire(&shared, OTHER).is_none());

        drop(first);
        assert!(ConnectionSlot::acquire(&shared, OTHER).is_some());
    }

    #[test]
    fn requests_are_rate_limited_per_ip() {
        let shared = shared(Limits {
            max_connections: 1,
            max_connections_per_ip: 1,
            max_requests_per_second: 2,
        });
        assert!(allow_request(&shared, IP));
        assert!(allow_request(&shared, IP));
        assert!(!allow_request(&shared, IP));
        assert!(allow_request(&shared, OTHER));
    }

    #[test]
    fn the_nearest_forwarded_address_is_used() {
        let shared = shared(Limits::default());
        let peer: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "1.2.3.4, 10.0.0.1".parse().unwrap());
        assert_eq!(client_ip(&shared, peer, &headers), IP);
        assert_eq!(client_ip(&shared, peer, &HeaderMap::new()), peer.ip());
    }

    #[test]
    fn the_router_has_no_write_routes() {
        let source = include_str!("lib.rs");
        for method in ["post(", "put(", "delete(", "patch("] {
            let routes = source.split("pub fn router").nth(1).unwrap();
            let routes = routes.split("#[cfg(test)]").next().unwrap();
            assert!(!routes.contains(method), "found a write route: {method}");
        }
    }
}
