//! The generic live feed handler shared by every venue.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use ringbook_clock::Clock;
use ringbook_core::Event;
use ringbook_ring::Producer;
use tokio::sync::{mpsc, watch};
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::adapter::{
    Action, ConnEvent, Endpoint, FrameParser, Input, InstrumentMeta, InstrumentSpec, ParseOutput,
    RestMethod, RestRequest, VenueAdapter,
};
use crate::recording::{RecordKind, RecorderHandle, Recording};

const INBOUND_QUEUE: usize = 1 << 14;
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const REST_RETRY_DELAY: Duration = Duration::from_secs(2);
const KEEPALIVE_PERIOD: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Where a feed delivers its events.
pub trait Publish: Send {
    /// Returns how many events can be published right now without waiting.
    fn free_slots(&mut self) -> usize;

    /// Publishes one event, which must fit.
    fn publish(&mut self, event: Event);
}

impl Publish for Producer<Event> {
    fn free_slots(&mut self) -> usize {
        Producer::free_slots(self)
    }

    fn publish(&mut self, event: Event) {
        let published = self.push(event);
        debug_assert!(published.is_ok(), "the caller checks for free slots");
    }
}

/// What a live feed needs besides its adapter.
#[derive(Clone, Debug)]
pub struct FeedConfig {
    /// The instruments to subscribe to.
    pub instruments: Vec<InstrumentSpec>,
    /// The raw metadata response, stored at the start of a recording.
    pub metadata: Arc<Vec<u8>>,
    /// The process-wide clock.
    pub clock: Clock,
    /// Receives raw inputs, if this run is recording.
    pub recorder: Option<RecorderHandle>,
}

enum Inbound {
    Frame {
        stream: u8,
        received: ringbook_core::EventTime,
        data: Vec<u8>,
    },
    Rest {
        tag: u32,
        received: ringbook_core::EventTime,
        body: Vec<u8>,
    },
    Conn {
        stream: u8,
        tag: u32,
        received: ringbook_core::EventTime,
        event: ConnEvent,
    },
}

/// Makes a REST request and returns its body.
async fn send_request(client: &reqwest::Client, request: &RestRequest) -> Result<Vec<u8>, String> {
    let builder = match request.method {
        RestMethod::Get => client.get(&request.url),
        RestMethod::Post => client
            .post(&request.url)
            .header("content-type", "application/json")
            .body(request.body.clone().unwrap_or_default()),
    };
    let response = builder.send().await.map_err(|error| error.to_string())?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    let bytes = response.bytes().await.map_err(|error| error.to_string())?;
    Ok(bytes.to_vec())
}

/// Fetches a venue's metadata for the given symbols.
///
/// # Errors
///
/// Returns a description of the failure if the request fails or the venue
/// doesn't list a symbol.
pub async fn fetch_metadata<A: VenueAdapter>(
    adapter: &A,
    symbols: &[String],
) -> Result<(Vec<InstrumentMeta>, Vec<u8>), String> {
    let client = reqwest::Client::new();
    let body = send_request(&client, &adapter.metadata_request())
        .await
        .map_err(|error| format!("{}: metadata request failed: {error}", adapter.name()))?;
    let metas = adapter
        .parse_metadata(&body, symbols)
        .map_err(|error| format!("{}: {error}", adapter.name()))?;
    Ok((metas, body))
}

/// Keeps one WebSocket connected, forwarding frames until `shutdown` fires.
async fn run_connection(
    stream: u8,
    endpoint: Endpoint,
    clock: Clock,
    inbound: mpsc::Sender<Inbound>,
    mut shutdown: watch::Receiver<bool>,
) {
    let notify = |event: ConnEvent| Inbound::Conn {
        stream,
        tag: 0,
        received: clock.event_time(),
        event,
    };

    while !*shutdown.borrow() {
        let connection = timeout(CONNECT_TIMEOUT, connect_async(endpoint.url.as_str())).await;
        let mut socket = match connection {
            Ok(Ok((socket, _))) => socket,
            Ok(Err(error)) => {
                tracing::warn!(url = endpoint.url, %error, "connect failed");
                let _ = inbound.send(notify(ConnEvent::Disconnected)).await;
                sleep(RECONNECT_DELAY).await;
                continue;
            }
            Err(_) => {
                tracing::warn!(url = endpoint.url, "connect timed out");
                let _ = inbound.send(notify(ConnEvent::Disconnected)).await;
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        tracing::info!(url = endpoint.url, "connected");
        let _ = inbound.send(notify(ConnEvent::Connected)).await;
        let mut subscribed = true;
        for subscription in &endpoint.subscriptions {
            if socket
                .send(Message::text(subscription.clone()))
                .await
                .is_err()
            {
                subscribed = false;
                break;
            }
        }

        let mut keepalive = interval(KEEPALIVE_PERIOD);
        keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
        keepalive.tick().await;

        let mut ending = if subscribed {
            None
        } else {
            Some(ConnEvent::Disconnected)
        };
        while ending.is_none() {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = socket.close(None).await;
                        return;
                    }
                }
                _ = keepalive.tick(), if endpoint.keepalive.is_some() => {
                    if let Some(message) = &endpoint.keepalive
                        && socket.send(Message::text(message.clone())).await.is_err()
                    {
                        ending = Some(ConnEvent::Disconnected);
                    }
                }
                next = timeout(endpoint.stall_timeout, socket.next()) => match next {
                    Err(_) => ending = Some(ConnEvent::StallTimeout),
                    Ok(None | Some(Err(_))) => ending = Some(ConnEvent::Disconnected),
                    Ok(Some(Ok(Message::Text(text)))) => {
                        let frame = Inbound::Frame {
                            stream,
                            received: clock.event_time(),
                            data: text.as_bytes().to_vec(),
                        };
                        if inbound.send(frame).await.is_err() {
                            return;
                        }
                    }
                    Ok(Some(Ok(Message::Close(_)))) => ending = Some(ConnEvent::Disconnected),
                    Ok(Some(Ok(_))) => {}
                },
            }
        }

        if let Some(event) = ending {
            tracing::warn!(url = endpoint.url, ?event, "connection lost");
            let _ = inbound.send(notify(event)).await;
            if event == ConnEvent::StallTimeout {
                let _ = inbound.send(notify(ConnEvent::Disconnected)).await;
            }
        }
        drop(socket);
        sleep(RECONNECT_DELAY).await;
    }
}

struct Pipeline<'a, P, O> {
    parser: P,
    output: ParseOutput,
    publisher: O,
    clock: &'a Clock,
    recorder: Option<&'a RecorderHandle>,
    sequence: u64,
    venue_name: &'static str,
}

impl<P: FrameParser, O: Publish> Pipeline<'_, P, O> {
    fn record(
        &mut self,
        received: ringbook_core::EventTime,
        kind: RecordKind,
        stream: u8,
        tag: u32,
        payload: &[u8],
    ) {
        self.sequence += 1;
        if let Some(recorder) = self.recorder {
            recorder.record(Recording {
                received,
                sequence: self.sequence,
                kind,
                stream,
                tag,
                payload: payload.to_vec(),
            });
        }
    }

    async fn process(&mut self, inbound: &Inbound) {
        let (received, input) = match inbound {
            Inbound::Frame {
                stream,
                received,
                data,
            } => {
                self.record(*received, RecordKind::Frame, *stream, 0, data);
                (
                    *received,
                    Input::Frame {
                        stream: *stream,
                        data,
                    },
                )
            }
            Inbound::Rest {
                tag,
                received,
                body,
            } => {
                self.record(*received, RecordKind::Rest, 0, *tag, body);
                (*received, Input::Rest { tag: *tag, body })
            }
            Inbound::Conn {
                stream,
                tag,
                received,
                event,
            } => {
                metrics::counter!(
                    "ringbook_feed_connection_events_total",
                    "venue" => self.venue_name,
                    "event" => format!("{event:?}"),
                )
                .increment(1);
                self.record(*received, RecordKind::Conn, *stream, *tag, &[*event as u8]);
                (
                    *received,
                    Input::Conn {
                        stream: *stream,
                        tag: *tag,
                        event: *event,
                    },
                )
            }
        };

        // Measurement starts as the input reaches the parser.
        self.output.begin(received, self.clock.measure());
        self.parser.on_input(input, &mut self.output);
        self.publish_or_resync(received).await;
    }

    /// Publishes a whole batch, so a partial batch can never be seen.
    async fn publish_or_resync(&mut self, received: ringbook_core::EventTime) {
        if self.publisher.free_slots() < self.output.events.len() {
            let actions = std::mem::take(&mut self.output.actions);
            self.record(
                received,
                RecordKind::Conn,
                0,
                0,
                &[ConnEvent::RingOverflow as u8],
            );
            self.output.begin(received, self.clock.measure());
            self.parser.on_input(
                Input::Conn {
                    stream: 0,
                    tag: 0,
                    event: ConnEvent::RingOverflow,
                },
                &mut self.output,
            );
            self.output.actions.extend(actions);
            tracing::warn!("feed ring full: discarded a batch and started a resync");
            while self.publisher.free_slots() < self.output.events.len() {
                sleep(Duration::from_millis(1)).await;
            }
        }
        for event in self.output.events.drain(..) {
            self.publisher.publish(event);
        }
    }
}

fn spawn_rest(
    client: &reqwest::Client,
    tag: u32,
    request: RestRequest,
    clock: &Clock,
    inbound: &mpsc::Sender<Inbound>,
    delay: Duration,
) {
    let client = client.clone();
    let clock = clock.clone();
    let inbound = inbound.clone();
    tokio::spawn(async move {
        sleep(delay).await;
        let message = match send_request(&client, &request).await {
            Ok(body) => Inbound::Rest {
                tag,
                received: clock.event_time(),
                body,
            },
            Err(error) => {
                tracing::warn!(url = request.url, error, "REST request failed");
                Inbound::Conn {
                    stream: 0,
                    tag,
                    received: clock.event_time(),
                    event: ConnEvent::RestFailed,
                }
            }
        };
        let _ = inbound.send(message).await;
    });
}

/// Runs a venue's connections and parser until `shutdown` fires.
pub async fn run_feed<A: VenueAdapter>(
    adapter: Arc<A>,
    venue: ringbook_core::VenueId,
    config: FeedConfig,
    publisher: impl Publish,
    shutdown: watch::Receiver<bool>,
) {
    let symbols: Vec<String> = config
        .instruments
        .iter()
        .map(|instrument| instrument.meta.symbol.clone())
        .collect();
    let (inbound_sender, mut inbound) = mpsc::channel(INBOUND_QUEUE);

    let mut connections = Vec::new();
    for (stream, endpoint) in adapter.endpoints(&symbols).into_iter().enumerate() {
        connections.push(tokio::spawn(run_connection(
            stream as u8,
            endpoint,
            config.clock.clone(),
            inbound_sender.clone(),
            shutdown.clone(),
        )));
    }

    let mut pipeline = Pipeline {
        parser: adapter.parser(venue, config.instruments.clone()),
        output: ParseOutput::default(),
        publisher,
        clock: &config.clock,
        recorder: config.recorder.as_ref(),
        sequence: 0,
        venue_name: adapter.name(),
    };
    pipeline.record(
        config.clock.event_time(),
        RecordKind::Metadata,
        0,
        0,
        &config.metadata,
    );

    let client = reqwest::Client::new();
    let mut shutdown = shutdown;
    loop {
        let message = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            message = inbound.recv() => match message {
                Some(message) => message,
                None => break,
            },
        };
        let retrying = matches!(
            message,
            Inbound::Conn {
                event: ConnEvent::RestFailed,
                ..
            }
        );
        pipeline.process(&message).await;

        let delay = if retrying {
            REST_RETRY_DELAY
        } else {
            Duration::ZERO
        };
        for action in pipeline.output.actions.drain(..) {
            let Action::Rest { tag, request } = action;
            spawn_rest(&client, tag, request, &config.clock, &inbound_sender, delay);
        }
    }

    for connection in connections {
        let _ = connection.await;
    }
}
