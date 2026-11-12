//! Follows the scaling events that the server publishes through its API.
//!
//! The events are the ledger of the deployment: each has a sequence number, which names it as
//! `events/{seq}`, and a later event has a greater one. Live events arrive on the state stream
//! (`GET /v1/state:watch`); older ones come from the history list (`GET /v1/events`). An
//! [`EventFollower`] joins the two into one ascending list and extends it from the stream, so a
//! test awaits an action by reading frames until a condition holds, never by sleeping.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use gang_state::sse::Decoder;
use gang_tls::{HttpProtocol, HttpRequest, HttpTransport, ServerTrust};
use gang_types::Url;
use gang_wire::page::{PAGE_SIZE_MAX, PageToken};
use gang_wire::routes::{LIST_EVENTS, Route, WATCH_STATE};
use gang_wire::{Event, EventChange, Frame, ListEventsResponse, State, name};
use http::Method;
use http::header::{ACCEPT, AUTHORIZATION, HeaderName, HeaderValue};
use tokio::sync::Notify;

use crate::errors::{E2eError, Result};
use crate::http::{Chunks, Http, Read};
use crate::identity::Credentials;
use crate::tls_client;

/// The media type of the state stream.
const EVENT_STREAM: &str = "text/event-stream";

/// The query parameter that sets the size of a page of a list.
const PAGE_SIZE: &str = "pageSize";

/// The query parameter that resumes a list after the previous page.
const PAGE_TOKEN: &str = "pageToken";

/// The scaling events of a deployment, held in ascending order and kept current from the state
/// stream.
///
/// Connection setup and response headers are bounded; the open stream is governed by the
/// deadline given to each wait.
pub struct EventFollower {
    /// The open stream and the events read from it.
    feed: Feed,
}

impl EventFollower {
    /// Follows the events of the API at `api`, reached with `credentials` over server-TLS. Every
    /// request carries the credentials' bearer token. `handshake_timeout` bounds connection
    /// setup and response headers.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the TLS client cannot be built, the token is not a header value,
    /// the stream cannot be opened or does not start with a snapshot, or the history cannot be
    /// listed; [`E2eError::Parse`] when `api` cannot be joined with a route or a frame or event
    /// is malformed; [`E2eError::Io`] when the runtime cannot start; [`E2eError::Timeout`] when
    /// connection setup or response headers exceed `handshake_timeout`.
    pub fn open(api: &Url, credentials: &Credentials, handshake_timeout: Duration) -> Result<Self> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", credentials.token))
            .map_err(E2eError::system_from(
                "the bearer token is not a header value",
            ))?;
        authorization.set_sensitive(true);
        Self::build(
            api,
            credentials.authority.as_deref(),
            vec![(AUTHORIZATION, authorization)],
            handshake_timeout,
        )
    }

    /// Follows the events at `api` without presenting credentials.
    ///
    /// # Errors
    ///
    /// Returns an error when the client, stream, snapshot, or history cannot be opened.
    pub fn open_with(api: &Url, handshake_timeout: Duration) -> Result<Self> {
        Self::build(api, None, Vec::new(), handshake_timeout)
    }

    /// Builds the client and opens the feed with the given request headers.
    fn build(
        api: &Url,
        authority: Option<&std::path::Path>,
        headers: Vec<(HeaderName, HeaderValue)>,
        handshake_timeout: Duration,
    ) -> Result<Self> {
        let certificate = authority.map(tls_client::certificate_pem).transpose()?;
        let transport = if api.scheme() == "https" {
            let trust = certificate
                .as_ref()
                .map_or(ServerTrust::System, ServerTrust::Ca);
            HttpTransport::Tls { trust }
        } else {
            HttpTransport::Plain
        };
        let client = Http::try_new(transport, HttpProtocol::Http2, handshake_timeout)?;
        let feed = Feed::open(
            client,
            headers,
            route_url(api, &WATCH_STATE)?,
            route_url(api, &LIST_EVENTS)?,
            handshake_timeout,
        )?;
        Ok(Self { feed })
    }

    /// Every event the server recorded and this follower has read, oldest first.
    #[must_use]
    pub fn events(&self) -> &[Event] {
        &self.feed.held.events
    }

    /// The sequence number of the newest event held, if any.
    #[must_use]
    pub fn newest_seq(&self) -> Option<u64> {
        self.feed.held.seqs.last().copied()
    }

    /// Reads frames until `done` holds for the events held, and returns them, oldest first, or
    /// until `timeout` has passed since the call.
    ///
    /// `done` is evaluated on the events held first, then after each frame that is read. The wait
    /// is on the stream: a delta appends its events, a heartbeat changes nothing, and a snapshot
    /// after the first, or a delta that does not follow the last one applied, makes the follower
    /// reopen the stream and merge the new snapshot and the history behind it. The deadline is a
    /// watchdog, not a polling interval; an in-flight request can overrun it by up to the client's
    /// request timeout. A follower whose wait timed out must be dropped.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`] when `done` still rejects the events once `timeout` has passed;
    /// [`E2eError::System`] when the stream ends, fails or is closed by the server (the error
    /// names the reason), or the reopened stream or the history cannot be read;
    /// [`E2eError::Parse`] when a frame or an event is malformed.
    pub fn wait_for(
        &mut self,
        done: impl FnMut(&[Event]) -> bool,
        timeout: Duration,
    ) -> Result<&[Event]> {
        self.wait(done, timeout, None)?
            .ok_or_else(|| E2eError::system("the scaling history wait stopped unexpectedly"))
    }

    /// Like [`Self::wait_for`], but returns `Ok(None)` as soon as `stop` is notified. A follower
    /// that was stopped must be dropped.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::wait_for`].
    pub fn wait_for_or_stop(
        &mut self,
        done: impl FnMut(&[Event]) -> bool,
        timeout: Duration,
        stop: &Notify,
    ) -> Result<Option<&[Event]>> {
        self.wait(done, timeout, Some(stop))
    }

    /// Waits for the condition or an optional stop notification.
    fn wait(
        &mut self,
        mut done: impl FnMut(&[Event]) -> bool,
        timeout: Duration,
        stop: Option<&Notify>,
    ) -> Result<Option<&[Event]>> {
        let deadline = Instant::now() + timeout;
        // Requests use the client timeout, so a deadline can be exceeded by one request.
        while !done(&self.feed.held.events) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    "the scaling history to satisfy the awaited condition",
                    timeout,
                ));
            }
            let followed = self
                .feed
                .follow(deadline, stop)
                .map_err(|error| match error {
                    E2eError::Timeout { .. } => E2eError::timeout(
                        "the scaling history to satisfy the awaited condition",
                        timeout,
                    ),
                    other => other,
                })?;
            if !followed {
                return Ok(None);
            }
        }
        Ok(Some(&self.feed.held.events))
    }
}

/// The open state stream of a deployment and what has been read from it.
struct Feed {
    /// The HTTP client used for the stream and history.
    client: Http,
    /// Headers sent with each request.
    headers: Vec<(HeaderName, HeaderValue)>,
    /// The full URL of the state stream.
    watch_url: Url,
    /// The full URL of the event history.
    events_url: Url,
    /// The open response of the state stream.
    stream: Stream,
    /// The sequence number of the last snapshot or delta applied.
    seq: u64,
    /// The events read so far.
    held: Held,
}

impl Feed {
    /// Opens the stream, takes the events of its snapshot and lists the history behind them.
    fn open(
        client: Http,
        headers: Vec<(HeaderName, HeaderValue)>,
        watch_url: Url,
        events_url: Url,
        handshake_timeout: Duration,
    ) -> Result<Self> {
        let (stream, snapshot) = Stream::open(
            &client,
            &headers,
            &watch_url,
            Instant::now() + handshake_timeout,
        )?;
        let mut feed = Self {
            client,
            headers,
            watch_url,
            events_url,
            stream,
            seq: 0,
            held: Held::default(),
        };
        feed.merge(snapshot, None)?;
        Ok(feed)
    }

    /// Reads the next frame and applies it.
    fn follow(&mut self, deadline: Instant, stop: Option<&Notify>) -> Result<bool> {
        let Some(frame) = self.stream.next(deadline, stop)? else {
            return Ok(false);
        };
        match frame {
            Frame::Snapshot(snapshot) => self.merge(snapshot, Some(deadline)),
            Frame::Delta(delta) if delta.base == self.seq => {
                self.seq = delta.seq;
                for event in delta.events {
                    self.held.insert(event)?;
                }
                Ok(())
            }
            Frame::Delta(_) => self.reopen(deadline),
            Frame::Tick(_) => Ok(()),
            Frame::Closed(closed) => Err(E2eError::system(format!(
                "the state stream was closed by the server: {}",
                closed.reason
            ))),
        }?;
        Ok(true)
    }

    /// Replaces the stream with a new one and merges its snapshot, because the frames of the old
    /// one no longer follow each other.
    fn reopen(&mut self, deadline: Instant) -> Result<()> {
        let (stream, snapshot) =
            Stream::open(&self.client, &self.headers, &self.watch_url, deadline)?;
        self.stream = stream;
        self.merge(snapshot, Some(deadline))
    }

    /// Adds the events of `snapshot` that are not held, then, when they fill the snapshot window,
    /// follows the history newest first and adds its events too. The newest event held before the
    /// snapshot is the floor: the listing stops at the first page that contains an event with a
    /// sequence number at or below the floor, or when a page has no next page token. With no
    /// event held yet there is no floor, and every page is followed until the token is absent.
    fn merge(&mut self, snapshot: State, deadline: Option<Instant>) -> Result<()> {
        let floor = self.held.seqs.last().copied();
        self.seq = snapshot.seq;
        let window_full = usize::try_from(snapshot.events_limit)
            .is_ok_and(|limit| snapshot.events.len() == limit);
        for event in snapshot.events {
            self.held.insert(event)?;
        }
        if !window_full {
            return Ok(());
        }
        let mut token: Option<PageToken> = None;
        loop {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(E2eError::timeout(
                    "reading the scaling history",
                    Duration::ZERO,
                ));
            }
            let page = self.list(token.as_ref())?;
            let mut reached = false;
            for event in page.events {
                let seq = sequence(&event)?;
                reached |= floor.is_some_and(|floor| seq <= floor);
                self.held.insert(event)?;
            }
            match page.next_page_token {
                Some(next) if !reached => token = Some(next),
                _ => return Ok(()),
            }
        }
    }

    /// One page of the history, newest first, resuming after `token`.
    fn list(&self, token: Option<&PageToken>) -> Result<ListEventsResponse> {
        let mut url = self.events_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair(PAGE_SIZE, &PAGE_SIZE_MAX.to_string());
            if let Some(token) = token {
                query.append_pair(PAGE_TOKEN, token.as_str());
            }
        }
        let response = self.client.send(HttpRequest {
            method: Method::GET,
            url: &url,
            bearer: None,
            headers: &self.headers,
            body: None,
        })?;
        if !response.status.is_success() {
            let body = String::from_utf8_lossy(&response.body);
            return Err(E2eError::system(format!(
                "{url} answered {}: {body}",
                response.status
            )));
        }
        serde_json::from_slice(&response.body).map_err(E2eError::parse("the events list"))
    }
}

/// The open response of the state stream and the frames decoded from it.
struct Stream {
    /// The response, whose body is the stream.
    response: Chunks,
    /// Turns the body into frames.
    decoder: Decoder,
    /// Frames decoded from a chunk and not yet taken.
    pending: VecDeque<Frame>,
}

impl Stream {
    /// Opens the stream at `url` and reads its first frame, which must be the snapshot.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the request fails, the server answers a non-success status (the
    /// error carries the body, which is the error envelope), or the first frame is not a snapshot
    /// (a `closed` frame names its reason).
    fn open(
        client: &Http,
        headers: &[(HeaderName, HeaderValue)],
        url: &Url,
        deadline: Instant,
    ) -> Result<(Self, State)> {
        let mut headers = headers.to_vec();
        headers.push((ACCEPT, HeaderValue::from_static(EVENT_STREAM)));
        let mut response = client.open(HttpRequest {
            method: Method::GET,
            url,
            bearer: None,
            headers: &headers,
            body: None,
        })?;
        let status = response.status();
        if !status.is_success() {
            let mut body = Vec::new();
            while let Some(chunk) = response.next_within(
                deadline.saturating_duration_since(Instant::now()),
                "reading the state stream error",
            )? {
                body.extend_from_slice(&chunk);
            }
            let body = String::from_utf8_lossy(&body);
            return Err(E2eError::system(format!("{url} answered {status}: {body}")));
        }
        let mut stream = Self {
            response,
            decoder: Decoder::new(),
            pending: VecDeque::new(),
        };
        match stream.next(deadline, None)? {
            Some(Frame::Snapshot(snapshot)) => Ok((stream, snapshot)),
            Some(Frame::Closed(closed)) => Err(E2eError::system(format!(
                "the state stream was closed by the server before its snapshot: {}",
                closed.reason
            ))),
            Some(other) => Err(E2eError::system(format!(
                "the state stream starts with `{}`, not a snapshot",
                other.event_name()
            ))),
            None => Err(E2eError::system(
                "the state stream stopped before its snapshot",
            )),
        }
    }

    /// Waits for the next frame.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the connection fails or the stream ends;
    /// [`E2eError::Parse`] when the bytes are not frames.
    fn next(&mut self, deadline: Instant, stop: Option<&Notify>) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.pending.pop_front() {
                return Ok(Some(frame));
            }
            let budget = deadline.saturating_duration_since(Instant::now());
            let chunk = match stop {
                Some(stop) => match self.response.next_within_or_stop(
                    budget,
                    "reading the state stream",
                    stop,
                )? {
                    Read::Chunk(chunk) => Some(chunk),
                    Read::Ended => None,
                    Read::Stopped => return Ok(None),
                },
                None => self
                    .response
                    .next_within(budget, "reading the state stream")?,
            }
            .ok_or_else(|| E2eError::system("the state stream ended"))?;
            let frames = self
                .decoder
                .push(&chunk)
                .map_err(E2eError::parse("the state stream"))?;
            self.pending.extend(frames);
        }
    }
}

/// The events read so far, ascending by sequence number and unique by name.
#[derive(Default)]
struct Held {
    /// The sequence number of each event, in the order of `events`.
    seqs: Vec<u64>,
    /// The events, oldest first.
    events: Vec<Event>,
}

impl Held {
    /// Adds `event` in its place unless one of the same name is held. Returns whether it was
    /// added.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when the name of `event` is not `events/{seq}`.
    fn insert(&mut self, event: Event) -> Result<bool> {
        let seq = sequence(&event)?;
        Ok(match self.seqs.binary_search(&seq) {
            Ok(_) => false,
            Err(at) => {
                self.seqs.insert(at, seq);
                self.events.insert(at, event);
                true
            }
        })
    }
}

/// The full URL of `route`, which has no placeholder, on the API at `api`.
///
/// # Errors
///
/// [`E2eError::Parse`] when the path cannot be joined with `api`.
fn route_url(api: &Url, route: &Route) -> Result<Url> {
    api.join(route.template)
        .map_err(E2eError::parse("the api url"))
}

/// The first [`EventChange::ExitedEarly`] event after `after`, or after any sequence if it is
/// `None`. Events with invalid names are skipped.
#[must_use]
pub fn early_exit(events: &[Event], after: Option<u64>) -> Option<&Event> {
    events.iter().find(|event| {
        matches!(event.change, EventChange::ExitedEarly { .. })
            && sequence(event).is_ok_and(|seq| after.is_none_or(|baseline| seq > baseline))
    })
}

/// Describes an early exit, including its fleet, instance, and failure.
#[must_use]
pub fn describe_early_exit(event: &Event) -> String {
    match &event.change {
        EventChange::ExitedEarly { instance, failure } => format!(
            "a worker of {} failed soon after its launch: instance {instance}: {failure}",
            event.fleet
        ),
        _ => format!("event {} is not an early exit", event.name),
    }
}

/// The number of workers connected at the end of `history`: registrations less deregistrations
/// over every fleet, never below zero.
#[must_use]
pub fn connected_workers(history: &[Event]) -> usize {
    history
        .iter()
        .fold(0, |connected: usize, event| match event.change {
            EventChange::WorkerRegistered { .. } => connected.saturating_add(1),
            EventChange::WorkerDeregistered { .. } => connected.saturating_sub(1),
            _ => connected,
        })
}

/// Whether `history` records a scale-out of any fleet.
#[must_use]
pub fn scale_out_recorded(history: &[Event]) -> bool {
    history
        .iter()
        .any(|event| matches!(event.change, EventChange::ScaledOut { .. }))
}

/// Whether at least one worker has connected and none is connected now; a history in which no
/// worker ever registered is not drained.
#[must_use]
pub fn fleet_drained(history: &[Event]) -> bool {
    connected_workers(history) == 0
        && history
            .iter()
            .any(|event| matches!(event.change, EventChange::WorkerRegistered { .. }))
}

/// The events recorded after the one with sequence number `since`; a scenario judges a step on
/// these, never on the history of earlier steps. `history` is ascending, as an
/// [`EventFollower`] holds it.
#[must_use]
pub fn events_after_seq(history: &[Event], since: u64) -> &[Event] {
    &history[history.partition_point(|event| sequence(event).is_ok_and(|seq| seq <= since))..]
}

/// The sequence number of `event`, which its name `events/{seq}` carries.
///
/// # Errors
///
/// [`E2eError::Parse`] when the name is not `events/{seq}`.
pub(crate) fn sequence(event: &Event) -> Result<u64> {
    name::parse::<name::Events>(&event.name).map_err(E2eError::parse("an event name"))
}

#[cfg(test)]
mod tests {
    use gang_clock::Timestamp;
    use gang_wire::{Event, EventChange};

    use super::{
        connected_workers, early_exit, events_after_seq, fleet_drained, scale_out_recorded,
    };

    fn event(change: EventChange) -> Event {
        Event {
            name: "events/1".to_owned(),
            create_time: Timestamp::from_unix_millis(0),
            fleet: "fleets/fleet-arm64".to_owned(),
            cause: None,
            change,
        }
    }

    fn worker(id: &str) -> String {
        format!("workers/{id}")
    }

    #[test]
    fn early_exit_finds_the_first_exited_early_event() {
        let history = [
            event(EventChange::ScaledOut { from: 0, to: 1 }),
            event(EventChange::ExitedEarly {
                instance: "instances/first".to_owned(),
                failure: "first failure".to_owned(),
            }),
            event(EventChange::ExitedEarly {
                instance: "instances/second".to_owned(),
                failure: "second failure".to_owned(),
            }),
        ];
        assert_eq!(early_exit(&history, None), Some(&history[1]));
    }

    #[test]
    fn early_exit_is_none_without_one() {
        assert!(early_exit(&[], None).is_none());
        assert!(early_exit(&[event(EventChange::ScaledOut { from: 0, to: 1 })], None).is_none());
    }

    #[test]
    fn early_exit_ignores_exits_up_to_the_baseline() {
        let history = [Event {
            name: "events/2".to_owned(),
            ..event(EventChange::ExitedEarly {
                instance: "instances/first".to_owned(),
                failure: "failure".to_owned(),
            })
        }];
        assert!(early_exit(&history, Some(2)).is_none());
        assert_eq!(early_exit(&history, Some(1)), Some(&history[0]));

        let invalid = [Event {
            name: "invalid".to_owned(),
            ..history[0].clone()
        }];
        assert!(early_exit(&invalid, None).is_none());
    }

    #[test]
    fn connected_workers_counts_registrations_less_deregistrations() {
        let history = [
            event(EventChange::WorkerRegistered {
                worker: worker("worker-1"),
            }),
            event(EventChange::WorkerRegistered {
                worker: worker("worker-2"),
            }),
            event(EventChange::WorkerDeregistered {
                worker: worker("worker-1"),
            }),
        ];
        assert_eq!(connected_workers(&history), 1);
    }

    #[test]
    fn connected_workers_never_goes_below_zero() {
        let history = [event(EventChange::WorkerDeregistered {
            worker: worker("worker-1"),
        })];
        assert_eq!(connected_workers(&history), 0);
    }

    #[test]
    fn only_a_scaled_out_event_records_a_scale_out() {
        let drain = [event(EventChange::DrainStarted {
            worker: worker("worker-1"),
        })];
        assert!(!scale_out_recorded(&drain));

        let scaled = [event(EventChange::ScaledOut { from: 1, to: 2 })];
        assert!(scale_out_recorded(&scaled));
    }

    #[test]
    fn fleet_drained_needs_a_registered_worker_and_none_connected() {
        let scaled_out = event(EventChange::ScaledOut { from: 0, to: 1 });
        let registered = event(EventChange::WorkerRegistered {
            worker: worker("worker-1"),
        });
        let deregistered = event(EventChange::WorkerDeregistered {
            worker: worker("worker-1"),
        });
        assert!(!fleet_drained(&[]));
        assert!(!fleet_drained(std::slice::from_ref(&scaled_out)));
        assert!(!fleet_drained(&[scaled_out.clone(), registered.clone()]));
        assert!(fleet_drained(&[scaled_out, registered, deregistered]));
    }

    #[test]
    fn events_after_seq_is_the_suffix_after_the_cursor() {
        let history: Vec<Event> = (1..=3)
            .map(|seq| Event {
                name: format!("events/{seq}"),
                ..event(EventChange::ScaledOut {
                    from: seq,
                    to: seq + 1,
                })
            })
            .collect();
        assert_eq!(events_after_seq(&history, 0), &history[..]);
        assert_eq!(events_after_seq(&history, 2), &history[2..]);
        assert!(events_after_seq(&history, 3).is_empty());
        assert!(events_after_seq(&history, 99).is_empty());
    }
}
