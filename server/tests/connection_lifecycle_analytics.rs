//! What the connection lifecycle actually emits, driven through a real
//! websocket against a real server.
//!
//! `session_started` used to fire the moment a session id was minted — BEFORE
//! the token was verified — so it could carry no account, and it counted every
//! attempt including the ones that went on to be refused. Splitting the socket
//! from the session fixes the first problem and creates the chance to cause a
//! second one: move `session_started` behind verification without landing the
//! refusals anywhere, and the failure arm of every authentication funnel
//! silently disappears. Nothing crashes when that happens. The rows simply
//! stop existing.
//!
//! So these tests assert the whole set of events a connection produces, not
//! just the presence of the one being added. A refusal has to come out as
//! `connection_started` + `connection_ended` carrying the reason and the
//! version, with NO session pair — that is what keeps the funnel computable and
//! a version rollout countable.
//!
//! The pair also has to be JOINABLE. An accept and a close that cannot be
//! matched to each other answer no per-connection question — "how long did
//! sockets that never authenticated survive?", "did this client retry?" — so
//! the `connection_id` assertions below are as load-bearing as the counts.
//!
//! Requires the ordinary integration dependencies: Valkey on 6379 and the
//! LocalStack DynamoDB the rest of the suite uses (`./test-deps.sh`).

mod common;

use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use server::analytics::event::EventOrigin;
use server::analytics::proto;
use server::analytics::{AnalyticsEmitter, EmitterConfig};
use server::api::jwt::JwtManager;
use server::lifecycle::WS_PROTOCOL_VERSION;
use server::ws_server::{JwtVerifier, WSMessage};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

use crate::common::{MockJwtVerifier, TestClient, TestEnvironment, is_unsolicited_push};

/// Every test here starts servers and mutates process-wide environment, and
/// they all share the one process-global analytics sink.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

static CAPTURE: OnceLock<tokio::sync::Mutex<Receiver<proto::Event>>> = OnceLock::new();

/// The sink every event in this process lands in.
///
/// `sink::install` keeps the FIRST sink it is given, so installing here — before
/// any server is started — is what makes the test the consumer instead of the
/// server's own flusher. Nothing is stubbed on the way: these are the events
/// production would put on the Valkey stream.
fn capture() -> &'static tokio::sync::Mutex<Receiver<proto::Event>> {
    CAPTURE.get_or_init(|| {
        let (emitter, receiver) = AnalyticsEmitter::new(EmitterConfig { buffer: 8_192 });
        server::analytics::sink::install(
            emitter,
            EventOrigin {
                environment: "test".to_owned(),
                region: "use1".to_owned(),
                aws_region: "us-east-1".to_owned(),
                instance_id: "1:lifecycle".to_owned(),
            },
        );
        tokio::sync::Mutex::new(receiver)
    })
}

/// Events observed during one test, in emission order.
struct Observed {
    events: Vec<proto::Event>,
}

impl Observed {
    /// Drains anything a previous test left behind, so a count here is a count
    /// of THIS connection.
    ///
    /// Drains until the channel has been quiet for a whole window rather than
    /// once: `connection_ended` is emitted as the previous test's socket task
    /// unwinds, which can happen after that test's last assertion. One late
    /// close arriving here would be counted as this test's, and
    /// `connection_started` carries no identity to filter it out by.
    async fn fresh(receiver: &mut Receiver<proto::Event>) -> Self {
        let mut quiet_windows = 0;
        while quiet_windows < 4 {
            if receiver.try_recv().is_ok() {
                quiet_windows = 0;
                continue;
            }
            quiet_windows += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Self { events: Vec::new() }
    }

    /// Collects until `event_name` shows up, or the budget runs out.
    async fn drain_until(&mut self, receiver: &mut Receiver<proto::Event>, event_name: &str) {
        self.wait_for(receiver, event_name, 1).await;
    }

    /// Collects until `event_name` has been seen `count` times, or the budget
    /// runs out.
    ///
    /// Polling rather than a fixed sleep: the close events are emitted on the
    /// socket's own task after it unwinds, and a sleep long enough to be
    /// reliable would be long enough to hide a regression that made them late.
    ///
    /// Everything already queued is drained on every pass, so a test that
    /// asserts "nothing else was emitted" — or that a second socket has NOT
    /// closed yet — sees the extras rather than leaving them in the channel.
    async fn wait_for(
        &mut self,
        receiver: &mut Receiver<proto::Event>,
        event_name: &str,
        count: usize,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            while let Ok(event) = receiver.try_recv() {
                self.events.push(event);
            }
            if self.count(event_name) >= count {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "never saw {count} x {event_name}; observed {:?}",
                    self.lifecycle_names()
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// The connection-lifecycle events only. A running server also emits
    /// guest, login, lobby and game events, and none of them are this test's
    /// subject.
    fn lifecycle_names(&self) -> Vec<&str> {
        self.events
            .iter()
            .map(|event| event.event_name.as_str())
            .filter(|name| {
                matches!(
                    *name,
                    "connection_started" | "connection_ended" | "session_started" | "session_ended"
                )
            })
            .collect()
    }

    fn count(&self, event_name: &str) -> usize {
        self.events
            .iter()
            .filter(|event| event.event_name == event_name)
            .count()
    }

    fn only(&self, event_name: &str) -> &proto::Event {
        let mut matching = self
            .events
            .iter()
            .filter(|event| event.event_name == event_name);
        let found = matching
            .next()
            .unwrap_or_else(|| panic!("no {event_name} in {:?}", self.lifecycle_names()));
        assert!(
            matching.next().is_none(),
            "more than one {event_name} in {:?}",
            self.lifecycle_names()
        );
        found
    }

    fn identity(&self, event_name: &str) -> &proto::Identity {
        self.only(event_name)
            .identity
            .as_ref()
            .unwrap_or_else(|| panic!("{event_name} carried no identity"))
    }

    fn connection_ended(&self) -> &proto::ConnectionEnded {
        match self.only("connection_ended").payload.as_ref() {
            Some(proto::event::Payload::ConnectionEnded(ended)) => ended,
            other => panic!("connection_ended carried {other:?}"),
        }
    }

    /// The id every `connection_started` reported, in emission order.
    ///
    /// Order is the only thing that tells two accepts apart — they are
    /// deliberately identity-free — so it is what a pairing assertion has to
    /// lean on.
    fn accept_ids(&self) -> Vec<&str> {
        self.events
            .iter()
            .filter_map(|event| match event.payload.as_ref() {
                Some(proto::event::Payload::ConnectionStarted(started)) => {
                    Some(started.connection_id.as_str())
                }
                _ => None,
            })
            .collect()
    }

    /// Every `connection_ended`, in emission order.
    fn closes(&self) -> Vec<&proto::ConnectionEnded> {
        self.events
            .iter()
            .filter_map(|event| match event.payload.as_ref() {
                Some(proto::event::Payload::ConnectionEnded(ended)) => Some(ended),
                _ => None,
            })
            .collect()
    }
}

/// Starts a server on `env` whose verifier admits exactly ONE token.
///
/// Strict rather than accept-any so a rejected token is a genuine verification
/// failure — accept-any would instead fail later, on a missing user record,
/// which is a different path through `authenticate_ws_connection`.
async fn admit_only(env: &mut TestEnvironment, token: &str, user_id: i32) -> Result<()> {
    let verifier =
        Arc::new(MockJwtVerifier::new().with_token(token, user_id)) as Arc<dyn JwtVerifier>;
    env.add_server_with_jwt_verifier(
        JwtManager::new("test_secret_key_for_testing"),
        verifier,
        false,
    )
    .await?;
    Ok(())
}

fn handshake(token: &str, protocol_version: u16) -> WSMessage {
    WSMessage::Authenticate {
        token: token.to_owned(),
        protocol_version,
        anon_id: Some("2f1c2f1c-2f1c-2f1c-2f1c-2f1c2f1c2f1c".to_owned()),
        distribution: None,
    }
}

/// The reason the event moved behind verification at all: it can finally say
/// WHO the session belongs to. A `session_started` with a null `user_id` is
/// the defect this replaces.
#[tokio::test]
async fn a_verified_handshake_starts_a_session_that_names_the_account() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut receiver = capture().lock().await;
    let mut observed = Observed::fresh(&mut receiver).await;

    let mut env = TestEnvironment::new("connection_lifecycle_analytics").await?;
    // The account has to exist before the handshake: verification reads the
    // record back and refuses if the guest and pool claims disagree with it.
    let user_id = env.create_user().await?;
    admit_only(&mut env, "good-token", user_id).await?;

    let mut client = TestClient::connect(&env.ws_addr(0).expect("a server")).await?;
    client
        .send_message(handshake("good-token", WS_PROTOCOL_VERSION))
        .await?;
    observed.drain_until(&mut receiver, "session_started").await;

    let started_session_id = {
        let identity = observed.identity("session_started");
        assert_eq!(
            identity.user_id,
            Some(i64::from(user_id)),
            "a started session must name the account it authenticated as"
        );
        assert!(!identity.is_guest);
        assert_eq!(
            identity.anon_id.as_deref(),
            Some("2f1c2f1c-2f1c-2f1c-2f1c-2f1c2f1c2f1c")
        );
        identity
            .session_id
            .clone()
            .expect("the session id is the only key later events join on")
    };

    client.disconnect().await?;
    observed
        .drain_until(&mut receiver, "connection_ended")
        .await;

    assert_eq!(
        observed.count("connection_started"),
        1,
        "exactly one accept per connection"
    );
    assert_eq!(observed.count("connection_ended"), 1);
    let accept_id = observed.accept_ids()[0];
    assert!(
        !accept_id.is_empty(),
        "an accept with no id is a join key to nothing"
    );
    assert_eq!(
        observed.connection_ended().connection_id,
        accept_id,
        "an accept and its own close must be joinable, or no per-connection \
         question is answerable from these rows"
    );
    let ended = observed.identity("session_ended");
    assert_eq!(
        ended.user_id,
        Some(i64::from(user_id)),
        "a session's end must be attributable to the same account as its start"
    );
    assert_eq!(
        ended.session_id.as_deref(),
        Some(started_session_id.as_str()),
        "start and end must name the same session"
    );
    assert_eq!(
        observed.identity("connection_ended").user_id,
        Some(i64::from(user_id)),
        "an authenticated connection's close is attributable"
    );

    env.shutdown().await?;
    Ok(())
}

/// THE TRAP, end to end. A token that does not verify must produce no session
/// at all — and must still be countable, or the funnel loses its failures.
#[tokio::test]
async fn a_rejected_token_produces_a_socket_with_no_session() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut receiver = capture().lock().await;
    let mut observed = Observed::fresh(&mut receiver).await;

    let mut env = TestEnvironment::new("connection_lifecycle_analytics").await?;
    admit_only(&mut env, "good-token", 1).await?;

    let mut client = TestClient::connect(&env.ws_addr(0).expect("a server")).await?;
    client
        .send_message(handshake("forged-token", WS_PROTOCOL_VERSION))
        .await?;
    observed
        .drain_until(&mut receiver, "connection_ended")
        .await;

    assert_eq!(
        observed.lifecycle_names(),
        vec!["connection_started", "connection_ended"],
        "an unverified token must produce no session_started and no session_ended"
    );
    let ended = observed.connection_ended();
    assert_eq!(ended.close_reason, "authentication_failed");
    assert_eq!(
        ended.protocol_version,
        Some(i64::from(WS_PROTOCOL_VERSION)),
        "the attempt has to stay countable by the version it reported"
    );
    let identity = observed.identity("connection_ended");
    assert_eq!(identity.user_id, None, "nobody was ever verified");
    assert_eq!(
        identity.session_id, None,
        "a session id minted for a failed handshake names no session_started row"
    );
    assert_eq!(
        identity.anon_id.as_deref(),
        Some("2f1c2f1c-2f1c-2f1c-2f1c-2f1c2f1c2f1c"),
        "the browser behind a refused client is what a rollout needs"
    );

    let _ = client.disconnect().await;
    env.shutdown().await?;
    Ok(())
}

/// The other refusal, and the one this whole design has to keep countable: a
/// client whose gameplay protocol the server will not speak. It is refused
/// before the token is even looked at, so `session_started` never fires — and
/// the version it asked for survives only on `connection_ended`.
#[tokio::test]
async fn a_rejected_protocol_version_produces_a_socket_with_no_session() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut receiver = capture().lock().await;
    let mut observed = Observed::fresh(&mut receiver).await;

    let mut env = TestEnvironment::new("connection_lifecycle_analytics").await?;
    admit_only(&mut env, "good-token", 1).await?;
    let stale_version = WS_PROTOCOL_VERSION - 1;

    let mut client = TestClient::connect(&env.ws_addr(0).expect("a server")).await?;
    client
        .send_message(handshake("good-token", stale_version))
        .await?;
    // The denial is the server's proof it took the refusal path, rather than
    // simply never getting round to the handshake. The gateway subscribes every
    // socket to the user-count broadcast before the handshake is even read, so
    // an unsolicited push can arrive first and has to be skipped.
    loop {
        match client.receive_message().await? {
            WSMessage::AccessDenied { reason } => {
                assert!(reason.contains("protocol"), "unexpected denial: {reason}");
                break;
            }
            other if is_unsolicited_push(&other) => {}
            other => panic!("expected AccessDenied, got {other:?}"),
        }
    }
    client.disconnect().await?;
    observed
        .drain_until(&mut receiver, "connection_ended")
        .await;

    assert_eq!(
        observed.lifecycle_names(),
        vec!["connection_started", "connection_ended"],
        "a refused client must produce no session pair"
    );
    let ended = observed.connection_ended();
    assert_eq!(ended.close_reason, "protocol_rejected");
    assert_eq!(
        ended.protocol_version,
        Some(i64::from(stale_version)),
        "a version rollout is counted off exactly this column"
    );

    env.shutdown().await?;
    Ok(())
}

/// The denominator. A socket that says nothing at all is still one accept and
/// one close — the property every funnel divides by, and the one a handshake
/// can never suppress.
#[tokio::test]
async fn every_socket_produces_exactly_one_start_and_one_end() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut receiver = capture().lock().await;
    let mut observed = Observed::fresh(&mut receiver).await;

    let mut env = TestEnvironment::new("connection_lifecycle_analytics").await?;
    admit_only(&mut env, "good-token", 1).await?;
    let address = env.ws_addr(0).expect("a server");

    for _ in 0..3 {
        let client = TestClient::connect(&address).await?;
        client.disconnect().await?;
    }
    observed
        .wait_for(&mut receiver, "connection_ended", 3)
        .await;

    assert_eq!(observed.count("connection_started"), 3);
    assert_eq!(observed.count("connection_ended"), 3);
    let accepts: HashSet<&str> = observed.accept_ids().into_iter().collect();
    let closes: HashSet<&str> = observed
        .closes()
        .into_iter()
        .map(|ended| ended.connection_id.as_str())
        .collect();
    assert_eq!(accepts.len(), 3, "three sockets, three distinct ids");
    assert_eq!(
        accepts, closes,
        "every accept must be closed by a row naming it, and no close may name \
         a socket that was never accepted"
    );
    assert_eq!(
        observed.count("session_started"),
        0,
        "a socket that never handshook has no session"
    );
    assert_eq!(observed.count("session_ended"), 0);

    env.shutdown().await?;
    Ok(())
}

/// The pairing property under CONCURRENCY, which is the case a shared-state
/// bug actually shows up in.
///
/// Two sockets are open at the same time. Their accepts are deliberately
/// identity-free, so the only thing telling them apart is emission ORDER — the
/// two connects are therefore made one at a time, each accept awaited before
/// the next. Their closes are made distinguishable by outcome: the first is
/// refused for its protocol version, the second simply hangs up.
///
/// That is what makes the assertion sharp. An id taken from anywhere
/// process-wide — a static, a last-writer-wins cell, a counter read at close —
/// gives both accepts the same value and fails the distinctness check. An id
/// that is per-connection but crosses between the two passes that check and
/// fails the pairing check below it.
#[tokio::test]
async fn two_concurrent_connections_never_cross_their_ids() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut receiver = capture().lock().await;
    let mut observed = Observed::fresh(&mut receiver).await;

    let mut env = TestEnvironment::new("connection_lifecycle_analytics").await?;
    admit_only(&mut env, "good-token", 1).await?;
    let address = env.ws_addr(0).expect("a server");

    let mut refused = TestClient::connect(&address).await?;
    observed
        .wait_for(&mut receiver, "connection_started", 1)
        .await;
    // Connected only after the first accept has been observed, so the two
    // `connection_started` rows are in a known order.
    let survivor = TestClient::connect(&address).await?;
    observed
        .wait_for(&mut receiver, "connection_started", 2)
        .await;

    let accepts = observed.accept_ids();
    assert_eq!(accepts.len(), 2, "two sockets, two accepts");
    assert!(
        !accepts[0].is_empty() && !accepts[1].is_empty(),
        "an accept with no id is a join key to nothing"
    );
    assert_ne!(
        accepts[0], accepts[1],
        "two live sockets sharing an id would fold every per-connection \
         question into a single bucket"
    );
    let (first_accept, second_accept) = (accepts[0].to_owned(), accepts[1].to_owned());

    // Refuse the FIRST socket while the second is still open, so its close is
    // emitted with a live sibling in the process to be confused with.
    refused
        .send_message(handshake("good-token", WS_PROTOCOL_VERSION - 1))
        .await?;
    loop {
        match refused.receive_message().await? {
            WSMessage::AccessDenied { reason } => {
                assert!(reason.contains("protocol"), "unexpected denial: {reason}");
                break;
            }
            other if is_unsolicited_push(&other) => {}
            other => panic!("expected AccessDenied, got {other:?}"),
        }
    }
    refused.disconnect().await?;
    observed
        .wait_for(&mut receiver, "connection_ended", 1)
        .await;

    assert_eq!(
        observed.count("connection_ended"),
        1,
        "the second socket is still open, so exactly one close is due"
    );
    let first_close = observed.closes()[0];
    assert_eq!(
        first_close.close_reason, "protocol_rejected",
        "the refused socket is the one that closed"
    );
    assert_eq!(
        first_close.connection_id, first_accept,
        "the refused socket's close must name the accept that preceded it, \
         not the socket still open beside it"
    );

    survivor.disconnect().await?;
    observed
        .wait_for(&mut receiver, "connection_ended", 2)
        .await;

    let closes = observed.closes();
    assert_eq!(closes.len(), 2);
    assert_eq!(
        closes[1].close_reason, "socket_closed",
        "the second socket hung up rather than being refused"
    );
    assert_eq!(
        closes[1].connection_id, second_accept,
        "the second socket's close must name its own accept, not the earlier \
         one that already closed"
    );

    env.shutdown().await?;
    Ok(())
}
