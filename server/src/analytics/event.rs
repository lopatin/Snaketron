//! Event construction and serialization.
//!
//! The envelope is built in exactly one place so every event carries the same
//! identity, timing, and origin fields. Serialization is **proto3 canonical
//! JSON**: the protos supply the schema, JSON supplies the encoding, which
//! keeps the raw tier human-inspectable and readable by Athena and DuckDB with
//! no descriptor.

use anyhow::{Context, Result};
use prost_reflect::{DynamicMessage, MessageDescriptor, SerializeOptions};
use uuid::Uuid;

use super::proto;

/// Origin facts shared by every event emitted from this task.
#[derive(Debug, Clone)]
pub struct EventOrigin {
    pub environment: String,
    /// Logical region (use1 / euw1).
    pub region: String,
    pub aws_region: String,
    /// `{server_id}:{boot_id}`.
    pub instance_id: String,
}

impl EventOrigin {
    pub fn from_env(region: &str, instance_id: &str) -> Self {
        Self {
            environment: std::env::var("SNAKETRON_ENVIRONMENT")
                .unwrap_or_else(|_| "dev".to_owned()),
            region: region.to_owned(),
            aws_region: std::env::var("SNAKETRON_AWS_REGION").unwrap_or_default(),
            instance_id: instance_id.to_owned(),
        }
    }
}

/// Who an event is about. All fields optional: the top of the funnel happens
/// before any account exists.
#[derive(Debug, Clone, Default)]
pub struct EventIdentity {
    pub user_id: Option<i64>,
    pub anon_id: Option<String>,
    pub session_id: Option<String>,
    pub is_guest: bool,
    pub is_stress_test: bool,
}

impl From<EventIdentity> for proto::Identity {
    fn from(value: EventIdentity) -> Self {
        Self {
            user_id: value.user_id,
            anon_id: value.anon_id,
            session_id: value.session_id,
            is_guest: value.is_guest,
            is_stress_test: value.is_stress_test,
        }
    }
}

/// Current wall clock in epoch milliseconds.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Builds an envelope around a payload, stamped with the current clock.
///
/// `event_id` is a UUIDv7 so it is both unique and time-sortable, which gives
/// the Iceberg table a naturally clustered dedup key. `occurred_at_ms` is
/// stamped at the origin, and no downstream component may rewrite it — that is
/// what makes event-time partitioning honest after a replay.
pub fn envelope(
    origin: &EventOrigin,
    identity: EventIdentity,
    payload: proto::event::Payload,
) -> proto::Event {
    envelope_at(origin, identity, payload, now_ms())
}

/// Builds an envelope around a payload that was observed at `occurred_at_ms`.
///
/// Exists for producers that read the clock somewhere other than where they
/// build the event — the websocket hooks capture the time on the connection's
/// own task and project on the exporter's, and a re-read at projection time
/// would report the drain's clock as the frame's. Every other field is
/// origin-supplied and cannot go stale, so the timestamp is the only one that
/// has to travel.
pub fn envelope_at(
    origin: &EventOrigin,
    identity: EventIdentity,
    payload: proto::event::Payload,
    occurred_at_ms: i64,
) -> proto::Event {
    proto::Event {
        event_id: Uuid::now_v7().to_string(),
        event_name: payload_name(&payload).to_owned(),
        event_version: 1,
        occurred_at_ms,
        environment: origin.environment.clone(),
        region: origin.region.clone(),
        aws_region: origin.aws_region.clone(),
        instance_id: origin.instance_id.clone(),
        identity: Some(identity.into()),
        payload: Some(payload),
    }
}

/// The registry name for a payload arm.
///
/// Derived from the oneof rather than passed in, so the name on the wire can
/// never drift from the payload it describes.
pub fn payload_name(payload: &proto::event::Payload) -> &'static str {
    use proto::event::Payload as P;
    match payload {
        P::GuestCreated(_) => "guest_created",
        P::AccountRegistered(_) => "account_registered",
        P::GuestConverted(_) => "guest_converted",
        P::UserLogin(_) => "user_login",
        P::SessionStarted(_) => "session_started",
        P::SessionEnded(_) => "session_ended",
        P::LobbyCreated(_) => "lobby_created",
        P::LobbyJoined(_) => "lobby_joined",
        P::LobbyLeft(_) => "lobby_left",
        P::LobbyPreferencesSet(_) => "lobby_preferences_set",
        P::QueueEntered(_) => "queue_entered",
        P::QueueLeft(_) => "queue_left",
        P::MatchCommitted(_) => "match_committed",
        P::GameStarted(_) => "game_started",
        P::GameCompleted(_) => "game_completed",
        P::GamePlayerResult(_) => "game_player_result",
        P::WebsocketMessage(_) => "websocket_message",
    }
}

/// Serializes to one line of proto3 canonical JSON.
///
/// `use_proto_field_name` is on so the JSON keys match the proto field names
/// exactly — the Iceberg column names derive from those same names, and a
/// camelCase key would break the correspondence (and Athena, which rejects
/// uppercase column names outright).
///
/// 64-bit integers are QUOTED, which is what the proto3 JSON mapping
/// specifies. Emitting them bare would spare a view one cast, but the reader
/// has to agree with the writer, and `observability/athena/` declares those
/// columns `string` and casts. Canonical is the version of that agreement a
/// third tool will also guess correctly.
pub fn to_json_line(event: &proto::Event) -> Result<String> {
    let dynamic = DynamicMessage::decode(event_descriptor()?.clone(), encode(event).as_slice())
        .context("re-decoding analytics event for JSON serialization")?;

    let options = SerializeOptions::new()
        .stringify_64_bit_integers(true)
        .use_proto_field_name(true)
        .skip_default_fields(false);

    let mut buffer = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut buffer);
    dynamic
        .serialize_with_options(&mut serializer, &options)
        .context("serializing analytics event")?;

    let line = String::from_utf8(buffer).context("analytics event JSON was not UTF-8")?;
    debug_assert!(
        !line.contains('\n'),
        "an NDJSON line must not contain a newline"
    );
    Ok(line)
}

/// The `Event` descriptor, resolved once.
///
/// Held in a `OnceLock` because serialization runs per event and decoding the
/// whole descriptor set each time would be pure waste.
fn event_descriptor() -> Result<&'static MessageDescriptor> {
    static DESCRIPTOR: std::sync::OnceLock<Option<MessageDescriptor>> = std::sync::OnceLock::new();
    DESCRIPTOR
        .get_or_init(|| {
            super::schema::descriptor_pool()
                .ok()
                .and_then(|pool| super::schema::event_descriptor(&pool).ok())
        })
        .as_ref()
        .context("analytics Event descriptor unavailable")
}

fn encode(event: &proto::Event) -> Vec<u8> {
    use prost::Message;
    let mut buffer = Vec::with_capacity(event.encoded_len());
    event
        .encode(&mut buffer)
        .expect("encoding into a Vec cannot fail");
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin() -> EventOrigin {
        EventOrigin {
            environment: "test".to_owned(),
            region: "use1".to_owned(),
            aws_region: "us-east-1".to_owned(),
            instance_id: "42:boot".to_owned(),
        }
    }

    fn guest() -> proto::event::Payload {
        proto::event::Payload::GuestCreated(proto::GuestCreated {
            mmr: 1000,
            matchmaking_pool: "public".to_owned(),
        })
    }

    #[test]
    fn an_envelope_carries_origin_identity_and_a_derived_name() {
        let event = envelope(
            &origin(),
            EventIdentity {
                user_id: Some(7),
                is_guest: true,
                ..Default::default()
            },
            guest(),
        );
        assert_eq!(event.event_name, "guest_created");
        assert_eq!(event.region, "use1");
        assert_eq!(event.instance_id, "42:boot");
        assert_eq!(event.identity.as_ref().unwrap().user_id, Some(7));
        assert!(event.identity.as_ref().unwrap().is_guest);
        assert!(event.occurred_at_ms > 0);
    }

    /// UUIDv7 is time-sortable, which is what gives the Iceberg table a
    /// naturally clustered dedup key.
    #[test]
    fn event_ids_are_unique_and_time_sortable() {
        let mut ids = Vec::new();
        for _ in 0..30 {
            ids.push(envelope(&origin(), EventIdentity::default(), guest()).event_id);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "UUIDv7 must sort in creation order");
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "ids must be unique");
        assert_eq!(ids[0].as_bytes()[14], b'7', "must be UUID version 7");
    }

    /// The name must come from the payload arm, never from a caller-supplied
    /// string that could drift from it.
    #[test]
    fn every_payload_arm_has_a_distinct_registry_name() {
        use proto::event::Payload as P;
        let arms: Vec<P> = vec![
            P::GuestCreated(Default::default()),
            P::AccountRegistered(Default::default()),
            P::GuestConverted(Default::default()),
            P::UserLogin(Default::default()),
            P::SessionStarted(Default::default()),
            P::SessionEnded(Default::default()),
            P::LobbyCreated(Default::default()),
            P::LobbyJoined(Default::default()),
            P::LobbyLeft(Default::default()),
            P::LobbyPreferencesSet(Default::default()),
            P::QueueEntered(Default::default()),
            P::QueueLeft(Default::default()),
            P::MatchCommitted(Default::default()),
            P::GameStarted(Default::default()),
            P::GameCompleted(Default::default()),
            P::GamePlayerResult(Default::default()),
            P::WebsocketMessage(Default::default()),
        ];
        let names: Vec<&str> = arms.iter().map(payload_name).collect();
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "names must be distinct: {names:?}"
        );
        assert_eq!(names.len(), 17, "every registry entry must be covered");
    }

    /// One line, snake_case keys, and QUOTED 64-bit integers — each of which
    /// a view depends on. The quoting is the proto3 JSON mapping, and
    /// `observability/athena/` declares those columns `string` to match.
    #[test]
    fn serialization_is_single_line_snake_case_json() {
        let event = envelope(
            &origin(),
            EventIdentity {
                user_id: Some(7),
                ..Default::default()
            },
            guest(),
        );
        let line = to_json_line(&event).unwrap();
        assert!(!line.contains('\n'), "NDJSON lines must not wrap");
        assert!(
            line.contains("\"event_id\""),
            "keys must be proto field names"
        );
        assert!(
            !line.contains("\"eventId\""),
            "camelCase would break Athena"
        );
        assert!(
            line.contains("\"occurred_at_ms\":\""),
            "64-bit integers must be quoted per the proto3 JSON mapping"
        );

        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["event_name"], "guest_created");
        // Quoted per the proto3 JSON mapping, matching the Athena DDL.
        assert_eq!(parsed["identity"]["user_id"], "7");
        assert_eq!(parsed["guest_created"]["mmr"], "1000");
    }

    #[test]
    fn an_absent_identity_field_round_trips_as_null() {
        let event = envelope(&origin(), EventIdentity::default(), guest());
        let parsed: serde_json::Value =
            serde_json::from_str(&to_json_line(&event).unwrap()).unwrap();
        assert!(parsed["identity"]["user_id"].is_null());
        assert_eq!(parsed["identity"]["is_guest"], false);
    }
}
