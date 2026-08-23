//! Skin storage on the single main table.
//!
//! Item families, all under the existing `pk`/`sk` table so skins need no new
//! table and no new operational surface:
//!
//! ```text
//! SKIN#{id}     META              the skin
//!               REV#{n:06}        one immutable revision
//!               REVIEW#{ms}       one review request or decision
//! USER#{id}     SKINOWN#{skin}    a permanent grant
//! ```
//!
//! Three index entries make the three real queries cheap:
//!
//! - GSI1 `SKIN_PUBLISHED#{kind}` / published-at — the browse listing, newest
//!   first. Written only while the skin is published, so the index is sparse
//!   and a withdrawn skin leaves it rather than being filtered out of it.
//! - GSI1 `SKINREF#{content_ref}` on each revision — the render path, which
//!   starts from a hash out of a match snapshot and knows no skin id. The hash
//!   is in the *partition* key rather than the sort key so revisions spread
//!   across partitions instead of piling into one.
//! - GSI2 `SKIN_OWNER#{user}` / created-at — "things I made".

use anyhow::{Context, Result, anyhow};
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

use crate::skin_store::{
    GrantSource, NewRevision, Publication, Skin, SkinGrant, SkinKind, SkinNamespace, SkinRevision,
};

/// Sort keys are zero-padded so lexicographic order is numeric order; six
/// digits is a million revisions of one skin, which no author will reach.
pub fn revision_sort_key(revision: u32) -> String {
    format!("REV#{revision:06}")
}

pub fn skin_partition(skin_id: i32) -> String {
    format!("SKIN#{skin_id}")
}

pub fn grant_sort_key(skin_id: i32) -> String {
    format!("SKINOWN#{skin_id}")
}

pub fn published_index_partition(kind: SkinKind) -> String {
    format!("SKIN_PUBLISHED#{}", kind.as_str())
}

pub fn owner_index_partition(user_id: i32) -> String {
    format!("SKIN_OWNER#{user_id}")
}

pub fn content_ref_index_partition(content_ref: &str) -> String {
    format!("SKINREF#{content_ref}")
}

pub fn create_idempotency_partition(user_id: i32) -> String {
    format!("SKIN_CREATE#{user_id}")
}

pub fn create_idempotency_sort_key(key: &str) -> String {
    let digest = skin_schema::content::reference_for_bytes(key.as_bytes());
    format!("IDEMP#{}", digest.trim_start_matches("sha256:"))
}

pub fn create_idempotency_item(
    user_id: i32,
    key: &str,
    request_hash: &str,
    skin_id: i32,
    created_at_ms: i64,
) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        string(create_idempotency_partition(user_id)),
    );
    item.insert("sk".to_string(), string(create_idempotency_sort_key(key)));
    item.insert("skinId".to_string(), number(skin_id));
    item.insert("requestHash".to_string(), string(request_hash));
    item.insert("createdAtMs".to_string(), number(created_at_ms));
    item
}

/// The one partition every open review request sits in.
///
/// A constant partition would normally be a hot-partition mistake, and here it
/// deliberately is not: an entry exists only while a request is *open* and is
/// deleted when it is decided, so the partition holds the queue rather than
/// every request ever made. If the queue is ever large enough for this to hurt,
/// the queue itself is the problem.
pub const REVIEW_QUEUE_PARTITION: &str = "SKIN_REVIEW_QUEUE";

/// The sort key of the marker item that puts a skin in the review queue.
pub const REVIEW_QUEUE_SORT_KEY: &str = "REVIEWQUEUE";

/// A retry-stable key for one exact moderation decision.
pub fn review_decision_sort_key(
    publication: Publication,
    revision: Option<u32>,
    content_ref: Option<&str>,
    actor_user_id: i32,
    reason: Option<&str>,
) -> String {
    let identity = format!(
        "{}\n{}\n{}\n{}\n{}",
        publication.as_str(),
        revision.map_or_else(String::new, |value| value.to_string()),
        content_ref.unwrap_or_default(),
        actor_user_id,
        reason.unwrap_or_default(),
    );
    let digest = skin_schema::content::reference_for_bytes(identity.as_bytes());
    format!("REVIEW#{}", digest.trim_start_matches("sha256:"))
}

/// The immutable audit row written in the same transaction as publication.
pub fn review_decision_item(
    skin_id: i32,
    publication: Publication,
    revision: Option<u32>,
    content_ref: Option<&str>,
    actor_user_id: i32,
    reason: Option<&str>,
    at_ms: i64,
) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), string(skin_partition(skin_id)));
    item.insert(
        "sk".to_string(),
        string(review_decision_sort_key(
            publication,
            revision,
            content_ref,
            actor_user_id,
            reason,
        )),
    );
    item.insert("publication".to_string(), string(publication.as_str()));
    item.insert("actorUserId".to_string(), number(actor_user_id));
    item.insert("atMs".to_string(), number(at_ms));
    if let Some(revision) = revision {
        item.insert("revision".to_string(), number(revision));
    }
    if let Some(content_ref) = content_ref {
        item.insert("contentRef".to_string(), string(content_ref));
    }
    if let Some(reason) = reason {
        item.insert("reason".to_string(), string(reason));
    }
    item
}

/// The marker item itself.
pub fn review_queue_item(skin_id: i32, requested_at_ms: i64) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), string(skin_partition(skin_id)));
    item.insert("sk".to_string(), string(REVIEW_QUEUE_SORT_KEY));
    item.insert("skinId".to_string(), number(skin_id));
    item.insert("requestedAtMs".to_string(), number(requested_at_ms));
    item.insert("gsi1pk".to_string(), string(REVIEW_QUEUE_PARTITION));
    // Oldest first when read forward: a review queue is a queue.
    item.insert("gsi1sk".to_string(), string(sortable(requested_at_ms)));
    item
}

/// Build the attribute map for a skin's META item.
pub fn skin_item(skin: &Skin) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), string(skin_partition(skin.skin_id)));
    item.insert("sk".to_string(), string("META"));
    item.insert("skinId".to_string(), number(skin.skin_id));
    item.insert("kind".to_string(), string(skin.kind.as_str()));
    item.insert("namespace".to_string(), string(skin.namespace.as_str()));
    item.insert("creatorUserId".to_string(), number(skin.creator_user_id));
    if let Some(username) = &skin.creator_username {
        item.insert("creatorUsername".to_string(), string(username));
    }
    item.insert("name".to_string(), string(&skin.name));
    item.insert("publication".to_string(), string(skin.publication.as_str()));
    if let Some(pending) = skin.pending_revision {
        item.insert("pendingRevision".to_string(), number(pending));
    }
    item.insert("priceBux".to_string(), number(skin.price_bux));
    item.insert("headRevision".to_string(), number(skin.head_revision));
    item.insert("headContentRef".to_string(), string(&skin.head_content_ref));
    if let Some(published) = skin.published_revision {
        item.insert("publishedRevision".to_string(), number(published));
    }
    if let Some(reference) = &skin.published_content_ref {
        item.insert("publishedContentRef".to_string(), string(reference));
    }
    item.insert("createdAtMs".to_string(), number(skin.created_at_ms));
    item.insert("updatedAtMs".to_string(), number(skin.updated_at_ms));
    item.insert("ownerCount".to_string(), number(skin.owner_count));
    item.insert("wearerCount".to_string(), number(skin.wearer_count));
    if let Some(published_at) = skin.published_at_ms {
        item.insert("publishedAtMs".to_string(), number(published_at));
    }

    // "Things I made" is always available; the browse index entry is written
    // only while the skin is actually published, which is what keeps that
    // index sparse and the listing free of filtering.
    item.insert(
        "gsi2pk".to_string(),
        string(owner_index_partition(skin.creator_user_id)),
    );
    item.insert("gsi2sk".to_string(), string(sortable(skin.created_at_ms)));
    if skin.namespace.is_publishable() && skin.publication.is_browsable() {
        item.insert(
            "gsi1pk".to_string(),
            string(published_index_partition(skin.kind)),
        );
        item.insert(
            "gsi1sk".to_string(),
            string(sortable(skin.published_at_ms.unwrap_or(skin.updated_at_ms))),
        );
    }
    item
}

/// Read a skin back out of its item.
pub fn skin_from_item(item: &HashMap<String, AttributeValue>) -> Result<Skin> {
    Ok(Skin {
        skin_id: read_number(item, "skinId").ok_or_else(|| anyhow!("skin item has no id"))?,
        kind: read_string(item, "kind")
            .and_then(|value| SkinKind::parse(&value))
            .ok_or_else(|| anyhow!("skin item has no usable kind"))?,
        // Rows created before evaluation fixtures existed are production
        // skins. Unknown new values fail closed into the non-publishable
        // namespace rather than silently entering the catalogue.
        namespace: match read_string(item, "namespace") {
            None => SkinNamespace::Production,
            Some(value) => SkinNamespace::parse(&value).unwrap_or(SkinNamespace::Evaluation),
        },
        creator_user_id: read_number(item, "creatorUserId")
            .ok_or_else(|| anyhow!("skin item has no creator"))?,
        creator_username: read_string(item, "creatorUsername"),
        name: read_string(item, "name").unwrap_or_default(),
        // An unreadable publication is treated as disabled rather than as
        // published: a storage-level surprise must fail closed, because the
        // failure mode on the other side is showing content nobody approved.
        publication: read_string(item, "publication")
            .and_then(|value| Publication::parse(&value))
            .unwrap_or(Publication::Disabled),
        pending_revision: read_number::<u32>(item, "pendingRevision"),
        price_bux: read_number(item, "priceBux").unwrap_or(0),
        head_revision: read_number(item, "headRevision").unwrap_or(1),
        published_revision: read_number::<u32>(item, "publishedRevision"),
        head_content_ref: read_string(item, "headContentRef").unwrap_or_default(),
        published_content_ref: read_string(item, "publishedContentRef"),
        created_at_ms: read_number(item, "createdAtMs").unwrap_or(0),
        updated_at_ms: read_number(item, "updatedAtMs").unwrap_or(0),
        published_at_ms: read_number::<i64>(item, "publishedAtMs"),
        // Absent on every item written before the counters existed, which is
        // the same thing as none counted yet.
        owner_count: read_number(item, "ownerCount").unwrap_or(0),
        wearer_count: read_number(item, "wearerCount").unwrap_or(0),
    })
}

/// Build the attribute map for one revision.
pub fn revision_item(
    skin_id: i32,
    revision: u32,
    new: &NewRevision<'_>,
    created_at_ms: i64,
) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), string(skin_partition(skin_id)));
    item.insert("sk".to_string(), string(revision_sort_key(revision)));
    item.insert("skinId".to_string(), number(skin_id));
    item.insert("revision".to_string(), number(revision));
    item.insert("contentRef".to_string(), string(new.content_ref));
    item.insert("document".to_string(), string(new.document));
    if !new.texture_refs.is_empty() {
        item.insert(
            "textureRefs".to_string(),
            AttributeValue::Ss(new.texture_refs.to_vec()),
        );
    }
    item.insert("validatedSchema".to_string(), number(new.validated_schema));
    item.insert("reviewApproved".to_string(), AttributeValue::Bool(false));
    item.insert(
        "containsText".to_string(),
        AttributeValue::Bool(new.contains_text),
    );
    item.insert("createdAtMs".to_string(), number(created_at_ms));
    item.insert(
        "gsi1pk".to_string(),
        string(content_ref_index_partition(new.content_ref)),
    );
    // Several skins may intentionally contain identical canonical document
    // bytes. Include both identities so the index order is total and stable;
    // the read path still evaluates every candidate's visibility rather than
    // treating the first row as the document's moderation state.
    item.insert(
        "gsi1sk".to_string(),
        string(format!(
            "SKIN#{skin_id:010}#{}",
            revision_sort_key(revision)
        )),
    );
    item
}

pub fn revision_from_item(item: &HashMap<String, AttributeValue>) -> Result<SkinRevision> {
    let document = read_string(item, "document").unwrap_or_default();
    let contains_text = match item.get("containsText") {
        Some(AttributeValue::Bool(value)) => *value,
        _ => crate::skin_store::document_contains_authored_text(&document),
    };
    Ok(SkinRevision {
        skin_id: read_number(item, "skinId").ok_or_else(|| anyhow!("revision has no skin id"))?,
        revision: read_number(item, "revision").ok_or_else(|| anyhow!("revision has no number"))?,
        content_ref: read_string(item, "contentRef")
            .ok_or_else(|| anyhow!("revision has no content reference"))?,
        document,
        texture_refs: match item.get("textureRefs") {
            Some(AttributeValue::Ss(refs)) => {
                let mut refs = refs.clone();
                refs.sort();
                refs.dedup();
                refs
            }
            _ => Vec::new(),
        },
        validated_schema: read_number(item, "validatedSchema").unwrap_or(1),
        exposed_at_ms: read_number::<i64>(item, "exposedAtMs"),
        review_approved: matches!(item.get("reviewApproved"), Some(AttributeValue::Bool(true))),
        review_rejected: matches!(item.get("reviewRejected"), Some(AttributeValue::Bool(true))),
        contains_text,
        created_at_ms: read_number(item, "createdAtMs").unwrap_or(0),
    })
}

pub fn grant_item(
    user_id: i32,
    skin_id: i32,
    source: GrantSource,
    price_paid_bux: u32,
    acquired_at_ms: i64,
) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), string(format!("USER#{user_id}")));
    item.insert("sk".to_string(), string(grant_sort_key(skin_id)));
    item.insert("skinId".to_string(), number(skin_id));
    item.insert("source".to_string(), string(source.as_str()));
    item.insert("pricePaidBux".to_string(), number(price_paid_bux));
    item.insert("acquiredAtMs".to_string(), number(acquired_at_ms));
    item
}

pub fn grant_from_item(item: &HashMap<String, AttributeValue>) -> Option<SkinGrant> {
    Some(SkinGrant {
        skin_id: read_number(item, "skinId")?,
        acquired_at_ms: read_number(item, "acquiredAtMs").unwrap_or(0),
        price_paid_bux: read_number(item, "pricePaidBux").unwrap_or(0),
        source: read_string(item, "source")
            .and_then(|value| GrantSource::parse(&value))
            .unwrap_or(GrantSource::Grant),
    })
}

/// Timestamps as fixed-width strings, so a sort key orders by time.
///
/// Milliseconds since the epoch fit in 13 digits until the year 33658; the
/// width is fixed at 20 anyway, because a key whose length changes sorts wrong
/// exactly once and then confusingly forever.
fn sortable(ms: i64) -> String {
    format!("{ms:020}")
}

fn string(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}

fn number(value: impl ToString) -> AttributeValue {
    AttributeValue::N(value.to_string())
}

fn read_string(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
    match item.get(key) {
        Some(AttributeValue::S(value)) => Some(value.clone()),
        _ => None,
    }
}

fn read_number<T: std::str::FromStr>(
    item: &HashMap<String, AttributeValue>,
    key: &str,
) -> Option<T> {
    match item.get(key) {
        Some(AttributeValue::N(value)) => value.parse().ok(),
        _ => None,
    }
}

/// Encode a pagination cursor. DynamoDB hands back a whole key map; the API
/// only ever resumes one index at a time, so the sort key alone is enough and
/// is far less to hand a client.
pub fn encode_cursor(key: &HashMap<String, AttributeValue>) -> Option<String> {
    let parts: Vec<String> = ["pk", "sk", "gsi1pk", "gsi1sk", "gsi2pk", "gsi2sk"]
        .iter()
        .filter_map(|name| read_string(key, name).map(|value| format!("{name}={value}")))
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("|"))
    }
}

/// Decode a cursor back into an exclusive start key, ignoring anything
/// malformed: a bad cursor restarts the listing rather than failing it.
pub fn decode_cursor(cursor: &str) -> Option<HashMap<String, AttributeValue>> {
    let mut key = HashMap::new();
    for part in cursor.split('|') {
        let (name, value) = part.split_once('=')?;
        if !matches!(
            name,
            "pk" | "sk" | "gsi1pk" | "gsi1sk" | "gsi2pk" | "gsi2sk"
        ) {
            return None;
        }
        key.insert(name.to_string(), string(value));
    }
    if key.is_empty() { None } else { Some(key) }
}

pub fn context_for(what: &str) -> String {
    format!("Failed to {what}")
}

pub type ItemMap = HashMap<String, AttributeValue>;

pub fn ensure_ok<T>(value: Option<T>, what: &str) -> Result<T> {
    value.with_context(|| context_for(what))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_rows_persist_texture_and_text_gates_and_have_total_index_order() {
        let refs = vec![
            format!("sha256:{}", "b".repeat(64)),
            format!("sha256:{}", "a".repeat(64)),
        ];
        let document = "{}";
        let content_ref = skin_schema::content::reference_for_bytes(document.as_bytes());
        let new = NewRevision {
            document,
            content_ref: &content_ref,
            texture_refs: &refs,
            validated_schema: 2,
            contains_text: true,
        };
        let item = revision_item(17, 3, &new, 100);
        let stored = revision_from_item(&item).expect("round trips");
        let mut expected_refs = refs;
        expected_refs.sort();
        assert_eq!(stored.texture_refs, expected_refs);
        assert!(stored.contains_text);
        assert!(!stored.review_approved);
        assert!(!stored.review_rejected);
        assert_eq!(
            read_string(&item, "gsi1sk").as_deref(),
            Some("SKIN#0000000017#REV#000003")
        );

        let mut legacy = item;
        legacy.remove("containsText");
        assert!(
            revision_from_item(&legacy)
                .expect("legacy row reads")
                .contains_text,
            "an absent gate on malformed legacy bytes must fail closed"
        );
    }

    #[test]
    fn retry_keys_and_review_audits_are_stable_and_payload_bound() {
        assert_eq!(
            create_idempotency_sort_key("concept-1"),
            create_idempotency_sort_key("concept-1")
        );
        assert_ne!(
            create_idempotency_sort_key("concept-1"),
            create_idempotency_sort_key("concept-2")
        );
        let first = review_decision_sort_key(
            Publication::Published,
            Some(4),
            Some("sha256:abc"),
            9,
            Some("approved"),
        );
        assert_eq!(
            first,
            review_decision_sort_key(
                Publication::Published,
                Some(4),
                Some("sha256:abc"),
                9,
                Some("approved"),
            )
        );
        assert_ne!(
            first,
            review_decision_sort_key(
                Publication::Published,
                Some(4),
                Some("sha256:different"),
                9,
                Some("approved"),
            )
        );
    }
}
