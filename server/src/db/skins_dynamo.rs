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
    GrantSource, NewRevision, Publication, Skin, SkinGrant, SkinKind, SkinRevision,
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

/// Build the attribute map for a skin's META item.
pub fn skin_item(skin: &Skin) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), string(skin_partition(skin.skin_id)));
    item.insert("sk".to_string(), string("META"));
    item.insert("skinId".to_string(), number(skin.skin_id));
    item.insert("kind".to_string(), string(skin.kind.as_str()));
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
    if skin.publication.is_browsable() {
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
    item.insert("createdAtMs".to_string(), number(created_at_ms));
    item.insert(
        "gsi1pk".to_string(),
        string(content_ref_index_partition(new.content_ref)),
    );
    item.insert("gsi1sk".to_string(), string(revision_sort_key(revision)));
    item
}

pub fn revision_from_item(item: &HashMap<String, AttributeValue>) -> Result<SkinRevision> {
    Ok(SkinRevision {
        skin_id: read_number(item, "skinId").ok_or_else(|| anyhow!("revision has no skin id"))?,
        revision: read_number(item, "revision").ok_or_else(|| anyhow!("revision has no number"))?,
        content_ref: read_string(item, "contentRef")
            .ok_or_else(|| anyhow!("revision has no content reference"))?,
        document: read_string(item, "document").unwrap_or_default(),
        texture_refs: match item.get("textureRefs") {
            Some(AttributeValue::Ss(refs)) => refs.clone(),
            _ => Vec::new(),
        },
        validated_schema: read_number(item, "validatedSchema").unwrap_or(1),
        exposed_at_ms: read_number::<i64>(item, "exposedAtMs"),
        review_approved: matches!(item.get("reviewApproved"), Some(AttributeValue::Bool(true))),
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
