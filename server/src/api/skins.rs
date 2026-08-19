//! Browsing the catalogue, and recording what a player is wearing.
//!
//! Until this module existed, a player's skin choice lived only in their own
//! browser: `User.selected_skin` was read at match preparation and written
//! nowhere, so every remote player rendered as classic no matter what its owner
//! had picked. These two routes close that gap — the catalogue is browsable
//! without an account, and equipping writes the choice somewhere the match
//! preparation path can actually find it.
//!
//! Equipping validates against the catalogue rather than trusting the client,
//! for the reason `crate::skin_catalog` exists: a reference travels into other
//! players' renderers, so the server decides what is real before it can travel.

use axum::{
    Extension, Json,
    extract::{Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::api::auth::AuthState;
use crate::api::middleware::AuthUser;
use crate::skin_catalog::{self, BASE_REF_PREFIX, CatalogEntry, MAX_SKIN_REF_LENGTH, SkinKind};

/// How long a browser may reuse the catalogue. Built-ins change only when the
/// server is redeployed, so this is generous without being able to strand a
/// player on a stale list for long.
const CATALOG_CACHE_SECONDS: u32 = 300;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowseQuery {
    /// Which slot to list. Absent means snake skins, the main column.
    #[serde(default)]
    pub kind: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct BrowseResponse {
    pub skins: Vec<CatalogEntry>,
}

/// What a player is wearing, as the client needs to see it.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Equipment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_skin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_base: Option<String>,
}

/// An equip request.
///
/// Each slot is three-valued and the encoding says so: an absent field leaves
/// that slot alone, an explicit `null` clears it back to the default look, and
/// a string equips. Collapsing absent and null would make "equip a skin" and
/// "clear my base" the same request.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EquipRequest {
    #[serde(default, deserialize_with = "double_option")]
    pub selected_skin: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub selected_base: Option<Option<String>>,
}

fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug)]
pub enum SkinsApiError {
    /// The reference does not name anything this build can draw. Unlike match
    /// preparation, which quietly falls back to classic rather than refusing a
    /// join, an explicit equip is worth an error: the player is looking at the
    /// result and deserves to know it did not take.
    UnknownSkin(String),
    Internal(anyhow::Error),
}

impl IntoResponse for SkinsApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::UnknownSkin(reference) => (
                StatusCode::BAD_REQUEST,
                format!("{reference} is not a skin this server knows"),
            ),
            Self::Internal(error) => {
                error!(?error, "skins API error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
            }
        };
        let mut response = (status, Json(serde_json::json!({ "error": message }))).into_response();
        no_store(&mut response);
        response
    }
}

/// The catalogue, for anyone — browsing needs no account.
pub async fn browse(Query(query): Query<BrowseQuery>) -> Response {
    let skins = match query.kind.as_deref() {
        Some("base") => skin_catalog::base_catalog(),
        _ => skin_catalog::CATALOG.to_vec(),
    };

    let mut response = Json(BrowseResponse { skins }).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_str(&format!("public, max-age={CATALOG_CACHE_SECONDS}"))
            .expect("a formatted cache-control header is always valid"),
    );
    response
}

/// Record what the authenticated player is wearing.
pub async fn set_equipment(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Json(request): Json<EquipRequest>,
) -> Result<Response, SkinsApiError> {
    let snake = validate_slot(request.selected_skin.as_ref(), SkinKind::Snake)?;
    let base = validate_slot(request.selected_base.as_ref(), SkinKind::Base)?;

    state
        .db
        .set_user_equipment(auth_user.user_id, snake, base)
        .await
        .map_err(SkinsApiError::Internal)?;

    // Report the whole slot set back rather than echoing the request, so a
    // client that only sent one slot still learns the state it is now in.
    let user = state
        .db
        .get_user_by_id(auth_user.user_id)
        .await
        .map_err(SkinsApiError::Internal)?;
    let equipment = user
        .map(|user| Equipment {
            selected_skin: user.selected_skin,
            selected_base: user.selected_base,
        })
        .unwrap_or_default();

    let mut response = Json(equipment).into_response();
    no_store(&mut response);
    Ok(response)
}

/// Turn one requested slot into the storage layer's three-valued form,
/// refusing anything the catalogue does not contain.
fn validate_slot(
    requested: Option<&Option<String>>,
    kind: SkinKind,
) -> Result<Option<Option<&'static str>>, SkinsApiError> {
    let Some(slot) = requested else {
        return Ok(None);
    };
    let Some(reference) = slot else {
        return Ok(Some(None));
    };

    let trimmed = reference.trim();
    if trimmed.is_empty() {
        return Ok(Some(None));
    }
    if trimmed.len() > MAX_SKIN_REF_LENGTH {
        return Err(SkinsApiError::UnknownSkin(format!(
            "a {}-character reference",
            trimmed.len()
        )));
    }

    // Resolve to the catalogue's own `&'static str` rather than storing the
    // caller's bytes: what lands in the database is then always a reference
    // this build compiled, never merely one that compared equal to it.
    let known = match kind {
        SkinKind::Snake => skin_catalog::CATALOG
            .iter()
            .find(|entry| entry.reference == trimmed)
            .map(|entry| entry.reference),
        SkinKind::Base => trimmed
            .strip_prefix(BASE_REF_PREFIX)
            .and_then(|inner| {
                skin_catalog::CATALOG
                    .iter()
                    .find(|entry| entry.reference == inner)
            })
            .map(|entry| base_reference(entry.reference)),
    };

    known
        .map(|reference| Some(Some(reference)))
        .ok_or_else(|| SkinsApiError::UnknownSkin(trimmed.to_string()))
}

/// The stored form of a base reference.
///
/// Bases are snake references wearing a prefix, and the set is small and
/// compile-time known, so the prefixed form can be a `&'static str` too rather
/// than a string allocated per request.
fn base_reference(snake_reference: &'static str) -> &'static str {
    static PREFIXED: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    let prefixed = PREFIXED.get_or_init(|| {
        skin_catalog::CATALOG
            .iter()
            .map(|entry| format!("{BASE_REF_PREFIX}{}", entry.reference))
            .collect()
    });
    let index = skin_catalog::CATALOG
        .iter()
        .position(|entry| entry.reference == snake_reference)
        .expect("callers pass a reference they just found in the catalogue");
    prefixed[index].as_str()
}

fn no_store(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn equip(json: &str) -> EquipRequest {
        serde_json::from_str(json).expect("valid equip request")
    }

    /// The three-valued encoding is the whole point of the request type: these
    /// three bodies must mean three different things.
    #[test]
    fn absent_null_and_present_are_three_distinct_requests() {
        let untouched = equip(r#"{"selectedBase":"base:aurora@1"}"#);
        assert_eq!(
            untouched.selected_skin, None,
            "absent leaves the slot alone"
        );

        let cleared = equip(r#"{"selectedSkin":null}"#);
        assert_eq!(cleared.selected_skin, Some(None), "null clears the slot");

        let equipped = equip(r#"{"selectedSkin":"aurora@1"}"#);
        assert_eq!(equipped.selected_skin, Some(Some("aurora@1".to_string())));
    }

    #[test]
    fn an_empty_request_touches_nothing() {
        let request = equip("{}");
        assert_eq!(
            validate_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap(),
            None
        );
        assert_eq!(
            validate_slot(request.selected_base.as_ref(), SkinKind::Base).unwrap(),
            None
        );
    }

    #[test]
    fn a_known_skin_resolves_to_the_catalogues_own_string() {
        let request = equip(r#"{"selectedSkin":"  aurora@1  "}"#);
        let resolved = validate_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap();
        assert_eq!(resolved, Some(Some("aurora@1")));
    }

    #[test]
    fn a_base_slot_only_accepts_prefixed_references() {
        let prefixed = equip(r#"{"selectedBase":"base:tidewave@1"}"#);
        assert_eq!(
            validate_slot(prefixed.selected_base.as_ref(), SkinKind::Base).unwrap(),
            Some(Some("base:tidewave@1"))
        );

        let bare = equip(r#"{"selectedBase":"tidewave@1"}"#);
        assert!(
            validate_slot(bare.selected_base.as_ref(), SkinKind::Base).is_err(),
            "a bare snake reference is not a base"
        );
    }

    /// Match preparation forgives an unknown reference because refusing a join
    /// over cosmetics is worse than a wrong-looking snake. An explicit equip is
    /// the opposite case: nobody is mid-join, and silence would leave the player
    /// believing a choice took when it did not.
    #[test]
    fn an_unknown_skin_is_refused_rather_than_silently_defaulted() {
        for body in [
            r#"{"selectedSkin":"nonesuch@9"}"#,
            r#"{"selectedSkin":"../../etc/passwd"}"#,
            r#"{"selectedSkin":"<script>alert(1)</script>"}"#,
            r#"{"selectedSkin":"sha256:0000000000000000000000000000000000000000000000000000000000000000"}"#,
        ] {
            let request = equip(body);
            assert!(
                validate_slot(request.selected_skin.as_ref(), SkinKind::Snake).is_err(),
                "{body} should have been refused"
            );
        }
    }

    #[test]
    fn an_over_long_reference_is_refused_without_being_echoed_back() {
        let long = "a".repeat(MAX_SKIN_REF_LENGTH + 1);
        let request = equip(&format!(r#"{{"selectedSkin":"{long}"}}"#));
        let error = validate_slot(request.selected_skin.as_ref(), SkinKind::Snake)
            .expect_err("an over-long reference is not a skin");
        let SkinsApiError::UnknownSkin(message) = error else {
            panic!("expected an unknown-skin error");
        };
        assert!(
            !message.contains(&long),
            "an over-long reference must not be reflected back into the response"
        );
    }

    #[test]
    fn whitespace_only_is_treated_as_clearing_the_slot() {
        let request = equip(r#"{"selectedSkin":"   "}"#);
        assert_eq!(
            validate_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap(),
            Some(None)
        );
    }

    #[test]
    fn unknown_fields_are_refused_so_a_typo_is_not_silently_ignored() {
        assert!(serde_json::from_str::<EquipRequest>(r#"{"selectedSkim":"aurora@1"}"#).is_err());
    }
}
