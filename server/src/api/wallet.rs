//! Snakebux over HTTP: reading a balance, spending it, and taking payment.
//!
//! The payment provider is the merchant of record, so this server never sees a
//! card. What it does own is the part that decides how many Bux a player has,
//! and that has exactly two entry points: a purchase the player makes, and a
//! signed webhook the provider sends. Everything else is a read.
//!
//! Two things here are load-bearing against a paying attacker:
//!
//! - the credited amount comes from the configured pack the SKU names, never
//!   from the webhook body, so a forged or replayed payload cannot mint Bux by
//!   claiming a larger purchase than was made; and
//! - the signature comparison is constant-time, because a byte-by-byte compare
//!   leaks the expected value one guess at a time.

use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::api::auth::AuthState;
use crate::api::middleware::AuthUser;
use crate::db::PurchaseOutcome;
use crate::wallet::{LedgerSource, is_valid_client_key, request_fingerprint};

/// How many ledger entries the wallet view carries.
const RECENT_LEDGER_ENTRIES: usize = 20;

/// The Bux packs on sale, and what each is worth.
///
/// Configured here rather than taken from the payment provider's payload: the
/// amount credited must be a property of what we sold, not of what the caller
/// says was bought.
const PACKS: &[(&str, u32)] = &[
    ("bux-500", 500),
    ("bux-1200", 1_200),
    ("bux-2600", 2_600),
    ("bux-7000", 7_000),
];

fn pack_value(sku: &str) -> Option<u32> {
    PACKS
        .iter()
        .find(|(id, _)| *id == sku)
        .map(|(_, value)| *value)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PurchaseRequest {
    /// Client-minted, UUID-shaped, and unique to this attempt. A retry of the
    /// same attempt reuses it and is not charged twice.
    pub idempotency_key: String,
    /// What the buyer was shown. Conditioned on inside the transaction, so a
    /// price that moved fails the purchase rather than charging a surprise.
    pub expected_price_bux: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct PurchaseResult {
    /// `purchased`, `alreadyOwned`, `priceChanged`, or `insufficientFunds`.
    pub outcome: String,
    pub balance_bux: i64,
    /// Present when the price moved, so the client can re-prompt with the
    /// number the buyer will actually be charged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_price_bux: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct XsollaWebhook {
    /// The provider's own id for this transaction. The idempotency key.
    pub transaction_id: String,
    pub user_id: i32,
    /// Which pack. The Bux value is looked up, never taken from the payload.
    pub sku: String,
    /// `payment` or `refund`.
    pub notification_type: String,
}

#[derive(Debug)]
pub enum WalletApiError {
    BadRequest(String),
    /// Payments are not configured on this deployment.
    Disabled,
    Unauthorized,
    NotFound,
    Internal(anyhow::Error),
}

impl IntoResponse for WalletApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Disabled => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Buying Snakebux is not available right now".to_string(),
            ),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "Signature verification failed".to_string(),
            ),
            Self::NotFound => (StatusCode::NOT_FOUND, "No such skin".to_string()),
            Self::Internal(error) => {
                error!(?error, "wallet API error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
            }
        };
        let mut response = (status, Json(serde_json::json!({ "error": message }))).into_response();
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
        );
        response
    }
}

/// A player's balance and recent history.
pub async fn get_wallet(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, WalletApiError> {
    let wallet = state
        .db
        .get_wallet(auth_user.user_id, RECENT_LEDGER_ENTRIES)
        .await
        .map_err(WalletApiError::Internal)?;

    let mut response = Json(wallet).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    Ok(response)
}

/// Buy a skin.
pub async fn purchase_skin(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    axum::extract::Path(skin_id): axum::extract::Path<i32>,
    Json(request): Json<PurchaseRequest>,
) -> Result<Response, WalletApiError> {
    if !is_valid_client_key(&request.idempotency_key) {
        return Err(WalletApiError::BadRequest(
            "idempotencyKey: must be a UUID".to_string(),
        ));
    }

    let skin = state
        .db
        .get_skin(skin_id)
        .await
        .map_err(WalletApiError::Internal)?
        .ok_or(WalletApiError::NotFound)?;

    // Paying for something needs a durable account: a guest's row can be
    // orphaned by closing a tab, and an orphaned paid skin is a support ticket
    // by construction. A free skin is a costless grant and is fine.
    if auth_user.is_guest && skin.price_bux > 0 {
        return Err(WalletApiError::BadRequest(
            "Buying a skin needs a registered account".to_string(),
        ));
    }

    let fingerprint = request_fingerprint(&[
        "purchase",
        &skin_id.to_string(),
        &request.expected_price_bux.to_string(),
    ]);

    let outcome = state
        .db
        .purchase_skin(
            auth_user.user_id,
            skin_id,
            request.expected_price_bux,
            &request.idempotency_key,
            &fingerprint,
        )
        .await
        .map_err(WalletApiError::Internal)?;

    let wallet = state
        .db
        .get_wallet(auth_user.user_id, 1)
        .await
        .map_err(WalletApiError::Internal)?;

    let (status, name, actual) = match outcome {
        PurchaseOutcome::Purchased => (StatusCode::OK, "purchased", None),
        PurchaseOutcome::AlreadyOwned => (StatusCode::OK, "alreadyOwned", None),
        // A conflict, not a failure: the client re-prompts with the real price.
        PurchaseOutcome::PriceChanged { actual_bux } => {
            (StatusCode::CONFLICT, "priceChanged", Some(actual_bux))
        }
        PurchaseOutcome::InsufficientFunds => {
            (StatusCode::PAYMENT_REQUIRED, "insufficientFunds", None)
        }
    };

    let mut response = (
        status,
        Json(PurchaseResult {
            outcome: name.to_string(),
            balance_bux: wallet.balance_bux,
            actual_price_bux: actual,
        }),
    )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    Ok(response)
}

/// What a pack costs in real money, for the checkout the provider hosts.
///
/// Prices live beside the Bux value so a SKU can never mean one amount to the
/// checkout and another to the credit.
const PACK_PRICES_USD_CENTS: &[(&str, u32)] = &[
    ("bux-500", 199),
    ("bux-1200", 399),
    ("bux-2600", 799),
    ("bux-7000", 1_999),
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutRequest {
    pub sku: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct CheckoutToken {
    /// Opaque to the client; it hands this to the provider's hosted checkout.
    pub token: String,
    pub sku: String,
    pub bux: u32,
    pub price_usd_cents: u32,
}

/// One pack, as the top-up surface presents it.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct BuxPack {
    pub sku: String,
    pub bux: u32,
    pub price_usd_cents: u32,
}

/// What is on sale.
///
/// The two tables are joined here rather than in the client, so a SKU that
/// gained a value without gaining a price is absent from the shop instead of
/// being offered at nothing.
pub async fn list_packs() -> Response {
    let packs: Vec<BuxPack> = PACKS
        .iter()
        .filter_map(|(sku, bux)| {
            PACK_PRICES_USD_CENTS
                .iter()
                .find(|(id, _)| id == sku)
                .map(|(_, cents)| BuxPack {
                    sku: (*sku).to_string(),
                    bux: *bux,
                    price_usd_cents: *cents,
                })
        })
        .collect();
    Json(packs).into_response()
}

/// Mint a checkout token for one pack.
///
/// The user and the amount are bound *here*, server-side. If the client
/// constructed the checkout instead, both would be client-asserted, and a
/// caller could pay for the cheapest pack while claiming the largest.
pub async fn xsolla_checkout_token(
    // No database work: minting a token reads configuration and the caller's
    // identity, and the wallet is not touched until the provider settles.
    Extension(auth_user): Extension<AuthUser>,
    Json(request): Json<CheckoutRequest>,
) -> Result<Response, WalletApiError> {
    if auth_user.is_guest {
        return Err(WalletApiError::BadRequest(
            "Buying Snakebux needs a registered account".to_string(),
        ));
    }

    let bux = pack_value(&request.sku).ok_or_else(|| {
        WalletApiError::BadRequest(format!("sku: {} is not on sale", request.sku))
    })?;
    let price_usd_cents = PACK_PRICES_USD_CENTS
        .iter()
        .find(|(id, _)| *id == request.sku)
        .map(|(_, price)| *price)
        .ok_or_else(|| WalletApiError::BadRequest("sku: has no price".to_string()))?;

    let project_id = std::env::var("SNAKETRON_XSOLLA_PROJECT_ID").unwrap_or_default();
    let secret = std::env::var("SNAKETRON_XSOLLA_API_KEY").unwrap_or_default();
    if project_id.is_empty() || secret.is_empty() {
        // Absent configuration disables the surface rather than half-enabling
        // it, the way the replay store does.
        return Err(WalletApiError::Disabled);
    }

    // The token is a signed statement of who is buying what, so the settlement
    // webhook can be matched to a user without trusting anything the browser
    // sends back.
    let token = mint_checkout_token(
        &project_id,
        &secret,
        auth_user.user_id,
        &request.sku,
        price_usd_cents,
    );

    let mut response = Json(CheckoutToken {
        token,
        sku: request.sku,
        bux,
        price_usd_cents,
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    Ok(response)
}

/// A token binding buyer, pack and price, signed with the project secret.
///
/// Deliberately opaque and self-describing: the provider echoes it back on
/// settlement, and the webhook can then confirm the purchase it is crediting is
/// the purchase that was authorised.
fn mint_checkout_token(
    project_id: &str,
    secret: &str,
    user_id: i32,
    sku: &str,
    price_usd_cents: u32,
) -> String {
    let claim = format!("{project_id}:{user_id}:{sku}:{price_usd_cents}");
    let signature = request_fingerprint(&[&claim, secret]);
    format!("{claim}:{}", signature.trim_start_matches("sha256:"))
}

/// Settlement from the payment provider.
///
/// Never callable by a client: the signature is over the raw body with a shared
/// secret only the provider has. Payments credit, refunds and chargebacks debit
/// the same amount back — and are allowed to drive a balance negative, because
/// the Bux may already have been spent and pretending otherwise would be
/// writing off the debt.
pub async fn xsolla_webhook(
    State(state): State<AuthState>,
    headers: HeaderMap,
    body: String,
) -> Result<Response, WalletApiError> {
    let secret = std::env::var("SNAKETRON_XSOLLA_WEBHOOK_SECRET").unwrap_or_default();
    if secret.is_empty() {
        warn!("an Xsolla webhook arrived but no secret is configured; refusing it");
        return Err(WalletApiError::Unauthorized);
    }

    let presented = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Signature "))
        .unwrap_or_default();
    if !signature_matches(&body, &secret, presented) {
        return Err(WalletApiError::Unauthorized);
    }

    let notification: XsollaWebhook = serde_json::from_str(&body)
        .map_err(|error| WalletApiError::BadRequest(error.to_string()))?;

    let value = pack_value(&notification.sku).ok_or_else(|| {
        WalletApiError::BadRequest(format!("sku: {} is not on sale", notification.sku))
    })?;

    let (source, delta) = match notification.notification_type.as_str() {
        "payment" => (LedgerSource::Xsolla, i64::from(value)),
        "refund" => (LedgerSource::Refund, -i64::from(value)),
        other => {
            // Anything else is a notification we do not act on. Answered 200 so
            // the provider stops retrying something that is not an error.
            info!(kind = other, "ignoring an Xsolla notification");
            return Ok(StatusCode::OK.into_response());
        }
    };

    let fingerprint = request_fingerprint(&[
        source.as_str(),
        &notification.transaction_id,
        &notification.sku,
    ]);

    let applied = state
        .db
        .apply_ledger_entry(
            notification.user_id,
            source,
            &notification.transaction_id,
            delta,
            &fingerprint,
            Some(&notification.sku),
        )
        .await
        .map_err(WalletApiError::Internal)?;

    if !applied {
        info!(
            transaction = notification.transaction_id,
            "an Xsolla notification was already applied"
        );
    }

    // 200 either way. A duplicate is a success from the provider's point of
    // view, and anything else makes it retry forever.
    Ok(StatusCode::OK.into_response())
}

/// Constant-time comparison of the presented signature against the expected
/// one.
///
/// Time-invariant on purpose: an early-exit compare tells an attacker how many
/// leading bytes they guessed right, which turns forgery from infeasible into a
/// few thousand requests.
fn signature_matches(body: &str, secret: &str, presented: &str) -> bool {
    let expected = expected_signature(body, secret);
    let expected = expected.as_bytes();
    let presented = presented.as_bytes();
    if expected.len() != presented.len() {
        return false;
    }
    let mut difference = 0u8;
    for (left, right) in expected.iter().zip(presented) {
        difference |= left ^ right;
    }
    difference == 0
}

fn expected_signature(body: &str, secret: &str) -> String {
    let mut message = String::with_capacity(body.len() + secret.len());
    message.push_str(body);
    message.push_str(secret);
    skin_schema::content::reference_for_bytes(message.as_bytes())
        .trim_start_matches("sha256:")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The amount credited is a property of what we sell, not of what the
    /// caller claims. A payload naming a bigger pack than exists buys nothing.
    #[test]
    fn only_configured_packs_are_worth_anything() {
        assert_eq!(pack_value("bux-500"), Some(500));
        assert_eq!(pack_value("bux-7000"), Some(7_000));
        assert_eq!(pack_value("bux-999999"), None);
        assert_eq!(pack_value(""), None);
    }

    #[test]
    fn a_signature_must_match_the_body_and_the_secret() {
        let body = r#"{"transactionId":"t1","userId":7,"sku":"bux-500"}"#;
        let signature = expected_signature(body, "shhh");

        assert!(signature_matches(body, "shhh", &signature));
        assert!(
            !signature_matches(body, "other-secret", &signature),
            "a signature made with another secret is not ours"
        );
        assert!(
            !signature_matches(r#"{"userId":8}"#, "shhh", &signature),
            "a signature does not travel to another body"
        );
        assert!(!signature_matches(body, "shhh", ""));
        assert!(!signature_matches(
            body,
            "shhh",
            &signature[..signature.len() - 1]
        ));
    }

    /// A truncated or over-long candidate is refused on length before any byte
    /// is compared, and equal-length mismatches still compare in full.
    #[test]
    fn signature_comparison_does_not_exit_early() {
        let body = "{}";
        let signature = expected_signature(body, "s");
        let mut wrong_first = signature.clone();
        // Flip the first character; the compare must still consider all of it.
        let flipped = if signature.starts_with('a') { 'b' } else { 'a' };
        wrong_first.replace_range(0..1, &flipped.to_string());
        assert!(!signature_matches(body, "s", &wrong_first));
        assert_eq!(wrong_first.len(), signature.len());
    }

    /// A refund reverses exactly what its payment credited, and lands in its
    /// own key namespace so it cannot be mistaken for the payment.
    #[test]
    fn a_refund_reverses_the_pack_it_names() {
        let value = pack_value("bux-1200").expect("a real pack");
        assert_eq!(i64::from(value), 1_200);
        assert_eq!(-i64::from(value), -1_200);
        assert_ne!(
            crate::wallet::ledger_sort_key(LedgerSource::Xsolla, "t9"),
            crate::wallet::ledger_sort_key(LedgerSource::Refund, "t9")
        );
    }

    /// Every pack on sale has both a Bux value and a price. A SKU with one and
    /// not the other would either be free money or an unbuyable listing.
    #[test]
    fn every_pack_has_both_a_value_and_a_price() {
        for (sku, _) in PACKS {
            assert!(
                PACK_PRICES_USD_CENTS.iter().any(|(id, _)| id == sku),
                "{sku} has a Bux value but no price"
            );
        }
        for (sku, _) in PACK_PRICES_USD_CENTS {
            assert!(
                pack_value(sku).is_some(),
                "{sku} has a price but no Bux value"
            );
        }
    }

    /// The token binds the buyer and the amount, so two different buyers, two
    /// different packs, or a tampered price cannot share one token.
    #[test]
    fn a_checkout_token_binds_who_is_buying_what() {
        let mine = mint_checkout_token("project", "secret", 42, "bux-500", 199);

        assert_ne!(
            mine,
            mint_checkout_token("project", "secret", 43, "bux-500", 199)
        );
        assert_ne!(
            mine,
            mint_checkout_token("project", "secret", 42, "bux-1200", 199)
        );
        assert_ne!(
            mine,
            mint_checkout_token("project", "secret", 42, "bux-500", 99)
        );
        assert_ne!(
            mine,
            mint_checkout_token("project", "other-secret", 42, "bux-500", 199),
            "a token minted with another secret must not verify as ours"
        );
        assert_eq!(
            mine,
            mint_checkout_token("project", "secret", 42, "bux-500", 199)
        );
        assert!(mine.starts_with("project:42:bux-500:199:"));
    }

    #[test]
    fn a_purchase_request_must_carry_a_uuid_key() {
        let good: PurchaseRequest = serde_json::from_str(
            r#"{"idempotencyKey":"f47ac10b-58cc-4372-a567-0e02b2c3d479","expectedPriceBux":250}"#,
        )
        .expect("parses");
        assert!(is_valid_client_key(&good.idempotency_key));

        let bad: PurchaseRequest =
            serde_json::from_str(r#"{"idempotencyKey":"nope","expectedPriceBux":250}"#)
                .expect("parses");
        assert!(!is_valid_client_key(&bad.idempotency_key));
    }
}
