//! Snakebux over HTTP: reading a balance, spending it, and taking payment.
//!
//! The payment provider is the merchant of record, so this server never sees a
//! card. What it does own is the part that decides how many Bux a player has,
//! and that has exactly two entry points: a purchase the player makes, and a
//! signed webhook the provider sends. Everything else is a read.
//!
//! Four things here are load-bearing against a paying attacker:
//!
//! - the checkout token is minted by Xsolla, server-to-server, from our
//!   merchant credentials — a token this process invented would be a string
//!   Pay Station has never issued;
//! - the credited amount comes from the configured pack the SKU names, never
//!   from the webhook body, so a forged or replayed payload cannot mint Bux by
//!   claiming a larger purchase than was made;
//! - a reversal gives back exactly what its payment credited, read from the
//!   ledger row that payment wrote, so editing the pack table cannot change
//!   what an old refund takes away; and
//! - signature verification is constant-time and lives in [`crate::xsolla`],
//!   because a byte-by-byte compare leaks the expected value one guess at a
//!   time.
//!
//! Purchases are also gated on distribution. CrazyGames prohibits exposing
//! in-app purchases without portal approval (`CRAZYGAMES.md`), so the portal
//! build is served an empty shop and a refusal rather than a checkout.

use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::ads::ClientDistribution;
use crate::api::auth::AuthState;
use crate::api::middleware::AuthUser;
use crate::db::PurchaseOutcome;
use crate::wallet::{LedgerSource, is_valid_client_key, request_fingerprint};
use crate::xsolla::{self, Notification, NotificationError, Settlement};

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

#[derive(Debug)]
pub enum WalletApiError {
    BadRequest(String),
    /// Payments are not configured on this deployment.
    Disabled,
    /// Payments exist but are not offered on this distribution.
    Forbidden(String),
    Unauthorized,
    NotFound,
    /// The provider could not be reached, or refused us. Distinct from an
    /// internal fault because the caller may usefully retry, and because a
    /// misconfigured merchant account looks nothing like a bug in this server.
    Provider(anyhow::Error),
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
            Self::Forbidden(message) => (StatusCode::FORBIDDEN, message),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "Signature verification failed".to_string(),
            ),
            Self::NotFound => (StatusCode::NOT_FOUND, "No such skin".to_string()),
            Self::Provider(error) => {
                error!(?error, "the payment provider could not mint a checkout");
                (
                    StatusCode::BAD_GATEWAY,
                    "The payment provider is not responding. Try again in a moment.".to_string(),
                )
            }
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
        // Also a conflict rather than a failure: the skin was on sale when the
        // page was drawn and is not now. Deliberately not 402 — telling a
        // solvent buyer they are short would send them to top up for something
        // no amount of Bux can buy.
        PurchaseOutcome::NotPurchasable => (StatusCode::CONFLICT, "notPurchasable", None),
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
    /// Where to send the browser. Comes from the provider, so the client never
    /// has to know whether this deployment is pointed at sandbox or production
    /// — getting that wrong client-side is a checkout that silently 404s.
    pub payment_url: String,
    pub sku: String,
    pub bux: u32,
    pub price_usd_cents: u32,
    /// Whether this checkout is a test one. Surfaced so the UI can say so
    /// rather than letting a tester wonder whether they just spent money.
    pub sandbox: bool,
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

/// The distribution this request came from.
///
/// Session metadata the client asserts, exactly as it already asserts it on
/// the WebSocket handshake: this routes policy, it does not authenticate
/// anything. A caller who lies about it gains only the ability to see a shop
/// their own portal build would have hidden — and lying is not what the gate
/// is for. It exists so the CrazyGames *build* does not present purchases the
/// portal has not approved.
fn distribution_of(headers: &HeaderMap) -> ClientDistribution {
    headers
        .get("x-snaketron-distribution")
        .and_then(|value| value.to_str().ok())
        .and_then(ClientDistribution::parse)
        .unwrap_or(ClientDistribution::Web)
}

/// Whether real money may be taken on this distribution.
///
/// CrazyGames prohibits exposing in-app purchases without portal approval
/// (`CRAZYGAMES.md`). Itch and the direct web build are ours to sell on. When
/// the portal does approve purchases this becomes one arm of a match, not a
/// re-architecture.
fn purchases_allowed(distribution: ClientDistribution) -> bool {
    match distribution {
        ClientDistribution::Web | ClientDistribution::Itch => true,
        ClientDistribution::CrazyGames => false,
    }
}

/// What is on sale.
///
/// The two tables are joined here rather than in the client, so a SKU that
/// gained a value without gaining a price is absent from the shop instead of
/// being offered at nothing. A distribution that may not sell, and a
/// deployment with no merchant account, both return an empty shop — which is
/// what the wallet renders as "not available here" without needing to know
/// which of the two happened.
pub async fn list_packs(State(state): State<AuthState>, headers: HeaderMap) -> Response {
    if state.payments.is_none() || !purchases_allowed(distribution_of(&headers)) {
        return Json(Vec::<BuxPack>::new()).into_response();
    }

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
/// The token comes from Xsolla, not from here. That is the whole security
/// property: Pay Station will only take money against a token it issued, and
/// it issues one only to a caller holding our merchant credentials. The buyer
/// and the price are bound into that request server-side, so a client cannot
/// pay for the cheapest pack while claiming the largest.
pub async fn xsolla_checkout_token(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
    Json(request): Json<CheckoutRequest>,
) -> Result<Response, WalletApiError> {
    let distribution = distribution_of(&headers);
    if !purchases_allowed(distribution) {
        return Err(WalletApiError::Forbidden(
            "Snakebux cannot be bought in this version of the game".to_string(),
        ));
    }

    // Paying needs a durable account for the same reason buying a skin does: a
    // guest row can be orphaned by closing a tab, and an orphaned *paid* row is
    // a support ticket about real money.
    if auth_user.is_guest {
        return Err(WalletApiError::BadRequest(
            "Buying Snakebux needs a registered account".to_string(),
        ));
    }

    let Some(payments) = state.payments.as_ref() else {
        // Absent configuration disables the surface rather than half-enabling
        // it, the way the replay store does.
        return Err(WalletApiError::Disabled);
    };

    let bux = pack_value(&request.sku).ok_or_else(|| {
        WalletApiError::BadRequest(format!("sku: {} is not on sale", request.sku))
    })?;
    let price_usd_cents = PACK_PRICES_USD_CENTS
        .iter()
        .find(|(id, _)| *id == request.sku)
        .map(|(_, price)| *price)
        .ok_or_else(|| WalletApiError::BadRequest("sku: has no price".to_string()))?;

    let handoff = xsolla::create_payment_token(
        &payments.config,
        &payments.client,
        auth_user.user_id,
        &request.sku,
        price_usd_cents,
    )
    .await
    .map_err(WalletApiError::Provider)?;

    info!(
        user_id = auth_user.user_id,
        sku = request.sku,
        external_id = handoff.external_id,
        sandbox = payments.config.sandbox,
        "minted a Snakebux checkout"
    );

    let mut response = Json(CheckoutToken {
        token: handoff.token,
        payment_url: handoff.payment_url,
        sku: request.sku,
        bux,
        price_usd_cents,
        sandbox: payments.config.sandbox,
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    Ok(response)
}

/// Settlement from the payment provider.
///
/// Never callable by a client: the signature is over the raw body with a shared
/// secret only the provider has. Payments credit, refunds and chargebacks debit
/// the same amount back — and are allowed to drive a balance negative, because
/// the Bux may already have been spent and pretending otherwise would be
/// writing off the debt.
///
/// The status codes are Xsolla's contract, not ours. It retries anything that
/// is not a 2xx, and stops on a 400 carrying its error envelope. So: a
/// duplicate is a success, a notification we do not act on is a success, and a
/// 400 is reserved for a message that will never become processable however
/// many times it is redelivered.
pub async fn xsolla_webhook(
    State(state): State<AuthState>,
    headers: HeaderMap,
    body: String,
) -> Result<Response, WalletApiError> {
    let Some(payments) = state.payments.as_ref() else {
        warn!("an Xsolla webhook arrived but payments are not configured; refusing it");
        return Err(WalletApiError::Disabled);
    };
    let config = &payments.config;

    let source = source_address(&headers);
    if !config.accepts_source(source) {
        warn!(
            ?source,
            "an Xsolla webhook arrived from an unlisted address"
        );
        return Ok(refuse(
            StatusCode::FORBIDDEN,
            "INVALID_PARAMETER",
            "This address is not permitted to deliver notifications",
        ));
    }

    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Signature "))
        .unwrap_or_default();
    if !config.signature_matches(&body, presented) {
        warn!("an Xsolla webhook failed signature verification");
        return Ok(refuse(
            StatusCode::UNAUTHORIZED,
            "INVALID_SIGNATURE",
            "Signature verification failed",
        ));
    }

    let notification: Notification = match serde_json::from_str(&body) {
        Ok(notification) => notification,
        Err(error) => {
            warn!(%error, "an Xsolla webhook could not be read");
            return Ok(refuse(
                StatusCode::BAD_REQUEST,
                "INVALID_PARAMETER",
                format!("Notification could not be read: {error}"),
            ));
        }
    };

    match notification {
        // Asked before Pay Station will take money. A truthful answer here is
        // what stops a player paying for an account that does not exist —
        // answering "fine" to everything, which is what an unhandled type used
        // to do, makes the check worthless.
        Notification::UserValidation { user } => {
            let Some(user_id) = user.snaketron_user_id() else {
                return Ok(refuse(
                    StatusCode::BAD_REQUEST,
                    "INVALID_USER",
                    "That is not a Snaketron user id",
                ));
            };
            let found = state
                .db
                .get_user_by_id(user_id)
                .await
                .map_err(WalletApiError::Internal)?;
            match found {
                Some(user) if !user.is_guest => Ok(StatusCode::NO_CONTENT.into_response()),
                Some(_) => Ok(refuse(
                    StatusCode::BAD_REQUEST,
                    "INVALID_USER",
                    "Guest accounts cannot buy Snakebux",
                )),
                None => Ok(refuse(
                    StatusCode::BAD_REQUEST,
                    "INVALID_USER",
                    "No such player",
                )),
            }
        }

        Notification::Payment(settlement) => {
            settle(state.db.as_ref(), config, settlement, Reversal::No).await
        }
        Notification::Refund(settlement) => {
            settle(state.db.as_ref(), config, settlement, Reversal::Yes).await
        }

        Notification::Other => {
            // Order lifecycle, subscriptions, AFS decisions. Acknowledged so
            // the provider stops retrying something that is not an error.
            info!("ignoring an Xsolla notification we do not act on");
            Ok(StatusCode::NO_CONTENT.into_response())
        }
    }
}

/// Which direction a settlement moves the balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reversal {
    No,
    Yes,
}

/// A notification we will not act on, and what to tell the provider about it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Refusal {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl Refusal {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn unprocessable(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    /// Render in the envelope Xsolla reads.
    ///
    /// A bare status with an unrecognised body is treated as a delivery
    /// failure and retried; this is shown in the publisher account and stops
    /// the retries.
    fn into_response(self) -> Response {
        (
            self.status,
            Json(NotificationError::new(self.code, self.message)),
        )
            .into_response()
    }
}

/// One movement, decided.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Movement {
    source: LedgerSource,
    delta: i64,
    note: String,
}

/// Refuse a sandbox transaction on a deployment that is not itself sandboxed.
///
/// On production this is exactly the shape free money would take. Refused
/// rather than ignored, so the mistake shows up in the publisher account
/// instead of being silently dropped.
fn admits_transaction(
    settlement: &Settlement,
    config: &xsolla::XsollaConfig,
) -> Result<(), Refusal> {
    if settlement.transaction.is_dry_run() && !config.honours_dry_run() {
        error!(
            transaction = settlement.transaction.id,
            "a sandbox Xsolla transaction reached a production deployment; refusing to credit it"
        );
        return Err(Refusal::unprocessable(
            "INVALID_PARAMETER",
            "This deployment does not accept sandbox transactions",
        ));
    }
    Ok(())
}

/// Whose balance this settlement moves.
///
/// Prefers the id we attached at checkout over the one in the user block: we
/// wrote it, and it round-trips through the provider untouched. When both are
/// present they must agree, because a disagreement is either a provider bug or
/// someone else's transaction — and crediting the wrong player is worse than
/// crediting nobody.
fn recipient_of(settlement: &Settlement) -> Result<i32, Refusal> {
    match (
        settlement.custom_parameters.snaketron_user_id,
        settlement.user.snaketron_user_id(),
    ) {
        (Some(ours), Some(theirs)) if ours != theirs => {
            error!(
                ours,
                theirs, "an Xsolla settlement names two different players"
            );
            Err(Refusal::unprocessable(
                "INVALID_USER",
                "The notification names two different players",
            ))
        }
        (Some(ours), _) => Ok(ours),
        (None, Some(theirs)) => Ok(theirs),
        (None, None) => Err(Refusal::unprocessable(
            "INVALID_USER",
            "That is not a Snaketron user id",
        )),
    }
}

/// How far, and in which direction, the balance moves.
///
/// `original` is the credit this reversal undoes, when one was recorded. A
/// refund gives back exactly what its payment credited: the pack table is
/// configuration and may have been repriced since the payment settled, so
/// re-deriving the amount from the SKU would let a repricing change what an
/// old refund takes away.
fn movement_for(
    settlement: &Settlement,
    reversal: Reversal,
    original: Option<&crate::wallet::LedgerEntry>,
) -> Result<Movement, Refusal> {
    let sku = settlement.custom_parameters.sku.as_deref();

    match reversal {
        Reversal::No => {
            let Some(sku) = sku else {
                return Err(Refusal::unprocessable(
                    "INVALID_PARAMETER",
                    "The notification names no pack",
                ));
            };
            let Some(value) = pack_value(sku) else {
                return Err(Refusal::unprocessable(
                    "INVALID_PARAMETER",
                    format!("sku: {sku} is not on sale"),
                ));
            };
            Ok(Movement {
                source: LedgerSource::Xsolla,
                delta: i64::from(value),
                note: sku.to_string(),
            })
        }
        Reversal::Yes => match original {
            Some(entry) => Ok(Movement {
                source: LedgerSource::Refund,
                delta: -entry.delta,
                note: entry.note.clone().unwrap_or_else(|| "refund".to_string()),
            }),
            // A reversal for a payment we never recorded. Falling back to the
            // named pack is better than dropping it; refusing is better than
            // guessing when there is nothing to fall back to.
            None => {
                let Some(value) = sku.and_then(pack_value) else {
                    return Err(Refusal::unprocessable(
                        "INVALID_PARAMETER",
                        "No matching payment, and the notification names no pack",
                    ));
                };
                warn!(
                    transaction = settlement.transaction.id,
                    "reversing a refund against the pack table; no payment was recorded"
                );
                Ok(Movement {
                    source: LedgerSource::Refund,
                    delta: -i64::from(value),
                    note: sku.unwrap_or_default().to_string(),
                })
            }
        },
    }
}

/// Apply one settled payment, or one reversal of one.
///
/// Every decision above is made by the pure helpers; this is the part that
/// talks to the database, and it is deliberately the part with no branches
/// worth arguing about.
async fn settle(
    db: &dyn crate::db::Database,
    config: &xsolla::XsollaConfig,
    settlement: Settlement,
    reversal: Reversal,
) -> Result<Response, WalletApiError> {
    if let Err(refusal) = admits_transaction(&settlement, config) {
        return Ok(refusal.into_response());
    }

    let user_id = match recipient_of(&settlement) {
        Ok(user_id) => user_id,
        Err(refusal) => return Ok(refusal.into_response()),
    };

    let transaction = settlement.transaction.id.clone();

    // Only a reversal needs the original, and only a reversal pays for the
    // read.
    let original = match reversal {
        Reversal::No => None,
        Reversal::Yes => db
            .get_ledger_entry(user_id, LedgerSource::Xsolla, &transaction)
            .await
            .map_err(WalletApiError::Internal)?,
    };

    let movement = match movement_for(&settlement, reversal, original.as_ref()) {
        Ok(movement) => movement,
        Err(refusal) => return Ok(refusal.into_response()),
    };

    let fingerprint =
        request_fingerprint(&[movement.source.as_str(), &transaction, &movement.note]);

    let applied = db
        .apply_ledger_entry(
            user_id,
            movement.source,
            &transaction,
            movement.delta,
            &fingerprint,
            Some(&movement.note),
        )
        .await
        .map_err(WalletApiError::Internal)?;

    if applied {
        info!(
            user_id,
            transaction,
            delta = movement.delta,
            source = movement.source.as_str(),
            "applied an Xsolla settlement"
        );
    } else {
        info!(
            transaction,
            source = movement.source.as_str(),
            "an Xsolla notification was already applied"
        );
    }

    // 204 either way. A duplicate is a success from the provider's point of
    // view, and anything else makes it retry forever.
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Refuse a notification before it is parsed far enough to have a settlement.
fn refuse(status: StatusCode, code: &'static str, message: impl Into<String>) -> Response {
    Refusal::new(status, code, message).into_response()
}

/// The address a webhook was delivered from, as far as the proxy in front of
/// us will say. `None` when nothing claims to know, which satisfies no
/// allowlist.
fn source_address(headers: &HeaderMap) -> Option<std::net::IpAddr> {
    // Left-most entry is the original client; the load balancer appends its
    // own hop. Only meaningful because everything reaching this process has
    // passed through our proxy — a directly-exposed server would be reading a
    // header the caller writes.
    headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .and_then(|value| value.trim().parse().ok())
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

    fn settlement(body: &str) -> Settlement {
        match serde_json::from_str::<Notification>(body).expect("a notification parses") {
            Notification::Payment(settlement) | Notification::Refund(settlement) => settlement,
            other => panic!("expected a settlement, got {other:?}"),
        }
    }

    fn payment(sku: Option<&str>, user_id: i32, dry_run: bool) -> Settlement {
        let custom = match sku {
            Some(sku) => format!(r#"{{"sku":"{sku}","snaketron_user_id":{user_id}}}"#),
            None => format!(r#"{{"snaketron_user_id":{user_id}}}"#),
        };
        settlement(&format!(
            r#"{{"notification_type":"payment",
                 "user":{{"id":"{user_id}"}},
                 "transaction":{{"id":991,"dry_run":{}}},
                 "custom_parameters":{custom}}}"#,
            i32::from(dry_run)
        ))
    }

    fn credit(delta: i64, note: &str) -> crate::wallet::LedgerEntry {
        crate::wallet::LedgerEntry {
            source: LedgerSource::Xsolla,
            idempotency_key: "991".to_string(),
            delta,
            request_hash: "sha256:whatever".to_string(),
            created_at_ms: 0,
            note: Some(note.to_string()),
        }
    }

    fn sandbox(sandbox: bool) -> xsolla::XsollaConfig {
        xsolla::XsollaConfig::for_test(sandbox)
    }

    /// A credit is worth what we sell the pack for, never what the payload
    /// says the buyer paid.
    #[test]
    fn a_payment_credits_the_configured_pack() {
        let movement = movement_for(&payment(Some("bux-1200"), 42, false), Reversal::No, None)
            .expect("a known pack settles");
        assert_eq!(movement.source, LedgerSource::Xsolla);
        assert_eq!(movement.delta, 1_200);
        assert_eq!(movement.note, "bux-1200");

        assert_eq!(
            movement_for(&payment(Some("bux-999999"), 42, false), Reversal::No, None)
                .expect_err("an unknown pack is worth nothing")
                .code,
            "INVALID_PARAMETER"
        );
        assert_eq!(
            movement_for(&payment(None, 42, false), Reversal::No, None)
                .expect_err("a payment naming no pack is unprocessable")
                .code,
            "INVALID_PARAMETER"
        );
    }

    /// The reason a refund reads the ledger rather than the pack table: a
    /// repricing between payment and refund must not change what is taken
    /// back. Here the pack has been repriced from 1200 to 5000 and the refund
    /// still returns the 1200 that were actually granted.
    #[test]
    fn a_refund_returns_exactly_what_its_payment_credited() {
        let refund = payment(Some("bux-1200"), 42, false);

        let against_ledger = movement_for(&refund, Reversal::Yes, Some(&credit(5_000, "bux-5000")))
            .expect("a recorded payment can be reversed");
        assert_eq!(against_ledger.source, LedgerSource::Refund);
        assert_eq!(
            against_ledger.delta, -5_000,
            "the reversal follows the ledger, not today's price list"
        );

        // With no recorded payment there is nothing to follow, and the named
        // pack is the only remaining evidence.
        let fallback =
            movement_for(&refund, Reversal::Yes, None).expect("a named pack can still be reversed");
        assert_eq!(fallback.delta, -1_200);

        assert_eq!(
            movement_for(&payment(None, 42, false), Reversal::Yes, None)
                .expect_err("nothing to reverse and nothing to reverse it by")
                .code,
            "INVALID_PARAMETER"
        );

        // The two land in different key namespaces, so a reversal can never be
        // mistaken for its own original payment.
        assert_ne!(
            crate::wallet::ledger_sort_key(LedgerSource::Xsolla, "t9"),
            crate::wallet::ledger_sort_key(LedgerSource::Refund, "t9")
        );
    }

    /// Sandbox money is not money. A production deployment refuses it, and a
    /// sandbox deployment is the only place it may credit — which is what
    /// makes an end-to-end test possible without opening a hole in prod.
    #[test]
    fn a_sandbox_transaction_only_settles_on_a_sandbox_deployment() {
        let dry_run = payment(Some("bux-500"), 42, true);
        let live = payment(Some("bux-500"), 42, false);

        assert_eq!(
            admits_transaction(&dry_run, &sandbox(false))
                .expect_err("production refuses test money")
                .code,
            "INVALID_PARAMETER"
        );
        assert!(admits_transaction(&dry_run, &sandbox(true)).is_ok());
        assert!(admits_transaction(&live, &sandbox(false)).is_ok());
        assert!(admits_transaction(&live, &sandbox(true)).is_ok());
    }

    /// The user the token was minted for wins, and a settlement that names two
    /// different players credits neither.
    #[test]
    fn a_settlement_credits_the_player_the_checkout_was_minted_for() {
        assert_eq!(
            recipient_of(&payment(Some("bux-500"), 42, false)).expect("agreeing ids"),
            42
        );

        let disagreeing = settlement(
            r#"{"notification_type":"payment",
                "user":{"id":"7"},
                "transaction":{"id":991},
                "custom_parameters":{"sku":"bux-500","snaketron_user_id":42}}"#,
        );
        assert_eq!(
            recipient_of(&disagreeing)
                .expect_err("two players is nobody")
                .code,
            "INVALID_USER"
        );

        // Only the provider's block: still usable, because it is the id we put
        // in the token's user field.
        let provider_only = settlement(
            r#"{"notification_type":"payment",
                "user":{"id":"7"},
                "transaction":{"id":991},
                "custom_parameters":{"sku":"bux-500"}}"#,
        );
        assert_eq!(recipient_of(&provider_only).expect("one player"), 7);

        let nobody = settlement(
            r#"{"notification_type":"payment",
                "user":{"id":"not-a-number"},
                "transaction":{"id":991},
                "custom_parameters":{"sku":"bux-500"}}"#,
        );
        assert_eq!(
            recipient_of(&nobody).expect_err("no player").code,
            "INVALID_USER"
        );
    }

    /// CrazyGames prohibits exposing purchases without portal approval, so the
    /// portal build gets no shop and no checkout.
    #[test]
    fn purchases_are_offered_on_every_distribution_but_the_portal() {
        assert!(purchases_allowed(ClientDistribution::Web));
        assert!(purchases_allowed(ClientDistribution::Itch));
        assert!(!purchases_allowed(ClientDistribution::CrazyGames));
    }

    /// An absent or unreadable header is the direct web build, which is the
    /// only default that cannot accidentally *enable* a surface.
    #[test]
    fn a_missing_distribution_header_reads_as_the_web_build() {
        let mut headers = HeaderMap::new();
        assert_eq!(distribution_of(&headers), ClientDistribution::Web);

        headers.insert("x-snaketron-distribution", "crazygames".parse().unwrap());
        assert_eq!(distribution_of(&headers), ClientDistribution::CrazyGames);

        headers.insert("x-snaketron-distribution", "itch".parse().unwrap());
        assert_eq!(distribution_of(&headers), ClientDistribution::Itch);

        headers.insert("x-snaketron-distribution", "nonsense".parse().unwrap());
        assert_eq!(distribution_of(&headers), ClientDistribution::Web);
    }

    /// Xsolla reads the proxy's view of the peer, not the socket, because
    /// everything reaching this process has been through a load balancer.
    #[test]
    fn the_source_address_is_the_left_most_forwarded_hop() {
        let mut headers = HeaderMap::new();
        assert_eq!(source_address(&headers), None);

        headers.insert("x-forwarded-for", "185.30.20.7, 10.0.0.1".parse().unwrap());
        assert_eq!(
            source_address(&headers),
            Some("185.30.20.7".parse().unwrap())
        );

        let mut only_real_ip = HeaderMap::new();
        only_real_ip.insert("x-real-ip", "198.51.100.4".parse().unwrap());
        assert_eq!(
            source_address(&only_real_ip),
            Some("198.51.100.4".parse().unwrap())
        );

        let mut garbage = HeaderMap::new();
        garbage.insert("x-forwarded-for", "not-an-address".parse().unwrap());
        assert_eq!(source_address(&garbage), None);
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
