//! Xsolla, the merchant of record.
//!
//! We have exactly two conversations with the provider, and they are
//! deliberately asymmetric.
//!
//! **Outbound**, we ask Pay Station for a token. The token is created
//! server-to-server with our merchant credentials, which is the entire point:
//! it is Xsolla's own statement of "this user is buying this, for this much",
//! so the browser never gets to assert an identity or a price. A token minted
//! locally would be a string Pay Station has never heard of.
//!
//! **Inbound**, Xsolla tells us what settled. That message is the only thing
//! that mints currency, so everything here is written against a caller who is
//! not Xsolla: the signature is SHA-1 over `body + secret` compared in constant
//! time, the source address may be pinned to an allowlist, and the amount
//! credited is looked up from our own pack table using the SKU we sent in
//! `custom_parameters` — never read out of the payload.
//!
//! Two smaller things carry more weight than their size suggests:
//!
//! - `dry_run` marks a sandbox transaction. Crediting one on a production
//!   deployment would be free money, so a dry-run settlement is only honoured
//!   when this deployment is itself configured for sandbox.
//! - Xsolla retries any non-2xx forever and treats a 400 as "stop, this is
//!   broken". Responses here are chosen for that contract, not for HTTP
//!   aesthetics: a duplicate is a success, an unknown notification is a
//!   success, and only a genuinely unprocessable message is a 400.

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::time::Duration;
use tracing::{info, warn};

pub const MERCHANT_ID_ENV: &str = "SNAKETRON_XSOLLA_MERCHANT_ID";
pub const PROJECT_ID_ENV: &str = "SNAKETRON_XSOLLA_PROJECT_ID";
pub const API_KEY_ENV: &str = "SNAKETRON_XSOLLA_API_KEY";
pub const WEBHOOK_SECRET_ENV: &str = "SNAKETRON_XSOLLA_WEBHOOK_SECRET";
pub const SANDBOX_ENV: &str = "SNAKETRON_XSOLLA_SANDBOX";
pub const RETURN_URL_ENV: &str = "SNAKETRON_XSOLLA_RETURN_URL";
pub const WEBHOOK_IPS_ENV: &str = "SNAKETRON_XSOLLA_WEBHOOK_ALLOWED_IPS";

const TOKEN_API: &str = "https://api.xsolla.com/merchant/v2/merchants";
const PAY_STATION: &str = "https://secure.xsolla.com/paystation4/";
const SANDBOX_PAY_STATION: &str = "https://sandbox-secure.xsolla.com/paystation4/";

/// The addresses Xsolla delivers webhooks from, as published in their webhook
/// documentation.
///
/// Applied when `SNAKETRON_XSOLLA_WEBHOOK_ALLOWED_IPS` is unset, so pinning is
/// the default rather than something to remember. Setting that variable
/// replaces this list wholesale, which is the escape hatch if Xsolla changes
/// the ranges before this constant is updated — a settlement refused for
/// coming from an unlisted address logs the address it came from.
///
/// The Login-product addresses are deliberately absent: nothing here receives
/// Login webhooks, and an allowlist should name what we actually expect.
const DEFAULT_WEBHOOK_SOURCES: &[&str] = &[
    "185.30.20.0/24",
    "185.30.21.0/24",
    "185.30.22.0/24",
    "185.30.23.0/24",
    "34.102.38.178",
    "34.94.43.207",
    "35.236.73.234",
    "34.94.69.44",
    "34.102.22.197",
];

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Everything needed to sell a pack and to believe a settlement.
///
/// Constructed whole or not at all: a deployment with a project id but no API
/// key cannot take payment, and starting up as though it could would fail at
/// the moment a player clicks buy rather than at the moment someone deployed
/// it wrong.
#[derive(Debug, Clone)]
pub struct XsollaConfig {
    pub merchant_id: String,
    pub project_id: u32,
    /// Server-to-server credential for the token API. Never leaves this
    /// process except as an `Authorization` header to Xsolla.
    api_key: String,
    /// The project's secret key, which signs webhooks.
    webhook_secret: String,
    /// Whether tokens are minted in Xsolla's sandbox. Also the gate on
    /// honouring `dry_run` settlements.
    pub sandbox: bool,
    /// Where Pay Station sends the player when they are done.
    pub return_url: Option<String>,
    /// Source addresses permitted to deliver webhooks. Empty means unpinned,
    /// and the signature is then the only authentication.
    pub allowed_ips: Vec<IpPrefix>,
}

impl XsollaConfig {
    /// Read configuration, or decide payments are not part of this deployment.
    ///
    /// `Ok(None)` is the "no payments here" answer and is normal — local
    /// development, CI, and any environment that has no merchant account run
    /// this way. `Err` is reserved for configuration that is present and
    /// wrong, which is a deploy-time mistake worth refusing to start over.
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Option<Self>> {
        let present = |value: Option<String>| {
            value
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };

        let merchant_id = present(lookup(MERCHANT_ID_ENV));
        let project_id = present(lookup(PROJECT_ID_ENV));
        let api_key = present(lookup(API_KEY_ENV));
        let webhook_secret = present(lookup(WEBHOOK_SECRET_ENV));

        // Nothing configured at all is a deliberate state, not an error.
        if merchant_id.is_none()
            && project_id.is_none()
            && api_key.is_none()
            && webhook_secret.is_none()
        {
            return Ok(None);
        }

        let required = |value: Option<String>, name: &str| {
            value.ok_or_else(|| {
                anyhow!("{name} must be set to take payment; unset every SNAKETRON_XSOLLA_* variable to disable payments instead")
            })
        };

        let merchant_id = required(merchant_id, MERCHANT_ID_ENV)?;
        let project_id = required(project_id, PROJECT_ID_ENV)?;
        let api_key = required(api_key, API_KEY_ENV)?;
        let webhook_secret = required(webhook_secret, WEBHOOK_SECRET_ENV)?;

        let project_id: u32 = project_id
            .parse()
            .with_context(|| format!("{PROJECT_ID_ENV} must be the numeric project id"))?;

        // Production semantics are the default. The opposite default would
        // mean a deployment that forgot this variable quietly credits Xsolla's
        // test payments as though they were money.
        let sandbox = match present(lookup(SANDBOX_ENV)) {
            None => false,
            Some(value) => match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => true,
                "false" | "0" | "no" => false,
                other => {
                    return Err(anyhow!("{SANDBOX_ENV} must be true or false, not {other}"));
                }
            },
        };

        let return_url = present(lookup(RETURN_URL_ENV));

        let allowed_ips = match present(lookup(WEBHOOK_IPS_ENV)) {
            // Unset means the published ranges, not "anything goes".
            None => DEFAULT_WEBHOOK_SOURCES
                .iter()
                .map(|entry| IpPrefix::parse(entry))
                .collect::<Result<Vec<_>>>()
                .expect("the built-in webhook source list must parse"),
            // An explicitly empty value is the deliberate opt-out, for a
            // deployment behind a proxy that rewrites the source address.
            Some(value) if value == "*" => Vec::new(),
            Some(value) => value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(IpPrefix::parse)
                .collect::<Result<Vec<_>>>()
                .with_context(|| {
                    format!("{WEBHOOK_IPS_ENV} must be comma-separated IPs or CIDRs")
                })?,
        };

        Ok(Some(Self {
            merchant_id,
            project_id,
            api_key,
            webhook_secret,
            sandbox,
            return_url,
            allowed_ips,
        }))
    }

    /// Where the browser is sent to pay.
    pub fn pay_station_url(&self, token: &str) -> String {
        let base = if self.sandbox {
            SANDBOX_PAY_STATION
        } else {
            PAY_STATION
        };
        format!("{base}?token={}", percent_encode(token))
    }

    /// Whether a webhook arriving from `source` may be considered at all.
    ///
    /// An empty allowlist permits everything — the deliberate opt-out for a
    /// deployment whose proxy rewrites the source address. Otherwise the
    /// address must be one Xsolla publishes.
    ///
    /// A **sandbox** deployment additionally accepts loopback and delivery
    /// that arrived without passing through a proxy at all, because that is
    /// exactly what a local test looks like and a deployment that cannot take
    /// real money has nothing worth pinning. Production does neither: there,
    /// an unproxied request is not something Xsolla could have sent.
    pub fn accepts_source(&self, source: Option<IpAddr>) -> bool {
        if self.allowed_ips.is_empty() {
            return true;
        }
        match source {
            None => self.sandbox,
            Some(address) => {
                if self.sandbox && address.is_loopback() {
                    return true;
                }
                self.allowed_ips
                    .iter()
                    .any(|prefix| prefix.contains(address))
            }
        }
    }

    /// Whether a settlement marked as a sandbox transaction may move money.
    pub fn honours_dry_run(&self) -> bool {
        self.sandbox
    }

    /// Constant-time check that a webhook body was signed with our secret.
    ///
    /// Xsolla signs `sha1(body + secret)` and presents it as
    /// `Authorization: Signature <hex>`. The comparison is time-invariant
    /// because an early-exit compare tells a forger how many leading bytes
    /// they guessed right, which is the difference between infeasible and a
    /// few thousand requests.
    pub fn signature_matches(&self, body: &str, presented: &str) -> bool {
        let mut message = Vec::with_capacity(body.len() + self.webhook_secret.len());
        message.extend_from_slice(body.as_bytes());
        message.extend_from_slice(self.webhook_secret.as_bytes());
        let expected = sha1_hex(&message);

        let expected = expected.as_bytes();
        let presented = presented.trim().as_bytes();
        if expected.len() != presented.len() {
            return false;
        }
        let mut difference = 0u8;
        for (left, right) in expected.iter().zip(presented) {
            // ASCII-case-insensitive: the digest is hex, and a peer that sends
            // it uppercase is not a forger.
            difference |= left.to_ascii_lowercase() ^ right.to_ascii_lowercase();
        }
        difference == 0
    }

    /// A configuration for tests that exercise settlement policy.
    ///
    /// Only the sandbox flag is meaningful here; the credentials are never
    /// used because nothing in a unit test reaches the provider.
    #[cfg(test)]
    pub fn for_test(sandbox: bool) -> Self {
        Self {
            merchant_id: "merchant".to_string(),
            project_id: 1,
            api_key: "api-key".to_string(),
            webhook_secret: "secret".to_string(),
            sandbox,
            return_url: None,
            allowed_ips: Vec::new(),
        }
    }

    /// Log what this deployment will do, without printing either secret.
    pub fn describe(&self) {
        info!(
            merchant_id = %self.merchant_id,
            project_id = self.project_id,
            sandbox = self.sandbox,
            pinned_sources = self.allowed_ips.len(),
            return_url = self.return_url.as_deref().unwrap_or("<none>"),
            "Snakebux payments enabled"
        );
        if self.sandbox {
            warn!(
                "Xsolla is in SANDBOX mode: test payments will credit real Snakebux balances. \
                 Set {SANDBOX_ENV}=false for production."
            );
        }
        if self.allowed_ips.is_empty() {
            warn!(
                "Xsolla webhooks are NOT pinned to a source address ({WEBHOOK_IPS_ENV} is \"*\"); \
                 the signature is the only authentication"
            );
        }
    }
}

/// The payment provider as the rest of the server holds it: the configuration
/// and one pooled HTTP client.
///
/// Absent on any deployment without a merchant account, which is the normal
/// state for development and CI. Every payment surface checks for it and
/// answers "not available here" rather than pretending to sell something.
#[derive(Debug, Clone)]
pub struct Payments {
    pub config: XsollaConfig,
    pub client: reqwest::Client,
}

impl Payments {
    pub fn from_env() -> Result<Option<Self>> {
        let Some(config) = XsollaConfig::from_env()? else {
            info!("Snakebux payments disabled: no {MERCHANT_ID_ENV} configured");
            return Ok(None);
        };
        config.describe();
        Ok(Some(Self {
            config,
            client: http_client()?,
        }))
    }
}

/// One entry in the webhook source allowlist: a bare address or a CIDR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpPrefix {
    address: IpAddr,
    bits: u8,
}

impl IpPrefix {
    pub fn parse(value: &str) -> Result<Self> {
        let (address, bits) = match value.split_once('/') {
            Some((address, bits)) => {
                let bits: u8 = bits
                    .parse()
                    .with_context(|| format!("{value} has a non-numeric prefix length"))?;
                (address, Some(bits))
            }
            None => (value, None),
        };

        let address: IpAddr = address
            .parse()
            .with_context(|| format!("{value} is not an IP address"))?;
        let width = if address.is_ipv4() { 32 } else { 128 };
        let bits = bits.unwrap_or(width);
        if bits > width {
            return Err(anyhow!(
                "{value} has a prefix length wider than the address"
            ));
        }
        Ok(Self { address, bits })
    }

    pub fn contains(&self, candidate: IpAddr) -> bool {
        match (self.address, candidate) {
            (IpAddr::V4(network), IpAddr::V4(candidate)) => {
                masked(&network.octets(), &candidate.octets(), self.bits)
            }
            (IpAddr::V6(network), IpAddr::V6(candidate)) => {
                masked(&network.octets(), &candidate.octets(), self.bits)
            }
            // A v4 allowlist entry does not silently match a v6 peer, or the
            // other way round.
            _ => false,
        }
    }
}

/// Whether two addresses agree on their leading `bits` bits.
fn masked(network: &[u8], candidate: &[u8], bits: u8) -> bool {
    let whole_bytes = usize::from(bits / 8);
    let remainder = bits % 8;
    if network[..whole_bytes] != candidate[..whole_bytes] {
        return false;
    }
    if remainder == 0 {
        return true;
    }
    let mask = 0xffu8 << (8 - remainder);
    network[whole_bytes] & mask == candidate[whole_bytes] & mask
}

/// Hex SHA-1, which is the digest Xsolla signs webhooks with.
///
/// SHA-1 is not a defensible choice for anything we get to choose, and it is
/// not one we chose: it is what the provider sends, and verifying it is how we
/// tell their message from someone else's. The security of the scheme rests on
/// the shared secret, not on the digest's collision resistance.
fn sha1_hex(bytes: &[u8]) -> String {
    use sha1::{Digest, Sha1};

    let digest = Sha1::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push(hex_digit(byte >> 4));
        hex.push(hex_digit(byte & 0x0f));
    }
    hex
}

fn hex_digit(nibble: u8) -> char {
    char::from(if nibble < 10 {
        b'0' + nibble
    } else {
        b'a' + nibble - 10
    })
}

/// Percent-encode everything that is not unreserved, so a token can be dropped
/// into a query string whatever the provider decides to put in it.
fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(hex_digit(byte >> 4).to_ascii_uppercase());
            encoded.push(hex_digit(byte & 0x0f).to_ascii_uppercase());
        }
    }
    encoded
}

// ---- Minting a checkout ---------------------------------------------------

#[derive(Debug, Serialize)]
struct TokenRequest<'a> {
    user: TokenUser<'a>,
    settings: TokenSettings<'a>,
    purchase: TokenPurchase<'a>,
    /// Echoed back on every notification about this transaction. This is how
    /// the webhook learns which pack was bought without trusting a price.
    custom_parameters: CustomParameters,
}

#[derive(Debug, Serialize)]
struct TokenUser<'a> {
    id: HiddenValue<'a>,
}

#[derive(Debug, Serialize)]
struct HiddenValue<'a> {
    value: &'a str,
    hidden: bool,
}

#[derive(Debug, Serialize)]
struct TokenSettings<'a> {
    project_id: u32,
    currency: &'a str,
    language: &'a str,
    /// `"sandbox"` or absent. Xsolla reads the presence of this field, so it
    /// must not be serialized as null on a production deployment.
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    return_url: Option<&'a str>,
    /// Our own id for this attempt, echoed back as `transaction.external_id`.
    external_id: &'a str,
}

#[derive(Debug, Serialize)]
struct TokenPurchase<'a> {
    checkout: TokenCheckout<'a>,
}

#[derive(Debug, Serialize)]
struct TokenCheckout<'a> {
    currency: &'a str,
    amount: f64,
}

/// The parameters we attach going out and read coming back.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CustomParameters {
    /// Which pack. Present on everything we mint; absent on anything we did
    /// not, which is exactly the signal the webhook needs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sku: Option<String>,
    /// The buyer as we know them, so a settlement can be reconciled against
    /// the user the token was minted for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snaketron_user_id: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    token: String,
    /// Xsolla's own link to the hosted checkout. Preferred over building one,
    /// because it already carries whatever host and query the account is
    /// configured for.
    #[serde(default)]
    link_to_ps: Option<String>,
}

/// A minted checkout, ready to hand to the browser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutHandoff {
    pub token: String,
    pub payment_url: String,
    pub external_id: String,
}

pub fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("Failed to build the Xsolla HTTP client")
}

/// Ask Pay Station for a token binding this buyer to this pack at this price.
///
/// Everything that decides what is being sold is passed in from server-side
/// configuration. The only client-supplied value that reaches Xsolla is the
/// SKU, and it has already been checked against the pack table by the time it
/// gets here.
pub async fn create_payment_token(
    config: &XsollaConfig,
    client: &reqwest::Client,
    user_id: i32,
    sku: &str,
    price_usd_cents: u32,
) -> Result<CheckoutHandoff> {
    let external_id = uuid::Uuid::new_v4().to_string();
    let user = user_id.to_string();
    let request = TokenRequest {
        user: TokenUser {
            id: HiddenValue {
                value: &user,
                hidden: true,
            },
        },
        settings: TokenSettings {
            project_id: config.project_id,
            currency: "USD",
            language: "en",
            mode: config.sandbox.then_some("sandbox"),
            return_url: config.return_url.as_deref(),
            external_id: &external_id,
        },
        purchase: TokenPurchase {
            checkout: TokenCheckout {
                currency: "USD",
                amount: f64::from(price_usd_cents) / 100.0,
            },
        },
        custom_parameters: CustomParameters {
            sku: Some(sku.to_string()),
            snaketron_user_id: Some(user_id),
        },
    };

    let url = format!("{TOKEN_API}/{}/token", config.merchant_id);
    let response = client
        .post(&url)
        .basic_auth(&config.merchant_id, Some(&config.api_key))
        .json(&request)
        .send()
        .await
        .context("Failed to reach Xsolla to mint a checkout token")?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("Failed to read Xsolla's token response")?;

    if !status.is_success() {
        // Xsolla's error bodies name the offending field, which is most of the
        // value in diagnosing a misconfigured account. It carries no card data
        // and no personal data, so it is safe to log.
        return Err(anyhow!(
            "Xsolla refused to mint a checkout token: HTTP {status}: {}",
            body.chars().take(500).collect::<String>()
        ));
    }

    let token: TokenResponse = serde_json::from_str(&body)
        .context("Xsolla's token response was not the expected shape")?;

    let payment_url = token
        .link_to_ps
        .unwrap_or_else(|| config.pay_station_url(&token.token));

    Ok(CheckoutHandoff {
        token: token.token,
        payment_url,
        external_id,
    })
}

// ---- Reading a settlement -------------------------------------------------

/// What Xsolla is telling us.
///
/// Only the three variants we act on are modelled. Everything else — order
/// lifecycle, subscriptions, AFS decisions — parses as `Other` and is
/// acknowledged, because an unrecognised message is not a failure and must not
/// be retried at us forever.
#[derive(Debug, Deserialize)]
#[serde(tag = "notification_type", rename_all = "snake_case")]
pub enum Notification {
    /// "Does this user exist?", sent before Pay Station will take money.
    UserValidation {
        user: NotificationUser,
    },
    Payment(Settlement),
    Refund(Settlement),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub struct Settlement {
    pub user: NotificationUser,
    pub transaction: NotificationTransaction,
    #[serde(default)]
    pub custom_parameters: CustomParameters,
}

#[derive(Debug, Deserialize)]
pub struct NotificationUser {
    /// Xsolla sends this as a string even though ours is numeric.
    pub id: String,
}

impl NotificationUser {
    /// The Snaketron user this is about, if the id is one of ours at all.
    pub fn snaketron_user_id(&self) -> Option<i32> {
        self.id.trim().parse().ok()
    }
}

#[derive(Debug, Deserialize)]
pub struct NotificationTransaction {
    /// The provider's transaction id, and our idempotency key. Documented as a
    /// number; accepted as either, because a key we fail to parse is a payment
    /// we fail to credit.
    #[serde(deserialize_with = "scalar_as_string")]
    pub id: String,
    #[serde(default)]
    pub external_id: Option<String>,
    /// Present and truthy on sandbox transactions.
    #[serde(default, deserialize_with = "optional_flag")]
    pub dry_run: Option<bool>,
}

impl NotificationTransaction {
    pub fn is_dry_run(&self) -> bool {
        self.dry_run.unwrap_or(false)
    }
}

/// Accept a JSON number, string, or boolean as a string.
fn scalar_as_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::String(value) => Ok(value),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        other => Err(D::Error::custom(format!(
            "expected a scalar, found {other}"
        ))),
    }
}

/// Accept `1`, `0`, `true`, `false`, `"1"`, or null as a flag.
fn optional_flag<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Bool(value)) => Ok(Some(value)),
        Some(serde_json::Value::Number(value)) => Ok(Some(value.as_i64().unwrap_or(0) != 0)),
        Some(serde_json::Value::String(value)) => {
            Ok(Some(matches!(value.as_str(), "1" | "true" | "yes")))
        }
        Some(other) => Err(D::Error::custom(format!("expected a flag, found {other}"))),
    }
}

/// The error envelope Xsolla expects when we refuse a notification.
///
/// Shape matters: an unrecognised body is treated as a delivery failure and
/// retried, whereas this is read, shown in the publisher account, and stops
/// the retries.
#[derive(Debug, Serialize)]
pub struct NotificationError {
    pub error: NotificationErrorBody,
}

#[derive(Debug, Serialize)]
pub struct NotificationErrorBody {
    pub code: &'static str,
    pub message: String,
}

impl NotificationError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            error: NotificationErrorBody {
                code,
                message: message.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl FnMut(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    fn config() -> XsollaConfig {
        XsollaConfig::from_lookup(lookup(&[
            (MERCHANT_ID_ENV, "12345"),
            (PROJECT_ID_ENV, "67890"),
            (API_KEY_ENV, "api-key"),
            (WEBHOOK_SECRET_ENV, "secret"),
            (SANDBOX_ENV, "true"),
        ]))
        .expect("valid configuration")
        .expect("configured")
    }

    /// The digest is the one thing here we cannot choose, so it is pinned to
    /// published vectors rather than to our own implementation's output.
    #[test]
    fn sha1_matches_the_published_vectors() {
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            sha1_hex(b"The quick brown fox jumps over the lazy dog"),
            "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12"
        );
    }

    /// The signature `scripts/xsolla-webhook.sh` produces, pinned here so the
    /// local test instrument and the server cannot drift apart. Regenerate
    /// with:
    ///
    /// ```text
    /// printf '%s%s' '{"notification_type":"payment"}' secret \
    ///   | openssl dgst -sha1 -hex | awk '{print $NF}'
    /// ```
    #[test]
    fn the_local_signing_tool_produces_the_signature_we_verify() {
        let config = config();
        assert!(config.signature_matches(
            r#"{"notification_type":"payment"}"#,
            "a4ac2a62218b3ff4117059eb2c9953cb73bcf677"
        ));
    }

    /// Xsolla's documented scheme, stated as a test because getting it wrong
    /// rejects every real webhook while every synthetic one still passes.
    #[test]
    fn a_webhook_signature_is_sha1_of_the_body_and_the_secret() {
        let config = config();
        let body = r#"{"notification_type":"payment"}"#;
        let expected = sha1_hex(format!("{body}secret").as_bytes());

        assert!(config.signature_matches(body, &expected));
        assert!(
            config.signature_matches(body, &expected.to_uppercase()),
            "hex case is not a forgery"
        );
        assert!(
            config.signature_matches(body, &format!("  {expected}  ")),
            "surrounding whitespace is not a forgery"
        );

        assert!(!config.signature_matches(body, ""));
        assert!(!config.signature_matches(body, &expected[..expected.len() - 1]));
        assert!(
            !config.signature_matches(r#"{"notification_type":"refund"}"#, &expected),
            "a signature does not travel to another body"
        );
        assert!(
            !config.signature_matches(body, &sha1_hex(format!("{body}other").as_bytes())),
            "another secret does not sign our webhooks"
        );
    }

    /// The real payment payload, nested the way Xsolla actually sends it. The
    /// previous flat shape parsed nothing that ever arrived.
    #[test]
    fn a_payment_notification_parses_as_xsolla_sends_it() {
        let body = r#"{
            "notification_type": "payment",
            "purchase": {
                "checkout": {"currency": "USD", "amount": 1.99},
                "total": {"currency": "USD", "amount": 1.99}
            },
            "user": {"ip": "127.0.0.1", "id": "42", "name": "player", "country": "US"},
            "transaction": {
                "id": 87654321,
                "external_id": "4a1b",
                "payment_date": "2026-08-23T19:25:25+04:00",
                "payment_method": 1380,
                "dry_run": 1
            },
            "payment_details": {"payment": {"currency": "USD", "amount": 1.99}},
            "custom_parameters": {"sku": "bux-500", "snaketron_user_id": 42}
        }"#;

        let Notification::Payment(settlement) =
            serde_json::from_str::<Notification>(body).expect("a payment parses")
        else {
            panic!("a payment must parse as a payment");
        };

        assert_eq!(settlement.user.snaketron_user_id(), Some(42));
        assert_eq!(settlement.transaction.id, "87654321");
        assert_eq!(settlement.transaction.external_id.as_deref(), Some("4a1b"));
        assert!(settlement.transaction.is_dry_run());
        assert_eq!(settlement.custom_parameters.sku.as_deref(), Some("bux-500"));
        assert_eq!(settlement.custom_parameters.snaketron_user_id, Some(42));
    }

    #[test]
    fn a_refund_and_a_validation_parse_as_themselves() {
        let refund = serde_json::from_str::<Notification>(
            r#"{
                "notification_type": "refund",
                "user": {"id": "42"},
                "transaction": {"id": "87654321"},
                "refund_details": {"code": 1, "reason": "user request"},
                "custom_parameters": {"sku": "bux-500"}
            }"#,
        )
        .expect("a refund parses");
        let Notification::Refund(settlement) = refund else {
            panic!("a refund must parse as a refund");
        };
        assert_eq!(settlement.transaction.id, "87654321");
        assert!(
            !settlement.transaction.is_dry_run(),
            "an absent dry_run is a live transaction"
        );

        let validation = serde_json::from_str::<Notification>(
            r#"{"notification_type": "user_validation", "user": {"id": "42"}}"#,
        )
        .expect("a validation parses");
        let Notification::UserValidation { user } = validation else {
            panic!("a validation must parse as a validation");
        };
        assert_eq!(user.snaketron_user_id(), Some(42));
    }

    /// Anything we do not act on still has to parse, because the alternative
    /// is a 400 that Xsolla surfaces as a broken integration.
    #[test]
    fn an_unhandled_notification_parses_rather_than_failing() {
        for kind in [
            "order_paid",
            "order_canceled",
            "afs_reject",
            "create_subscription",
            "something_invented_next_year",
        ] {
            let body = format!(r#"{{"notification_type": "{kind}", "unexpected": {{"a": 1}}}}"#);
            assert!(
                matches!(
                    serde_json::from_str::<Notification>(&body),
                    Ok(Notification::Other)
                ),
                "{kind} must parse as an ignorable notification"
            );
        }
    }

    #[test]
    fn a_transaction_id_survives_being_a_number_or_a_string() {
        for raw in ["87654321", "\"87654321\""] {
            let body = format!(
                r#"{{"notification_type":"payment","user":{{"id":"7"}},"transaction":{{"id":{raw}}}}}"#
            );
            let Notification::Payment(settlement) =
                serde_json::from_str::<Notification>(&body).expect("parses")
            else {
                panic!("a payment must parse as a payment");
            };
            assert_eq!(settlement.transaction.id, "87654321");
        }
    }

    #[test]
    fn configuration_is_all_or_nothing() {
        assert!(
            XsollaConfig::from_lookup(lookup(&[]))
                .expect("an empty environment is valid")
                .is_none(),
            "no configuration disables payments"
        );

        // Half-configured is a deploy mistake, and failing here is the only
        // place it can be noticed before a player clicks buy.
        assert!(
            XsollaConfig::from_lookup(lookup(&[(MERCHANT_ID_ENV, "12345")])).is_err(),
            "a merchant id alone cannot take payment"
        );
        assert!(
            XsollaConfig::from_lookup(lookup(&[
                (MERCHANT_ID_ENV, "12345"),
                (PROJECT_ID_ENV, "67890"),
                (API_KEY_ENV, "api-key"),
            ]))
            .is_err(),
            "a deployment that cannot verify a webhook must not mint tokens"
        );
        assert!(
            XsollaConfig::from_lookup(lookup(&[
                (MERCHANT_ID_ENV, "12345"),
                (PROJECT_ID_ENV, "not-a-number"),
                (API_KEY_ENV, "api-key"),
                (WEBHOOK_SECRET_ENV, "secret"),
            ]))
            .is_err(),
            "a non-numeric project id is refused"
        );
    }

    /// Sandbox is opt-in. A deployment that forgets the variable behaves as
    /// production, which refuses test money rather than minting it.
    #[test]
    fn sandbox_is_off_unless_asked_for() {
        let production = XsollaConfig::from_lookup(lookup(&[
            (MERCHANT_ID_ENV, "1"),
            (PROJECT_ID_ENV, "2"),
            (API_KEY_ENV, "k"),
            (WEBHOOK_SECRET_ENV, "s"),
        ]))
        .expect("valid")
        .expect("configured");
        assert!(!production.sandbox);
        assert!(!production.honours_dry_run());
        assert!(production.pay_station_url("t").starts_with(PAY_STATION));

        let sandbox = config();
        assert!(sandbox.honours_dry_run());
        assert!(
            sandbox
                .pay_station_url("t")
                .starts_with(SANDBOX_PAY_STATION)
        );

        assert!(
            XsollaConfig::from_lookup(lookup(&[
                (MERCHANT_ID_ENV, "1"),
                (PROJECT_ID_ENV, "2"),
                (API_KEY_ENV, "k"),
                (WEBHOOK_SECRET_ENV, "s"),
                (SANDBOX_ENV, "perhaps"),
            ]))
            .is_err(),
            "an unreadable sandbox flag is refused rather than guessed"
        );
    }

    #[test]
    fn a_token_is_escaped_into_the_pay_station_url() {
        let config = config();
        let url = config.pay_station_url("a b/c+d=");
        assert!(url.ends_with("?token=a%20b%2Fc%2Bd%3D"), "{url}");
    }

    /// Pinning is the default, not something to remember to switch on. The
    /// published ranges are baked in and an unlisted address is refused.
    #[test]
    fn webhook_sources_are_pinned_to_the_published_ranges_by_default() {
        let production = XsollaConfig::from_lookup(lookup(&[
            (MERCHANT_ID_ENV, "1"),
            (PROJECT_ID_ENV, "2"),
            (API_KEY_ENV, "k"),
            (WEBHOOK_SECRET_ENV, "s"),
        ]))
        .expect("valid")
        .expect("configured");

        assert!(
            !production.allowed_ips.is_empty(),
            "unset must not mean open"
        );
        assert!(production.accepts_source(Some("185.30.20.1".parse().unwrap())));
        assert!(production.accepts_source(Some("185.30.23.254".parse().unwrap())));
        assert!(production.accepts_source(Some("34.102.38.178".parse().unwrap())));
        assert!(!production.accepts_source(Some("203.0.113.9".parse().unwrap())));
        assert!(
            !production.accepts_source(Some("34.102.38.179".parse().unwrap())),
            "a bare address is a /32, not a neighbourhood"
        );
        assert!(
            !production.accepts_source(None),
            "an unproxied request is not something Xsolla could have sent"
        );
        assert!(
            !production.accepts_source(Some("127.0.0.1".parse().unwrap())),
            "production does not trust loopback"
        );
    }

    /// The local-testing rule, stated as a test because it is a deliberate
    /// asymmetry: a sandbox deployment takes delivery from this machine so the
    /// signing tool works, and production does not.
    #[test]
    fn a_sandbox_deployment_also_takes_delivery_from_this_machine() {
        let sandbox = config();
        assert!(sandbox.accepts_source(Some("127.0.0.1".parse().unwrap())));
        assert!(sandbox.accepts_source(Some("::1".parse().unwrap())));
        assert!(sandbox.accepts_source(None));
        assert!(sandbox.accepts_source(Some("185.30.20.1".parse().unwrap())));
        assert!(
            !sandbox.accepts_source(Some("203.0.113.9".parse().unwrap())),
            "sandbox still refuses a stranger on the internet"
        );
    }

    /// An explicit list replaces the built-in one, and `*` is the opt-out for
    /// a proxy that rewrites the source address.
    #[test]
    fn an_explicit_allowlist_replaces_the_default_and_a_star_disables_it() {
        let pinned = XsollaConfig::from_lookup(lookup(&[
            (MERCHANT_ID_ENV, "1"),
            (PROJECT_ID_ENV, "2"),
            (API_KEY_ENV, "k"),
            (WEBHOOK_SECRET_ENV, "s"),
            (WEBHOOK_IPS_ENV, "198.51.100.7, 10.1.2.0/23"),
        ]))
        .expect("valid")
        .expect("configured");
        assert!(pinned.accepts_source(Some("198.51.100.7".parse().unwrap())));
        assert!(pinned.accepts_source(Some("10.1.3.5".parse().unwrap())));
        assert!(
            !pinned.accepts_source(Some("185.30.20.1".parse().unwrap())),
            "an explicit list replaces the built-in one rather than adding to it"
        );

        let open = XsollaConfig::from_lookup(lookup(&[
            (MERCHANT_ID_ENV, "1"),
            (PROJECT_ID_ENV, "2"),
            (API_KEY_ENV, "k"),
            (WEBHOOK_SECRET_ENV, "s"),
            (WEBHOOK_IPS_ENV, "*"),
        ]))
        .expect("valid")
        .expect("configured");
        assert!(open.allowed_ips.is_empty());
        assert!(open.accepts_source(Some("203.0.113.9".parse().unwrap())));
        assert!(open.accepts_source(None));
    }

    /// Every built-in entry must parse, because `from_lookup` unwraps them.
    #[test]
    fn the_built_in_webhook_sources_all_parse() {
        for entry in DEFAULT_WEBHOOK_SOURCES {
            IpPrefix::parse(entry).unwrap_or_else(|error| panic!("{entry}: {error}"));
        }
    }

    #[test]
    fn prefixes_mask_partial_bytes_and_do_not_cross_address_families() {
        let prefix = IpPrefix::parse("10.1.2.0/23").unwrap();
        assert!(prefix.contains("10.1.2.5".parse().unwrap()));
        assert!(prefix.contains("10.1.3.5".parse().unwrap()));
        assert!(!prefix.contains("10.1.4.5".parse().unwrap()));

        let v6 = IpPrefix::parse("2001:db8::/32").unwrap();
        assert!(v6.contains("2001:db8::1".parse().unwrap()));
        assert!(!v6.contains("2001:db9::1".parse().unwrap()));
        assert!(
            !v6.contains("10.1.2.5".parse().unwrap()),
            "a v6 prefix does not match a v4 peer"
        );

        assert!(IpPrefix::parse("10.0.0.0/33").is_err());
        assert!(IpPrefix::parse("not-an-address").is_err());
    }
}
