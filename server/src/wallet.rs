//! Snake Bux: the balance, and the ledger that guards every change to it.
//!
//! Two rules do all the work here, and both exist because this is the one part
//! of the system where a bug costs real money.
//!
//! **Every mutation is a ledger row first.** The balance is only ever moved by
//! an atomic add guarded by a conditional write of a row naming that exact
//! change. A duplicate webhook, a double-clicked purchase, or a retried admin
//! grant finds its row already present and changes nothing.
//!
//! **The keyspace is namespaced by source.** Credits from the payment provider,
//! debits from purchases, and administrative grants live in separate key
//! prefixes. Without that, a client that can mint its own purchase keys could
//! pre-insert a key matching a payment it was about to make, and the real
//! credit would arrive, find the key taken, conclude "already applied", and
//! silently never pay out.
//!
//! The balance is signed, which looks wrong for a currency until you consider
//! refunds: a player can spend Bux and then charge back the payment that bought
//! them. Going negative is the honest representation of that, and blocking
//! further spending until it is repaid is the containment.

use serde::{Deserialize, Serialize};

/// Where a ledger entry came from. Each is its own key namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum LedgerSource {
    /// A settled payment. Keyed by the provider's transaction id.
    Xsolla,
    /// A reversal of one. Keyed by the same transaction id in its own space,
    /// so a refund cannot be mistaken for its own original payment.
    Refund,
    /// A skin purchase. Keyed by a UUID the client mints.
    Purchase,
    /// Support tooling.
    Admin,
}

impl LedgerSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Xsolla => "XSOLLA",
            Self::Refund => "REFUND",
            Self::Purchase => "PURCHASE",
            Self::Admin => "ADMIN",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "XSOLLA" => Some(Self::Xsolla),
            "REFUND" => Some(Self::Refund),
            "PURCHASE" => Some(Self::Purchase),
            "ADMIN" => Some(Self::Admin),
            _ => None,
        }
    }

    /// Whether a client may name a key in this namespace.
    ///
    /// Only purchases. A client that could write into the payment namespace
    /// could pre-empt its own incoming credit.
    pub fn is_client_mintable(self) -> bool {
        matches!(self, Self::Purchase)
    }
}

/// One entry, and the only thing that may move a balance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct LedgerEntry {
    pub source: LedgerSource,
    /// Unique within its source namespace.
    pub idempotency_key: String,
    /// Signed: credits are positive, purchases and refunds negative.
    pub delta: i64,
    /// A hash of the request that produced this entry.
    ///
    /// Replaying a key with a *different* request is a conflict rather than a
    /// duplicate — key K used for skin A and then for skin B must not quietly
    /// return A's result.
    pub request_hash: String,
    pub created_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// A player's wallet as the client sees it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Wallet {
    pub balance_bux: i64,
    /// Most recent entries, newest first.
    pub recent: Vec<LedgerEntry>,
}

impl Wallet {
    /// Whether this wallet may spend.
    ///
    /// A negative balance is a reversed payment whose Bux were already spent.
    /// Spending is blocked until it is repaid, which is the containment for a
    /// chargeback rather than an attempt to prevent one.
    pub fn may_spend(&self, amount_bux: u32) -> bool {
        self.balance_bux >= 0 && self.balance_bux >= i64::from(amount_bux)
    }
}

/// A client-minted idempotency key, checked for shape.
///
/// UUID-shaped and nothing else. The point is not secrecy — the key lives
/// under the caller's own user partition — but that a key cannot be crafted to
/// collide with a provider's transaction id format and end up in the wrong
/// namespace if a prefix is ever mishandled.
pub fn is_valid_client_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| {
        if matches!(index, 8 | 13 | 18 | 23) {
            *byte == b'-'
        } else {
            byte.is_ascii_hexdigit()
        }
    })
}

/// The sort key one entry lives under.
pub fn ledger_sort_key(source: LedgerSource, idempotency_key: &str) -> String {
    format!("TXN#{}#{idempotency_key}", source.as_str())
}

/// A stable fingerprint of what a request asked for.
///
/// Compared on replay so a reused key with different contents is refused
/// instead of silently returning the first call's outcome.
pub fn request_fingerprint(parts: &[&str]) -> String {
    let joined = parts.join("\u{1f}");
    skin_schema::content::reference_for_bytes(joined.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The namespacing rule, stated as a test because the failure it prevents
    /// is invisible: a client pre-inserting a key that matches an incoming
    /// payment would make the real credit look like a duplicate, and the player
    /// would pay and receive nothing.
    #[test]
    fn a_client_key_cannot_occupy_a_payments_ledger_slot() {
        let provider_transaction = "abc123";
        let payment = ledger_sort_key(LedgerSource::Xsolla, provider_transaction);
        let purchase = ledger_sort_key(LedgerSource::Purchase, provider_transaction);
        assert_ne!(payment, purchase);

        assert!(LedgerSource::Purchase.is_client_mintable());
        assert!(!LedgerSource::Xsolla.is_client_mintable());
        assert!(!LedgerSource::Refund.is_client_mintable());
        assert!(!LedgerSource::Admin.is_client_mintable());
    }

    /// A refund is not its own payment, even though both name the same
    /// provider transaction.
    #[test]
    fn a_refund_does_not_collide_with_the_payment_it_reverses() {
        assert_ne!(
            ledger_sort_key(LedgerSource::Xsolla, "txn-1"),
            ledger_sort_key(LedgerSource::Refund, "txn-1")
        );
    }

    #[test]
    fn client_keys_must_be_uuid_shaped() {
        assert!(is_valid_client_key("f47ac10b-58cc-4372-a567-0e02b2c3d479"));
        assert!(!is_valid_client_key("f47ac10b58cc4372a5670e02b2c3d479"));
        assert!(!is_valid_client_key("../../etc/passwd"));
        assert!(!is_valid_client_key(""));
        assert!(
            !is_valid_client_key(&"a".repeat(36)),
            "hex only, and dashes in place"
        );
        assert!(!is_valid_client_key("f47ac10b-58cc-4372-a567-0e02b2c3d47z"));
    }

    /// The whole point of the signed balance: a player who spent Bux that were
    /// later charged back goes negative and cannot spend again until it is
    /// settled — rather than the debt being silently written off by an unsigned
    /// type saturating at zero.
    #[test]
    fn a_reversed_payment_leaves_a_debt_that_blocks_spending() {
        let solvent = Wallet {
            balance_bux: 500,
            recent: Vec::new(),
        };
        assert!(solvent.may_spend(500));
        assert!(!solvent.may_spend(501));

        let in_debt = Wallet {
            balance_bux: -250,
            recent: Vec::new(),
        };
        assert!(
            !in_debt.may_spend(0),
            "a debt blocks even a free transaction"
        );
        assert!(!in_debt.may_spend(10));
    }

    #[test]
    fn a_request_fingerprint_changes_with_the_request() {
        let buying_seven = request_fingerprint(&["purchase", "7", "250"]);
        assert_eq!(buying_seven, request_fingerprint(&["purchase", "7", "250"]));
        assert_ne!(buying_seven, request_fingerprint(&["purchase", "8", "250"]));
        assert_ne!(
            buying_seven,
            request_fingerprint(&["purchase", "7", "500"]),
            "the same skin at a different price is a different request"
        );
        // Field boundaries are real: "7","250" must not equal "72","50".
        assert_ne!(
            request_fingerprint(&["purchase", "7", "250"]),
            request_fingerprint(&["purchase", "72", "50"])
        );
    }

    #[test]
    fn sources_round_trip_through_their_stored_strings() {
        for source in [
            LedgerSource::Xsolla,
            LedgerSource::Refund,
            LedgerSource::Purchase,
            LedgerSource::Admin,
        ] {
            assert_eq!(LedgerSource::parse(source.as_str()), Some(source));
        }
        assert_eq!(LedgerSource::parse("txn"), None);
    }
}
