//! Operator-owned analytics exclusion.
//!
//! GameAnalytics has no server-side way to drop traffic after the fact: an
//! event that reaches their ingest is counted forever. "Don't count me" must
//! therefore be decided *before* the client sends anything, and the one thing
//! a browser cannot work out on its own is which public IP it is calling from.
//! That is the whole job of this route.
//!
//! The client combines this verdict with the two exclusions it can decide
//! locally — an explicit `?analytics=off` override and an administrator
//! account — so an operator on their own network is skipped even while signed
//! out, in a fresh browser profile, or on a phone they have never touched.

use axum::{
    Json,
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Comma-separated IP literals and CIDR blocks whose traffic is never
/// reported to the analytics provider. Unset (the default) excludes nobody.
pub const EXCLUDED_IPS_ENV: &str = "SNAKETRON_ANALYTICS_EXCLUDED_IPS";

/// Why the caller was excluded. `None` pairs with `excluded: false` and is the
/// only representation of "this session counts".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum AnalyticsExclusionReason {
    /// The caller's forwarded address matched the deployment's exclusion list.
    ExcludedAddress,
}

/// The server's half of the analytics decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct AnalyticsConsent {
    pub excluded: bool,
    pub reason: Option<AnalyticsExclusionReason>,
}

impl AnalyticsConsent {
    const COUNTED: Self = Self {
        excluded: false,
        reason: None,
    };

    const fn excluded(reason: AnalyticsExclusionReason) -> Self {
        Self {
            excluded: true,
            reason: Some(reason),
        }
    }
}

/// Report whether this caller's network should be reported to analytics.
///
/// Deliberately anonymous and DB-free: it runs on the very first page load,
/// before any session exists, and answering it must never depend on state that
/// could make it slow or fail. A failed request is not an error condition for
/// the client — it falls back to its cached verdict.
pub async fn get_analytics_consent(headers: HeaderMap) -> Response {
    let consent = resolve_consent(
        forwarded_client_ip(&headers).as_ref(),
        std::env::var(EXCLUDED_IPS_ENV).ok().as_deref(),
    );

    let mut response = Json(consent).into_response();
    // The verdict is per-caller. A shared cache serving one network's answer
    // to another is exactly the failure this route must not have.
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    response
}

fn resolve_consent(client_ip: Option<&IpAddr>, excluded_ips: Option<&str>) -> AnalyticsConsent {
    let (Some(client_ip), Some(excluded_ips)) = (client_ip, excluded_ips) else {
        return AnalyticsConsent::COUNTED;
    };

    if excluded_ips
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .any(|entry| entry_matches(entry, client_ip))
    {
        return AnalyticsConsent::excluded(AnalyticsExclusionReason::ExcludedAddress);
    }

    AnalyticsConsent::COUNTED
}

/// Resolve the caller's address from proxy headers.
///
/// Behind the load balancer the leftmost `X-Forwarded-For` entry is the real
/// client, and that is the entry an operator would recognize as "my IP". It is
/// also client-settable, which is harmless here in a way it would not be for
/// authorization or rate limiting: forging it can only *remove* the forger's
/// own events from the operator's analytics, never anyone else's.
fn forwarded_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    let forwarded = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .and_then(parse_address);
    if forwarded.is_some() {
        return forwarded;
    }

    headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_address)
}

/// Parse one address, tolerating the `host:port` and `[v6]:port` forms proxies
/// sometimes emit.
fn parse_address(raw: &str) -> Option<IpAddr> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    if let Ok(address) = raw.parse::<IpAddr>() {
        return Some(address);
    }

    // "[2001:db8::1]:443" and "[2001:db8::1]".
    if let Some(rest) = raw.strip_prefix('[')
        && let Some((inside, _)) = rest.split_once(']')
    {
        return inside.parse().ok();
    }

    // "203.0.113.4:51000". A bare IPv6 address also contains colons, but it
    // would have parsed above, so anything reaching here is host:port shaped.
    raw.rsplit_once(':')
        .and_then(|(host, _)| host.parse().ok())
        .map(IpAddr::V4)
}

/// Match one configured entry — a bare address or a CIDR block — against the
/// caller.
///
/// IPv4-mapped IPv6 callers (`::ffff:203.0.113.4`, which is how a dual-stack
/// proxy can report an IPv4 client) are compared in their canonical IPv4 form
/// so an operator does not have to list both spellings of one address.
fn entry_matches(entry: &str, client_ip: &IpAddr) -> bool {
    let client = canonical(*client_ip);

    let Some((network, prefix_len)) = entry.split_once('/') else {
        return entry
            .parse::<IpAddr>()
            .is_ok_and(|configured| canonical(configured) == client);
    };

    let Ok(network) = network.trim().parse::<IpAddr>() else {
        return false;
    };
    let Ok(prefix_len) = prefix_len.trim().parse::<u8>() else {
        return false;
    };

    match (canonical(network), client) {
        (IpAddr::V4(network), IpAddr::V4(client)) => {
            prefix_matches(&network.octets(), &client.octets(), prefix_len)
        }
        (IpAddr::V6(network), IpAddr::V6(client)) => {
            prefix_matches(&network.octets(), &client.octets(), prefix_len)
        }
        _ => false,
    }
}

/// Compare the leading `prefix_len` bits of two same-family addresses.
fn prefix_matches(network: &[u8], client: &[u8], prefix_len: u8) -> bool {
    let total_bits = (network.len() * 8) as u8;
    if prefix_len > total_bits {
        return false;
    }

    let whole_bytes = (prefix_len / 8) as usize;
    if network[..whole_bytes] != client[..whole_bytes] {
        return false;
    }

    let remaining_bits = prefix_len % 8;
    if remaining_bits == 0 {
        return true;
    }

    // A partial byte compares only its leading bits; `remaining_bits` is 1..=7
    // here, so the shift can never reach the width of u8.
    let mask = 0xffu8 << (8 - remaining_bits);
    network[whole_bytes] & mask == client[whole_bytes] & mask
}

/// Fold an IPv4-mapped IPv6 address down to the IPv4 address it carries.
fn canonical(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        v4 => v4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(raw: &str) -> IpAddr {
        raw.parse().expect("test address must parse")
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        headers
    }

    /// An unconfigured deployment counts everyone. Analytics must not require
    /// an operator to opt every player back in.
    #[test]
    fn nobody_is_excluded_without_configuration() {
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.4")), None),
            AnalyticsConsent::COUNTED
        );
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.4")), Some("")),
            AnalyticsConsent::COUNTED
        );
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.4")), Some("  ,  ")),
            AnalyticsConsent::COUNTED
        );
    }

    /// A caller whose address the proxies did not report cannot be matched
    /// against the list, and is counted rather than guessed at.
    #[test]
    fn an_unknown_address_is_counted() {
        assert_eq!(
            resolve_consent(None, Some("203.0.113.4")),
            AnalyticsConsent::COUNTED
        );
    }

    #[test]
    fn literal_addresses_match_exactly() {
        let list = Some("203.0.113.4, 198.51.100.7");
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.4")), list),
            AnalyticsConsent::excluded(AnalyticsExclusionReason::ExcludedAddress)
        );
        assert_eq!(
            resolve_consent(Some(&ip("198.51.100.7")), list),
            AnalyticsConsent::excluded(AnalyticsExclusionReason::ExcludedAddress)
        );
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.5")), list),
            AnalyticsConsent::COUNTED
        );
    }

    #[test]
    fn cidr_blocks_match_on_the_prefix_only() {
        let list = Some("203.0.113.0/24");
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.0")), list),
            AnalyticsConsent::excluded(AnalyticsExclusionReason::ExcludedAddress)
        );
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.255")), list),
            AnalyticsConsent::excluded(AnalyticsExclusionReason::ExcludedAddress)
        );
        assert_eq!(
            resolve_consent(Some(&ip("203.0.114.0")), list),
            AnalyticsConsent::COUNTED
        );
    }

    /// The partial-byte path is where hand-written prefix maths goes wrong, so
    /// it is pinned on both sides of the boundary.
    #[test]
    fn cidr_prefixes_that_split_a_byte_are_exact() {
        // 203.0.113.128/25 covers .128 through .255 and nothing below.
        let list = Some("203.0.113.128/25");
        assert!(entry_matches("203.0.113.128/25", &ip("203.0.113.128")));
        assert!(entry_matches("203.0.113.128/25", &ip("203.0.113.255")));
        assert!(!entry_matches("203.0.113.128/25", &ip("203.0.113.127")));
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.200")), list),
            AnalyticsConsent::excluded(AnalyticsExclusionReason::ExcludedAddress)
        );

        // /0 matches everything, /32 only the single address.
        assert!(entry_matches("0.0.0.0/0", &ip("198.51.100.7")));
        assert!(entry_matches("198.51.100.7/32", &ip("198.51.100.7")));
        assert!(!entry_matches("198.51.100.7/32", &ip("198.51.100.8")));
    }

    #[test]
    fn ipv6_literals_and_blocks_match() {
        assert!(entry_matches("2001:db8::1", &ip("2001:db8::1")));
        assert!(entry_matches("2001:db8::/32", &ip("2001:db8:1234::9")));
        assert!(!entry_matches("2001:db8::/32", &ip("2001:db9::1")));
        // Families never cross-match.
        assert!(!entry_matches("0.0.0.0/0", &ip("2001:db8::1")));
        assert!(!entry_matches("::/0", &ip("203.0.113.4")));
    }

    /// A dual-stack proxy may report an IPv4 client in mapped form. The
    /// operator configures the address they know, and both spellings hit it.
    #[test]
    fn ipv4_mapped_callers_match_their_ipv4_configuration() {
        assert!(entry_matches("203.0.113.4", &ip("::ffff:203.0.113.4")));
        assert!(entry_matches("203.0.113.0/24", &ip("::ffff:203.0.113.4")));
        assert!(entry_matches("::ffff:203.0.113.4", &ip("203.0.113.4")));
    }

    /// A malformed entry is ignored rather than poisoning the whole list — a
    /// typo in one address must not silently stop excluding the others.
    #[test]
    fn malformed_entries_are_skipped_without_discarding_the_list() {
        let list = Some("not-an-ip, 203.0.113.0/notanumber, 203.0.113.0/99, 198.51.100.7");
        assert_eq!(
            resolve_consent(Some(&ip("198.51.100.7")), list),
            AnalyticsConsent::excluded(AnalyticsExclusionReason::ExcludedAddress)
        );
        assert_eq!(
            resolve_consent(Some(&ip("203.0.113.4")), list),
            AnalyticsConsent::COUNTED
        );
    }

    #[test]
    fn the_leftmost_forwarded_entry_identifies_the_client() {
        assert_eq!(
            forwarded_client_ip(&headers(&[(
                "x-forwarded-for",
                "203.0.113.4, 198.51.100.7, 10.0.0.1"
            )])),
            Some(ip("203.0.113.4"))
        );
    }

    #[test]
    fn x_real_ip_is_the_fallback_only() {
        assert_eq!(
            forwarded_client_ip(&headers(&[
                ("x-forwarded-for", "203.0.113.4"),
                ("x-real-ip", "198.51.100.7"),
            ])),
            Some(ip("203.0.113.4"))
        );
        assert_eq!(
            forwarded_client_ip(&headers(&[("x-real-ip", "198.51.100.7")])),
            Some(ip("198.51.100.7"))
        );
        assert_eq!(forwarded_client_ip(&headers(&[])), None);
        assert_eq!(
            forwarded_client_ip(&headers(&[("x-forwarded-for", "garbage")])),
            None
        );
    }

    /// The handler itself: env plumbing, JSON shape, and the cache header that
    /// stops a shared cache serving one network's verdict to another.
    #[tokio::test]
    async fn the_route_answers_per_caller_and_is_never_cached() {
        use axum::{Router, body::Body, http::Request, routing::get};
        use tower::ServiceExt;

        // SAFETY: the value is scoped to this test and removed before it
        // returns, and no sibling test reads this variable.
        unsafe { std::env::set_var(EXCLUDED_IPS_ENV, "203.0.113.0/24") };

        let app = Router::new().route("/api/analytics/consent", get(get_analytics_consent));

        let excluded = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/analytics/consent")
                    .header("x-forwarded-for", "203.0.113.9, 10.0.0.1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            excluded.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache, no-store, must-revalidate, private"
        );
        let body = axum::body::to_bytes(excluded.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(
            &body[..],
            br#"{"excluded":true,"reason":"excludedAddress"}"#
        );

        let counted = app
            .oneshot(
                Request::builder()
                    .uri("/api/analytics/consent")
                    .header("x-forwarded-for", "198.51.100.7")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(counted.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"excluded":false,"reason":null}"#);

        unsafe { std::env::remove_var(EXCLUDED_IPS_ENV) };
    }

    #[test]
    fn forwarded_entries_may_carry_ports() {
        assert_eq!(parse_address("203.0.113.4:51000"), Some(ip("203.0.113.4")));
        assert_eq!(parse_address("[2001:db8::1]:443"), Some(ip("2001:db8::1")));
        assert_eq!(parse_address("[2001:db8::1]"), Some(ip("2001:db8::1")));
        assert_eq!(parse_address("2001:db8::1"), Some(ip("2001:db8::1")));
        assert_eq!(parse_address("   "), None);
        assert_eq!(parse_address("203.0.113.4:notaport:extra"), None);
    }
}
