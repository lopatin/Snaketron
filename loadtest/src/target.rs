//! Target discovery and preflight checks for the Snaketron load test.
//!
//! Production traffic enters through a public API origin and is then routed to
//! a regional WebSocket origin.  This module keeps that routing knowledge out
//! of the session runner.  It also deliberately owns construction of the HTTP
//! client so certificate validation cannot accidentally be disabled by a
//! caller.

use reqwest::header::{HeaderMap, SET_COOKIE};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const LOAD_TEST_USER_AGENT: &str = concat!("snaketron-loadtest/", env!("CARGO_PKG_VERSION"));
const STICKY_COOKIE_NAME: &str = "snaketron_sticky";

/// Inputs used to resolve and preflight a target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetOptions {
    /// Main site, API origin, or regional origin. A missing scheme implies HTTPS.
    pub target: String,
    /// Optional region ID. Matching is case-insensitive.
    pub region: Option<String>,
    /// Keep API discovery, health probes, and session endpoints on `target`.
    pub require_same_origin: bool,
}

impl TargetOptions {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            region: None,
            require_same_origin: false,
        }
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn requiring_same_origin(mut self) -> Self {
        self.require_same_origin = true;
        self
    }
}

/// Region metadata returned by `GET /api/regions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionMetadata {
    pub id: String,
    pub name: String,
    pub origin: String,
    #[serde(default)]
    pub ws_url: String,
}

/// Why a region was selected for the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegionSelection {
    Explicit,
    LowestLatencyHealthy,
}

/// A non-sensitive, run-local alias for a load-balancer backend hint.
///
/// The cookie value is intentionally never stored here. Aliases only remain
/// comparable while callers reuse the same [`BackendHintRegistry`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendHint {
    pub source: String,
    pub identifier: String,
}

/// Common measurements for a successful HTTP request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointObservation {
    pub url: String,
    pub status_code: u16,
    pub latency_ms: u64,
    pub observed_at_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_hint: Option<BackendHint>,
}

/// Result of probing one regional health endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionHealthObservation {
    pub region: RegionMetadata,
    pub health_url: String,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub observed_at_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_hint: Option<BackendHint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl RegionHealthObservation {
    fn failure(region: RegionMetadata, health_url: String, error: impl Into<String>) -> Self {
        Self {
            region,
            health_url,
            healthy: false,
            status_code: None,
            latency_ms: None,
            observed_at_unix_ms: unix_time_ms(),
            backend_hint: None,
            error: Some(error.into()),
        }
    }
}

/// One aggregate regional user-count sample.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserCountObservation {
    #[serde(flatten)]
    pub endpoint: EndpointObservation,
    pub counts: BTreeMap<String, u32>,
}

/// Active TTL-backed server-instance counts per region. The representation is
/// intentionally identical to user counts, but the endpoint never returns raw
/// server identifiers.
pub type ServerCountObservation = UserCountObservation;

/// Serializable outcome of resolving and preflighting the target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetPreflight {
    pub requested_target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_region: Option<String>,
    pub api_origin: String,
    pub regions_endpoint: EndpointObservation,
    pub region_selection: RegionSelection,
    pub selected_region: RegionMetadata,
    pub selected_origin: String,
    pub websocket_url: String,
    pub selected_health: RegionHealthObservation,
    pub region_health: Vec<RegionHealthObservation>,
    pub initial_user_counts: UserCountObservation,
    pub initial_server_counts: ServerCountObservation,
    pub completed_at_unix_ms: u64,
}

impl TargetPreflight {
    pub fn selected_region_user_count(&self) -> Option<u32> {
        self.initial_user_counts
            .counts
            .get(&self.selected_region.id)
            .copied()
    }
}

/// Errors returned before any virtual-user sessions are launched.
#[derive(Debug)]
pub enum TargetError {
    InvalidTarget {
        target: String,
        reason: String,
    },
    ClientBuild {
        source: reqwest::Error,
    },
    Request {
        operation: &'static str,
        url: String,
        source: reqwest::Error,
    },
    UnexpectedStatus {
        operation: &'static str,
        url: String,
        status: StatusCode,
    },
    InvalidResponse {
        operation: &'static str,
        url: String,
        source: reqwest::Error,
    },
    NoRegions {
        url: String,
    },
    RegionNotFound {
        requested: String,
        available: Vec<String>,
    },
    RegionUnhealthy {
        region: String,
        reason: String,
    },
    NoHealthyRegions {
        summary: String,
    },
}

impl fmt::Display for TargetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTarget { target, reason } => {
                write!(formatter, "invalid target '{target}': {reason}")
            }
            Self::ClientBuild { source } => {
                write!(
                    formatter,
                    "failed to build strict-TLS HTTP client: {source}"
                )
            }
            Self::Request {
                operation,
                url,
                source,
            } => write!(formatter, "{operation} request to {url} failed: {source}"),
            Self::UnexpectedStatus {
                operation,
                url,
                status,
            } => write!(
                formatter,
                "{operation} request to {url} returned HTTP {}",
                status.as_u16()
            ),
            Self::InvalidResponse {
                operation,
                url,
                source,
            } => write!(
                formatter,
                "{operation} response from {url} was invalid: {source}"
            ),
            Self::NoRegions { url } => {
                write!(formatter, "region discovery at {url} returned no regions")
            }
            Self::RegionNotFound {
                requested,
                available,
            } => write!(
                formatter,
                "requested region '{requested}' was not found (available: {})",
                available.join(", ")
            ),
            Self::RegionUnhealthy { region, reason } => {
                write!(
                    formatter,
                    "requested region '{region}' is not healthy: {reason}"
                )
            }
            Self::NoHealthyRegions { summary } => {
                write!(
                    formatter,
                    "no healthy Snaketron regions were found: {summary}"
                )
            }
        }
    }
}

impl Error for TargetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ClientBuild { source }
            | Self::Request { source, .. }
            | Self::InvalidResponse { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Converts raw sticky-cookie values into harmless run-local aliases.
///
/// The custom `Debug` implementation only exposes the number of observed
/// values, so accidentally logging the registry cannot reveal cookie contents.
#[derive(Clone, Default)]
pub struct BackendHintRegistry {
    aliases: Arc<Mutex<HashMap<String, String>>>,
}

impl fmt::Debug for BackendHintRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendHintRegistry")
            .field("observed_backend_count", &self.observed_backend_count())
            .finish()
    }
}

impl BackendHintRegistry {
    /// Inspect response headers and return a safe alias when Snaketron's sticky
    /// backend cookie is present. Raw cookie values remain private to this map.
    pub fn observe_headers(&self, headers: &HeaderMap) -> Option<BackendHint> {
        let raw_value = sticky_cookie_value(headers)?;
        self.observe_sticky_value(&raw_value)
    }

    /// Register a sticky-cookie value obtained from a non-reqwest handshake.
    /// This keeps WebSocket clients on a different `http` crate version from
    /// needing to pass their header map through reqwest's header types.
    pub fn observe_sticky_value(&self, raw_value: &str) -> Option<BackendHint> {
        let raw_value = raw_value.trim();
        if raw_value.is_empty() {
            return None;
        }

        let mut aliases = self.aliases.lock().unwrap_or_else(|lock| lock.into_inner());
        let next_identifier = format!("backend-{:04}", aliases.len() + 1);
        let identifier = aliases
            .entry(raw_value.to_string())
            .or_insert(next_identifier)
            .clone();

        Some(BackendHint {
            source: format!("cookie:{STICKY_COOKIE_NAME}"),
            identifier,
        })
    }

    pub fn observed_backend_count(&self) -> usize {
        self.aliases
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .len()
    }
}

/// Resolver that owns a certificate-validating reqwest client and backend
/// alias registry. Reuse it for later user-count samples so aliases correlate.
#[derive(Debug, Clone)]
pub struct TargetResolver {
    client: Client,
    backend_hints: BackendHintRegistry,
}

impl TargetResolver {
    pub fn new(request_timeout: Duration) -> Result<Self, TargetError> {
        // reqwest validates hostnames and certificate chains by default. Do not
        // add `danger_accept_invalid_certs` here, including for local testing.
        let client = Client::builder()
            .connect_timeout(request_timeout)
            .timeout(request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(LOAD_TEST_USER_AGENT)
            .build()
            .map_err(|source| TargetError::ClientBuild { source })?;

        Ok(Self {
            client,
            backend_hints: BackendHintRegistry::default(),
        })
    }

    pub fn with_default_timeout() -> Result<Self, TargetError> {
        Self::new(DEFAULT_REQUEST_TIMEOUT)
    }

    pub fn backend_hints(&self) -> BackendHintRegistry {
        self.backend_hints.clone()
    }

    /// Clone the same strict-TLS client for guest-authentication requests.
    pub fn http_client(&self) -> Client {
        self.client.clone()
    }

    /// Resolve the API and regional origins, probe every region, select one,
    /// and capture the initial aggregate user count.
    pub async fn preflight(&self, options: &TargetOptions) -> Result<TargetPreflight, TargetError> {
        let requested_target = normalize_target_url(&options.target)?;
        let api_origin = resolve_api_origin(&requested_target, options.require_same_origin)?;
        let (mut regions, regions_endpoint) = self.fetch_regions(&api_origin).await?;
        if regions.is_empty() {
            return Err(TargetError::NoRegions {
                url: regions_endpoint.url,
            });
        }
        normalize_region_endpoints(&mut regions)?;
        if options.require_same_origin {
            require_and_rewrite_same_origin_regions(&mut regions, &requested_target)?;
        }

        // Probe serially. Each latency is measured around only its own request,
        // so selection remains meaningful while avoiding another async runtime
        // dependency in this low-volume preflight path.
        let mut region_health = Vec::with_capacity(regions.len());
        for region in regions {
            region_health.push(self.probe_region(region).await);
        }

        let requested_region = options
            .region
            .as_deref()
            .map(str::trim)
            .filter(|region| !region.is_empty());
        let selected = select_region(&region_health, requested_region)?.clone();
        let region_selection = if requested_region.is_some() {
            RegionSelection::Explicit
        } else {
            RegionSelection::LowestLatencyHealthy
        };
        let selected_origin = normalized_region_origin(&selected.region)?;
        let websocket_url = websocket_url_for_region(&selected.region)?;
        let initial_user_counts = self.sample_user_counts(&api_origin).await?;
        let initial_server_counts = self.sample_server_counts(&api_origin).await?;

        Ok(TargetPreflight {
            requested_target: requested_target.to_string(),
            requested_region: requested_region.map(ToOwned::to_owned),
            api_origin: api_origin.to_string(),
            regions_endpoint,
            region_selection,
            selected_region: selected.region.clone(),
            selected_origin: selected_origin.to_string(),
            websocket_url: websocket_url.to_string(),
            selected_health: selected,
            region_health,
            initial_user_counts,
            initial_server_counts,
            completed_at_unix_ms: unix_time_ms(),
        })
    }

    /// Capture another aggregate count sample during ramp-up, hold, or drain.
    pub async fn sample_user_counts_from_origin(
        &self,
        api_origin: &str,
    ) -> Result<UserCountObservation, TargetError> {
        let api_origin = normalize_target_url(api_origin)?;
        self.sample_user_counts(&api_origin).await
    }

    /// Capture active server-instance counts without exposing server IDs.
    pub async fn sample_server_counts_from_origin(
        &self,
        api_origin: &str,
    ) -> Result<ServerCountObservation, TargetError> {
        let api_origin = normalize_target_url(api_origin)?;
        self.sample_server_counts(&api_origin).await
    }

    async fn fetch_regions(
        &self,
        api_origin: &Url,
    ) -> Result<(Vec<RegionMetadata>, EndpointObservation), TargetError> {
        let url = endpoint_url(api_origin, "/api/regions")?;
        let started = Instant::now();
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| TargetError::Request {
                operation: "region discovery",
                url: url.to_string(),
                source,
            })?;
        let status = response.status();
        let backend_hint = self.backend_hints.observe_headers(response.headers());
        if !status.is_success() {
            return Err(TargetError::UnexpectedStatus {
                operation: "region discovery",
                url: url.to_string(),
                status,
            });
        }

        let regions = response
            .json::<Vec<RegionMetadata>>()
            .await
            .map_err(|source| TargetError::InvalidResponse {
                operation: "region discovery",
                url: url.to_string(),
                source,
            })?;
        let endpoint = EndpointObservation {
            url: url.to_string(),
            status_code: status.as_u16(),
            latency_ms: elapsed_ms(started),
            observed_at_unix_ms: unix_time_ms(),
            backend_hint,
        };

        Ok((regions, endpoint))
    }

    async fn probe_region(&self, region: RegionMetadata) -> RegionHealthObservation {
        let origin = match normalized_region_origin(&region) {
            Ok(origin) => origin,
            Err(error) => {
                return RegionHealthObservation::failure(region, String::new(), error.to_string());
            }
        };
        let health_url = match endpoint_url(&origin, "/api/health") {
            Ok(url) => url,
            Err(error) => {
                return RegionHealthObservation::failure(
                    region,
                    origin.to_string(),
                    error.to_string(),
                );
            }
        };
        let started = Instant::now();
        let response = match self.client.get(health_url.clone()).send().await {
            Ok(response) => response,
            Err(source) => {
                return RegionHealthObservation {
                    region,
                    health_url: health_url.to_string(),
                    healthy: false,
                    status_code: None,
                    latency_ms: Some(elapsed_ms(started)),
                    observed_at_unix_ms: unix_time_ms(),
                    backend_hint: None,
                    error: Some(format!("health request failed: {source}")),
                };
            }
        };

        let status = response.status();
        let backend_hint = self.backend_hints.observe_headers(response.headers());
        let payload = if status.is_success() {
            response.json::<HealthPayload>().await
        } else {
            // Consume the body so the connection can be reused, but never put
            // an arbitrary proxy response body into the report.
            let _ = response.bytes().await;
            return RegionHealthObservation {
                region,
                health_url: health_url.to_string(),
                healthy: false,
                status_code: Some(status.as_u16()),
                latency_ms: Some(elapsed_ms(started)),
                observed_at_unix_ms: unix_time_ms(),
                backend_hint,
                error: Some(format!("health endpoint returned HTTP {}", status.as_u16())),
            };
        };

        match payload {
            Ok(payload) if payload.status.eq_ignore_ascii_case("ok") => RegionHealthObservation {
                region,
                health_url: health_url.to_string(),
                healthy: true,
                status_code: Some(status.as_u16()),
                latency_ms: Some(elapsed_ms(started)),
                observed_at_unix_ms: unix_time_ms(),
                backend_hint,
                error: None,
            },
            Ok(payload) => RegionHealthObservation {
                region,
                health_url: health_url.to_string(),
                healthy: false,
                status_code: Some(status.as_u16()),
                latency_ms: Some(elapsed_ms(started)),
                observed_at_unix_ms: unix_time_ms(),
                backend_hint,
                error: Some(format!(
                    "health payload reported unexpected status '{}'",
                    payload.status
                )),
            },
            Err(source) => RegionHealthObservation {
                region,
                health_url: health_url.to_string(),
                healthy: false,
                status_code: Some(status.as_u16()),
                latency_ms: Some(elapsed_ms(started)),
                observed_at_unix_ms: unix_time_ms(),
                backend_hint,
                error: Some(format!("health response was not valid JSON: {source}")),
            },
        }
    }

    async fn sample_user_counts(
        &self,
        api_origin: &Url,
    ) -> Result<UserCountObservation, TargetError> {
        self.sample_regional_counts(
            api_origin,
            "/api/regions/user-counts",
            "regional user-count sample",
        )
        .await
    }

    async fn sample_server_counts(
        &self,
        api_origin: &Url,
    ) -> Result<ServerCountObservation, TargetError> {
        self.sample_regional_counts(
            api_origin,
            "/api/regions/server-counts",
            "regional server-count sample",
        )
        .await
    }

    async fn sample_regional_counts(
        &self,
        api_origin: &Url,
        path: &'static str,
        operation: &'static str,
    ) -> Result<UserCountObservation, TargetError> {
        let url = endpoint_url(api_origin, path)?;
        let started = Instant::now();
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| TargetError::Request {
                operation,
                url: url.to_string(),
                source,
            })?;
        let status = response.status();
        let backend_hint = self.backend_hints.observe_headers(response.headers());
        if !status.is_success() {
            return Err(TargetError::UnexpectedStatus {
                operation,
                url: url.to_string(),
                status,
            });
        }

        let counts = response
            .json::<BTreeMap<String, u32>>()
            .await
            .map_err(|source| TargetError::InvalidResponse {
                operation,
                url: url.to_string(),
                source,
            })?;

        Ok(UserCountObservation {
            endpoint: EndpointObservation {
                url: url.to_string(),
                status_code: status.as_u16(),
                latency_ms: elapsed_ms(started),
                observed_at_unix_ms: unix_time_ms(),
                backend_hint,
            },
            counts,
        })
    }
}

#[derive(Debug, Deserialize)]
struct HealthPayload {
    status: String,
}

/// Convenience entry point for callers that only need one preflight.
pub async fn preflight_target(options: &TargetOptions) -> Result<TargetPreflight, TargetError> {
    TargetResolver::with_default_timeout()?
        .preflight(options)
        .await
}

/// Parse a target into a normalized HTTP(S) origin.
pub fn normalize_target_url(raw: &str) -> Result<Url, TargetError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(TargetError::InvalidTarget {
            target: raw.to_string(),
            reason: "target cannot be empty".to_string(),
        });
    }

    let candidate = if raw.contains("://") {
        raw.to_string()
    } else {
        format!("https://{raw}")
    };
    let mut url = Url::parse(&candidate).map_err(|error| TargetError::InvalidTarget {
        target: raw.to_string(),
        reason: error.to_string(),
    })?;
    validate_http_origin(&url, raw)?;
    url.set_path("/");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

/// Resolve the API origin. Public snaketron.io site/region hosts share the
/// dedicated `api.snaketron.io` origin; local and custom targets are same-origin.
pub fn derive_api_origin(target: &Url) -> Result<Url, TargetError> {
    validate_http_origin(target, target.as_str())?;
    let mut api_origin = target.clone();
    if target.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("snaketron.io")
            || host.to_ascii_lowercase().ends_with(".snaketron.io")
    }) {
        api_origin
            .set_host(Some("api.snaketron.io"))
            .map_err(|_| TargetError::InvalidTarget {
                target: target.to_string(),
                reason: "could not derive the Snaketron API host".to_string(),
            })?;
        api_origin
            .set_port(None)
            .map_err(|_| TargetError::InvalidTarget {
                target: target.to_string(),
                reason: "could not clear the API port".to_string(),
            })?;
        api_origin
            .set_scheme("https")
            .map_err(|_| TargetError::InvalidTarget {
                target: target.to_string(),
                reason: "could not require HTTPS for the production API".to_string(),
            })?;
    }
    api_origin.set_path("/");
    api_origin.set_query(None);
    api_origin.set_fragment(None);
    Ok(api_origin)
}

fn resolve_api_origin(target: &Url, require_same_origin: bool) -> Result<Url, TargetError> {
    if require_same_origin {
        validate_http_origin(target, target.as_str())?;
        Ok(target.clone())
    } else {
        derive_api_origin(target)
    }
}

/// Derive the WebSocket endpoint represented by an explicit HTTP(S) target.
pub fn websocket_url_for_target(target: &Url) -> Result<Url, TargetError> {
    validate_http_origin(target, target.as_str())?;
    let mut ws_url = target.clone();
    let desired_scheme = match target.scheme() {
        "http" => "ws",
        "https" => "wss",
        _ => unreachable!("validate_http_origin accepted only HTTP(S)"),
    };
    ws_url
        .set_scheme(desired_scheme)
        .map_err(|_| TargetError::InvalidTarget {
            target: target.to_string(),
            reason: "could not set WebSocket scheme".to_string(),
        })?;
    ws_url.set_path("/ws");
    ws_url.set_query(None);
    ws_url.set_fragment(None);
    Ok(ws_url)
}

/// Return the configured regional WebSocket URL or derive `/ws` from origin.
pub fn websocket_url_for_region(region: &RegionMetadata) -> Result<Url, TargetError> {
    let origin = normalized_region_origin(region)?;
    let target_label = format!("region '{}' WebSocket endpoint", region.id);
    let mut ws_url = if region.ws_url.trim().is_empty() {
        origin.clone()
    } else {
        Url::parse(region.ws_url.trim()).map_err(|error| TargetError::InvalidTarget {
            target: target_label.clone(),
            reason: format!("invalid WebSocket URL: {error}"),
        })?
    };
    if ws_url.query().is_some() || ws_url.fragment().is_some() {
        return Err(TargetError::InvalidTarget {
            target: target_label.clone(),
            reason: "WebSocket URL cannot contain a query string or fragment".to_string(),
        });
    }

    let desired_scheme = match ws_url.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" => "ws",
        "wss" => "wss",
        _ => {
            return Err(TargetError::InvalidTarget {
                target: target_label.clone(),
                reason: "WebSocket URL uses an unsupported scheme; expected ws or wss".to_string(),
            });
        }
    };
    ws_url
        .set_scheme(desired_scheme)
        .map_err(|_| TargetError::InvalidTarget {
            target: target_label.clone(),
            reason: "could not set WebSocket scheme".to_string(),
        })?;
    if ws_url.host_str().is_none() || !ws_url.username().is_empty() || ws_url.password().is_some() {
        return Err(TargetError::InvalidTarget {
            target: target_label.clone(),
            reason: "WebSocket URL must have a host and cannot contain credentials".to_string(),
        });
    }
    if origin.scheme() == "https" && ws_url.scheme() != "wss" {
        return Err(TargetError::InvalidTarget {
            target: target_label,
            reason: format!(
                "region '{}' cannot downgrade an HTTPS origin to an insecure WebSocket",
                region.id
            ),
        });
    }
    ws_url.set_path("/ws");
    ws_url.set_query(None);
    ws_url.set_fragment(None);
    Ok(ws_url)
}

fn require_and_rewrite_same_origin_regions(
    regions: &mut [RegionMetadata],
    requested_target: &Url,
) -> Result<(), TargetError> {
    let expected_websocket = websocket_url_for_target(requested_target)?;
    for region in regions.iter() {
        let advertised_origin = normalized_region_origin(region)?;
        if advertised_origin.origin() != requested_target.origin() {
            return Err(TargetError::InvalidTarget {
                target: requested_target.to_string(),
                reason: format!(
                    "region '{}' advertised a cross-origin HTTP endpoint in same-origin mode",
                    region.id
                ),
            });
        }
        let advertised_websocket = websocket_url_for_region(region)?;
        if advertised_websocket.origin() != expected_websocket.origin() {
            return Err(TargetError::InvalidTarget {
                target: requested_target.to_string(),
                reason: format!(
                    "region '{}' advertised a cross-origin WebSocket endpoint in same-origin mode",
                    region.id
                ),
            });
        }
    }
    for region in regions {
        region.origin = requested_target.to_string();
        region.ws_url = expected_websocket.to_string();
    }
    Ok(())
}

fn normalize_region_endpoints(regions: &mut [RegionMetadata]) -> Result<(), TargetError> {
    let endpoints = regions
        .iter()
        .map(|region| {
            Ok((
                normalized_region_origin(region)?,
                websocket_url_for_region(region)?,
            ))
        })
        .collect::<Result<Vec<_>, TargetError>>()?;
    for (region, (origin, websocket)) in regions.iter_mut().zip(endpoints) {
        region.origin = origin.to_string();
        region.ws_url = websocket.to_string();
    }
    Ok(())
}

/// Select an explicit healthy region, or the lowest-latency healthy region.
/// Latency ties are broken by region ID for deterministic reports.
pub fn select_region<'a>(
    observations: &'a [RegionHealthObservation],
    requested_region: Option<&str>,
) -> Result<&'a RegionHealthObservation, TargetError> {
    if let Some(requested) = requested_region
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let observation = observations
            .iter()
            .find(|observation| observation.region.id.eq_ignore_ascii_case(requested))
            .ok_or_else(|| TargetError::RegionNotFound {
                requested: requested.to_string(),
                available: observations
                    .iter()
                    .map(|observation| observation.region.id.clone())
                    .collect(),
            })?;
        if !observation.healthy {
            return Err(TargetError::RegionUnhealthy {
                region: observation.region.id.clone(),
                reason: observation
                    .error
                    .clone()
                    .unwrap_or_else(|| "health check failed".to_string()),
            });
        }
        return Ok(observation);
    }

    observations
        .iter()
        .filter(|observation| observation.healthy && observation.latency_ms.is_some())
        .min_by(|left, right| {
            left.latency_ms
                .cmp(&right.latency_ms)
                .then_with(|| left.region.id.cmp(&right.region.id))
        })
        .ok_or_else(|| TargetError::NoHealthyRegions {
            summary: observations
                .iter()
                .map(|observation| {
                    format!(
                        "{}: {}",
                        observation.region.id,
                        observation.error.as_deref().unwrap_or("unhealthy")
                    )
                })
                .collect::<Vec<_>>()
                .join("; "),
        })
}

fn normalized_region_origin(region: &RegionMetadata) -> Result<Url, TargetError> {
    let origin = normalize_target_url(&region.origin).map_err(|error| {
        let reason = match error {
            TargetError::InvalidTarget { reason, .. } if reason.contains("credentials") => {
                "HTTP origin cannot contain credentials".to_string()
            }
            TargetError::InvalidTarget { reason, .. } if reason.contains("scheme") => {
                "HTTP origin uses an unsupported scheme; expected http or https".to_string()
            }
            TargetError::InvalidTarget { reason, .. } if reason.contains("host") => {
                "HTTP origin must include a host".to_string()
            }
            _ => "invalid HTTP origin".to_string(),
        };
        TargetError::InvalidTarget {
            target: format!("region '{}' HTTP origin", region.id),
            reason,
        }
    })?;
    Ok(origin)
}

fn endpoint_url(origin: &Url, path: &str) -> Result<Url, TargetError> {
    origin
        .join(path)
        .map_err(|error| TargetError::InvalidTarget {
            target: origin.to_string(),
            reason: format!("could not construct endpoint '{path}': {error}"),
        })
}

fn validate_http_origin(url: &Url, original: &str) -> Result<(), TargetError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(TargetError::InvalidTarget {
            target: original.to_string(),
            reason: format!(
                "unsupported scheme '{}'; expected http or https",
                url.scheme()
            ),
        });
    }
    if url.host_str().is_none() {
        return Err(TargetError::InvalidTarget {
            target: original.to_string(),
            reason: "URL must include a host".to_string(),
        });
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(TargetError::InvalidTarget {
            target: original.to_string(),
            reason: "URL cannot contain credentials".to_string(),
        });
    }
    Ok(())
}

fn sticky_cookie_value(headers: &HeaderMap) -> Option<String> {
    for header in headers.get_all(SET_COOKIE).iter() {
        let Ok(header) = header.to_str() else {
            continue;
        };
        let Some(cookie_pair) = header.split(';').next() else {
            continue;
        };
        let Some((name, value)) = cookie_pair.split_once('=') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case(STICKY_COOKIE_NAME) && !value.trim().is_empty() {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    async fn spawn_mock_origin(response: String) -> (Url, Arc<AtomicUsize>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let request_count = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                request_count.fetch_add(1, Ordering::SeqCst);
                let mut request = [0_u8; 4096];
                let _ = socket.read(&mut request).await;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        (
            Url::parse(&format!("http://{address}/")).unwrap(),
            requests,
            task,
        )
    }

    fn mock_response(status: &str, headers: &[(&str, &str)], body: &str) -> String {
        let mut response = format!("HTTP/1.1 {status}\r\n");
        for (name, value) in headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str(&format!(
            "Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ));
        response
    }

    fn region(id: &str, origin: &str, ws_url: &str) -> RegionMetadata {
        RegionMetadata {
            id: id.to_string(),
            name: id.to_uppercase(),
            origin: origin.to_string(),
            ws_url: ws_url.to_string(),
        }
    }

    fn health(id: &str, healthy: bool, latency_ms: Option<u64>) -> RegionHealthObservation {
        RegionHealthObservation {
            region: region(id, &format!("https://{id}.example.com"), ""),
            health_url: format!("https://{id}.example.com/api/health"),
            healthy,
            status_code: healthy.then_some(200),
            latency_ms,
            observed_at_unix_ms: 1,
            backend_hint: None,
            error: (!healthy).then(|| "unavailable".to_string()),
        }
    }

    #[test]
    fn normalizes_target_to_an_origin_and_defaults_to_https() {
        let url = normalize_target_url("snaketron.io/play?mode=duel#top").unwrap();
        assert_eq!(url.as_str(), "https://snaketron.io/");
    }

    #[test]
    fn rejects_non_http_target_schemes_and_credentials() {
        assert!(normalize_target_url("wss://use1.snaketron.io/ws").is_err());
        assert!(normalize_target_url("https://user:secret@example.com").is_err());
    }

    #[test]
    fn maps_public_hosts_to_the_api_origin_but_keeps_custom_targets() {
        for host in [
            "snaketron.io",
            "www.snaketron.io",
            "api.snaketron.io",
            "use1.snaketron.io",
            "euw1.snaketron.io",
            "stg-29938169949-1.snaketron.io",
        ] {
            let public = normalize_target_url(&format!("https://{host}/play")).unwrap();
            assert_eq!(
                derive_api_origin(&public).unwrap().as_str(),
                "https://api.snaketron.io/"
            );
        }

        let staging =
            normalize_target_url("https://stg-29938169949-1.snaketron.io/private?token=x").unwrap();
        assert_eq!(
            resolve_api_origin(&staging, true).unwrap().as_str(),
            "https://stg-29938169949-1.snaketron.io/"
        );

        let local = normalize_target_url("http://localhost:8080/anything").unwrap();
        assert_eq!(
            derive_api_origin(&local).unwrap().as_str(),
            "http://localhost:8080/"
        );
    }

    #[test]
    fn same_origin_mode_validates_then_rewrites_region_endpoints() {
        let target = normalize_target_url("https://stg-123-1.snaketron.io/").unwrap();
        let mut regions = vec![region(
            "use1",
            "https://stg-123-1.snaketron.io/ignored",
            "wss://stg-123-1.snaketron.io/ignored",
        )];

        require_and_rewrite_same_origin_regions(&mut regions, &target).unwrap();
        assert_eq!(regions[0].origin, "https://stg-123-1.snaketron.io/");
        assert_eq!(regions[0].ws_url, "wss://stg-123-1.snaketron.io/ws");

        let mut escaped = vec![region(
            "use1",
            "https://use1.snaketron.io",
            "wss://use1.snaketron.io/ws",
        )];
        assert!(require_and_rewrite_same_origin_regions(&mut escaped, &target).is_err());
    }

    #[tokio::test]
    async fn strict_client_does_not_follow_a_cross_origin_redirect() {
        let (second_origin, second_requests, second_task) = spawn_mock_origin(mock_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            "[]",
        ))
        .await;
        let location = second_origin.join("api/regions").unwrap().to_string();
        let (target, target_requests, target_task) =
            spawn_mock_origin(mock_response("302 Found", &[("Location", &location)], "")).await;

        let resolver = TargetResolver::new(Duration::from_secs(2)).unwrap();
        let result = resolver
            .preflight(&TargetOptions::new(target.as_str()).requiring_same_origin())
            .await;

        assert!(matches!(
            result,
            Err(TargetError::UnexpectedStatus {
                operation: "region discovery",
                status: StatusCode::FOUND,
                ..
            })
        ));
        assert_eq!(target_requests.load(Ordering::SeqCst), 1);
        assert_eq!(second_requests.load(Ordering::SeqCst), 0);
        target_task.abort();
        second_task.abort();
    }

    #[tokio::test]
    async fn same_origin_mode_rejects_advertised_cross_origin_before_probing_it() {
        let (second_origin, second_requests, second_task) = spawn_mock_origin(mock_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            r#"{"status":"ok"}"#,
        ))
        .await;
        let regions = serde_json::to_string(&vec![RegionMetadata {
            id: "use1".to_string(),
            name: "USE1".to_string(),
            origin: second_origin.to_string(),
            ws_url: websocket_url_for_target(&second_origin)
                .unwrap()
                .to_string(),
        }])
        .unwrap();
        let (target, target_requests, target_task) = spawn_mock_origin(mock_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            &regions,
        ))
        .await;

        let resolver = TargetResolver::new(Duration::from_secs(2)).unwrap();
        let error = resolver
            .preflight(&TargetOptions::new(target.as_str()).requiring_same_origin())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("cross-origin HTTP endpoint"));
        assert_eq!(target_requests.load(Ordering::SeqCst), 1);
        assert_eq!(second_requests.load(Ordering::SeqCst), 0);
        target_task.abort();
        second_task.abort();
    }

    #[test]
    fn uses_or_derives_a_secure_websocket_url() {
        let configured = region(
            "use1",
            "https://use1.snaketron.io",
            "wss://games.snaketron.io/ignored",
        );
        assert_eq!(
            websocket_url_for_region(&configured).unwrap().as_str(),
            "wss://games.snaketron.io/ws"
        );

        let derived = region("local", "http://localhost:8080", "");
        assert_eq!(
            websocket_url_for_region(&derived).unwrap().as_str(),
            "ws://localhost:8080/ws"
        );

        let target = normalize_target_url("http://localhost:8080/").unwrap();
        assert_eq!(
            websocket_url_for_target(&target).unwrap().as_str(),
            "ws://localhost:8080/ws"
        );
    }

    #[test]
    fn rejects_websocket_query_secrets_without_echoing_them() {
        let configured = region(
            "use1",
            "https://use1.snaketron.io",
            "wss://use1.snaketron.io/ws?token=do-not-log#also-secret",
        );
        let error = websocket_url_for_region(&configured)
            .unwrap_err()
            .to_string();
        assert!(error.contains("query string or fragment"));
        assert!(!error.contains("do-not-log"));
        assert!(!error.contains("also-secret"));
    }

    #[test]
    fn region_http_origin_errors_and_normalization_do_not_leak_metadata() {
        let invalid = region(
            "use1",
            "https://user:credential-secret@example.test/?token=query-secret",
            "",
        );
        let error = normalized_region_origin(&invalid).unwrap_err().to_string();
        assert!(error.contains("region 'use1' HTTP origin"));
        assert!(!error.contains("credential-secret"));
        assert!(!error.contains("query-secret"));

        let query_only = region(
            "use1",
            "https://example.test/private?token=query-secret#fragment-secret",
            "",
        );
        assert_eq!(
            normalized_region_origin(&query_only).unwrap().as_str(),
            "https://example.test/"
        );
    }

    #[test]
    fn rejects_a_websocket_security_downgrade() {
        let insecure = region(
            "use1",
            "https://use1.snaketron.io",
            "ws://use1.snaketron.io/ws",
        );
        assert!(websocket_url_for_region(&insecure).is_err());
    }

    #[test]
    fn explicit_selection_is_case_insensitive_and_does_not_fall_back() {
        let observations = vec![
            health("use1", true, Some(30)),
            health("euw1", false, Some(10)),
        ];
        assert_eq!(
            select_region(&observations, Some("USE1"))
                .unwrap()
                .region
                .id,
            "use1"
        );
        assert!(matches!(
            select_region(&observations, Some("euw1")),
            Err(TargetError::RegionUnhealthy { .. })
        ));
        assert!(matches!(
            select_region(&observations, Some("ap1")),
            Err(TargetError::RegionNotFound { .. })
        ));
    }

    #[test]
    fn automatic_selection_uses_latency_then_region_id() {
        let observations = vec![
            health("use2", true, Some(12)),
            health("use1", true, Some(12)),
            health("euw1", false, Some(1)),
        ];
        assert_eq!(
            select_region(&observations, None).unwrap().region.id,
            "use1"
        );
    }

    #[test]
    fn automatic_selection_requires_a_healthy_latency_sample() {
        let observations = vec![health("use1", false, Some(2)), health("euw1", true, None)];
        assert!(matches!(
            select_region(&observations, None),
            Err(TargetError::NoHealthyRegions { .. })
        ));
    }

    #[test]
    fn sticky_backend_values_become_run_local_aliases() {
        let registry = BackendHintRegistry::default();
        let mut first_headers = HeaderMap::new();
        first_headers.append(
            SET_COOKIE,
            HeaderValue::from_static("session=secret; Path=/; HttpOnly"),
        );
        first_headers.append(
            SET_COOKIE,
            HeaderValue::from_static(
                "snaketron_sticky=raw-backend-value-one; Path=/; HttpOnly; Secure",
            ),
        );
        let first = registry.observe_headers(&first_headers).unwrap();
        assert_eq!(first.identifier, "backend-0001");
        assert!(!format!("{first:?}").contains("raw-backend-value-one"));
        assert!(!format!("{registry:?}").contains("raw-backend-value-one"));

        let same = registry.observe_headers(&first_headers).unwrap();
        assert_eq!(same.identifier, first.identifier);

        let mut second_headers = HeaderMap::new();
        second_headers.insert(
            SET_COOKIE,
            HeaderValue::from_static("snaketron_sticky=raw-backend-value-two; Path=/"),
        );
        let second = registry.observe_headers(&second_headers).unwrap();
        assert_eq!(second.identifier, "backend-0002");
        assert_eq!(registry.observed_backend_count(), 2);
    }
}
