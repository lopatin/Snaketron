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
}

impl TargetOptions {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            region: None,
        }
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
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
            .redirect(reqwest::redirect::Policy::limited(5))
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
        let api_origin = derive_api_origin(&requested_target)?;
        let (regions, regions_endpoint) = self.fetch_regions(&api_origin).await?;
        if regions.is_empty() {
            return Err(TargetError::NoRegions {
                url: regions_endpoint.url,
            });
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
        let selected_origin = normalized_region_origin(&selected.region)?.to_string();
        let websocket_url = websocket_url_for_region(&selected.region)?.to_string();
        let initial_user_counts = self.sample_user_counts(&api_origin).await?;
        let initial_server_counts = self.sample_server_counts(&api_origin).await?;

        Ok(TargetPreflight {
            requested_target: requested_target.to_string(),
            requested_region: requested_region.map(ToOwned::to_owned),
            api_origin: api_origin.to_string(),
            regions_endpoint,
            region_selection,
            selected_region: selected.region.clone(),
            selected_origin,
            websocket_url,
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

/// Return the configured regional WebSocket URL or derive `/ws` from origin.
pub fn websocket_url_for_region(region: &RegionMetadata) -> Result<Url, TargetError> {
    let origin = normalized_region_origin(region)?;
    let mut ws_url = if region.ws_url.trim().is_empty() {
        origin.clone()
    } else {
        Url::parse(region.ws_url.trim()).map_err(|error| TargetError::InvalidTarget {
            target: region.ws_url.clone(),
            reason: format!(
                "region '{}' has an invalid WebSocket URL: {error}",
                region.id
            ),
        })?
    };

    let desired_scheme = match ws_url.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" => "ws",
        "wss" => "wss",
        scheme => {
            return Err(TargetError::InvalidTarget {
                target: ws_url.to_string(),
                reason: format!(
                    "region '{}' WebSocket URL uses unsupported scheme '{scheme}'",
                    region.id
                ),
            });
        }
    };
    ws_url
        .set_scheme(desired_scheme)
        .map_err(|_| TargetError::InvalidTarget {
            target: ws_url.to_string(),
            reason: "could not set WebSocket scheme".to_string(),
        })?;
    if ws_url.host_str().is_none() || !ws_url.username().is_empty() || ws_url.password().is_some() {
        return Err(TargetError::InvalidTarget {
            target: ws_url.to_string(),
            reason: "WebSocket URL must have a host and cannot contain credentials".to_string(),
        });
    }
    if origin.scheme() == "https" && ws_url.scheme() != "wss" {
        return Err(TargetError::InvalidTarget {
            target: ws_url.to_string(),
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
    let origin =
        normalize_target_url(&region.origin).map_err(|error| TargetError::InvalidTarget {
            target: region.origin.clone(),
            reason: format!("region '{}' has an invalid origin: {error}", region.id),
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
    fn maps_public_hosts_to_the_api_origin_but_keeps_local_targets() {
        let public = normalize_target_url("https://use1.snaketron.io/play").unwrap();
        assert_eq!(
            derive_api_origin(&public).unwrap().as_str(),
            "https://api.snaketron.io/"
        );

        let local = normalize_target_url("http://localhost:8080/anything").unwrap();
        assert_eq!(
            derive_api_origin(&local).unwrap().as_str(),
            "http://localhost:8080/"
        );
    }

    #[test]
    fn uses_or_derives_a_secure_websocket_url() {
        let configured = region(
            "use1",
            "https://use1.snaketron.io",
            "wss://games.snaketron.io/ignored?token=none",
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
