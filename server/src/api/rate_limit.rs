use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::warn;

/// Rate limiter state that tracks requests per IP address
#[derive(Clone)]
pub struct RateLimiter {
    /// Map of IP addresses to their request history
    requests: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
    /// Maximum number of requests allowed
    max_requests: usize,
    /// Time window for rate limiting
    window: Duration,
}

impl RateLimiter {
    /// Creates a new rate limiter
    ///
    /// # Arguments
    /// * `max_requests` - Maximum number of requests allowed per window
    /// * `window` - Time window for rate limiting
    pub fn new(max_requests: usize, window: Duration) -> Self {
        Self {
            requests: Arc::new(Mutex::new(HashMap::new())),
            max_requests,
            window,
        }
    }

    /// Checks if a request from the given IP is allowed
    pub async fn check_request(&self, ip: String) -> bool {
        let mut requests = self.requests.lock().await;
        let now = Instant::now();

        // Get or create request history for this IP
        let request_times = requests.entry(ip.clone()).or_insert_with(Vec::new);

        // Remove old requests outside the window
        request_times.retain(|&time| now.duration_since(time) < self.window);

        // Check if we're under the limit
        if request_times.len() < self.max_requests {
            request_times.push(now);
            true
        } else {
            warn!("Rate limit exceeded for IP: {}", ip);
            false
        }
    }

    /// Periodically cleans up old entries to prevent memory growth
    pub async fn cleanup(&self) {
        let mut requests = self.requests.lock().await;
        let now = Instant::now();

        // Remove IPs that haven't made requests recently
        requests.retain(|_, times| {
            times.retain(|&time| now.duration_since(time) < self.window);
            !times.is_empty()
        });
    }
}

/// Extracts IP address from request headers or connection info
fn get_client_ip(headers: &HeaderMap) -> String {
    // Try to get IP from X-Forwarded-For header (for proxies)
    if let Some(forwarded_for) = headers.get("x-forwarded-for")
        && let Ok(value) = forwarded_for.to_str()
    {
        // Take the first IP in the chain
        if let Some(ip) = value.split(',').next() {
            return ip.trim().to_string();
        }
    }

    // Try to get IP from X-Real-IP header
    if let Some(real_ip) = headers.get("x-real-ip")
        && let Ok(value) = real_ip.to_str()
    {
        return value.to_string();
    }

    // Default to a placeholder if we can't determine the IP
    // In production, you might want to use the actual socket address
    "unknown".to_string()
}

/// Middleware for rate limiting specific endpoints
pub async fn rate_limit_middleware(
    State(limiter): State<RateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers().clone();
    let ip = get_client_ip(&headers);

    if !limiter.check_request(ip).await {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": "Too many requests. Please try again later."
            })),
        )
            .into_response();
    }

    next.run(request).await
}

/// Process-local limiter for public, resource-intensive routes. Unlike an IP
/// key derived from forwarding headers, this ceiling cannot be bypassed by
/// rotating a caller-controlled `X-Forwarded-For` value. Deployments may layer
/// a trusted-proxy-aware per-client limit in front of this global backstop.
pub async fn global_rate_limit_middleware(
    State(limiter): State<RateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    if !limiter.check_request("public-resource-route".into()).await {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": "Too many requests. Please try again later."
            })),
        )
            .into_response();
    }

    next.run(request).await
}

/// Creates a rate limiting layer for specific routes
pub fn rate_limit_layer(max_requests: usize, window_seconds: u64) -> RateLimiter {
    let limiter = RateLimiter::new(max_requests, Duration::from_secs(window_seconds));

    // Spawn a cleanup task that runs periodically
    let cleanup_limiter = limiter.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // Cleanup every 5 minutes
        loop {
            interval.tick().await;
            cleanup_limiter.cleanup().await;
        }
    });

    limiter
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, http::Request, middleware, routing::get};
    use tower::ServiceExt;

    #[tokio::test]
    async fn public_route_global_limit_cannot_be_evaded_with_spoofed_forwarded_for() {
        let limiter = RateLimiter::new(1, Duration::from_secs(60));
        let app = Router::new()
            .route("/api/games/7/replay", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn_with_state(
                limiter,
                global_rate_limit_middleware,
            ));

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/games/7/replay")
                    .header("x-forwarded-for", "198.51.100.1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(
                Request::builder()
                    .uri("/api/games/7/replay")
                    .header("x-forwarded-for", "203.0.113.99")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
