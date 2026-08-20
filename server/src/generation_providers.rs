//! The image models, behind one trait.
//!
//! Two providers because one of them will be down, rate-limited, or refusing a
//! prompt the other accepts, and a generation queue that stops when a single
//! vendor has a bad afternoon is not a feature anyone can rely on.
//!
//! Everything here is deliberately thin. The interesting parts of generation —
//! what to ask for, how many times, what it may cost — live in
//! `crate::generation`, which is testable without a network. This module knows
//! only how to turn one prompt into one PNG or one honest explanation of why
//! not.

use std::time::Duration;

use base64::Engine as _;
use serde::Deserialize;
use tracing::warn;

use crate::generation::{ImageProvider, ProviderOutcome};

/// How long to wait on an image model.
///
/// Generously longer than the JWT verifier's five seconds because these
/// genuinely take tens of seconds, and short enough that a hung vendor frees
/// the worker rather than occupying it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Largest image body worth reading.
///
/// A cap rather than trust: an unbounded read from a third party is an
/// out-of-memory condition with extra steps.
const MAX_IMAGE_BYTES: usize = 8 * 1024 * 1024;

/// How many reference images to actually send.
///
/// A cap rather than a preference: every reference is another image uploaded on
/// every attempt, and past a handful they stop steering and start costing.
const MAX_REFERENCES: usize = 4;

/// The size to ask a model for, given the size the texture actually needs.
///
/// Providers make a handful of fixed sizes; a coat is 768x64 and a sheet is
/// 320x320, and none of those is on any vendor's menu. This used to be sent
/// verbatim as `"size": "768x64"`, which no image API accepts — never noticed,
/// because nothing had ever called it.
///
/// So the request asks for the *closest shape the vendor makes*, and the pixel
/// pass crops a band of the right proportions and mirrors it. Wide targets get
/// the landscape option because a coat is twelve cells long and horizontal
/// detail is what it is short of; everything else gets the square.
fn provider_size(width: u32, height: u32) -> (u32, u32) {
    if width >= height * 2 {
        (1536, 1024)
    } else {
        (1024, 1024)
    }
}

fn client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
}

/// Decode a base64 image payload, refusing anything implausibly large before
/// it is allocated.
fn decode_image(encoded: &str) -> Result<Vec<u8>, String> {
    // Base64 is 4 characters per 3 bytes, so the encoded length bounds the
    // decoded one without decoding it first.
    if encoded.len() / 4 * 3 > MAX_IMAGE_BYTES {
        return Err(format!("image exceeds the {MAX_IMAGE_BYTES}-byte limit"));
    }
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| format!("image was not base64: {error}"))
}

/// Whether a provider's error text is it declining rather than failing.
///
/// The distinction matters because one is worth retrying and the other is
/// worth stopping: retrying a refusal just buys the same refusal again.
fn reads_as_refusal(status: reqwest::StatusCode, body: &str) -> bool {
    if status == reqwest::StatusCode::BAD_REQUEST {
        let lowered = body.to_lowercase();
        return lowered.contains("content_policy")
            || lowered.contains("content policy")
            || lowered.contains("safety")
            || lowered.contains("moderation")
            || lowered.contains("rejected");
    }
    false
}

#[derive(Debug, Deserialize)]
struct OpenAiImageResponse {
    data: Vec<OpenAiImage>,
}

#[derive(Debug, Deserialize)]
struct OpenAiImage {
    b64_json: Option<String>,
}

/// OpenAI's image endpoint.
pub struct OpenAiImages {
    client: reqwest::Client,
    api_key: String,
    model: String,
    /// Cost of one image, for the job ledger. Configured rather than looked up,
    /// because a budget that depends on a price list we cannot see is not a
    /// budget.
    usd_micros_per_image: u64,
}

impl OpenAiImages {
    /// Build from the environment, or `None` when it is not configured.
    ///
    /// Absence disables the provider rather than half-enabling it, the same way
    /// the replay store treats a missing bucket.
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("SNAKETRON_OPENAI_API_KEY").ok()?;
        if api_key.trim().is_empty() {
            return None;
        }
        Some(Self {
            client: client().ok()?,
            api_key,
            model: std::env::var("SNAKETRON_OPENAI_IMAGE_MODEL")
                .unwrap_or_else(|_| "gpt-image-1".to_string()),
            usd_micros_per_image: std::env::var("SNAKETRON_OPENAI_IMAGE_USD_MICROS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(40_000),
        })
    }
}

#[async_trait::async_trait]
impl ImageProvider for OpenAiImages {
    fn name(&self) -> &'static str {
        "openai"
    }

    async fn generate(
        &self,
        prompt: &str,
        width: u32,
        height: u32,
        references: &[Vec<u8>],
    ) -> ProviderOutcome {
        let (ask_width, ask_height) = provider_size(width, height);
        let size = format!("{ask_width}x{ask_height}");

        // With references this is an *edit*, which is a different endpoint and
        // a multipart body — the images are files, not JSON. Without them it is
        // a plain generation.
        let request = if references.is_empty() {
            self.client
                .post("https://api.openai.com/v1/images/generations")
                .bearer_auth(&self.api_key)
                .json(&serde_json::json!({
                    "model": self.model,
                    "prompt": prompt,
                    "size": size,
                    "n": 1,
                }))
        } else {
            let mut form = reqwest::multipart::Form::new()
                .text("model", self.model.clone())
                .text("prompt", prompt.to_string())
                .text("size", size)
                .text("n", "1");
            for (index, reference) in references.iter().take(MAX_REFERENCES).enumerate() {
                let part = reqwest::multipart::Part::bytes(reference.clone())
                    .file_name(format!("reference-{index}.png"))
                    .mime_str("image/png");
                match part {
                    Ok(part) => form = form.part("image[]", part),
                    Err(error) => {
                        return ProviderOutcome::Unavailable {
                            detail: format!("openai reference rejected: {error}"),
                        };
                    }
                }
            }
            self.client
                .post("https://api.openai.com/v1/images/edits")
                .bearer_auth(&self.api_key)
                .multipart(form)
        };

        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("openai request failed: {error}"),
                };
            }
        };

        let status = response.status();
        let text = match response.text().await {
            Ok(text) => text,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("openai response unreadable: {error}"),
                };
            }
        };

        if !status.is_success() {
            if reads_as_refusal(status, &text) {
                return ProviderOutcome::Refused {
                    reason: "the model declined this prompt".to_string(),
                };
            }
            warn!(%status, "openai image generation failed");
            return ProviderOutcome::Unavailable {
                detail: format!("openai returned {status}"),
            };
        }

        let parsed: OpenAiImageResponse = match serde_json::from_str(&text) {
            Ok(parsed) => parsed,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("openai response did not parse: {error}"),
                };
            }
        };

        let Some(encoded) = parsed.data.into_iter().find_map(|image| image.b64_json) else {
            return ProviderOutcome::Unavailable {
                detail: "openai returned no image".to_string(),
            };
        };

        match decode_image(&encoded) {
            Ok(png) => ProviderOutcome::Image {
                png,
                usd_micros: self.usd_micros_per_image,
            },
            Err(detail) => ProviderOutcome::Unavailable { detail },
        }
    }
}

/// OpenRouter, as the alternate.
pub struct OpenRouterImages {
    client: reqwest::Client,
    api_key: String,
    model: String,
    usd_micros_per_image: u64,
}

impl OpenRouterImages {
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("SNAKETRON_OPENROUTER_API_KEY").ok()?;
        if api_key.trim().is_empty() {
            return None;
        }
        Some(Self {
            client: client().ok()?,
            api_key,
            model: std::env::var("SNAKETRON_OPENROUTER_IMAGE_MODEL")
                .unwrap_or_else(|_| "google/gemini-2.5-flash-image".to_string()),
            usd_micros_per_image: std::env::var("SNAKETRON_OPENROUTER_IMAGE_USD_MICROS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(30_000),
        })
    }
}

#[derive(Debug, Deserialize)]
struct OpenRouterResponse {
    choices: Vec<OpenRouterChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterChoice {
    message: OpenRouterMessage,
}

#[derive(Debug, Deserialize)]
struct OpenRouterMessage {
    #[serde(default)]
    images: Vec<OpenRouterImage>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterImage {
    image_url: OpenRouterImageUrl,
}

#[derive(Debug, Deserialize)]
struct OpenRouterImageUrl {
    url: String,
}

#[async_trait::async_trait]
impl ImageProvider for OpenRouterImages {
    fn name(&self) -> &'static str {
        "openrouter"
    }

    async fn generate(
        &self,
        prompt: &str,
        _width: u32,
        _height: u32,
        _references: &[Vec<u8>],
    ) -> ProviderOutcome {
        let body = serde_json::json!({
            "model": self.model,
            "messages": [{ "role": "user", "content": prompt }],
            "modalities": ["image", "text"],
        });

        let response = match self
            .client
            .post("https://openrouter.ai/api/v1/chat/completions")
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("openrouter request failed: {error}"),
                };
            }
        };

        let status = response.status();
        let text = match response.text().await {
            Ok(text) => text,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("openrouter response unreadable: {error}"),
                };
            }
        };

        if !status.is_success() {
            if reads_as_refusal(status, &text) {
                return ProviderOutcome::Refused {
                    reason: "the model declined this prompt".to_string(),
                };
            }
            return ProviderOutcome::Unavailable {
                detail: format!("openrouter returned {status}"),
            };
        }

        let parsed: OpenRouterResponse = match serde_json::from_str(&text) {
            Ok(parsed) => parsed,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("openrouter response did not parse: {error}"),
                };
            }
        };

        let Some(url) = parsed
            .choices
            .into_iter()
            .flat_map(|choice| choice.message.images)
            .map(|image| image.image_url.url)
            .next()
        else {
            return ProviderOutcome::Unavailable {
                detail: "openrouter returned no image".to_string(),
            };
        };

        // Images come back as data URIs rather than links, which is what lets
        // this stay a single request with no second fetch to bound.
        let Some(encoded) = url.split(";base64,").nth(1) else {
            return ProviderOutcome::Unavailable {
                detail: "openrouter image was not an inline data URI".to_string(),
            };
        };

        match decode_image(encoded) {
            Ok(png) => ProviderOutcome::Image {
                png,
                usd_micros: self.usd_micros_per_image,
            },
            Err(detail) => ProviderOutcome::Unavailable { detail },
        }
    }
}

/// Every provider this deployment has keys for, in preference order.
/// Gemini, which takes prompt and reference images in one ordinary request.
///
/// Preferred first when it is configured, and the reason is measured rather
/// than assumed: on the same tiger-coat prompt it and OpenAI both returned
/// usable art, and both reach a perfect wrap once the pixel pass mirrors them,
/// so what separates them is that references are a native part of this
/// request shape rather than a second endpoint with a different body.
pub struct GeminiImages {
    client: reqwest::Client,
    api_key: String,
    model: String,
    usd_micros_per_image: u64,
}

impl GeminiImages {
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("SNAKETRON_GEMINI_API_KEY")
            .or_else(|_| std::env::var("GEMINI_API_KEY"))
            .ok()?;
        if api_key.trim().is_empty() {
            return None;
        }
        Some(Self {
            client: client().ok()?,
            api_key,
            model: std::env::var("SNAKETRON_GEMINI_IMAGE_MODEL")
                .unwrap_or_else(|_| "gemini-2.5-flash-image".to_string()),
            usd_micros_per_image: std::env::var("SNAKETRON_GEMINI_IMAGE_USD_MICROS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(39_000),
        })
    }
}

#[derive(Debug, Deserialize)]
struct GeminiResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
}

#[derive(Debug, Deserialize)]
struct GeminiCandidate {
    #[serde(default)]
    content: Option<GeminiContent>,
}

#[derive(Debug, Deserialize)]
struct GeminiContent {
    #[serde(default)]
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiPart {
    #[serde(default)]
    inline_data: Option<GeminiInlineData>,
}

#[derive(Debug, Deserialize)]
struct GeminiInlineData {
    data: String,
}

#[async_trait::async_trait]
impl ImageProvider for GeminiImages {
    fn name(&self) -> &'static str {
        "gemini"
    }

    async fn generate(
        &self,
        prompt: &str,
        width: u32,
        height: u32,
        references: &[Vec<u8>],
    ) -> ProviderOutcome {
        // References and the prompt are parts of one message, which is the
        // whole reason this provider is tried first.
        let mut parts = vec![serde_json::json!({ "text": prompt })];
        for reference in references.iter().take(MAX_REFERENCES) {
            parts.push(serde_json::json!({
                "inline_data": {
                    "mime_type": "image/png",
                    "data": base64::engine::general_purpose::STANDARD.encode(reference),
                }
            }));
        }
        let (ask_width, ask_height) = provider_size(width, height);
        let body = serde_json::json!({
            "contents": [{ "parts": parts }],
            "generationConfig": {
                "imageConfig": {
                    "aspectRatio": if ask_width > ask_height { "3:2" } else { "1:1" },
                }
            }
        });

        let response = match self
            .client
            .post(format!(
                "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent",
                self.model
            ))
            .header("x-goog-api-key", &self.api_key)
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("gemini request failed: {error}"),
                };
            }
        };

        let status = response.status();
        let text = match response.text().await {
            Ok(text) => text,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("gemini response unreadable: {error}"),
                };
            }
        };

        if !status.is_success() {
            warn!(status = %status, "gemini refused a generation");
            return if reads_as_refusal(status, &text) {
                ProviderOutcome::Refused {
                    reason: "the model declined this prompt".to_string(),
                }
            } else {
                ProviderOutcome::Unavailable {
                    detail: format!("gemini returned {status}"),
                }
            };
        }

        let parsed: GeminiResponse = match serde_json::from_str(&text) {
            Ok(parsed) => parsed,
            Err(error) => {
                return ProviderOutcome::Unavailable {
                    detail: format!("gemini response unparsed: {error}"),
                };
            }
        };

        let encoded = parsed
            .candidates
            .into_iter()
            .filter_map(|candidate| candidate.content)
            .flat_map(|content| content.parts)
            .find_map(|part| part.inline_data.map(|inline| inline.data));
        let Some(encoded) = encoded else {
            // A model answering in words rather than pixels has declined,
            // even when it does so with a 200.
            return ProviderOutcome::Refused {
                reason: "the model returned no image".to_string(),
            };
        };

        match decode_image(&encoded) {
            Ok(png) => ProviderOutcome::Image {
                png,
                usd_micros: self.usd_micros_per_image,
            },
            Err(detail) => ProviderOutcome::Unavailable {
                detail: format!("gemini {detail}"),
            },
        }
    }
}

pub fn configured_providers() -> Vec<Box<dyn ImageProvider>> {
    let mut providers: Vec<Box<dyn ImageProvider>> = Vec::new();
    // Gemini first: same quality on the prompts this was measured against, and
    // references ride in the ordinary request instead of a second endpoint.
    if let Some(gemini) = GeminiImages::from_env() {
        providers.push(Box::new(gemini));
    }
    if let Some(openai) = OpenAiImages::from_env() {
        providers.push(Box::new(openai));
    }
    if let Some(openrouter) = OpenRouterImages::from_env() {
        providers.push(Box::new(openrouter));
    }
    providers
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A decline and a failure are different outcomes with different costs:
    /// one is retried, the other is not.
    #[test]
    fn a_content_decline_is_told_apart_from_an_outage() {
        let bad = reqwest::StatusCode::BAD_REQUEST;
        assert!(reads_as_refusal(
            bad,
            r#"{"error":{"code":"content_policy_violation"}}"#
        ));
        assert!(reads_as_refusal(bad, "request rejected by safety system"));
        assert!(reads_as_refusal(bad, "Moderation blocked this prompt"));

        assert!(
            !reads_as_refusal(bad, r#"{"error":"invalid size"}"#),
            "a malformed request is our bug, not a decline"
        );
        assert!(!reads_as_refusal(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "slow down"
        ));
        assert!(!reads_as_refusal(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "safety"
        ));
    }

    #[test]
    fn an_oversized_payload_is_refused_before_it_is_decoded() {
        let encoded = "A".repeat(MAX_IMAGE_BYTES * 2);
        let error = decode_image(&encoded).expect_err("this would be 12MB decoded");
        assert!(error.contains("limit"));
    }

    #[test]
    fn a_real_payload_decodes() {
        let png = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        let encoded = base64::engine::general_purpose::STANDARD.encode(png);
        assert_eq!(decode_image(&encoded).expect("valid base64"), png);
        assert!(decode_image("not base64!!").is_err());
    }

    /// Absence disables a provider rather than producing one that fails on
    /// every call.
    #[test]
    fn a_provider_with_no_key_is_absent_rather_than_broken() {
        // SAFETY: single-threaded test, and the value is restored immediately.
        unsafe { std::env::remove_var("SNAKETRON_OPENAI_API_KEY") };
        assert!(OpenAiImages::from_env().is_none());

        unsafe { std::env::set_var("SNAKETRON_OPENAI_API_KEY", "   ") };
        assert!(
            OpenAiImages::from_env().is_none(),
            "a blank key is not a key"
        );
        unsafe { std::env::remove_var("SNAKETRON_OPENAI_API_KEY") };
    }
}
