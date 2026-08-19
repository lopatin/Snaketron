//! Permanent, public read surface for finished matches.
//!
//! Everything else that knows about a game is bounded: the live snapshot lives
//! in Redis for half an hour, the completed-game row and its replay object age
//! out under retention, and joining over the WebSocket requires having played
//! in the match. A link someone posts on the internet has to outlive all of
//! that, so this module reads only the canonical, TTL-free match summary and
//! serves it two ways:
//!
//! * `GET /api/games/:game_id/summary` — JSON for the in-app result page.
//! * `GET /g/:game_id` — a complete, self-contained HTML document with Open
//!   Graph / Twitter / JSON-LD metadata. Social crawlers never execute the
//!   single-page app's JavaScript, so a share preview can only come from a
//!   response like this one.
//!
//! The HTML path deliberately uses the *same* path shape as the client route
//! (`/g/:game_id`), so pointing a `/g/*` CDN behaviour at this origin makes the
//! shared URL crawlable without touching either code base.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Serialize;
use std::sync::Arc;
use tracing::warn;

use crate::db::Database;
use crate::db::models::{MatchHistoryPlayer, MatchHistorySummary};

/// Site origin used for canonical URLs, share links, and absolute asset URLs
/// in crawler metadata. Absolute URLs are mandatory here: crawlers do not honor
/// `<base href>` and never run the app that would resolve a relative one.
const PUBLIC_SITE_URL_ENV: &str = "SNAKETRON_PUBLIC_SITE_URL";
const DEFAULT_PUBLIC_SITE_URL: &str = "https://snaketron.io";

/// Public pages are cached hard because a finished match is immutable.
const PUBLIC_SUMMARY_CACHE_CONTROL: &str = "public, max-age=600, stale-while-revalidate=86400";

/// The two reads this module needs, named separately from `Database` so the
/// routes can be exercised without standing up a whole persistence layer —
/// the same split the replay routes use for `ReplayGameReader`.
#[async_trait::async_trait]
pub(crate) trait PublicGameReader: Send + Sync {
    async fn summary(&self, game_id: i32) -> anyhow::Result<Option<MatchHistorySummary>>;
    async fn latest_allocated_game_id(&self) -> anyhow::Result<Option<i32>>;
}

struct DatabasePublicGameReader {
    db: Arc<dyn Database>,
}

#[async_trait::async_trait]
impl PublicGameReader for DatabasePublicGameReader {
    async fn summary(&self, game_id: i32) -> anyhow::Result<Option<MatchHistorySummary>> {
        self.db.get_public_game_summary(game_id).await
    }

    async fn latest_allocated_game_id(&self) -> anyhow::Result<Option<i32>> {
        self.db.latest_allocated_game_id().await
    }
}

#[derive(Clone)]
pub struct PublicGameState {
    games: Arc<dyn PublicGameReader>,
    pub site_url: String,
}

impl PublicGameState {
    pub fn new(db: Arc<dyn Database>) -> Self {
        Self {
            games: Arc::new(DatabasePublicGameReader { db }),
            site_url: resolve_site_url(std::env::var(PUBLIC_SITE_URL_ENV).ok().as_deref()),
        }
    }
}

/// Normalize a configured site origin to a scheme-qualified, slash-free base.
/// A malformed value falls back to the canonical origin rather than emitting
/// half-formed URLs into crawler metadata.
pub(crate) fn resolve_site_url(configured: Option<&str>) -> String {
    let trimmed = configured.unwrap_or("").trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        DEFAULT_PUBLIC_SITE_URL.to_string()
    }
}

/// One player's public result. `xp_gained` and `mmr_delta` are deliberately
/// absent: they are progression details that belong to the player's own
/// history, not on a page anyone can open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct PublicGamePlayer {
    pub user_id: u32,
    pub username: String,
    pub team_id: Option<u8>,
    pub score: u32,
    pub team_score: Option<u32>,
    /// `win` | `loss` | `draw` | `removed` | `completed`.
    pub outcome: String,
    pub is_winner: bool,
}

/// The permanent public projection of a finished match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct PublicGameSummary {
    pub game_id: u32,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub started_at_ms: i64,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub ended_at_ms: i64,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub duration_ms: u64,
    pub mode: String,
    pub mode_label: String,
    pub queue_mode: String,
    pub is_team_game: bool,
    pub players: Vec<PublicGamePlayer>,
    pub winner_user_ids: Vec<u32>,
    /// Human-readable result line, also used verbatim as the share description.
    pub headline: String,
    /// Whether the Play-of-the-Game clip and full replay may still be fetched.
    /// Those are retention-bounded even though this summary is not.
    pub replay_available: bool,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub replay_available_until_ms: i64,
    pub share_url: String,
}

/// What the public endpoint knows about a game id.
///
/// `pending` is the honest answer for a match that is being played right now:
/// the durable row only appears at completion, so a link shared mid-match has
/// to resolve to "not finished yet" rather than to a 404 that says the match
/// never existed. Shaped like the highlight route's response for consistency.
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum PublicGameResponse {
    Final { summary: Box<PublicGameSummary> },
    Pending { game_id: u32, share_url: String },
}

#[derive(Serialize)]
struct PublicGameError {
    error: &'static str,
}

pub fn build_public_game_routes(db: Arc<dyn Database>) -> Router<crate::api::auth::AuthState> {
    public_game_route_template().with_state::<crate::api::auth::AuthState>(PublicGameState::new(db))
}

pub(crate) fn public_game_route_template() -> Router<PublicGameState> {
    Router::new()
        .route("/api/games/:game_id/summary", get(get_public_game_summary))
        .route("/g/:game_id", get(get_public_game_page))
        .route("/robots.txt", get(get_robots_txt))
}

/// Game ids are decimal `u32`s minted by a monotonic counter. Parsing here
/// rather than with a typed extractor keeps a junk path segment a clean 400
/// instead of an axum rejection page.
fn parse_public_game_id(raw: &str) -> Option<i32> {
    if raw.is_empty() || raw.len() > 10 || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    raw.parse::<i32>().ok().filter(|game_id| *game_id > 0)
}

async fn load_public_summary(
    state: &PublicGameState,
    game_id: i32,
) -> Result<Option<PublicGameResponse>, ()> {
    let summary = match state.games.summary(game_id).await {
        // Load-test matches are real rows, but they are not anybody's match.
        Ok(Some(summary)) if summary.is_stress_test => return Ok(None),
        Ok(Some(summary)) => Some(summary),
        Ok(None) => None,
        Err(error) => {
            warn!(game_id, %error, "Failed to read the public match summary");
            return Err(());
        }
    };

    if let Some(summary) = summary {
        return Ok(Some(PublicGameResponse::Final {
            summary: Box::new(project_public_summary(&summary, &state.site_url)),
        }));
    }

    // No summary yet. An id at or below the allocation high-water mark has
    // been handed to a real match, so the honest answer is "not finished",
    // not "no such match".
    match state.games.latest_allocated_game_id().await {
        Ok(Some(latest)) if game_id <= latest => Ok(Some(PublicGameResponse::Pending {
            game_id: game_id as u32,
            share_url: format!("{}/g/{game_id}", state.site_url),
        })),
        Ok(_) => Ok(None),
        Err(error) => {
            // Failing closed here would turn a live match's share link into a
            // permanent-looking 404, so treat an unreadable counter as "unknown".
            warn!(game_id, %error, "Failed to read the game id allocation mark");
            Ok(None)
        }
    }
}

async fn get_public_game_summary(
    State(state): State<PublicGameState>,
    Path(raw_game_id): Path<String>,
) -> Response {
    let Some(game_id) = parse_public_game_id(&raw_game_id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(PublicGameError {
                error: "invalid game id",
            }),
        )
            .into_response();
    };

    match load_public_summary(&state, game_id).await {
        Ok(Some(summary)) => {
            let mut response = Json(summary).into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static(PUBLIC_SUMMARY_CACHE_CONTROL),
            );
            response
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(PublicGameError {
                error: "game not found",
            }),
        )
            .into_response(),
        Err(()) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(PublicGameError {
                error: "match summary unavailable",
            }),
        )
            .into_response(),
    }
}

async fn get_public_game_page(
    State(state): State<PublicGameState>,
    Path(raw_game_id): Path<String>,
) -> Response {
    let Some(game_id) = parse_public_game_id(&raw_game_id) else {
        return html_response(
            StatusCode::BAD_REQUEST,
            render_missing_game_page(&state.site_url, "That is not a Snaketron match link."),
            "no-store",
        );
    };

    match load_public_summary(&state, game_id).await {
        Ok(Some(PublicGameResponse::Final { summary })) => html_response(
            StatusCode::OK,
            render_game_page(&summary, &state.site_url),
            PUBLIC_SUMMARY_CACHE_CONTROL,
        ),
        // Deliberately `noindex` and barely cached: this page has no result on
        // it yet, and indexing it would enter the empty version into search.
        Ok(Some(PublicGameResponse::Pending { game_id, .. })) => html_response(
            StatusCode::OK,
            render_pending_game_page(&state.site_url, game_id),
            "public, max-age=15",
        ),
        // A genuine 404 is the right answer for a crawler: it de-indexes the
        // URL instead of recording an empty page under a 200.
        Ok(None) => html_response(
            StatusCode::NOT_FOUND,
            render_missing_game_page(&state.site_url, "This match could not be found."),
            "public, max-age=60",
        ),
        Err(()) => html_response(
            StatusCode::SERVICE_UNAVAILABLE,
            render_missing_game_page(
                &state.site_url,
                "Match results are temporarily unavailable.",
            ),
            "no-store",
        ),
    }
}

async fn get_robots_txt(State(state): State<PublicGameState>) -> Response {
    let body = format!(
        "User-agent: *\nAllow: /g/\nDisallow: /api/\nDisallow: /qa/\nSitemap: {}/sitemap.xml\n",
        state.site_url
    );
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
        .into_response()
}

fn html_response(status: StatusCode, body: String, cache_control: &'static str) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, cache_control),
        ],
        body,
    )
        .into_response()
}

pub(crate) fn project_public_summary(
    summary: &MatchHistorySummary,
    site_url: &str,
) -> PublicGameSummary {
    let is_team_game = summary
        .players
        .iter()
        .any(|player| player.team_id.is_some());
    let players: Vec<PublicGamePlayer> = summary
        .players
        .iter()
        .map(|player| PublicGamePlayer {
            user_id: player.user_id,
            username: player.username.clone(),
            team_id: player.team_id,
            score: player.score,
            team_score: player.team_score,
            outcome: player.outcome.clone(),
            is_winner: summary.winner_user_ids.contains(&player.user_id),
        })
        .collect();

    PublicGameSummary {
        game_id: summary.game_id,
        started_at_ms: summary.started_at_ms,
        ended_at_ms: summary.ended_at_ms,
        duration_ms: summary.duration_ms,
        mode: summary.mode.clone(),
        mode_label: summary.mode_label.clone(),
        queue_mode: summary.queue_mode.clone(),
        is_team_game,
        headline: headline_for(summary),
        winner_user_ids: summary.winner_user_ids.clone(),
        replay_available: summary.snapshot_available_until_ms
            > chrono::Utc::now().timestamp_millis(),
        replay_available_until_ms: summary.snapshot_available_until_ms,
        share_url: format!("{site_url}/g/{}", summary.game_id),
        players,
    }
}

/// The single sentence that appears as the page description, the social-card
/// subtitle, and the in-app result headline. Kept server-side so all three
/// always agree.
fn headline_for(summary: &MatchHistorySummary) -> String {
    let mode = &summary.mode_label;
    if summary.players.is_empty() {
        return format!("A Snaketron {mode} match.");
    }
    if summary.mode == "solo" {
        let score = summary.players.iter().map(|p| p.score).max().unwrap_or(0);
        let name = &summary.players[0].username;
        return format!("{name} scored {score} in a Snaketron {mode} run.");
    }

    let winners: Vec<&MatchHistoryPlayer> = summary
        .players
        .iter()
        .filter(|player| summary.winner_user_ids.contains(&player.user_id))
        .collect();
    let scoreline = scoreline_for(summary);

    if winners.is_empty() {
        return format!("{} — a drawn Snaketron {mode}.", scoreline);
    }
    let names = join_names(winners.iter().map(|player| player.username.as_str()));
    format!("{names} won a Snaketron {mode}, {scoreline}.")
}

/// `12–7` for a two-sided match, otherwise the descending point spread.
fn scoreline_for(summary: &MatchHistorySummary) -> String {
    let mut team_scores: Vec<(u8, u32)> = Vec::new();
    for player in &summary.players {
        if let (Some(team_id), Some(team_score)) = (player.team_id, player.team_score)
            && !team_scores.iter().any(|(id, _)| *id == team_id)
        {
            team_scores.push((team_id, team_score));
        }
    }
    if team_scores.len() >= 2 {
        team_scores.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
        return team_scores
            .iter()
            .map(|(_, score)| score.to_string())
            .collect::<Vec<_>>()
            .join("–");
    }

    let mut scores: Vec<u32> = summary.players.iter().map(|player| player.score).collect();
    scores.sort_unstable_by(|left, right| right.cmp(left));
    scores
        .iter()
        .map(|score| score.to_string())
        .collect::<Vec<_>>()
        .join("–")
}

fn join_names<'a>(names: impl Iterator<Item = &'a str>) -> String {
    let names: Vec<&str> = names.collect();
    match names.len() {
        0 => String::new(),
        1 => names[0].to_string(),
        2 => format!("{} and {}", names[0], names[1]),
        _ => format!(
            "{} and {}",
            names[..names.len() - 1].join(", "),
            names[names.len() - 1]
        ),
    }
}

pub(crate) fn format_duration(duration_ms: u64) -> String {
    let total_seconds = duration_ms / 1_000;
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    if minutes == 0 {
        format!("{seconds}s")
    } else {
        format!("{minutes}m {seconds:02}s")
    }
}

fn format_date(ended_at_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ended_at_ms)
        .map(|moment| moment.format("%-d %B %Y").to_string())
        .unwrap_or_default()
}

fn iso_8601(timestamp_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .map(|moment| moment.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

/// Minimal HTML text escaping. Usernames are player-controlled and land in both
/// element text and quoted attribute values, so every one of these five
/// characters has to go — `filter_chat_message` does not run on nicknames.
pub(crate) fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

/// Make a JSON document safe to embed inside a `<script>` element.
///
/// HTML escaping is wrong here — it would corrupt the JSON — but a player
/// nickname containing `</script>` would otherwise close the block and turn
/// structured data into markup. `\u003c` and friends are valid JSON string
/// escapes, so the payload stays parseable while no tag delimiter survives.
pub(crate) fn escape_json_for_script(json: &str) -> String {
    json.replace('<', r"\u003c")
        .replace('>', r"\u003e")
        .replace('&', r"\u0026")
}

fn document(title: &str, head: String, body: String) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
{head}
<style>
:root {{ color-scheme: light; }}
* {{ box-sizing: border-box; }}
body {{ margin: 0; padding: 32px 20px 56px; background: #f8fafc; color: rgba(0,0,0,0.78);
  font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }}
main {{ max-width: 640px; margin: 0 auto; }}
.card {{ padding: 24px; border: 2px solid #d1d5db; border-radius: 8px; background: #fff; }}
.kicker {{ margin: 0 0 6px; color: #6b7280; font-size: 11px; font-weight: 800;
  letter-spacing: 0.8px; text-transform: uppercase; }}
h1 {{ margin: 0 0 10px; font-size: 24px; font-weight: 800; line-height: 1.25; }}
.meta {{ margin: 0; color: #6b7280; font-size: 13px; }}
ol {{ margin: 22px 0 0; padding: 0; list-style: none; }}
li {{ display: flex; align-items: baseline; justify-content: space-between; gap: 12px;
  padding: 10px 0; border-top: 1px solid #eef2f6; font-size: 14px; }}
li b {{ font-weight: 800; }}
.tag {{ margin-left: 8px; padding: 2px 6px; border-radius: 4px; background: #eff6ff;
  color: #2563eb; font-size: 10px; font-weight: 800; letter-spacing: 0.6px; text-transform: uppercase; }}
.cta {{ display: inline-block; margin-top: 24px; padding: 12px 20px; border: 2px solid #3b82f6;
  border-radius: 8px; background: #3b82f6; color: #fff; font-size: 13px; font-weight: 800;
  letter-spacing: 0.7px; text-transform: uppercase; text-decoration: none; }}
.foot {{ margin: 18px 0 0; color: #9ca3af; font-size: 12px; }}
.foot a {{ color: #6b7280; }}
@media (prefers-color-scheme: dark) {{
  :root {{ color-scheme: dark; }}
  body {{ background: #0b1120; color: rgba(255,255,255,0.86); }}
  .card {{ border-color: #1f2937; background: #111827; }}
  li {{ border-top-color: #1f2937; }}
  .tag {{ background: rgba(59,130,246,0.16); color: #93c5fd; }}
}}
</style>
</head>
<body>
<main>
{body}
</main>
</body>
</html>
"#
    )
}

fn render_game_page(summary: &PublicGameSummary, site_url: &str) -> String {
    let title = format!(
        "{} · Snaketron match #{}",
        escape_html(&summary.mode_label),
        summary.game_id
    );
    let description = escape_html(&summary.headline);
    let canonical = escape_html(&summary.share_url);
    let image = format!("{site_url}/SnaketronLogo.png");
    let played_on = format_date(summary.ended_at_ms);

    let structured_data = serde_json::json!({
        "@context": "https://schema.org",
        "@type": "SportsEvent",
        "name": format!("Snaketron {} match #{}", summary.mode_label, summary.game_id),
        "description": summary.headline,
        "url": summary.share_url,
        "startDate": iso_8601(summary.started_at_ms),
        "endDate": iso_8601(summary.ended_at_ms),
        "eventStatus": "https://schema.org/EventScheduled",
        "eventAttendanceMode": "https://schema.org/OnlineEventAttendanceMode",
        "location": {
            "@type": "VirtualLocation",
            "url": site_url,
        },
        "competitor": summary
            .players
            .iter()
            .map(|player| serde_json::json!({"@type": "Person", "name": player.username}))
            .collect::<Vec<_>>(),
    });

    let head = format!(
        r#"<meta name="description" content="{description}">
<link rel="canonical" href="{canonical}">
<meta property="og:type" content="article">
<meta property="og:site_name" content="Snaketron">
<meta property="og:title" content="{title}">
<meta property="og:description" content="{description}">
<meta property="og:url" content="{canonical}">
<meta property="og:image" content="{image}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{title}">
<meta name="twitter:description" content="{description}">
<meta name="twitter:image" content="{image}">
<script type="application/ld+json">{structured_data}</script>"#,
        image = escape_html(&image),
        structured_data = escape_json_for_script(&structured_data.to_string()),
    );

    let mut standings = String::new();
    let mut ranked: Vec<&PublicGamePlayer> = summary.players.iter().collect();
    ranked.sort_by(|left, right| {
        right
            .is_winner
            .cmp(&left.is_winner)
            .then(right.score.cmp(&left.score))
            .then(left.username.cmp(&right.username))
    });
    for player in ranked {
        standings.push_str(&format!(
            "<li><span>{name}{badge}</span><b>{score}</b></li>",
            name = escape_html(&player.username),
            badge = if player.is_winner {
                "<span class=\"tag\">Winner</span>"
            } else {
                ""
            },
            score = player.score,
        ));
    }

    let body = format!(
        r#"<div class="card">
<p class="kicker">Snaketron · {mode}{queue}</p>
<h1>{description}</h1>
<p class="meta">Match #{game_id} · {duration} · {played_on}</p>
<ol>{standings}</ol>
<a class="cta" href="{site_url}/?utm_source=match-share">Play Snaketron</a>
</div>
<p class="foot">A permanent record of a finished Snaketron match. <a href="{site_url}/">snaketron.io</a></p>"#,
        mode = escape_html(&summary.mode_label),
        queue = if summary.queue_mode == "competitive" {
            " · Ranked"
        } else {
            ""
        },
        game_id = summary.game_id,
        duration = format_duration(summary.duration_ms),
        played_on = escape_html(&played_on),
        site_url = escape_html(site_url),
    );

    document(&title, head, body)
}

fn render_pending_game_page(site_url: &str, game_id: u32) -> String {
    let head = format!(
        r#"<meta name="robots" content="noindex">
<meta name="description" content="This Snaketron match is still being played.">
<link rel="canonical" href="{site_url}/g/{game_id}">"#,
        site_url = escape_html(site_url),
    );
    let body = format!(
        r#"<div class="card">
<p class="kicker">Snaketron</p>
<h1>This match is still being played.</h1>
<p class="meta">Match #{game_id} · results appear here as soon as it finishes.</p>
<a class="cta" href="{site_url}/?utm_source=match-share">Play Snaketron</a>
</div>"#,
        site_url = escape_html(site_url),
    );
    document("Snaketron match in progress", head, body)
}

fn render_missing_game_page(site_url: &str, reason: &str) -> String {
    let head = format!(
        r#"<meta name="robots" content="noindex">
<meta name="description" content="{reason}">
<link rel="canonical" href="{site_url}/">"#,
        reason = escape_html(reason),
        site_url = escape_html(site_url),
    );
    let body = format!(
        r#"<div class="card">
<p class="kicker">Snaketron</p>
<h1>{reason}</h1>
<p class="meta">Match links are permanent, so a missing one usually means the id was mistyped.</p>
<a class="cta" href="{site_url}/">Play Snaketron</a>
</div>"#,
        reason = escape_html(reason),
        site_url = escape_html(site_url),
    );
    document("Snaketron", head, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::MatchHistoryPlayer;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn player(
        user_id: u32,
        username: &str,
        team_id: Option<u8>,
        score: u32,
        team_score: Option<u32>,
        outcome: &str,
    ) -> MatchHistoryPlayer {
        MatchHistoryPlayer {
            user_id,
            username: username.to_string(),
            team_id,
            score,
            team_score,
            xp_gained: 40,
            mmr_delta: Some(12),
            outcome: outcome.to_string(),
        }
    }

    fn duel_summary() -> MatchHistorySummary {
        MatchHistorySummary {
            schema_version: 1,
            game_id: 4242,
            started_at_ms: 1_700_000_000_000,
            ended_at_ms: 1_700_000_180_000,
            duration_ms: 180_000,
            mode: "duel".to_string(),
            mode_label: "Duel".to_string(),
            queue_mode: "competitive".to_string(),
            is_private: false,
            is_stress_test: false,
            completed_by_inactivity: false,
            players: vec![
                player(7, "Ada", Some(0), 12, Some(12), "win"),
                player(9, "Grace", Some(1), 7, Some(7), "loss"),
            ],
            winner_user_ids: vec![7],
            snapshot_available_until_ms: i64::MAX,
        }
    }

    #[test]
    fn public_projection_drops_progression_and_marks_winners() {
        let public = project_public_summary(&duel_summary(), "https://snaketron.io");

        assert_eq!(public.share_url, "https://snaketron.io/g/4242");
        assert!(public.is_team_game);
        assert!(public.players.iter().any(|player| player.is_winner));
        // Progression is a private detail and must not appear on a page anyone
        // can open; the serialized shape is the contract, so assert on it.
        let json = serde_json::to_string(&public).expect("serializes");
        assert!(!json.contains("xp"), "public summary leaked xp: {json}");
        assert!(!json.contains("mmr"), "public summary leaked mmr: {json}");
    }

    #[test]
    fn headline_reads_as_a_share_description() {
        assert_eq!(
            headline_for(&duel_summary()),
            "Ada won a Snaketron Duel, 12–7."
        );

        let mut drawn = duel_summary();
        drawn.winner_user_ids.clear();
        assert_eq!(headline_for(&drawn), "12–7 — a drawn Snaketron Duel.");

        let mut solo = duel_summary();
        solo.mode = "solo".to_string();
        solo.mode_label = "Solo".to_string();
        solo.players = vec![player(7, "Ada", None, 31, None, "completed")];
        solo.winner_user_ids.clear();
        assert_eq!(
            headline_for(&solo),
            "Ada scored 31 in a Snaketron Solo run."
        );
    }

    /// Usernames are player-authored and are interpolated into both element
    /// text and quoted attributes. A rendered page must never carry one raw.
    #[test]
    fn rendered_page_escapes_player_authored_text() {
        let mut summary = duel_summary();
        summary.players[0].username = "<script>alert(1)</script>".to_string();
        summary.players[1].username = "\" onload=\"x".to_string();

        let public = project_public_summary(&summary, "https://snaketron.io");
        let html = render_game_page(&public, "https://snaketron.io");

        assert!(!html.contains("<script>alert(1)"));
        assert!(!html.contains("\" onload=\""));
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));

        // The JSON-LD block is the subtle one: HTML escaping would corrupt the
        // JSON, so it uses JSON unicode escapes instead. Nothing that could
        // close the script element may survive into it.
        let ld_start = html
            .find("application/ld+json")
            .expect("structured data present");
        let ld_block = &html[ld_start..];
        let ld_end = ld_block.find("</script>").expect("structured data closes");
        assert!(!ld_block[..ld_end].contains('<'));
        assert!(
            serde_json::from_str::<serde_json::Value>(
                ld_block[ld_block.find('{').expect("json object")..ld_end].trim()
            )
            .is_ok()
        );
    }

    #[test]
    fn rendered_page_carries_crawler_metadata() {
        let public = project_public_summary(&duel_summary(), "https://snaketron.io");
        let html = render_game_page(&public, "https://snaketron.io");

        for expected in [
            "<meta property=\"og:title\"",
            "<meta property=\"og:description\"",
            "<meta property=\"og:image\"",
            "<meta name=\"twitter:card\" content=\"summary_large_image\">",
            "<link rel=\"canonical\" href=\"https://snaketron.io/g/4242\">",
            "application/ld+json",
            "SportsEvent",
        ] {
            assert!(
                html.contains(expected),
                "missing {expected} in rendered page"
            );
        }
    }

    #[test]
    fn site_url_falls_back_when_unusable() {
        assert_eq!(
            resolve_site_url(Some("https://staging.snaketron.io/")),
            "https://staging.snaketron.io"
        );
        assert_eq!(resolve_site_url(Some("  ")), DEFAULT_PUBLIC_SITE_URL);
        assert_eq!(
            resolve_site_url(Some("snaketron.io")),
            DEFAULT_PUBLIC_SITE_URL
        );
        assert_eq!(resolve_site_url(None), DEFAULT_PUBLIC_SITE_URL);
    }

    #[test]
    fn game_ids_accept_only_positive_decimal_u32s() {
        assert_eq!(parse_public_game_id("4242"), Some(4242));
        assert_eq!(parse_public_game_id("0"), None);
        assert_eq!(parse_public_game_id("-1"), None);
        assert_eq!(parse_public_game_id("4242abc"), None);
        assert_eq!(parse_public_game_id("99999999999"), None);
        assert_eq!(parse_public_game_id(""), None);
    }

    #[test]
    fn durations_read_as_match_lengths() {
        assert_eq!(format_duration(9_400), "9s");
        assert_eq!(format_duration(65_000), "1m 05s");
        assert_eq!(format_duration(3_601_000), "60m 01s");
    }
    /// Stands in for the database so the routes can be exercised directly.
    struct StubReader {
        summary: Option<MatchHistorySummary>,
        latest_allocated: Option<i32>,
    }

    #[async_trait::async_trait]
    impl PublicGameReader for StubReader {
        async fn summary(&self, game_id: i32) -> anyhow::Result<Option<MatchHistorySummary>> {
            Ok(self
                .summary
                .clone()
                .filter(|summary| summary.game_id as i32 == game_id))
        }

        async fn latest_allocated_game_id(&self) -> anyhow::Result<Option<i32>> {
            Ok(self.latest_allocated)
        }
    }

    fn app(summary: Option<MatchHistorySummary>, latest_allocated: Option<i32>) -> Router {
        public_game_route_template().with_state(PublicGameState {
            games: Arc::new(StubReader {
                summary,
                latest_allocated,
            }),
            site_url: "https://snaketron.io".to_string(),
        })
    }

    async fn get(app: Router, path: &str) -> Response {
        app.oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn body_string(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn a_finished_match_resolves_as_json_and_as_a_crawlable_page() {
        let summary = duel_summary();

        let response = get(
            app(Some(summary.clone()), Some(9000)),
            "/api/games/4242/summary",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(PUBLIC_SUMMARY_CACHE_CONTROL)
        );
        let json: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
        assert_eq!(json["status"], "final");
        assert_eq!(json["summary"]["gameId"], 4242);

        let page = get(app(Some(summary), Some(9000)), "/g/4242").await;
        assert_eq!(page.status(), StatusCode::OK);
        assert_eq!(
            page.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/html; charset=utf-8")
        );
        let html = body_string(page).await;
        assert!(html.contains("og:title"));
        assert!(html.contains("Ada won a Snaketron Duel"));
    }

    /// A link shared from a live match must not read as "no such match": the
    /// durable row only appears at completion, so an id below the allocation
    /// mark is a real game that has not finished.
    #[tokio::test]
    async fn a_match_still_being_played_is_pending_rather_than_missing() {
        let response = get(app(None, Some(9000)), "/api/games/8000/summary").await;
        assert_eq!(response.status(), StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
        assert_eq!(json["status"], "pending");
        assert_eq!(json["game_id"], 8000);

        let page = get(app(None, Some(9000)), "/g/8000").await;
        assert_eq!(page.status(), StatusCode::OK);
        let html = body_string(page).await;
        assert!(html.contains("still being played"));
        // An unfinished page must never enter a search index.
        assert!(html.contains(r#"<meta name="robots" content="noindex">"#));
    }

    #[tokio::test]
    async fn an_id_that_was_never_issued_is_a_real_404() {
        let response = get(app(None, Some(9000)), "/api/games/99999/summary").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let page = get(app(None, Some(9000)), "/g/99999").await;
        assert_eq!(page.status(), StatusCode::NOT_FOUND);
        assert!(body_string(page).await.contains("could not be found"));
    }

    /// Load-test matches are real rows that are nobody's match.
    #[tokio::test]
    async fn stress_test_matches_are_not_publicly_addressable() {
        let mut summary = duel_summary();
        summary.is_stress_test = true;

        let response = get(app(Some(summary), Some(9000)), "/api/games/4242/summary").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// A private or code-gated match stays readable, matching the decision the
    /// replay and highlight routes already pin: at launch every runtime game is
    /// public.
    #[tokio::test]
    async fn private_matches_follow_the_existing_public_replay_policy() {
        let mut summary = duel_summary();
        summary.is_private = true;

        let response = get(app(Some(summary), Some(9000)), "/api/games/4242/summary").await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn a_malformed_id_is_rejected_before_any_read() {
        let response = get(app(None, None), "/api/games/abc/summary").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn robots_allows_match_pages_and_keeps_the_api_out() {
        let response = get(app(None, None), "/robots.txt").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response).await;
        assert!(body.contains("Allow: /g/"));
        assert!(body.contains("Disallow: /api/"));
    }
}
