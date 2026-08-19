//! Resolving a player's public name to the lobby they can be joined in.
//!
//! This backs `snaketron.io/play/<username>` links: the visitor knows only a
//! name, and needs the lobby code that `JoinLobby` actually takes.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Serialize;
use tracing::warn;

use crate::http_server::HttpServerState;

/// Where a `/play/<username>` link should send the visitor.
///
/// The three outcomes are distinguished so the client can say something true:
/// "no player called that" is a different mistake from "they are offline", and
/// only one of them is worth retrying.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
#[serde(rename_all = "camelCase")]
pub struct PlayerLobbyResponse {
    /// The player's name as stored, which may differ from the link's casing.
    pub username: String,
    /// The lobby to join. `None` whenever `status` is not `online`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lobby_code: Option<String>,
    pub status: PlayerLobbyStatus,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
#[serde(rename_all = "camelCase")]
pub enum PlayerLobbyStatus {
    /// The player is present in a lobby that can be joined.
    Online,
    /// The account exists but holds no live lobby presence.
    Offline,
    /// No account is addressable under this name.
    NotFound,
}

/// `GET /api/players/:username/lobby`
///
/// Anonymous on purpose: the whole point of the link is that it works before
/// the visitor has an account. It therefore exposes nothing beyond what the
/// link already asserts — whether that player is currently reachable, and the
/// lobby code needed to join them. No id, no rating, no session.
///
/// Only registered accounts are addressable. Guest display names are neither
/// unique nor reserved (`create_guest_user` deliberately skips the usernames
/// table, and `UpdateNickname` lets any guest take any name), so resolving
/// them would let a guest rename themselves onto someone else's invite link
/// and intercept it. Guests therefore read as `NotFound`.
pub async fn get_player_lobby(
    State(state): State<HttpServerState>,
    Path(username): Path<String>,
) -> Result<Json<PlayerLobbyResponse>, StatusCode> {
    // Cheap shape check before a database round trip; the routes below are
    // reachable by anyone with a URL bar.
    if username.is_empty() || username.len() > 64 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let user = match state.db.get_user_by_username(&username).await {
        Ok(user) => user,
        Err(error) => {
            warn!(username, "Failed to resolve player by username: {error:#}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // A guest row reachable through the usernames table would be an upgraded
    // account mid-transition; treat it as unaddressable either way.
    let Some(user) = user.filter(|user| !user.is_guest) else {
        return Ok(Json(PlayerLobbyResponse {
            username,
            lobby_code: None,
            status: PlayerLobbyStatus::NotFound,
        }));
    };

    let user_id = match u32::try_from(user.id) {
        Ok(user_id) => user_id,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let lobby_code = match state.lobby_manager.get_user_lobby_code(user_id).await {
        Ok(lobby_code) => lobby_code,
        Err(error) => {
            warn!(
                username = user.username,
                "Failed to read player presence: {error:#}"
            );
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Presence outlives its lobby if the lobby was torn down inside the lease
    // window. Confirm the lobby is really there rather than sending the
    // visitor into a join that will fail.
    let lobby_code = match lobby_code {
        Some(lobby_code) => state
            .lobby_manager
            .get_lobby_metadata(&lobby_code)
            .await
            .ok()
            .flatten()
            .map(|_| lobby_code),
        None => None,
    };

    Ok(Json(match lobby_code {
        Some(lobby_code) => PlayerLobbyResponse {
            username: user.username,
            lobby_code: Some(lobby_code),
            status: PlayerLobbyStatus::Online,
        },
        None => PlayerLobbyResponse {
            username: user.username,
            lobby_code: None,
            status: PlayerLobbyStatus::Offline,
        },
    }))
}
