//! Projects a durable completion record into analytics events.
//!
//! `CompletionRecordV1` is already the authoritative, immutable business fact:
//! it carries the whole final `GameState` plus the typed effects. So this is a
//! projection, not a derivation — there is no second source of truth to drift
//! from, and no new logic that could disagree with what was actually paid out.

use std::collections::HashMap;

use crate::completion::{CompletionEffect, CompletionRecordV1};

use super::event::{EventIdentity, EventOrigin, envelope};
use super::proto;

/// Per-player facts gathered from the typed effects.
#[derive(Default)]
struct PlayerOutcome {
    won: bool,
    mmr_delta: i64,
    xp_awarded: i64,
    score: i64,
}

/// Builds `game_completed` plus one `game_player_result` per player.
///
/// Returned as a batch because they describe one indivisible fact; emitting
/// them separately would let a partial batch make a game look playerless.
pub fn project(record: &CompletionRecordV1, origin: &EventOrigin) -> Vec<proto::Event> {
    let state = &record.final_state;
    let game_type = format!("{:?}", state.game_type);
    let queue_mode = format!("{:?}", state.queue_mode);

    let mut outcomes: HashMap<u32, PlayerOutcome> = HashMap::new();
    for effect in &record.effects {
        let Some(user_id) = effect.user_id() else {
            continue;
        };
        let entry = outcomes.entry(user_id).or_default();
        match effect {
            CompletionEffect::AddXp { amount, .. } => entry.xp_awarded += i64::from(*amount),
            CompletionEffect::AddMmr { delta, .. } => entry.mmr_delta += i64::from(*delta),
            CompletionEffect::UpdateRanking { won, .. } => entry.won |= *won,
            CompletionEffect::InsertHighScore { score, .. } => {
                entry.score = entry.score.max(i64::from(*score))
            }
            CompletionEffect::PersistGame { .. } => {}
        }
    }

    // Scores live on the final state keyed by snake; fold them in for players
    // who never produced a high-score effect.
    for (user_id, player) in &state.players {
        if let Some(score) = state.scores.get(&player.snake_id) {
            let entry = outcomes.entry(*user_id).or_default();
            entry.score = entry.score.max(i64::from(*score));
        }
    }

    let duration_ms = (record.ended_at_ms - state.start_ms).max(0);
    // A single winner only exists when exactly one player won; a team game or
    // a draw deliberately reports none rather than picking arbitrarily.
    let winners: Vec<u32> = outcomes
        .iter()
        .filter(|(_, outcome)| outcome.won)
        .map(|(user_id, _)| *user_id)
        .collect();
    let winner_user_id = match winners.as_slice() {
        [only] => Some(i64::from(*only)),
        _ => None,
    };

    let end_reason = if state.completed_by_inactivity {
        "inactivity"
    } else {
        "normal"
    };

    let identity = |user_id: Option<i64>| EventIdentity {
        user_id,
        is_stress_test: state.is_stress_test,
        ..Default::default()
    };

    let mut events = Vec::with_capacity(outcomes.len() + 1);
    events.push(envelope(
        origin,
        identity(None),
        proto::event::Payload::GameCompleted(proto::GameCompleted {
            game_id: i64::from(record.game_id),
            game_type: game_type.clone(),
            queue_mode: queue_mode.clone(),
            duration_ms,
            player_count: state.players.len() as i64,
            completed_by_inactivity: state.completed_by_inactivity,
            winner_user_id,
            end_reason: end_reason.to_owned(),
        }),
    ));

    // Sorted so a replay of the same record produces the same event order,
    // which keeps the resulting object key stable.
    let mut ordered: Vec<(&u32, &PlayerOutcome)> = outcomes.iter().collect();
    ordered.sort_by_key(|(user_id, _)| **user_id);

    for (user_id, outcome) in ordered {
        events.push(envelope(
            origin,
            identity(Some(i64::from(*user_id))),
            proto::event::Payload::GamePlayerResult(proto::GamePlayerResult {
                game_id: i64::from(record.game_id),
                user_id: i64::from(*user_id),
                score: outcome.score,
                won: outcome.won,
                mmr_delta: outcome.mmr_delta,
                xp_awarded: outcome.xp_awarded,
                game_type: game_type.clone(),
                queue_mode: queue_mode.clone(),
            }),
        ));
    }

    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{GameState, GameType, QueueMode};

    fn origin() -> EventOrigin {
        EventOrigin {
            environment: "test".to_owned(),
            region: "use1".to_owned(),
            aws_region: "us-east-1".to_owned(),
            instance_id: "1:boot".to_owned(),
        }
    }

    fn record(effects: Vec<CompletionEffect>, inactivity: bool) -> CompletionRecordV1 {
        let mut state = GameState::new(
            20,
            20,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Competitive,
            Some(1),
            0,
        );
        state.start_ms = 1_000;
        state.completed_by_inactivity = inactivity;
        CompletionRecordV1 {
            schema_version: crate::completion::COMPLETION_SCHEMA_VERSION,
            game_id: 42,
            partition_id: 2,
            revision: uuid::Uuid::new_v4(),
            ended_at_ms: 61_000,
            server_id: 7,
            season: None,
            recording: None,
            recording_canonical_bytes: None,
            recording_journal: None,
            play_of_the_game: None,
            final_state: state,
            effects,
        }
    }

    fn ranking(user_id: u32, won: bool) -> CompletionEffect {
        CompletionEffect::UpdateRanking {
            id: format!("rank-{user_id}"),
            user_id,
            username: format!("u{user_id}"),
            queue_mode: QueueMode::Competitive,
            game_type: GameType::TeamMatch { per_team: 1 },
            region: "use1".to_owned(),
            season: 1,
            won,
        }
    }

    #[test]
    fn a_completion_yields_one_game_event_plus_one_per_player() {
        let events = project(
            &record(vec![ranking(1, true), ranking(2, false)], false),
            &origin(),
        );
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event_name, "game_completed");
        assert!(
            events[1..]
                .iter()
                .all(|e| e.event_name == "game_player_result")
        );
    }

    #[test]
    fn duration_comes_from_the_record_not_the_clock() {
        let events = project(&record(vec![], false), &origin());
        let proto::event::Payload::GameCompleted(completed) =
            events[0].payload.as_ref().unwrap().clone()
        else {
            panic!("expected game_completed");
        };
        assert_eq!(completed.duration_ms, 60_000);
        assert_eq!(completed.game_id, 42);
    }

    /// Per-player payouts must match the effects exactly — this projection is
    /// the reason no separate derivation is needed.
    #[test]
    fn player_results_carry_the_effect_payouts() {
        let effects = vec![
            ranking(1, true),
            CompletionEffect::AddMmr {
                id: "m".to_owned(),
                user_id: 1,
                username: "u1".to_owned(),
                delta: 17,
                queue_mode: QueueMode::Competitive,
            },
            CompletionEffect::AddXp {
                id: "x".to_owned(),
                user_id: 1,
                username: "u1".to_owned(),
                amount: 250,
            },
        ];
        let events = project(&record(effects, false), &origin());
        let proto::event::Payload::GamePlayerResult(result) =
            events[1].payload.as_ref().unwrap().clone()
        else {
            panic!("expected game_player_result");
        };
        assert_eq!(result.user_id, 1);
        assert!(result.won);
        assert_eq!(result.mmr_delta, 17);
        assert_eq!(result.xp_awarded, 250);
    }

    /// A draw or a team game must not invent a single winner.
    #[test]
    fn multiple_winners_report_no_single_winner() {
        let events = project(
            &record(vec![ranking(1, true), ranking(2, true)], false),
            &origin(),
        );
        let proto::event::Payload::GameCompleted(completed) =
            events[0].payload.as_ref().unwrap().clone()
        else {
            panic!("expected game_completed");
        };
        assert_eq!(completed.winner_user_id, None);
    }

    #[test]
    fn an_inactivity_completion_is_labelled_distinctly() {
        let events = project(&record(vec![], true), &origin());
        let proto::event::Payload::GameCompleted(completed) =
            events[0].payload.as_ref().unwrap().clone()
        else {
            panic!("expected game_completed");
        };
        assert!(completed.completed_by_inactivity);
        assert_eq!(completed.end_reason, "inactivity");
    }

    /// Ordering must be stable so a replay produces the same object key.
    #[test]
    fn player_events_are_emitted_in_a_stable_order() {
        let effects = vec![ranking(9, false), ranking(3, false), ranking(5, false)];
        let ids: Vec<i64> = project(&record(effects, false), &origin())[1..]
            .iter()
            .map(|event| match event.payload.as_ref().unwrap() {
                proto::event::Payload::GamePlayerResult(r) => r.user_id,
                _ => panic!("expected a player result"),
            })
            .collect();
        assert_eq!(ids, vec![3, 5, 9], "must be sorted by user id");
    }
}
