use anyhow::{Context, Result, bail};

pub const PLAYER_IDLE_GRACE_MS_ENV: &str = "SNAKETRON_PLAYER_IDLE_GRACE_MS";
pub const PLAYER_IDLE_COUNTDOWN_MS_ENV: &str = "SNAKETRON_PLAYER_IDLE_COUNTDOWN_MS";

const DEFAULT_PLAYER_IDLE_GRACE_MS: u32 = 10_000;
const DEFAULT_PLAYER_IDLE_COUNTDOWN_MS: u32 = 10_000;

/// Server-owned inactivity policy for newly created multiplayer matches.
///
/// The client receives only the resolved total timeout and countdown in the
/// game snapshot. Keeping the two operator-facing phases here lets either be
/// changed without rebuilding or redeploying the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlayerIdleConfig {
    idle_grace_ms: u32,
    kick_countdown_ms: u32,
}

impl PlayerIdleConfig {
    pub fn new(idle_grace_ms: u32, kick_countdown_ms: u32) -> Result<Self> {
        if idle_grace_ms == 0 {
            bail!("player idle grace period must be positive");
        }
        if kick_countdown_ms == 0 {
            bail!("player idle kick countdown must be positive");
        }
        idle_grace_ms
            .checked_add(kick_countdown_ms)
            .context("player idle grace period plus kick countdown exceeds u32 milliseconds")?;

        Ok(Self {
            idle_grace_ms,
            kick_countdown_ms,
        })
    }

    pub fn idle_grace_ms(self) -> u32 {
        self.idle_grace_ms
    }

    pub fn kick_countdown_ms(self) -> u32 {
        self.kick_countdown_ms
    }

    pub fn total_timeout_ms(self) -> u32 {
        // Every instance is created through `new` or the checked constants in
        // `Default`, so this addition cannot overflow.
        self.idle_grace_ms + self.kick_countdown_ms
    }
}

impl Default for PlayerIdleConfig {
    fn default() -> Self {
        Self {
            idle_grace_ms: DEFAULT_PLAYER_IDLE_GRACE_MS,
            kick_countdown_ms: DEFAULT_PLAYER_IDLE_COUNTDOWN_MS,
        }
    }
}

/// Resolve optional environment values without reading process-global state,
/// which keeps startup parsing deterministic and straightforward to test.
pub fn resolve_player_idle_config(
    idle_grace_ms: Option<&str>,
    kick_countdown_ms: Option<&str>,
) -> Result<PlayerIdleConfig> {
    fn parse(name: &str, value: Option<&str>, default: u32) -> Result<u32> {
        match value {
            Some(value) => value
                .trim()
                .parse::<u32>()
                .with_context(|| format!("{name} must be a positive number of milliseconds")),
            None => Ok(default),
        }
    }

    PlayerIdleConfig::new(
        parse(
            PLAYER_IDLE_GRACE_MS_ENV,
            idle_grace_ms,
            DEFAULT_PLAYER_IDLE_GRACE_MS,
        )?,
        parse(
            PLAYER_IDLE_COUNTDOWN_MS_ENV,
            kick_countdown_ms,
            DEFAULT_PLAYER_IDLE_COUNTDOWN_MS,
        )?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_ten_seconds_idle_then_a_ten_second_countdown() {
        let config = resolve_player_idle_config(None, None).unwrap();

        assert_eq!(config.idle_grace_ms(), 10_000);
        assert_eq!(config.kick_countdown_ms(), 10_000);
        assert_eq!(config.total_timeout_ms(), 20_000);
    }

    #[test]
    fn independently_configures_both_inactivity_phases() {
        let config = resolve_player_idle_config(Some(" 12345 "), Some("6789")).unwrap();

        assert_eq!(config.idle_grace_ms(), 12_345);
        assert_eq!(config.kick_countdown_ms(), 6_789);
        assert_eq!(config.total_timeout_ms(), 19_134);
    }

    #[test]
    fn rejects_invalid_inactivity_configuration() {
        for (grace, countdown) in [
            (Some("0"), None),
            (None, Some("0")),
            (Some("ten"), None),
            (None, Some("10.5")),
            (Some("4294967295"), Some("1")),
        ] {
            assert!(
                resolve_player_idle_config(grace, countdown).is_err(),
                "accepted grace={grace:?}, countdown={countdown:?}"
            );
        }
    }
}
