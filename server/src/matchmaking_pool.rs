use serde::{Deserialize, Serialize};

/// Server-attested matchmaking isolation boundary.
///
/// Clients never select this value in lobby or queue messages. It is minted
/// into their signed JWT by the guest-auth endpoint and then carried through
/// lobby admission and physically separate Redis queues.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum MatchmakingPool {
    #[default]
    Public,
    Stress,
}

impl MatchmakingPool {
    pub const ALL: [Self; 2] = [Self::Public, Self::Stress];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Stress => "stress",
        }
    }
}

impl std::fmt::Display for MatchmakingPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
