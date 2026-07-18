mod ai;
mod arena;
mod constants;
mod fingerprint;
mod game_engine;
mod game_state;
pub mod replay;
mod snake;
pub mod trace;

pub mod util;

pub use ai::*;
pub use constants::*;
pub use fingerprint::SyncHasher;
pub use game_engine::*;
pub use game_state::*;
pub use snake::*;
pub use util::PseudoRandom;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServerTimestamp(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClientTimestamp(u64);

pub fn add(left: u32, right: u32) -> u32 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
