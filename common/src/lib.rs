mod snake;
mod arena;
mod game_state;
mod game_engine;
mod ai;

pub mod util;

pub use snake::*;
pub use game_state::*;
pub use game_engine::*;
pub use ai::*;
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
