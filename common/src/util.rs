
use serde::{Deserialize, Serialize};

// Simple pseudorandom number generator using xorshift algorithm
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PseudoRandom {
    state: u64,
}

impl PseudoRandom {
    pub fn new(seed: u64) -> Self {
        // Ensure we don't start with 0 state as xorshift doesn't work with 0
        let state = if seed == 0 { 0x1234567890abcdef } else { seed };
        PseudoRandom { state }
    }
    
    pub fn next_u32(&mut self) -> u32 {
        // xorshift64 algorithm
        self.state ^= self.state << 13;
        self.state ^= self.state >> 17;
        self.state ^= self.state << 5;
        (self.state >> 32) as u32
    }
    
    pub fn next_u16(&mut self) -> u16 {
        self.next_u32() as u16
    }
    
    pub fn next_u64(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 17;
        self.state ^= self.state << 5;
        self.state
    }
}

