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

    /// Generate a random f32 in range [0.0, 1.0)
    pub fn next_f32(&mut self) -> f32 {
        // Use upper 24 bits of next_u32 for better distribution
        let value = (self.next_u32() >> 8) as f32;
        value / 16777216.0 // 2^24
    }

    /// Generate a normally distributed random value using Box-Muller transform
    /// Returns a value centered at `mean` with the given `std_dev` (standard deviation)
    pub fn next_normal(&mut self, mean: f32, std_dev: f32) -> f32 {
        // Box-Muller transform: convert two uniform random values to one normal random value
        let u1 = self.next_f32();
        let u2 = self.next_f32();

        // Avoid log(0) by ensuring u1 is never exactly 0
        let u1 = if u1 == 0.0 { 0.000001 } else { u1 };

        let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos();
        mean + z0 * std_dev
    }
}
