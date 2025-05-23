
// Trait for randomly generating numbers.
// Will have different implementations for server and client.
pub trait RandomGenerator {
    fn random_u32(&self) -> u32;
}

