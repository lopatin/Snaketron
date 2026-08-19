pub mod admin;
pub mod analytics;
pub mod auth;
pub mod crazygames;
pub mod games;
pub mod jwt;
pub mod leaderboard;
pub mod middleware;
pub mod news;
pub mod players;
pub mod rate_limit;
pub mod regions;
pub mod server;
pub mod skins;
pub mod textures;
pub mod wallet;

pub use server::run_api_server;
