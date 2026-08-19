pub mod admin;
pub mod auth;
pub mod crazygames;
pub mod games;
pub mod jwt;
pub mod leaderboard;
pub mod middleware;
pub mod news;
pub mod rate_limit;
pub mod regions;
pub mod server;

pub use server::run_api_server;
