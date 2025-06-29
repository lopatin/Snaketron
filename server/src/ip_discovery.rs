use anyhow::{Context, Result};
use std::env;
use tracing::{info, warn};

/// Discovers the server's own IP address based on the platform
pub async fn discover_own_ip() -> Result<String> {
    // Check if we're running in AWS Fargate
    if env::var("IS_FARGATE").unwrap_or_default() == "true" {
        discover_fargate_ip().await
    } else {
        // Default to Docker/local environment
        discover_docker_ip().await
    }
}

/// Discovers IP address in AWS Fargate using ECS metadata endpoint
async fn discover_fargate_ip() -> Result<String> {
    info!("Discovering IP address in AWS Fargate environment");
    
    // Get the metadata URI from environment
    let metadata_uri = env::var("ECS_CONTAINER_METADATA_URI_V4")
        .context("ECS_CONTAINER_METADATA_URI_V4 not set - not running in Fargate?")?;
    
    // Fetch task metadata
    let task_metadata_url = format!("{}/task", metadata_uri);
    let response = reqwest::get(&task_metadata_url)
        .await
        .context("Failed to fetch ECS task metadata")?;
    
    let metadata: serde_json::Value = response.json()
        .await
        .context("Failed to parse ECS task metadata")?;
    
    // Extract the private IP address
    let ip = metadata["Containers"][0]["Networks"][0]["IPv4Addresses"][0]
        .as_str()
        .context("Failed to extract IP from ECS metadata")?
        .to_string();
    
    info!("Discovered Fargate IP: {}", ip);
    Ok(ip)
}

/// Discovers IP address in Docker/local environment
async fn discover_docker_ip() -> Result<String> {
    info!("Discovering IP address in Docker/local environment");
    
    // First try to get the hostname
    let hostname = hostname::get()
        .context("Failed to get hostname")?
        .to_string_lossy()
        .to_string();
    
    // Try to resolve the hostname to an IP
    match tokio::net::lookup_host(format!("{}:0", hostname)).await {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                let ip = addr.ip().to_string();
                info!("Discovered Docker IP via hostname resolution: {}", ip);
                return Ok(ip);
            }
        }
        Err(e) => {
            warn!("Failed to resolve hostname {}: {}", hostname, e);
        }
    }
    
    // Fallback: try to get IP from network interfaces
    use pnet::datalink;
    
    for interface in datalink::interfaces() {
        // Skip loopback and down interfaces
        if interface.is_loopback() || !interface.is_up() {
            continue;
        }
        
        for ip in interface.ips {
            match ip {
                pnet::ipnetwork::IpNetwork::V4(ipv4) => {
                    let addr = ipv4.ip().to_string();
                    // Skip localhost and link-local addresses
                    if !addr.starts_with("127.") && !addr.starts_with("169.254.") {
                        info!("Discovered Docker IP from interface {}: {}", interface.name, addr);
                        return Ok(addr);
                    }
                }
                _ => continue,
            }
        }
    }
    
    // Final fallback - use bind address if nothing else works
    warn!("Could not discover IP address, falling back to 0.0.0.0");
    Ok("0.0.0.0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_discover_docker_ip() {
        // This test will vary based on the environment
        let result = discover_docker_ip().await;
        assert!(result.is_ok());
        let ip = result.unwrap();
        // Should return some IP address
        assert!(!ip.is_empty());
    }
}