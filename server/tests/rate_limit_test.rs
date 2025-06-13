use anyhow::Result;
use axum::http::StatusCode;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

mod common;
use common::test_environment::TestEnvironment;

#[tokio::test(flavor = "multi_thread")]
async fn test_username_check_rate_limiting() -> Result<()> {
    // Skip this test if database is not available
    if std::env::var("DATABASE_URL").is_err() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return Ok(());
    }

    let mut env = TestEnvironment::new("test_username_check_rate_limiting").await?;
    env.start_servers(1).await?;
    
    let api_addr = env.api_addr(0).expect("Server 0 should exist");
    let client = reqwest::Client::new();
    
    // Make 10 requests quickly (should succeed)
    for i in 0..10 {
        let response = client
            .post(&format!("http://{}/api/auth/check-username", api_addr))
            .json(&json!({
                "username": format!("testuser{}", i)
            }))
            .send()
            .await?;
            
        assert_eq!(response.status(), StatusCode::OK, "Request {} should succeed", i + 1);
    }
    
    // The 11th request should be rate limited
    let response = client
        .post(&format!("http://{}/api/auth/check-username", api_addr))
        .json(&json!({
            "username": "testuser11"
        }))
        .send()
        .await?;
        
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    
    let error_response: serde_json::Value = response.json().await?;
    assert_eq!(
        error_response["error"],
        "Too many requests. Please try again later."
    );
    
    // Wait for a minute and try again
    println!("Waiting 60 seconds for rate limit window to reset...");
    sleep(Duration::from_secs(60)).await;
    
    // Should be able to make requests again
    let response = client
        .post(&format!("http://{}/api/auth/check-username", api_addr))
        .json(&json!({
            "username": "testuser12"
        }))
        .send()
        .await?;
        
    assert_eq!(response.status(), StatusCode::OK, "Request after cooldown should succeed");
    
    env.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rate_limiting_per_ip() -> Result<()> {
    // Skip this test if database is not available
    if std::env::var("DATABASE_URL").is_err() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return Ok(());
    }

    let mut env = TestEnvironment::new("test_rate_limiting_per_ip").await?;
    env.start_servers(1).await?;
    
    let api_addr = env.api_addr(0).expect("Server 0 should exist");
    
    // Create two clients with different headers to simulate different IPs
    let client1 = reqwest::Client::new();
    let client2 = reqwest::Client::new();
    
    // Client 1 makes 10 requests
    for i in 0..10 {
        let response = client1
            .post(&format!("http://{}/api/auth/check-username", api_addr))
            .header("X-Real-IP", "192.168.1.1")
            .json(&json!({
                "username": format!("client1user{}", i)
            }))
            .send()
            .await?;
            
        assert_eq!(response.status(), StatusCode::OK, "Client 1 request {} should succeed", i + 1);
    }
    
    // Client 2 should still be able to make requests
    for i in 0..5 {
        let response = client2
            .post(&format!("http://{}/api/auth/check-username", api_addr))
            .header("X-Real-IP", "192.168.1.2")
            .json(&json!({
                "username": format!("client2user{}", i)
            }))
            .send()
            .await?;
            
        assert_eq!(response.status(), StatusCode::OK, "Client 2 request {} should succeed", i + 1);
    }
    
    // Client 1's 11th request should be rate limited
    let response = client1
        .post(&format!("http://{}/api/auth/check-username", api_addr))
        .header("X-Real-IP", "192.168.1.1")
        .json(&json!({
            "username": "client1user11"
        }))
        .send()
        .await?;
        
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS, "Client 1 should be rate limited");
    
    env.cleanup().await?;
    Ok(())
}