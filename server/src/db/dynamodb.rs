use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, Projection, ProjectionType, ReturnValue, ScalarAttributeType,
};
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use tracing::{debug, info, warn};

use super::Database;
use super::models::*;

pub struct DynamoDatabase {
    client: Client,
    table_prefix: String,
}

impl DynamoDatabase {
    pub async fn new() -> Result<Self> {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);

        let table_prefix =
            std::env::var("DYNAMODB_TABLE_PREFIX").unwrap_or_else(|_| "snaketron".to_string());

        info!(
            "Initialized DynamoDB client with table prefix: {}",
            table_prefix
        );

        let db = Self {
            client,
            table_prefix,
        };

        // Ensure all required tables exist
        db.ensure_tables_exist().await?;

        Ok(db)
    }

    fn main_table(&self) -> String {
        format!("{}-main", self.table_prefix)
    }

    fn usernames_table(&self) -> String {
        format!("{}-usernames", self.table_prefix)
    }

    fn game_codes_table(&self) -> String {
        format!("{}-game-codes", self.table_prefix)
    }

    fn rankings_table(&self) -> String {
        // Single table for all seasons - season is stored in the partition key
        format!("{}-rankings", self.table_prefix)
    }

    fn high_scores_table(&self) -> String {
        format!("{}-highscores", self.table_prefix)
    }

    async fn ensure_tables_exist(&self) -> Result<()> {
        // Create main table with GSI indexes
        self.create_main_table_if_not_exists().await?;

        // Create usernames table
        self.create_usernames_table_if_not_exists().await?;

        // Create game codes table
        self.create_game_codes_table_if_not_exists().await?;

        // Create rankings table (single table for all seasons)
        self.create_rankings_table_if_not_exists().await?;

        // Create high scores table (for solo mode leaderboards)
        self.create_high_scores_table_if_not_exists().await?;

        Ok(())
    }

    async fn create_main_table_if_not_exists(&self) -> Result<()> {
        let table_name = self.main_table();

        // Check if table exists
        match self
            .client
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
        {
            Ok(_) => {
                debug!("Table {} already exists", table_name);
                return Ok(());
            }
            Err(e) => {
                // Any error in describe_table likely means the table doesn't exist
                // In LocalStack, this could be various error types
                debug!(
                    "Table {} does not exist (error: {}), creating it",
                    table_name, e
                );
                // Table doesn't exist, proceed to create it
            }
        }

        info!("Creating DynamoDB table: {}", table_name);

        // Define attributes
        let pk_attr = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()?;
        let sk_attr = AttributeDefinition::builder()
            .attribute_name("sk")
            .attribute_type(ScalarAttributeType::S)
            .build()?;
        let gsi1pk_attr = AttributeDefinition::builder()
            .attribute_name("gsi1pk")
            .attribute_type(ScalarAttributeType::S)
            .build()?;
        let gsi1sk_attr = AttributeDefinition::builder()
            .attribute_name("gsi1sk")
            .attribute_type(ScalarAttributeType::S)
            .build()?;
        let gsi2pk_attr = AttributeDefinition::builder()
            .attribute_name("gsi2pk")
            .attribute_type(ScalarAttributeType::S)
            .build()?;
        let gsi2sk_attr = AttributeDefinition::builder()
            .attribute_name("gsi2sk")
            .attribute_type(ScalarAttributeType::S)
            .build()?;

        // Define key schema
        let pk_key = KeySchemaElement::builder()
            .attribute_name("pk")
            .key_type(KeyType::Hash)
            .build()?;
        let sk_key = KeySchemaElement::builder()
            .attribute_name("sk")
            .key_type(KeyType::Range)
            .build()?;

        // Define GSI1
        let gsi1_pk = KeySchemaElement::builder()
            .attribute_name("gsi1pk")
            .key_type(KeyType::Hash)
            .build()?;
        let gsi1_sk = KeySchemaElement::builder()
            .attribute_name("gsi1sk")
            .key_type(KeyType::Range)
            .build()?;
        let gsi1 = GlobalSecondaryIndex::builder()
            .index_name("GSI1")
            .key_schema(gsi1_pk)
            .key_schema(gsi1_sk)
            .projection(
                Projection::builder()
                    .projection_type(ProjectionType::All)
                    .build(),
            )
            .build()?;

        // Define GSI2
        let gsi2_pk = KeySchemaElement::builder()
            .attribute_name("gsi2pk")
            .key_type(KeyType::Hash)
            .build()?;
        let gsi2_sk = KeySchemaElement::builder()
            .attribute_name("gsi2sk")
            .key_type(KeyType::Range)
            .build()?;
        let gsi2 = GlobalSecondaryIndex::builder()
            .index_name("GSI2")
            .key_schema(gsi2_pk)
            .key_schema(gsi2_sk)
            .projection(
                Projection::builder()
                    .projection_type(ProjectionType::All)
                    .build(),
            )
            .build()?;

        // Create table
        self.client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(pk_attr)
            .attribute_definitions(sk_attr)
            .attribute_definitions(gsi1pk_attr)
            .attribute_definitions(gsi1sk_attr)
            .attribute_definitions(gsi2pk_attr)
            .attribute_definitions(gsi2sk_attr)
            .key_schema(pk_key)
            .key_schema(sk_key)
            .global_secondary_indexes(gsi1)
            .global_secondary_indexes(gsi2)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .context("Failed to create main table")?;

        info!("Created DynamoDB table: {}", table_name);
        Ok(())
    }

    async fn create_usernames_table_if_not_exists(&self) -> Result<()> {
        let table_name = self.usernames_table();

        // Check if table exists
        match self
            .client
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
        {
            Ok(_) => {
                debug!("Table {} already exists", table_name);
                return Ok(());
            }
            Err(e) => {
                // Any error in describe_table likely means the table doesn't exist
                debug!(
                    "Table {} does not exist (error: {}), creating it",
                    table_name, e
                );
                // Table doesn't exist, proceed to create it
            }
        }

        info!("Creating DynamoDB table: {}", table_name);

        let username_attr = AttributeDefinition::builder()
            .attribute_name("username")
            .attribute_type(ScalarAttributeType::S)
            .build()?;

        let username_key = KeySchemaElement::builder()
            .attribute_name("username")
            .key_type(KeyType::Hash)
            .build()?;

        self.client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(username_attr)
            .key_schema(username_key)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .context("Failed to create usernames table")?;

        info!("Created DynamoDB table: {}", table_name);
        Ok(())
    }

    async fn create_game_codes_table_if_not_exists(&self) -> Result<()> {
        let table_name = self.game_codes_table();

        // Check if table exists
        match self
            .client
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
        {
            Ok(_) => {
                debug!("Table {} already exists", table_name);
                return Ok(());
            }
            Err(e) => {
                // Any error in describe_table likely means the table doesn't exist
                debug!(
                    "Table {} does not exist (error: {}), creating it",
                    table_name, e
                );
                // Table doesn't exist, proceed to create it
            }
        }

        info!("Creating DynamoDB table: {}", table_name);

        let game_code_attr = AttributeDefinition::builder()
            .attribute_name("gameCode")
            .attribute_type(ScalarAttributeType::S)
            .build()?;

        let game_code_key = KeySchemaElement::builder()
            .attribute_name("gameCode")
            .key_type(KeyType::Hash)
            .build()?;

        self.client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(game_code_attr)
            .key_schema(game_code_key)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .context("Failed to create game codes table")?;

        info!("Created DynamoDB table: {}", table_name);
        Ok(())
    }

    async fn generate_id_for_entity(&self, entity_type: &str) -> Result<i32> {
        // Use DynamoDB atomic counter to generate unique IDs
        // Counter is stored with pk="COUNTER" and sk=entity_type (e.g., "USER", "SERVER", "GAME", "LOBBY")
        let response = self
            .client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s("COUNTER"))
            .key("sk", Self::av_s(entity_type))
            .update_expression("ADD #counter :increment")
            .expression_attribute_names("#counter", "counter")
            .expression_attribute_values(":increment", Self::av_n(1))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .context(format!("Failed to generate ID for {}", entity_type))?;

        // Extract the new counter value
        let counter = response
            .attributes
            .and_then(|attrs| Self::extract_number(&attrs, "counter"))
            .ok_or_else(|| anyhow!("Failed to extract counter value"))?;

        // If this is the first time (counter was 1), initialize it to 1000
        // This ensures we start at 1000 like the old in-memory counter
        let final_id = if counter == 1 {
            // Set counter to 1000
            self.client
                .update_item()
                .table_name(self.main_table())
                .key("pk", Self::av_s("COUNTER"))
                .key("sk", Self::av_s(entity_type))
                .update_expression("SET #counter = :init_value")
                .expression_attribute_names("#counter", "counter")
                .expression_attribute_values(":init_value", Self::av_n(1000))
                .send()
                .await
                .context(format!("Failed to initialize counter for {}", entity_type))?;
            1000
        } else {
            counter
        };

        debug!("Generated ID {} for entity type {}", final_id, entity_type);
        Ok(final_id)
    }

    fn av_s(s: impl Into<String>) -> AttributeValue {
        AttributeValue::S(s.into())
    }

    fn av_n(n: impl ToString) -> AttributeValue {
        AttributeValue::N(n.to_string())
    }

    fn av_bool(b: bool) -> AttributeValue {
        AttributeValue::Bool(b)
    }

    fn game_type_to_string(game_type: &common::GameType) -> String {
        match game_type {
            common::GameType::Solo => "solo".to_string(),
            common::GameType::TeamMatch { per_team: 1 } => "duel".to_string(),
            common::GameType::TeamMatch { per_team: 2 } => "2v2".to_string(),
            common::GameType::TeamMatch { per_team } => format!("team-{}", per_team),
            common::GameType::FreeForAll { .. } => "ffa".to_string(),
            common::GameType::Custom { .. } => "custom".to_string(),
        }
    }

    fn extract_string(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
        item.get(key).and_then(|v| v.as_s().ok()).cloned()
    }

    fn extract_number(item: &HashMap<String, AttributeValue>, key: &str) -> Option<i32> {
        item.get(key)
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<i32>().ok())
    }

    fn extract_bool(item: &HashMap<String, AttributeValue>, key: &str) -> Option<bool> {
        item.get(key).and_then(|v| v.as_bool().ok()).copied()
    }

    fn extract_datetime(
        item: &HashMap<String, AttributeValue>,
        key: &str,
    ) -> Result<DateTime<Utc>> {
        Self::extract_string(item, key)
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .ok_or_else(|| anyhow!("Missing or invalid datetime for key: {}", key))
    }
}

#[async_trait]
impl Database for DynamoDatabase {
    // Server operations
    async fn register_server(
        &self,
        grpc_address: &str,
        region: &str,
        origin: &str,
        ws_url: &str,
    ) -> Result<i32> {
        let server_id = self.generate_id_for_entity("SERVER").await?;
        let now = Utc::now();

        let mut item = HashMap::new();
        item.insert(
            "pk".to_string(),
            Self::av_s(format!("SERVER#{}", server_id)),
        );
        item.insert("sk".to_string(), Self::av_s("META"));
        item.insert("gsi1pk".to_string(), Self::av_s("SERVER"));
        item.insert("gsi1sk".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("gsi2pk".to_string(), Self::av_s(region));
        item.insert(
            "gsi2sk".to_string(),
            Self::av_s(format!("{}#SERVER#{}", now.to_rfc3339(), server_id)),
        );
        item.insert("id".to_string(), Self::av_n(server_id));
        item.insert("grpcAddress".to_string(), Self::av_s(grpc_address));
        item.insert("region".to_string(), Self::av_s(region));
        item.insert("origin".to_string(), Self::av_s(origin));
        item.insert("wsUrl".to_string(), Self::av_s(ws_url));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("status".to_string(), Self::av_s("active"));
        item.insert("currentGameCount".to_string(), Self::av_n(0));
        item.insert("maxGameCapacity".to_string(), Self::av_n(100));

        self.client
            .put_item()
            .table_name(self.main_table())
            .set_item(Some(item))
            .send()
            .await
            .context("Failed to register server")?;

        info!("Registered server {} in region {}", server_id, region);
        Ok(server_id)
    }

    async fn update_server_heartbeat(&self, server_id: i32) -> Result<()> {
        let now = Utc::now();

        self.client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("SERVER#{}", server_id)))
            .key("sk", Self::av_s("META"))
            .update_expression("SET lastHeartbeat = :now, gsi1sk = :gsi1sk, gsi2sk = :gsi2sk")
            .expression_attribute_values(":now", Self::av_s(now.to_rfc3339()))
            .expression_attribute_values(":gsi1sk", Self::av_s(now.to_rfc3339()))
            .expression_attribute_values(
                ":gsi2sk",
                Self::av_s(format!("{}#SERVER#{}", now.to_rfc3339(), server_id)),
            )
            .send()
            .await
            .context("Failed to update server heartbeat")?;

        debug!("Updated heartbeat for server {}", server_id);
        Ok(())
    }

    async fn update_server_status(&self, server_id: i32, status: &str) -> Result<()> {
        self.client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("SERVER#{}", server_id)))
            .key("sk", Self::av_s("META"))
            .update_expression("SET #status = :status")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":status", Self::av_s(status))
            .send()
            .await
            .context("Failed to update server status")?;

        info!("Updated server {} status to {}", server_id, status);
        Ok(())
    }

    async fn get_server_for_load_balancing(&self, region: &str) -> Result<i32> {
        let thirty_seconds_ago = Utc::now() - chrono::Duration::seconds(30);

        let response = self
            .client
            .query()
            .table_name(self.main_table())
            .index_name("GSI2")
            .key_condition_expression("gsi2pk = :region AND gsi2sk > :cutoff")
            .expression_attribute_values(":region", Self::av_s(region))
            .expression_attribute_values(":cutoff", Self::av_s(thirty_seconds_ago.to_rfc3339()))
            .projection_expression("id, currentGameCount")
            .send()
            .await
            .context("Failed to query servers for load balancing")?;

        let items = response.items.unwrap_or_default();

        // Find server with lowest game count
        let server = items
            .iter()
            .filter_map(|item| {
                let id = Self::extract_number(item, "id")?;
                let game_count = Self::extract_number(item, "currentGameCount").unwrap_or(0);
                Some((id, game_count))
            })
            .min_by_key(|(_, count)| *count)
            .ok_or_else(|| anyhow!("No active servers available in region {}", region))?;

        Ok(server.0)
    }

    async fn get_active_servers(&self, region: &str) -> Result<Vec<(i32, String)>> {
        let thirty_seconds_ago = Utc::now() - chrono::Duration::seconds(30);

        let response = self
            .client
            .query()
            .table_name(self.main_table())
            .index_name("GSI2")
            .key_condition_expression("gsi2pk = :region AND gsi2sk > :cutoff")
            .expression_attribute_values(":region", Self::av_s(region))
            .expression_attribute_values(":cutoff", Self::av_s(thirty_seconds_ago.to_rfc3339()))
            .projection_expression("id, grpcAddress")
            .send()
            .await
            .context("Failed to query active servers")?;

        let items = response.items.unwrap_or_default();

        let servers = items
            .iter()
            .filter_map(|item| {
                let id = Self::extract_number(item, "id")?;
                let address = Self::extract_string(item, "grpcAddress")?;
                Some((id, address))
            })
            .collect();

        Ok(servers)
    }

    async fn get_region_ws_url(&self, region: &str) -> Result<Option<String>> {
        let thirty_seconds_ago = Utc::now() - chrono::Duration::seconds(30);

        let response = self
            .client
            .query()
            .table_name(self.main_table())
            .index_name("GSI2")
            .key_condition_expression("gsi2pk = :region AND gsi2sk > :cutoff")
            .expression_attribute_values(":region", Self::av_s(region))
            .expression_attribute_values(":cutoff", Self::av_s(thirty_seconds_ago.to_rfc3339()))
            .projection_expression("wsUrl")
            .limit(1) // We only need one server's WS URL
            .send()
            .await
            .context("Failed to query region WebSocket URL")?;

        let items = response.items.unwrap_or_default();

        if let Some(item) = items.first() {
            Ok(Self::extract_string(item, "wsUrl"))
        } else {
            Ok(None)
        }
    }

    // User operations
    async fn create_user(&self, username: &str, password_hash: &str, mmr: i32) -> Result<User> {
        let user_id = self.generate_id_for_entity("USER").await?;
        let now = Utc::now();

        // First, try to create username entry (for uniqueness)
        let mut username_item = HashMap::new();
        username_item.insert("username".to_string(), Self::av_s(username));
        username_item.insert("userId".to_string(), Self::av_n(user_id));
        username_item.insert("passwordHash".to_string(), Self::av_s(password_hash));
        username_item.insert("mmr".to_string(), Self::av_n(mmr));
        username_item.insert("rankedMmr".to_string(), Self::av_n(1000));
        username_item.insert("casualMmr".to_string(), Self::av_n(1000));
        username_item.insert("xp".to_string(), Self::av_n(0));

        // This will fail if username already exists
        self.client
            .put_item()
            .table_name(self.usernames_table())
            .set_item(Some(username_item))
            .condition_expression("attribute_not_exists(username)")
            .send()
            .await
            .map_err(|_| anyhow!("Username already exists"))?;

        // Now create the main user record
        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(format!("USER#{}", user_id)));
        item.insert("sk".to_string(), Self::av_s("META"));
        item.insert("gsi1pk".to_string(), Self::av_s("USER"));
        item.insert("gsi1sk".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("id".to_string(), Self::av_n(user_id));
        item.insert("username".to_string(), Self::av_s(username));
        item.insert("passwordHash".to_string(), Self::av_s(password_hash));
        item.insert("mmr".to_string(), Self::av_n(mmr));
        item.insert("rankedMmr".to_string(), Self::av_n(1000));
        item.insert("casualMmr".to_string(), Self::av_n(1000));
        item.insert("xp".to_string(), Self::av_n(0));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("isGuest".to_string(), Self::av_bool(false));

        self.client
            .put_item()
            .table_name(self.main_table())
            .set_item(Some(item))
            .send()
            .await
            .context("Failed to create user")?;

        Ok(User {
            id: user_id,
            username: username.to_string(),
            password_hash: password_hash.to_string(),
            mmr,
            ranked_mmr: 1000,
            casual_mmr: 1000,
            xp: 0,
            created_at: now,
            is_guest: false,
            guest_token: None,
        })
    }

    async fn create_guest_user(&self, nickname: &str, guest_token: &str, mmr: i32) -> Result<User> {
        let user_id = self.generate_id_for_entity("USER").await?;
        let now = Utc::now();

        // Guest users are NOT added to the username table (no uniqueness constraint)
        // They are only stored in the main table

        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(format!("USER#{}", user_id)));
        item.insert("sk".to_string(), Self::av_s("META"));
        item.insert("gsi1pk".to_string(), Self::av_s("USER"));
        item.insert("gsi1sk".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("id".to_string(), Self::av_n(user_id));
        item.insert("username".to_string(), Self::av_s(nickname)); // Use nickname as username
        item.insert("passwordHash".to_string(), Self::av_s("")); // Empty password hash for guests
        item.insert("mmr".to_string(), Self::av_n(mmr));
        item.insert("rankedMmr".to_string(), Self::av_n(1000));
        item.insert("casualMmr".to_string(), Self::av_n(1000));
        item.insert("xp".to_string(), Self::av_n(0));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("isGuest".to_string(), Self::av_bool(true));
        item.insert("guestToken".to_string(), Self::av_s(guest_token));

        self.client
            .put_item()
            .table_name(self.main_table())
            .set_item(Some(item))
            .send()
            .await
            .context("Failed to create guest user")?;

        info!(
            "Created guest user {} with nickname '{}'",
            user_id, nickname
        );

        Ok(User {
            id: user_id,
            username: nickname.to_string(),
            password_hash: String::new(),
            mmr,
            ranked_mmr: 1000,
            casual_mmr: 1000,
            xp: 0,
            created_at: now,
            is_guest: true,
            guest_token: Some(guest_token.to_string()),
        })
    }

    async fn get_user_by_id(&self, user_id: i32) -> Result<Option<User>> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{}", user_id)))
            .key("sk", Self::av_s("META"))
            .consistent_read(true)
            .send()
            .await
            .context("Failed to get user")?;

        match response.item {
            Some(item) => {
                let user = User {
                    id: user_id,
                    username: Self::extract_string(&item, "username")
                        .ok_or_else(|| anyhow!("Missing username"))?,
                    password_hash: Self::extract_string(&item, "passwordHash").unwrap_or_default(),
                    mmr: Self::extract_number(&item, "mmr").unwrap_or(1000),
                    ranked_mmr: Self::extract_number(&item, "rankedMmr").unwrap_or(1000),
                    casual_mmr: Self::extract_number(&item, "casualMmr").unwrap_or(1000),
                    xp: Self::extract_number(&item, "xp").unwrap_or(0),
                    created_at: Self::extract_string(&item, "createdAt")
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(Utc::now),
                    is_guest: Self::extract_bool(&item, "isGuest").unwrap_or(false),
                    guest_token: Self::extract_string(&item, "guestToken"),
                };
                Ok(Some(user))
            }
            None => Ok(None),
        }
    }

    async fn get_user_by_username(&self, username: &str) -> Result<Option<User>> {
        // First get user ID from username table
        let response = self
            .client
            .get_item()
            .table_name(self.usernames_table())
            .key("username", Self::av_s(username))
            .send()
            .await
            .context("Failed to get user by username")?;

        match response.item {
            Some(item) => {
                let user_id = Self::extract_number(&item, "userId")
                    .ok_or_else(|| anyhow!("Missing user ID"))?;

                // Return user data directly from username table (it has all needed fields)
                let user = User {
                    id: user_id,
                    username: username.to_string(),
                    password_hash: Self::extract_string(&item, "passwordHash").unwrap_or_default(),
                    mmr: Self::extract_number(&item, "mmr").unwrap_or(1000),
                    ranked_mmr: Self::extract_number(&item, "rankedMmr").unwrap_or(1000),
                    casual_mmr: Self::extract_number(&item, "casualMmr").unwrap_or(1000),
                    xp: Self::extract_number(&item, "xp").unwrap_or(0),
                    created_at: Utc::now(), // Not stored in username table, use current time
                    is_guest: false,        // Users in username table are never guests
                    guest_token: None,
                };
                Ok(Some(user))
            }
            None => Ok(None),
        }
    }

    async fn update_user_mmr(&self, user_id: i32, mmr: i32) -> Result<()> {
        // Update main table
        self.client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{}", user_id)))
            .key("sk", Self::av_s("META"))
            .update_expression("SET mmr = :mmr")
            .expression_attribute_values(":mmr", Self::av_n(mmr))
            .send()
            .await
            .context("Failed to update user MMR")?;

        // Also need to update username table
        // First get username
        let user = self
            .get_user_by_id(user_id)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;

        self.client
            .update_item()
            .table_name(self.usernames_table())
            .key("username", Self::av_s(&user.username))
            .update_expression("SET mmr = :mmr")
            .expression_attribute_values(":mmr", Self::av_n(mmr))
            .send()
            .await
            .context("Failed to update user MMR in username table")?;

        Ok(())
    }

    async fn update_guest_username(&self, user_id: i32, username: &str) -> Result<()> {
        self.client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{}", user_id)))
            .key("sk", Self::av_s("META"))
            .update_expression("SET username = :username")
            .expression_attribute_values(":username", Self::av_s(username))
            .send()
            .await
            .context("Failed to update guest username")?;

        Ok(())
    }

    async fn add_user_xp(&self, user_id: i32, xp_to_add: i32) -> Result<i32> {
        // Atomic ADD operation in DynamoDB main table
        let response = self
            .client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{}", user_id)))
            .key("sk", Self::av_s("META"))
            .update_expression("ADD xp :xp_delta")
            .expression_attribute_values(":xp_delta", Self::av_n(xp_to_add))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .context("Failed to add user XP")?;

        // Extract and return new XP total
        let new_xp = response
            .attributes
            .and_then(|attrs| Self::extract_number(&attrs, "xp"))
            .unwrap_or(xp_to_add);

        // Also update username table for consistency
        let user = self
            .get_user_by_id(user_id)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;

        self.client
            .update_item()
            .table_name(self.usernames_table())
            .key("username", Self::av_s(&user.username))
            .update_expression("ADD xp :xp_delta")
            .expression_attribute_values(":xp_delta", Self::av_n(xp_to_add))
            .send()
            .await
            .context("Failed to update XP in username table")?;

        info!(
            "Added {} XP to user {} (new total: {})",
            xp_to_add, user_id, new_xp
        );
        Ok(new_xp)
    }

    async fn update_user_mmr_by_mode(
        &self,
        user_id: i32,
        mmr_delta: i32,
        queue_mode: &common::QueueMode,
    ) -> Result<i32> {
        // Determine which MMR field to update based on queue mode
        let mmr_field = match queue_mode {
            common::QueueMode::Competitive => "rankedMmr",
            common::QueueMode::Quickmatch => "casualMmr",
        };

        // Atomic ADD operation in DynamoDB main table
        let response = self
            .client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{}", user_id)))
            .key("sk", Self::av_s("META"))
            .update_expression(format!("ADD {} :mmr_delta", mmr_field))
            .expression_attribute_values(":mmr_delta", Self::av_n(mmr_delta))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .context("Failed to update user MMR")?;

        // Extract and return new MMR total
        let new_mmr = response
            .attributes
            .and_then(|attrs| Self::extract_number(&attrs, mmr_field))
            .unwrap_or(1000 + mmr_delta);

        // Also update username table for consistency
        let user = self
            .get_user_by_id(user_id)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;

        self.client
            .update_item()
            .table_name(self.usernames_table())
            .key("username", Self::av_s(&user.username))
            .update_expression(format!("ADD {} :mmr_delta", mmr_field))
            .expression_attribute_values(":mmr_delta", Self::av_n(mmr_delta))
            .send()
            .await
            .context("Failed to update MMR in username table")?;

        info!(
            "Updated {} for user {} by {} (new total: {})",
            mmr_field, user_id, mmr_delta, new_mmr
        );
        Ok(new_mmr)
    }

    async fn get_user_mmrs(&self, user_ids: &[i32]) -> Result<HashMap<i32, (i32, i32)>> {
        let mut mmr_map = HashMap::new();

        for &user_id in user_ids {
            if let Some(user) = self.get_user_by_id(user_id).await? {
                mmr_map.insert(user_id, (user.ranked_mmr, user.casual_mmr));
            }
        }

        Ok(mmr_map)
    }

    // Game operations
    async fn create_game(
        &self,
        server_id: i32,
        game_type: &JsonValue,
        game_mode: &str,
        is_private: bool,
        game_code: Option<&str>,
    ) -> Result<i32> {
        let game_id = self.generate_id_for_entity("GAME").await?;
        let now = Utc::now();

        // If game code provided, register it first
        if let Some(code) = game_code {
            let mut code_item = HashMap::new();
            code_item.insert("gameCode".to_string(), Self::av_s(code));
            code_item.insert("gameId".to_string(), Self::av_s(game_id.to_string()));
            code_item.insert("isPrivate".to_string(), Self::av_bool(is_private));
            code_item.insert("status".to_string(), Self::av_s("waiting"));

            self.client
                .put_item()
                .table_name(self.game_codes_table())
                .set_item(Some(code_item))
                .condition_expression("attribute_not_exists(gameCode)")
                .send()
                .await
                .map_err(|_| anyhow!("Game code already exists"))?;
        }

        // Create main game record
        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(format!("GAME#{}", game_id)));
        item.insert("sk".to_string(), Self::av_s("META"));
        item.insert("gsi1pk".to_string(), Self::av_s("GAME"));
        item.insert(
            "gsi1sk".to_string(),
            Self::av_s(format!("waiting#{}", now.to_rfc3339())),
        );
        item.insert("id".to_string(), Self::av_n(game_id));
        item.insert("serverId".to_string(), Self::av_n(server_id));
        item.insert("gameType".to_string(), Self::av_s(game_type.to_string()));
        item.insert("status".to_string(), Self::av_s("waiting"));
        item.insert("gameMode".to_string(), Self::av_s(game_mode));
        item.insert("isPrivate".to_string(), Self::av_bool(is_private));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("lastActivity".to_string(), Self::av_s(now.to_rfc3339()));

        if let Some(code) = game_code {
            item.insert("gameCode".to_string(), Self::av_s(code));
        }

        self.client
            .put_item()
            .table_name(self.main_table())
            .set_item(Some(item))
            .send()
            .await
            .context("Failed to create game")?;

        info!("Created game {} on server {}", game_id, server_id);
        Ok(game_id)
    }

    async fn get_game_by_id(&self, game_id: i32) -> Result<Option<Game>> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{}", game_id)))
            .key("sk", Self::av_s("META"))
            .send()
            .await
            .context("Failed to get game")?;

        match response.item {
            Some(item) => {
                let game = Game {
                    id: game_id,
                    server_id: Self::extract_number(&item, "serverId"),
                    game_type: Self::extract_string(&item, "gameType")
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or(json!({})),
                    game_state: Self::extract_string(&item, "gameState")
                        .and_then(|s| serde_json::from_str(&s).ok()),
                    status: Self::extract_string(&item, "status")
                        .unwrap_or_else(|| "waiting".to_string()),
                    ended_at: None,
                    last_activity: Utc::now(),
                    created_at: Utc::now(),
                    game_mode: Self::extract_string(&item, "gameMode")
                        .unwrap_or_else(|| "matchmaking".to_string()),
                    is_private: Self::extract_bool(&item, "isPrivate").unwrap_or(false),
                    game_code: Self::extract_string(&item, "gameCode"),
                };
                Ok(Some(game))
            }
            None => Ok(None),
        }
    }

    async fn get_game_by_code(&self, game_code: &str) -> Result<Option<Game>> {
        // First get game ID from game codes table
        let response = self
            .client
            .get_item()
            .table_name(self.game_codes_table())
            .key("gameCode", Self::av_s(game_code))
            .send()
            .await
            .context("Failed to get game by code")?;

        match response.item {
            Some(item) => {
                let game_id = Self::extract_string(&item, "gameId")
                    .and_then(|s| s.parse::<i32>().ok())
                    .ok_or_else(|| anyhow!("Invalid game ID"))?;

                self.get_game_by_id(game_id).await
            }
            None => Ok(None),
        }
    }

    async fn update_game_status(&self, game_id: i32, status: &str) -> Result<()> {
        let now = Utc::now();

        self.client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{}", game_id)))
            .key("sk", Self::av_s("META"))
            .update_expression("SET #status = :status, gsi1sk = :gsi1sk, lastActivity = :now")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":status", Self::av_s(status))
            .expression_attribute_values(
                ":gsi1sk",
                Self::av_s(format!("{}#{}", status, now.to_rfc3339())),
            )
            .expression_attribute_values(":now", Self::av_s(now.to_rfc3339()))
            .send()
            .await
            .context("Failed to update game status")?;

        Ok(())
    }

    async fn add_player_to_game(&self, game_id: i32, user_id: i32, team_id: i32) -> Result<()> {
        let now = Utc::now();

        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(format!("GAME#{}", game_id)));
        item.insert("sk".to_string(), Self::av_s(format!("PLAYER#{}", user_id)));
        item.insert("userId".to_string(), Self::av_n(user_id));
        item.insert("teamId".to_string(), Self::av_n(team_id));
        item.insert("joinedAt".to_string(), Self::av_s(now.to_rfc3339()));

        self.client
            .put_item()
            .table_name(self.main_table())
            .set_item(Some(item))
            .send()
            .await
            .context("Failed to add player to game")?;

        Ok(())
    }

    async fn get_game_players(&self, game_id: i32) -> Result<Vec<GamePlayer>> {
        let response = self
            .client
            .query()
            .table_name(self.main_table())
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", Self::av_s(format!("GAME#{}", game_id)))
            .expression_attribute_values(":prefix", Self::av_s("PLAYER#"))
            .send()
            .await
            .context("Failed to get game players")?;

        let items = response.items.unwrap_or_default();

        let players = items
            .iter()
            .filter_map(|item| {
                let user_id = Self::extract_number(item, "userId")?;
                let team_id = Self::extract_number(item, "teamId").unwrap_or(0);

                Some(GamePlayer {
                    id: 0, // Not used in DynamoDB
                    game_id,
                    user_id,
                    team_id,
                    joined_at: Utc::now(),
                })
            })
            .collect();

        Ok(players)
    }

    async fn get_player_count(&self, game_id: i32) -> Result<i64> {
        let players = self.get_game_players(game_id).await?;
        Ok(players.len() as i64)
    }

    // Custom lobby operations
    async fn create_custom_lobby(
        &self,
        game_code: &str,
        host_user_id: i32,
        settings: &JsonValue,
    ) -> Result<i32> {
        let lobby_id = self.generate_id_for_entity("LOBBY").await?;
        let now = Utc::now();
        let expires_at = now + chrono::Duration::hours(1);

        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(format!("LOBBY#{}", game_code)));
        item.insert("sk".to_string(), Self::av_s("META"));
        item.insert("id".to_string(), Self::av_n(lobby_id));
        item.insert("gameCode".to_string(), Self::av_s(game_code));
        item.insert("hostUserId".to_string(), Self::av_n(host_user_id));
        item.insert("settings".to_string(), Self::av_s(settings.to_string()));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("expiresAt".to_string(), Self::av_s(expires_at.to_rfc3339()));
        item.insert("state".to_string(), Self::av_s("waiting"));
        item.insert("ttl".to_string(), Self::av_n(expires_at.timestamp()));

        self.client
            .put_item()
            .table_name(self.main_table())
            .set_item(Some(item))
            .send()
            .await
            .context("Failed to create custom lobby")?;

        Ok(lobby_id)
    }

    async fn update_custom_lobby_game_id(&self, lobby_id: i32, game_id: i32) -> Result<()> {
        // Note: In real implementation, we'd need to query by lobby_id first to get the game_code
        // For now, this is simplified
        warn!("link_lobby_to_game: simplified implementation - would need to query by lobby_id");
        Ok(())
    }

    async fn get_custom_lobby_host(&self, game_id: i32) -> Result<Option<i32>> {
        // Note: In real implementation, we'd need to query lobbies by game_id
        // For now, return None
        warn!("get_custom_lobby_host: simplified implementation - returning None");
        Ok(None)
    }

    async fn get_custom_lobby_by_code(&self, game_code: &str) -> Result<Option<CustomLobby>> {
        // Query the game code index table
        let result = self
            .client
            .get_item()
            .table_name(format!("{}-game-codes", self.table_prefix))
            .key("gameCode", Self::av_s(game_code))
            .send()
            .await
            .ok(); // Return None if not found

        // For simplified implementation, return None
        warn!("get_custom_lobby_by_code: simplified implementation - returning None");
        Ok(None)
    }

    // Spectator operations
    async fn add_spectator_to_game(&self, game_id: i32, user_id: i32) -> Result<()> {
        let now = Utc::now();

        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(format!("GAME#{}", game_id)));
        item.insert(
            "sk".to_string(),
            Self::av_s(format!("SPECTATOR#{}", user_id)),
        );
        item.insert("userId".to_string(), Self::av_n(user_id));
        item.insert("joinedAt".to_string(), Self::av_s(now.to_rfc3339()));

        self.client
            .put_item()
            .table_name(self.main_table())
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
            .send()
            .await
            .ok(); // Ignore if already exists (idempotent)

        Ok(())
    }

    async fn upsert_ranking(
        &self,
        user_id: i32,
        username: &str,
        mmr: i32,
        queue_mode: &common::QueueMode,
        game_type: &common::GameType,
        region: &str,
        season: &str,
        won: bool,
    ) -> Result<()> {
        // Ensure table exists
        self.create_rankings_table_if_not_exists().await?;

        let queue_mode_str = match queue_mode {
            common::QueueMode::Competitive => "ranked",
            common::QueueMode::Quickmatch => "casual",
        };

        let game_type_str = Self::game_type_to_string(game_type);

        // Pad MMR to 8 digits for sorting (99999999 - mmr for descending order)
        let inverted_mmr = 99999999 - mmr.max(0).min(99999999);
        let padded_mmr = format!("{:08}", inverted_mmr);

        // Include season in PK for single-table design
        let pk = format!("RANKING#{}#{}#{}#{}", queue_mode_str, game_type_str, region, season);
        let sk = format!("MMR#{}#USER#{}", padded_mmr, user_id);

        // Try to get existing ranking to calculate delta
        let existing = self.get_user_ranking(user_id, queue_mode, game_type, region, season).await?;

        let (games_played, wins, losses, old_mmr) = match &existing {
            Some(entry) => {
                let new_wins = if won { entry.wins + 1 } else { entry.wins };
                let new_losses = if won { entry.losses } else { entry.losses + 1 };
                (entry.games_played + 1, new_wins, new_losses, Some(entry.mmr))
            }
            None => {
                let (wins, losses) = if won { (1, 0) } else { (0, 1) };
                (1, wins, losses, None)
            }
        };

        let now = Utc::now();
        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(&pk));
        item.insert("sk".to_string(), Self::av_s(&sk));
        item.insert("userId".to_string(), Self::av_n(user_id));
        item.insert("username".to_string(), Self::av_s(username));
        item.insert("mmr".to_string(), Self::av_n(mmr));
        item.insert("gamesPlayed".to_string(), Self::av_n(games_played));
        item.insert("wins".to_string(), Self::av_n(wins));
        item.insert("losses".to_string(), Self::av_n(losses));
        item.insert("region".to_string(), Self::av_s(region));
        item.insert("queueMode".to_string(), Self::av_s(queue_mode_str));
        item.insert("gameType".to_string(), Self::av_s(&game_type_str));
        item.insert("season".to_string(), Self::av_s(season));
        item.insert("updatedAt".to_string(), Self::av_s(now.to_rfc3339()));

        // Delete old entry if MMR changed (SK will be different)
        if let Some(prev_mmr) = old_mmr {
            if prev_mmr != mmr {
                let old_inverted = 99999999 - prev_mmr.max(0).min(99999999);
                let old_sk = format!("MMR#{}#USER#{}", format!("{:08}", old_inverted), user_id);

                self.client
                    .delete_item()
                    .table_name(self.rankings_table())
                    .key("pk", Self::av_s(&pk))
                    .key("sk", Self::av_s(&old_sk))
                    .send()
                    .await
                    .ok(); // Ignore errors
            }
        }

        // Insert new entry
        self.client
            .put_item()
            .table_name(self.rankings_table())
            .set_item(Some(item))
            .send()
            .await
            .context("Failed to upsert ranking")?;

        info!(
            "Updated ranking for user {} in {} {} {} (season: {}, MMR: {}, games: {}, W/L: {}/{})",
            user_id, queue_mode_str, game_type_str, region, season, mmr, games_played, wins, losses
        );

        Ok(())
    }

    async fn get_leaderboard(
        &self,
        queue_mode: &common::QueueMode,
        game_type: Option<&common::GameType>,
        region: Option<&str>,
        season: &str,
        limit: usize,
    ) -> Result<Vec<RankingEntry>> {
        // Ensure table exists
        self.create_rankings_table_if_not_exists().await?;

        let queue_mode_str = match queue_mode {
            common::QueueMode::Competitive => "ranked",
            common::QueueMode::Quickmatch => "casual",
        };

        // Query by region and game_type if specified, otherwise scan with filters
        let results = if let Some(game_type_ref) = game_type {
            let game_type_str = Self::game_type_to_string(game_type_ref);

            if let Some(reg) = region {
                // Query specific region, game type, and season
                let pk = format!("RANKING#{}#{}#{}#{}", queue_mode_str, game_type_str, reg, season);

                let response = self.client
                    .query()
                    .table_name(self.rankings_table())
                    .key_condition_expression("pk = :pk")
                    .expression_attribute_values(":pk", Self::av_s(&pk))
                    .limit(limit as i32)
                    .send()
                    .await
                    .context("Failed to query leaderboard")?;

                response.items.unwrap_or_default()
            } else {
                // Scan all regions for specific game type and season
                let response = self.client
                    .scan()
                    .table_name(self.rankings_table())
                    .filter_expression("begins_with(pk, :prefix)")
                    .expression_attribute_values(":prefix", Self::av_s(&format!("RANKING#{}#{}#", queue_mode_str, game_type_str)))
                    .limit(limit as i32)
                    .send()
                    .await
                    .context("Failed to scan leaderboard")?;

                response.items.unwrap_or_default()
            }
        } else {
            // Scan all game types and regions for a season
            let response = self.client
                .scan()
                .table_name(self.rankings_table())
                .filter_expression("begins_with(pk, :prefix)")
                .expression_attribute_values(":prefix", Self::av_s(&format!("RANKING#{}", queue_mode_str)))
                .limit(limit as i32)
                .send()
                .await
                .context("Failed to scan leaderboard")?;

            response.items.unwrap_or_default()
        };

        // Parse results into RankingEntry
        let mut entries: Vec<RankingEntry> = results
            .into_iter()
            .filter_map(|item| {
                Some(RankingEntry {
                    user_id: Self::extract_number(&item, "userId")?,
                    username: Self::extract_string(&item, "username")?,
                    mmr: Self::extract_number(&item, "mmr")?,
                    games_played: Self::extract_number(&item, "gamesPlayed")?,
                    wins: Self::extract_number(&item, "wins")?,
                    losses: Self::extract_number(&item, "losses")?,
                    region: Self::extract_string(&item, "region")?,
                    queue_mode: Self::extract_string(&item, "queueMode")?,
                    game_type: Self::extract_string(&item, "gameType").unwrap_or_else(|| "unknown".to_string()),
                    season: Self::extract_string(&item, "season")?,
                    updated_at: Self::extract_string(&item, "updatedAt")
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(Utc::now),
                })
            })
            .collect();

        // Sort by MMR descending (in case we scanned multiple regions)
        entries.sort_by(|a, b| b.mmr.cmp(&a.mmr));
        entries.truncate(limit);

        Ok(entries)
    }

    async fn get_user_ranking(
        &self,
        user_id: i32,
        queue_mode: &common::QueueMode,
        game_type: &common::GameType,
        region: &str,
        season: &str,
    ) -> Result<Option<RankingEntry>> {
        // Ensure table exists
        self.create_rankings_table_if_not_exists().await?;

        let queue_mode_str = match queue_mode {
            common::QueueMode::Competitive => "ranked",
            common::QueueMode::Quickmatch => "casual",
        };

        let game_type_str = Self::game_type_to_string(game_type);
        let pk = format!("RANKING#{}#{}#{}#{}", queue_mode_str, game_type_str, region, season);

        // Query all rankings for this PK and filter in memory for the user
        // We can't use filter on sk since it's a key attribute
        let response = self.client
            .query()
            .table_name(self.rankings_table())
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", Self::av_s(&pk))
            .send()
            .await
            .context("Failed to query rankings")?;

        let items = response.items.unwrap_or_default();

        // Filter in memory for the specific user
        let user_item = items.iter().find(|item| {
            Self::extract_number(item, "userId") == Some(user_id)
        });

        let item = match user_item {
            Some(item) => item,
            None => return Ok(None),
        };
        Ok(Some(RankingEntry {
            user_id: Self::extract_number(item, "userId").unwrap_or(user_id),
            username: Self::extract_string(item, "username").unwrap_or_default(),
            mmr: Self::extract_number(item, "mmr").unwrap_or(1000),
            games_played: Self::extract_number(item, "gamesPlayed").unwrap_or(0),
            wins: Self::extract_number(item, "wins").unwrap_or(0),
            losses: Self::extract_number(item, "losses").unwrap_or(0),
            region: Self::extract_string(item, "region").unwrap_or(region.to_string()),
            queue_mode: Self::extract_string(item, "queueMode").unwrap_or(queue_mode_str.to_string()),
            game_type: Self::extract_string(item, "gameType").unwrap_or(game_type_str.clone()),
            season: Self::extract_string(item, "season").unwrap_or(season.to_string()),
            updated_at: Self::extract_string(item, "updatedAt")
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now),
        }))
    }

    async fn insert_high_score(
        &self,
        game_id: &str,
        user_id: i32,
        username: &str,
        score: i32,
        game_type: &common::GameType,
        region: &str,
        season: &str,
    ) -> Result<()> {
        let game_type_str = Self::game_type_to_string(game_type);

        // PK: SCORE#{game_type}#{region} (e.g., SCORE#solo#us-east-1 or SCORE#solo#global)
        let pk = format!("SCORE#{}#{}", game_type_str, region);

        // SK: SCORE#{inverted_score}#GAME#{game_id}
        // Invert score for descending order (99999999 - score)
        let inverted_score = 99999999 - score;
        let sk = format!("SCORE#{:08}#GAME#{}", inverted_score, game_id);

        let timestamp = Utc::now().to_rfc3339();

        self.client
            .put_item()
            .table_name(self.high_scores_table())
            .item("pk", Self::av_s(&pk))
            .item("sk", Self::av_s(&sk))
            .item("gameId", Self::av_s(game_id))
            .item("userId", Self::av_n(user_id))
            .item("username", Self::av_s(username))
            .item("score", Self::av_n(score))
            .item("region", Self::av_s(region))
            .item("gameType", Self::av_s(&game_type_str))
            .item("season", Self::av_s(season))
            .item("timestamp", Self::av_s(&timestamp))
            .send()
            .await
            .context("Failed to insert high score")?;

        info!("Inserted high score for game {} (user: {}, score: {})", game_id, username, score);
        Ok(())
    }

    async fn get_high_scores(
        &self,
        game_type: &common::GameType,
        region: Option<&str>,
        season: &str,
        limit: usize,
    ) -> Result<Vec<HighScoreEntry>> {
        let game_type_str = Self::game_type_to_string(game_type);
        let region_str = region.unwrap_or("global");

        // PK for querying high scores
        let pk = format!("SCORE#{}#{}", game_type_str, region_str);

        // Query high scores table with season filter
        let response = self.client
            .query()
            .table_name(self.high_scores_table())
            .key_condition_expression("pk = :pk")
            .filter_expression("season = :season")
            .expression_attribute_values(":pk", Self::av_s(&pk))
            .expression_attribute_values(":season", Self::av_s(season))
            .limit(limit as i32)
            .send()
            .await
            .context("Failed to query high scores")?;

        let items = response.items.unwrap_or_default();

        // Parse results into HighScoreEntry
        let entries: Vec<HighScoreEntry> = items
            .into_iter()
            .filter_map(|item| {
                Some(HighScoreEntry {
                    game_id: Self::extract_string(&item, "gameId")?,
                    user_id: Self::extract_number(&item, "userId")?,
                    username: Self::extract_string(&item, "username")?,
                    score: Self::extract_number(&item, "score")?,
                    region: Self::extract_string(&item, "region")?,
                    game_type: Self::extract_string(&item, "gameType")?,
                    season: Self::extract_string(&item, "season")?,
                    timestamp: Self::extract_string(&item, "timestamp")
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(Utc::now),
                })
            })
            .collect();

        Ok(entries)
    }
}

// Private helper methods for rankings
impl DynamoDatabase {
    async fn create_rankings_table_if_not_exists(&self) -> Result<()> {
        let table_name = self.rankings_table();

        // Check if table exists
        match self.client.describe_table().table_name(&table_name).send().await {
            Ok(_) => {
                debug!("Rankings table {} already exists", table_name);
                return Ok(());
            }
            Err(_) => {
                info!("Creating rankings table: {}", table_name);
            }
        }

        // PK: RANKING#{queue_mode}#{game_type}#{region}#{season} (e.g., "RANKING#ranked#solo#us-east-1#2025-S1")
        // SK: MMR#{padded_mmr}#USER#{user_id} (e.g., "MMR#00001543#USER#1234")
        // This schema allows:
        // - Querying top players by queue_mode + game_type + region + season (sorted by MMR descending)
        // - Single table stores all seasons

        let pk_attr = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .context("Failed to build pk attribute")?;

        let sk_attr = AttributeDefinition::builder()
            .attribute_name("sk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .context("Failed to build sk attribute")?;

        let pk_key = KeySchemaElement::builder()
            .attribute_name("pk")
            .key_type(KeyType::Hash)
            .build()
            .context("Failed to build pk key")?;

        let sk_key = KeySchemaElement::builder()
            .attribute_name("sk")
            .key_type(KeyType::Range)
            .build()
            .context("Failed to build sk key")?;

        self.client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(pk_attr)
            .attribute_definitions(sk_attr)
            .key_schema(pk_key)
            .key_schema(sk_key)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .context("Failed to create rankings table")?;

        info!("Successfully created rankings table: {}", table_name);
        Ok(())
    }

    async fn create_high_scores_table_if_not_exists(&self) -> Result<()> {
        let table_name = self.high_scores_table();

        // Check if table exists
        match self.client.describe_table().table_name(&table_name).send().await {
            Ok(_) => {
                debug!("High scores table {} already exists", table_name);
                return Ok(());
            }
            Err(_) => {
                info!("Creating high scores table: {}", table_name);
            }
        }

        // PK: SCORE#{game_type}#{region} (e.g., "SCORE#solo#us-east-1" or "SCORE#solo#global")
        // SK: SCORE#{inverted_score}#GAME#{game_id} (e.g., "SCORE#99998457#GAME#1234")
        // GSI: UserScoreIndex with userId as PK and sk as SK for user-specific lookups
        // This schema allows:
        // - Querying top scores by game_type + region (sorted by score descending)
        // - Single table stores all seasons (season stored as attribute)

        let pk_attr = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .context("Failed to build pk attribute")?;

        let sk_attr = AttributeDefinition::builder()
            .attribute_name("sk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .context("Failed to build sk attribute")?;

        let user_id_attr = AttributeDefinition::builder()
            .attribute_name("userId")
            .attribute_type(ScalarAttributeType::N)
            .build()
            .context("Failed to build userId attribute")?;

        let pk_key = KeySchemaElement::builder()
            .attribute_name("pk")
            .key_type(KeyType::Hash)
            .build()
            .context("Failed to build pk key")?;

        let sk_key = KeySchemaElement::builder()
            .attribute_name("sk")
            .key_type(KeyType::Range)
            .build()
            .context("Failed to build sk key")?;

        // GSI for user-specific lookups
        let gsi_pk_key = KeySchemaElement::builder()
            .attribute_name("userId")
            .key_type(KeyType::Hash)
            .build()
            .context("Failed to build GSI pk key")?;

        let gsi_sk_key = KeySchemaElement::builder()
            .attribute_name("sk")
            .key_type(KeyType::Range)
            .build()
            .context("Failed to build GSI sk key")?;

        let gsi = GlobalSecondaryIndex::builder()
            .index_name("UserScoreIndex")
            .key_schema(gsi_pk_key)
            .key_schema(gsi_sk_key)
            .projection(
                Projection::builder()
                    .projection_type(ProjectionType::All)
                    .build(),
            )
            .build()
            .context("Failed to build GSI")?;

        self.client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(pk_attr)
            .attribute_definitions(sk_attr)
            .attribute_definitions(user_id_attr)
            .key_schema(pk_key)
            .key_schema(sk_key)
            .global_secondary_indexes(gsi)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .context("Failed to create high scores table")?;

        info!("Successfully created high scores table: {}", table_name);
        Ok(())
    }
}
