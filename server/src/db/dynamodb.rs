use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::create_table::{CreateTableError, CreateTableOutput};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, ConditionCheck,
    CreateGlobalSecondaryIndexAction, Delete, GlobalSecondaryIndex, GlobalSecondaryIndexUpdate,
    KeySchemaElement, KeyType, Projection, ProjectionType, Put, ReturnValue, ScalarAttributeType,
    TableStatus, TimeToLiveSpecification, TimeToLiveStatus, TransactWriteItem, Update,
};
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};
use std::{collections::HashMap, time::Duration};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::models::*;
use super::{Database, SERVER_HEARTBEAT_FRESHNESS_SECONDS, ServerRegistration};
use crate::completion::{
    CompletionEffect, CompletionRecordV1, EffectApplyResult, canonical_json_bytes,
};
use crate::season::Season;

pub struct DynamoDatabase {
    client: Client,
    table_prefix: String,
}

const COMPLETED_GAME_RETENTION_DAYS_ENV: &str = "SNAKETRON_COMPLETED_GAME_RETENTION_DAYS";
const DEFAULT_COMPLETED_GAME_RETENTION_DAYS: i64 = 30;
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;
const DYNAMODB_CONTROL_PLANE_MAX_ATTEMPTS: usize = 30;
const DYNAMODB_CONTROL_PLANE_RETRY_DELAY: Duration = Duration::from_secs(1);
const COMPLETION_RANKING_MAX_ATTEMPTS: usize = 16;
const DYNAMODB_RUNTIME_MAX_ATTEMPTS: u32 = 5;

/// How long a SERVER registration item lives past its last heartbeat before
/// DynamoDB TTL reaps it. Deliberately generous: staleness is already handled
/// by heartbeat-freshness cutoffs at read time, so TTL is pure registry
/// hygiene, and the wide margin ensures expiry can never race a live server
/// whose heartbeats are temporarily failing.
const SERVER_REGISTRATION_TTL_SECONDS: i64 = 3600;

/// Build a DynamoDB client with explicit timeouts. The SDK ships with no
/// response timeout, so a hung request would otherwise stall its caller
/// indefinitely without ever erroring. Every DynamoDB client in the server
/// must be built through this function.
fn dynamodb_retry_config() -> aws_config::retry::RetryConfig {
    aws_config::retry::RetryConfig::standard().with_max_attempts(DYNAMODB_RUNTIME_MAX_ATTEMPTS)
}

pub async fn dynamodb_client() -> Client {
    let timeouts = aws_config::timeout::TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(2))
        .operation_attempt_timeout(Duration::from_secs(5))
        .operation_timeout(Duration::from_secs(15))
        .build();
    // Completion waves can briefly consume a fresh table key range. Keep
    // admission and task registration on the SDK's operation-safe retry path:
    // each retry replays the same request, while counter ambiguity can only
    // leave an unused ID. The default is three attempts, which proved too
    // short during the fixed autoscaling envelope.
    let retries = dynamodb_retry_config();
    let config = aws_config::from_env()
        .timeout_config(timeouts)
        .retry_config(retries)
        .load()
        .await;
    Client::new(&config)
}

impl DynamoDatabase {
    pub async fn new() -> Result<Self> {
        let client = dynamodb_client().await;

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

        // Do this after all table creation calls so a newly created main table
        // has time to become active. This is also run for pre-existing tables.
        self.ensure_main_table_ttl_enabled().await?;

        Ok(())
    }

    async fn wait_for_table_active(&self, table_name: &str) -> Result<()> {
        let mut last_observation = "table status was not returned".to_string();

        for attempt in 1..=DYNAMODB_CONTROL_PLANE_MAX_ATTEMPTS {
            match self
                .client
                .describe_table()
                .table_name(table_name)
                .send()
                .await
            {
                Ok(response) => match response.table().and_then(|table| table.table_status()) {
                    Some(TableStatus::Active) => return Ok(()),
                    Some(status)
                        if matches!(status, TableStatus::Creating | TableStatus::Updating) =>
                    {
                        last_observation = format!("table status was {}", status.as_str());
                    }
                    Some(status) => {
                        return Err(anyhow!(
                            "Cannot configure TTL for DynamoDB table {} while its status is {}",
                            table_name,
                            status.as_str()
                        ));
                    }
                    None => {
                        last_observation = "table status was not returned".to_string();
                    }
                },
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_resource_not_found_exception()) =>
                {
                    // DescribeTable can briefly return ResourceNotFound immediately after
                    // CreateTable even though the create request succeeded.
                    last_observation = "table was not yet visible".to_string();
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("Failed to verify DynamoDB table {} status", table_name)
                    });
                }
            }

            if attempt < DYNAMODB_CONTROL_PLANE_MAX_ATTEMPTS {
                sleep(DYNAMODB_CONTROL_PLANE_RETRY_DELAY).await;
            }
        }

        Err(anyhow!(
            "DynamoDB table {} did not become ACTIVE after {} attempts; last observation: {}",
            table_name,
            DYNAMODB_CONTROL_PLANE_MAX_ATTEMPTS,
            last_observation
        ))
    }

    /// Completes a CreateTable call. The describe-then-create pattern in the
    /// create_*_table_if_not_exists functions can lose a race against another
    /// process bootstrapping the same tables (servers booting together, parallel
    /// tests); a lost race surfaces as ResourceInUseException and is treated as
    /// success once the winner's table is ACTIVE.
    async fn finish_table_creation(
        &self,
        table_name: &str,
        result: Result<CreateTableOutput, SdkError<CreateTableError>>,
    ) -> Result<()> {
        match result {
            Ok(_) => {
                info!("Created DynamoDB table: {}", table_name);
                Ok(())
            }
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_resource_in_use_exception()) =>
            {
                debug!(
                    "Table {} was created concurrently by another process",
                    table_name
                );
                self.wait_for_table_active(table_name).await
            }
            Err(error) => Err(error)
                .with_context(|| format!("Failed to create DynamoDB table {}", table_name)),
        }
    }

    async fn ensure_main_table_ttl_enabled(&self) -> Result<()> {
        let table_name = self.main_table();
        self.wait_for_table_active(&table_name).await?;

        let mut update_requested = false;
        let mut last_observation = "TTL status was not returned".to_string();
        let mut last_update_error = None;

        for attempt in 1..=DYNAMODB_CONTROL_PLANE_MAX_ATTEMPTS {
            let ttl_description = match self
                .client
                .describe_time_to_live()
                .table_name(&table_name)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_resource_not_found_exception()) =>
                {
                    last_observation = "table was not yet visible to the TTL API".to_string();
                    if attempt < DYNAMODB_CONTROL_PLANE_MAX_ATTEMPTS {
                        sleep(DYNAMODB_CONTROL_PLANE_RETRY_DELAY).await;
                    }
                    continue;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "Failed to describe TTL configuration for DynamoDB table {}",
                            table_name
                        )
                    });
                }
            };

            let description = ttl_description.time_to_live_description();
            let status = description.and_then(|description| description.time_to_live_status());
            let attribute_name = description.and_then(|description| description.attribute_name());

            match status {
                Some(status)
                    if matches!(
                        status,
                        TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
                    ) =>
                {
                    if attribute_name != Some("ttl") {
                        return Err(anyhow!(
                            "DynamoDB table {} has TTL status {} on attribute {:?}; expected attribute 'ttl'",
                            table_name,
                            status.as_str(),
                            attribute_name
                        ));
                    }

                    if update_requested {
                        info!(
                            "Verified TTL on attribute 'ttl' is {} for table {}",
                            status.as_str(),
                            table_name
                        );
                    } else {
                        debug!(
                            "TTL on attribute 'ttl' is already {} for table {}",
                            status.as_str(),
                            table_name
                        );
                    }
                    return Ok(());
                }
                Some(TimeToLiveStatus::Disabled) if !update_requested => {
                    last_observation = "TTL status was DISABLED".to_string();
                    let specification = TimeToLiveSpecification::builder()
                        .attribute_name("ttl")
                        .enabled(true)
                        .build()
                        .context("Failed to build main table TTL specification")?;

                    match self
                        .client
                        .update_time_to_live()
                        .table_name(&table_name)
                        .time_to_live_specification(specification)
                        .send()
                        .await
                    {
                        Ok(_) => {
                            update_requested = true;
                            info!(
                                "Requested TTL on attribute 'ttl' for table {}; verifying status",
                                table_name
                            );
                        }
                        Err(error) => {
                            let service_error = error.as_service_error();
                            let resource_is_transitioning = service_error.is_some_and(|error| {
                                error.is_resource_in_use_exception()
                                    || error.is_resource_not_found_exception()
                            });
                            let validation_requires_verification = service_error
                                .is_some_and(|error| error.code() == Some("ValidationException"));

                            if validation_requires_verification {
                                // UpdateTimeToLive is not idempotent. A concurrent or recent
                                // request can return ValidationException, so only a subsequent
                                // exact DescribeTimeToLive result is allowed to prove success.
                                update_requested = true;
                                last_update_error = Some(error.to_string());
                                warn!(
                                    "TTL update for table {} returned ValidationException; verifying the actual TTL status",
                                    table_name
                                );
                            } else if resource_is_transitioning {
                                last_update_error = Some(error.to_string());
                                warn!(
                                    "DynamoDB table {} changed while enabling TTL; retrying after status verification",
                                    table_name
                                );
                            } else {
                                return Err(error).with_context(|| {
                                    format!(
                                        "Failed to enable TTL for DynamoDB table {}",
                                        table_name
                                    )
                                });
                            }
                        }
                    }
                }
                Some(TimeToLiveStatus::Disabled) => {
                    last_observation = "TTL remained DISABLED after the update request".to_string();
                }
                Some(TimeToLiveStatus::Disabling) => {
                    last_observation = "TTL status was DISABLING".to_string();
                }
                Some(status) => {
                    last_observation = format!("TTL status was {}", status.as_str());
                }
                None => {
                    last_observation = "TTL status was not returned".to_string();
                }
            }

            if attempt < DYNAMODB_CONTROL_PLANE_MAX_ATTEMPTS {
                sleep(DYNAMODB_CONTROL_PLANE_RETRY_DELAY).await;
            }
        }

        let update_error = last_update_error
            .map(|error| format!("; last update error: {error}"))
            .unwrap_or_default();
        Err(anyhow!(
            "Could not verify TTL status ENABLING or ENABLED on attribute 'ttl' for DynamoDB table {} after {} attempts; last observation: {}{}",
            table_name,
            DYNAMODB_CONTROL_PLANE_MAX_ATTEMPTS,
            last_observation,
            update_error
        ))
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
        let result = self
            .client
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
            .await;
        self.finish_table_creation(&table_name, result).await
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

        let result = self
            .client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(username_attr)
            .key_schema(username_key)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        self.finish_table_creation(&table_name, result).await
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

        let result = self
            .client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(game_code_attr)
            .key_schema(game_code_key)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        self.finish_table_creation(&table_name, result).await
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
            .update_expression(
                "SET #counter = if_not_exists(#counter, :initial_value) + :increment",
            )
            .expression_attribute_names("#counter", "counter")
            .expression_attribute_values(":initial_value", Self::av_n(999))
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

        debug!("Generated ID {} for entity type {}", counter, entity_type);
        Ok(counter)
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
        // Handle numeric attributes stored as either Number or String in DynamoDB
        if let Some(val) = item
            .get(key)
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<i32>().ok())
        {
            return Some(val);
        }

        item.get(key)
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse::<i32>().ok())
    }

    fn extract_i64(item: &HashMap<String, AttributeValue>, key: &str) -> Option<i64> {
        if let Some(value) = item
            .get(key)
            .and_then(|value| value.as_n().ok())
            .and_then(|value| value.parse::<i64>().ok())
        {
            return Some(value);
        }

        item.get(key)
            .and_then(|value| value.as_s().ok())
            .and_then(|value| value.parse::<i64>().ok())
    }

    fn extract_bool(item: &HashMap<String, AttributeValue>, key: &str) -> Option<bool> {
        item.get(key).and_then(|v| v.as_bool().ok()).copied()
    }

    fn extract_optional_datetime(
        item: &HashMap<String, AttributeValue>,
        key: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let Some(value) = Self::extract_string(item, key) else {
            return Ok(None);
        };

        DateTime::parse_from_rfc3339(&value)
            .map(|datetime| Some(datetime.with_timezone(&Utc)))
            .with_context(|| format!("Invalid datetime for key: {}", key))
    }

    fn game_from_item(game_id: i32, item: &HashMap<String, AttributeValue>) -> Result<Game> {
        let created_at =
            Self::extract_optional_datetime(item, "createdAt")?.unwrap_or_else(Utc::now);
        let last_activity =
            Self::extract_optional_datetime(item, "lastActivity")?.unwrap_or(created_at);

        Ok(Game {
            id: game_id,
            server_id: Self::extract_number(item, "serverId"),
            game_type: Self::extract_string(item, "gameType")
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or(json!({})),
            game_state: Self::extract_string(item, "gameState")
                .and_then(|value| serde_json::from_str(&value).ok()),
            status: Self::extract_string(item, "status").unwrap_or_else(|| "waiting".to_string()),
            ended_at: Self::extract_optional_datetime(item, "endedAt")?,
            last_activity,
            created_at,
            game_mode: Self::extract_string(item, "gameMode")
                .unwrap_or_else(|| "matchmaking".to_string()),
            is_private: Self::extract_bool(item, "isPrivate").unwrap_or(false),
            game_code: Self::extract_string(item, "gameCode"),
        })
    }

    fn completed_game_retention_days(configured_value: Option<&str>) -> i64 {
        configured_value
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|days| *days > 0)
            .unwrap_or(DEFAULT_COMPLETED_GAME_RETENTION_DAYS)
    }

    fn item_is_expired(item: &HashMap<String, AttributeValue>, now_epoch_seconds: i64) -> bool {
        Self::extract_i64(item, "ttl").is_some_and(|ttl| ttl <= now_epoch_seconds)
    }

    fn runtime_game_identity(game_id: i32, game_state: &common::GameState) -> String {
        format!("{}:{}", game_id, game_state.start_ms)
    }

    async fn game_item_exists(&self, game_id: i32) -> Result<bool> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{}", game_id)))
            .key("sk", Self::av_s("META"))
            .consistent_read(true)
            .projection_expression("pk")
            .send()
            .await
            .context("Failed to check whether a durable game ID is already in use")?;

        Ok(response.item.is_some())
    }

    fn canonical_fingerprint<T: serde::Serialize>(value: &T) -> Result<String> {
        let bytes = canonical_json_bytes(value)
            .context("Failed to serialize canonical completion fingerprint")?;
        // This fingerprint detects internal identity/payload mismatches; it is
        // not an authentication primitive. FNV-1a/128 keeps the implementation
        // stable without introducing another cryptography dependency.
        let mut hash = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d_u128;
        for byte in bytes {
            hash ^= u128::from(byte);
            hash = hash.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b_u128);
        }
        Ok(format!("{hash:032x}"))
    }

    fn completion_record_hash(completion: &CompletionRecordV1) -> Result<String> {
        Self::canonical_fingerprint(completion)
    }

    fn completion_effect_hash(
        completion: &CompletionRecordV1,
        effect: &CompletionEffect,
    ) -> Result<String> {
        Self::canonical_fingerprint(&(completion, effect))
    }

    fn completion_revision_anchor(
        &self,
        completion: &CompletionRecordV1,
        record_hash: &str,
    ) -> Result<TransactWriteItem> {
        let revision = completion.revision.to_string();
        let anchor = Put::builder()
            .table_name(self.main_table())
            .item("pk", Self::av_s(format!("GAME#{}", completion.game_id)))
            .item("sk", Self::av_s("COMPLETION"))
            .item("gameId", Self::av_n(completion.game_id))
            .item("completionRevision", Self::av_s(&revision))
            .item("completionHash", Self::av_s(record_hash))
            .item("schemaVersion", Self::av_n(completion.schema_version))
            .item("endedAtMs", Self::av_n(completion.ended_at_ms))
            .condition_expression(concat!(
                "attribute_not_exists(pk) OR ",
                "(completionRevision=:revision AND completionHash=:hash)"
            ))
            .expression_attribute_values(":revision", Self::av_s(revision))
            .expression_attribute_values(":hash", Self::av_s(record_hash))
            .build()
            .context("Failed to build immutable completion revision anchor")?;
        Ok(TransactWriteItem::builder().put(anchor).build())
    }

    fn game_completion_revision_guard(
        &self,
        completion: &CompletionRecordV1,
    ) -> Result<TransactWriteItem> {
        let guard = ConditionCheck::builder()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{}", completion.game_id)))
            .key("sk", Self::av_s("META"))
            .condition_expression(
                "attribute_not_exists(completionRevision) OR completionRevision=:revision",
            )
            .expression_attribute_values(":revision", Self::av_s(completion.revision.to_string()))
            .build()
            .context("Failed to build completed-game revision guard")?;
        Ok(TransactWriteItem::builder().condition_check(guard).build())
    }

    fn completion_effect_dependency_guard(
        &self,
        completion: &CompletionRecordV1,
        dependency_id: &str,
    ) -> Result<TransactWriteItem> {
        let dependency = completion
            .effect(dependency_id)
            .ok_or_else(|| anyhow!("completion is missing dependency effect {dependency_id}"))?;
        let dependency_hash = Self::completion_effect_hash(completion, dependency)?;
        let guard = ConditionCheck::builder()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{}", completion.game_id)))
            .key(
                "sk",
                Self::av_s(format!("EFFECT#{}#{}", completion.revision, dependency_id)),
            )
            .condition_expression("effectHash=:effect_hash")
            .expression_attribute_values(":effect_hash", Self::av_s(dependency_hash))
            .build()
            .with_context(|| {
                format!("Failed to build completion dependency guard for {dependency_id}")
            })?;
        Ok(TransactWriteItem::builder().condition_check(guard).build())
    }

    fn completion_effect_marker(
        &self,
        completion: &CompletionRecordV1,
        effect: &CompletionEffect,
        effect_hash: &str,
    ) -> Result<TransactWriteItem> {
        let marker = Put::builder()
            .table_name(self.main_table())
            .item("pk", Self::av_s(format!("GAME#{}", completion.game_id)))
            .item(
                "sk",
                Self::av_s(format!("EFFECT#{}#{}", completion.revision, effect.id())),
            )
            .item("gameId", Self::av_n(completion.game_id))
            .item(
                "completionRevision",
                Self::av_s(completion.revision.to_string()),
            )
            .item("effectId", Self::av_s(effect.id()))
            .item("effectHash", Self::av_s(effect_hash))
            .item("appliedAtMs", Self::av_n(completion.ended_at_ms))
            .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
            .build()
            .context("Failed to build completion effect marker")?;
        Ok(TransactWriteItem::builder().put(marker).build())
    }

    async fn completion_effect_marker_hash(
        &self,
        completion: &CompletionRecordV1,
        effect: &CompletionEffect,
    ) -> Result<Option<String>> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{}", completion.game_id)))
            .key(
                "sk",
                Self::av_s(format!("EFFECT#{}#{}", completion.revision, effect.id())),
            )
            .consistent_read(true)
            .projection_expression("effectHash")
            .send()
            .await
            .context("Failed to read completion effect marker")?;
        Ok(response
            .item
            .as_ref()
            .and_then(|item| Self::extract_string(item, "effectHash")))
    }

    async fn completion_anchor_identity(
        &self,
        completion: &CompletionRecordV1,
    ) -> Result<Option<(String, String)>> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{}", completion.game_id)))
            .key("sk", Self::av_s("COMPLETION"))
            .consistent_read(true)
            .projection_expression("completionRevision, completionHash")
            .send()
            .await
            .context("Failed to read immutable completion revision anchor")?;
        Ok(response.item.and_then(|item| {
            Some((
                Self::extract_string(&item, "completionRevision")?,
                Self::extract_string(&item, "completionHash")?,
            ))
        }))
    }

    async fn completion_user_target(&self, user_id: u32) -> Result<(String, bool)> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{user_id}")))
            .key("sk", Self::av_s("META"))
            .consistent_read(true)
            .projection_expression("username, isGuest")
            .send()
            .await
            .context("Failed to read completion effect user")?;
        let item = response
            .item
            .ok_or_else(|| anyhow!("user {user_id} disappeared before completion effect"))?;
        let username = Self::extract_string(&item, "username")
            .ok_or_else(|| anyhow!("user {user_id} has no username"))?;
        Ok((
            username,
            Self::extract_bool(&item, "isGuest").unwrap_or(false),
        ))
    }

    async fn transact_completion_effect(
        &self,
        completion: &CompletionRecordV1,
        effect: &CompletionEffect,
        mut mutations: Vec<TransactWriteItem>,
    ) -> Result<EffectApplyResult> {
        let record_hash = Self::completion_record_hash(completion)?;
        let effect_hash = Self::completion_effect_hash(completion, effect)?;
        mutations.insert(
            0,
            self.completion_effect_marker(completion, effect, &effect_hash)?,
        );
        mutations.insert(
            1,
            self.completion_revision_anchor(completion, &record_hash)?,
        );
        if !matches!(effect, CompletionEffect::PersistGame { .. }) {
            mutations.insert(2, self.game_completion_revision_guard(completion)?);
            mutations.insert(
                3,
                self.completion_effect_dependency_guard(completion, "game")?,
            );
        }

        // The persistent conditional marker is the idempotency boundary. We
        // intentionally do not rely on DynamoDB's ten-minute client-token
        // window: a replay years later must still converge, and a conditional
        // cancellation lets us classify it by strongly reading the marker.
        match self
            .client
            .transact_write_items()
            .set_transact_items(Some(mutations))
            .send()
            .await
        {
            Ok(_) => Ok(EffectApplyResult::Applied),
            Err(error) => match self
                .completion_effect_marker_hash(completion, effect)
                .await?
            {
                Some(existing) if existing == effect_hash => Ok(EffectApplyResult::AlreadyApplied),
                Some(existing) => Err(anyhow!(
                    "completion effect {} for game {} reused revision {} with a different payload (stored {}, attempted {})",
                    effect.id(),
                    completion.game_id,
                    completion.revision,
                    existing,
                    effect_hash
                )),
                None => match self.completion_anchor_identity(completion).await? {
                    Some((revision, hash))
                        if revision != completion.revision.to_string() || hash != record_hash =>
                    {
                        Err(anyhow!(
                            "game {} already has immutable completion revision {} with hash {} (attempted {} with hash {})",
                            completion.game_id,
                            revision,
                            hash,
                            completion.revision,
                            record_hash
                        ))
                    }
                    _ => Err(error).context(format!(
                        "Failed to atomically apply completion effect {} for game {}",
                        effect.id(),
                        completion.game_id
                    )),
                },
            },
        }
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
        item.insert(
            "ttl".to_string(),
            Self::av_n(now.timestamp() + SERVER_REGISTRATION_TTL_SECONDS),
        );

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

    async fn update_server_heartbeat(
        &self,
        server_id: i32,
        registration: &ServerRegistration,
    ) -> Result<()> {
        let now = Utc::now();

        // A full upsert rather than a bare timestamp bump: if the registration
        // item was deleted out from under a live server (TTL reaper, manual
        // cleanup), this recreates it whole instead of leaving a partial item
        // or failing forever. if_not_exists preserves mutable counters.
        self.client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("SERVER#{}", server_id)))
            .key("sk", Self::av_s("META"))
            .update_expression(
                "SET lastHeartbeat = :now, gsi1sk = :gsi1sk, gsi2sk = :gsi2sk, #ttl = :ttl, \
                 gsi1pk = :gsi1pk, gsi2pk = :gsi2pk, id = :id, grpcAddress = :grpc, \
                 #region = :region, origin = :origin, wsUrl = :ws_url, \
                 createdAt = if_not_exists(createdAt, :now), \
                 #status = if_not_exists(#status, :active), \
                 currentGameCount = if_not_exists(currentGameCount, :zero), \
                 maxGameCapacity = if_not_exists(maxGameCapacity, :max_capacity)",
            )
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_names("#region", "region")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":now", Self::av_s(now.to_rfc3339()))
            .expression_attribute_values(":gsi1sk", Self::av_s(now.to_rfc3339()))
            .expression_attribute_values(
                ":gsi2sk",
                Self::av_s(format!("{}#SERVER#{}", now.to_rfc3339(), server_id)),
            )
            .expression_attribute_values(
                ":ttl",
                Self::av_n(now.timestamp() + SERVER_REGISTRATION_TTL_SECONDS),
            )
            .expression_attribute_values(":gsi1pk", Self::av_s("SERVER"))
            .expression_attribute_values(":gsi2pk", Self::av_s(&registration.region))
            .expression_attribute_values(":id", Self::av_n(server_id))
            .expression_attribute_values(":grpc", Self::av_s(&registration.grpc_address))
            .expression_attribute_values(":region", Self::av_s(&registration.region))
            .expression_attribute_values(":origin", Self::av_s(&registration.origin))
            .expression_attribute_values(":ws_url", Self::av_s(&registration.ws_url))
            .expression_attribute_values(":active", Self::av_s("active"))
            .expression_attribute_values(":zero", Self::av_n(0))
            .expression_attribute_values(":max_capacity", Self::av_n(100))
            .send()
            .await
            .context("Failed to update server heartbeat")?;

        debug!("Updated heartbeat for server {}", server_id);
        Ok(())
    }

    async fn update_server_status(&self, server_id: i32, status: &str) -> Result<()> {
        // Also stamp ttl so an item this upsert might create (e.g. status write
        // racing a TTL reap) is itself reaped instead of lingering forever.
        self.client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("SERVER#{}", server_id)))
            .key("sk", Self::av_s("META"))
            .update_expression("SET #status = :status, #ttl = :ttl")
            .expression_attribute_names("#status", "status")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":status", Self::av_s(status))
            .expression_attribute_values(
                ":ttl",
                Self::av_n(Utc::now().timestamp() + SERVER_REGISTRATION_TTL_SECONDS),
            )
            .send()
            .await
            .context("Failed to update server status")?;

        info!("Updated server {} status to {}", server_id, status);
        Ok(())
    }

    async fn get_server_for_load_balancing(&self, region: &str) -> Result<i32> {
        let cutoff = Utc::now() - chrono::Duration::seconds(SERVER_HEARTBEAT_FRESHNESS_SECONDS);

        let response = self
            .client
            .query()
            .table_name(self.main_table())
            .index_name("GSI2")
            .key_condition_expression("gsi2pk = :region AND gsi2sk > :cutoff")
            .expression_attribute_values(":region", Self::av_s(region))
            .expression_attribute_values(":cutoff", Self::av_s(cutoff.to_rfc3339()))
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
        let cutoff = Utc::now() - chrono::Duration::seconds(SERVER_HEARTBEAT_FRESHNESS_SECONDS);

        let response = self
            .client
            .query()
            .table_name(self.main_table())
            .index_name("GSI2")
            .key_condition_expression("gsi2pk = :region AND gsi2sk > :cutoff")
            .expression_attribute_values(":region", Self::av_s(region))
            .expression_attribute_values(":cutoff", Self::av_s(cutoff.to_rfc3339()))
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
        let cutoff = Utc::now() - chrono::Duration::seconds(SERVER_HEARTBEAT_FRESHNESS_SECONDS);

        let response = self
            .client
            .query()
            .table_name(self.main_table())
            .index_name("GSI2")
            .key_condition_expression("gsi2pk = :region AND gsi2sk > :cutoff")
            .expression_attribute_values(":region", Self::av_s(region))
            .expression_attribute_values(":cutoff", Self::av_s(cutoff.to_rfc3339()))
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
    async fn allocate_game_id(&self) -> Result<i32> {
        // Skip physically retained rows as an additional guard for restored/imported tables.
        for _ in 0..1024 {
            let candidate = self.generate_id_for_entity("GAME").await?;
            if !self.game_item_exists(candidate).await? {
                return Ok(candidate);
            }

            warn!(
                "Skipping durable game ID {} because a retained game already uses it",
                candidate
            );
        }

        Err(anyhow!(
            "Failed to allocate a free durable game ID after 1024 attempts"
        ))
    }

    async fn create_game(
        &self,
        server_id: i32,
        game_type: &JsonValue,
        game_mode: &str,
        is_private: bool,
        game_code: Option<&str>,
    ) -> Result<i32> {
        let game_id = self.allocate_game_id().await?;
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
            // Completion persistence races with immediate refreshes. A strongly consistent
            // read guarantees that once the upsert succeeds, reload cannot observe older
            // metadata without gameState.
            .consistent_read(true)
            .send()
            .await
            .context("Failed to get game")?;

        match response.item {
            Some(item) if Self::item_is_expired(&item, Utc::now().timestamp()) => {
                debug!(
                    "Treating expired completed game {} as absent while DynamoDB TTL deletion is pending",
                    game_id
                );
                Ok(None)
            }
            Some(item) => Ok(Some(Self::game_from_item(game_id, &item)?)),
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

    async fn upsert_completed_game(
        &self,
        game_id: i32,
        server_id: i32,
        game_state: &common::GameState,
    ) -> Result<()> {
        if !matches!(&game_state.status, common::GameStatus::Complete { .. }) {
            return Err(anyhow!(
                "Cannot persist game {} as completed while status is {:?}",
                game_id,
                game_state.status
            ));
        }

        let ended_at = Utc::now();
        let created_at =
            DateTime::<Utc>::from_timestamp_millis(game_state.start_ms).unwrap_or(ended_at);
        let configured_retention = std::env::var(COMPLETED_GAME_RETENTION_DAYS_ENV).ok();
        let retention_days = Self::completed_game_retention_days(configured_retention.as_deref());
        let ttl = ended_at
            .timestamp()
            .saturating_add(retention_days.saturating_mul(SECONDS_PER_DAY));

        if configured_retention
            .as_deref()
            .is_some_and(|value| value.parse::<i64>().ok().filter(|days| *days > 0).is_none())
        {
            warn!(
                "Invalid {} value {:?}; using the {} day default",
                COMPLETED_GAME_RETENTION_DAYS_ENV,
                configured_retention,
                DEFAULT_COMPLETED_GAME_RETENTION_DAYS
            );
        }

        let serialized_game_state = serde_json::to_string(game_state)
            .context("Failed to serialize completed game state")?;
        let serialized_game_type = serde_json::to_string(&game_state.game_type)
            .context("Failed to serialize completed game type")?;
        let runtime_identity = Self::runtime_game_identity(game_id, game_state);
        let game_mode = if matches!(&game_state.game_type, common::GameType::Custom { .. }) {
            "custom"
        } else {
            "matchmaking"
        };

        let mut update_expression = concat!(
            "SET gsi1pk = :gsi1pk, gsi1sk = :gsi1sk, id = :id, ",
            "serverId = :server_id, gameType = :game_type, gameState = :game_state, ",
            "#status = :status, endedAt = :ended_at, lastActivity = :last_activity, ",
            "createdAt = :created_at, gameMode = :game_mode, ",
            "isPrivate = :is_private, runtimeIdentity = :runtime_identity, #ttl = :ttl"
        )
        .to_string();

        if game_state.game_code.is_some() {
            update_expression.push_str(", gameCode = :game_code");
        }

        let mut request = self
            .client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{}", game_id)))
            .key("sk", Self::av_s("META"))
            .update_expression(update_expression)
            // Replays of the same completion are idempotent. A metadata-only row created
            // for this game on the same server may be adopted, but a different retained
            // game (which has gameState/runtimeIdentity) must never be overwritten.
            .condition_expression(concat!(
                "attribute_not_exists(pk) OR runtimeIdentity = :runtime_identity OR ",
                "(attribute_not_exists(runtimeIdentity) AND attribute_not_exists(gameState) ",
                "AND serverId = :server_id AND #status <> :status)"
            ))
            .expression_attribute_names("#status", "status")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":gsi1pk", Self::av_s("GAME"))
            .expression_attribute_values(
                ":gsi1sk",
                Self::av_s(format!("complete#{}", ended_at.to_rfc3339())),
            )
            .expression_attribute_values(":id", Self::av_n(game_id))
            .expression_attribute_values(":server_id", Self::av_n(server_id))
            .expression_attribute_values(":game_type", Self::av_s(serialized_game_type))
            .expression_attribute_values(":game_state", Self::av_s(serialized_game_state))
            .expression_attribute_values(":status", Self::av_s("complete"))
            .expression_attribute_values(":ended_at", Self::av_s(ended_at.to_rfc3339()))
            .expression_attribute_values(":last_activity", Self::av_s(ended_at.to_rfc3339()))
            .expression_attribute_values(":created_at", Self::av_s(created_at.to_rfc3339()))
            .expression_attribute_values(":game_mode", Self::av_s(game_mode))
            .expression_attribute_values(":runtime_identity", Self::av_s(runtime_identity))
            .expression_attribute_values(
                ":is_private",
                Self::av_bool(game_state.game_code.is_some()),
            )
            .expression_attribute_values(":ttl", Self::av_n(ttl));

        if let Some(game_code) = &game_state.game_code {
            request = request.expression_attribute_values(":game_code", Self::av_s(game_code));
        }

        request
            .send()
            .await
            .context("Failed to persist completed game state")?;

        info!(
            "Persisted completed game {} with a {} day retention TTL",
            game_id, retention_days
        );
        Ok(())
    }

    async fn apply_completion_effect(
        &self,
        completion: &CompletionRecordV1,
        effect: &CompletionEffect,
    ) -> Result<EffectApplyResult> {
        completion.validate_effect(effect)?;

        let max_attempts = if matches!(effect, CompletionEffect::UpdateRanking { .. }) {
            COMPLETION_RANKING_MAX_ATTEMPTS
        } else {
            1
        };
        for attempt in 0..max_attempts {
            let mutations = match effect {
                CompletionEffect::PersistGame { .. } => {
                    let ended_at = DateTime::<Utc>::from_timestamp_millis(completion.ended_at_ms)
                        .ok_or_else(|| anyhow!("invalid completion timestamp"))?;
                    let created_at =
                        DateTime::<Utc>::from_timestamp_millis(completion.final_state.start_ms)
                            .unwrap_or(ended_at);
                    let retention_days = Self::completed_game_retention_days(
                        std::env::var(COMPLETED_GAME_RETENTION_DAYS_ENV)
                            .ok()
                            .as_deref(),
                    );
                    let ttl = ended_at
                        .timestamp()
                        .saturating_add(retention_days.saturating_mul(SECONDS_PER_DAY));
                    let state_json = serde_json::to_string(&completion.final_state)
                        .context("Failed to serialize immutable final game state")?;
                    let game_type_json =
                        serde_json::to_string(&completion.final_state.game_type)
                            .context("Failed to serialize immutable final game type")?;
                    let game_mode = if matches!(
                        completion.final_state.game_type,
                        common::GameType::Custom { .. }
                    ) {
                        "custom"
                    } else {
                        "matchmaking"
                    };
                    let runtime_identity = Self::runtime_game_identity(
                        completion.game_id as i32,
                        &completion.final_state,
                    );

                    let mut expression = concat!(
                        "SET gsi1pk=:gsi1pk, gsi1sk=:gsi1sk, id=:id, serverId=:server, ",
                        "gameType=:game_type, gameState=:game_state, #status=:status, ",
                        "endedAt=:ended, lastActivity=:ended, createdAt=:created, ",
                        "gameMode=:mode, isPrivate=:private, runtimeIdentity=:runtime, ",
                        "completionRevision=:revision, #ttl=:ttl"
                    )
                    .to_string();
                    if completion.final_state.game_code.is_some() {
                        expression.push_str(", gameCode=:game_code");
                    }

                    let mut update = Update::builder()
                        .table_name(self.main_table())
                        .key("pk", Self::av_s(format!("GAME#{}", completion.game_id)))
                        .key("sk", Self::av_s("META"))
                        .update_expression(expression)
                        .condition_expression(concat!(
                            "attribute_not_exists(pk) OR completionRevision=:revision OR ",
                            "(attribute_not_exists(completionRevision) AND ",
                            "(runtimeIdentity=:runtime OR ",
                            "(attribute_not_exists(runtimeIdentity) AND ",
                            "attribute_not_exists(gameState) AND id=:id AND #status<>:status)))"
                        ))
                        .expression_attribute_names("#status", "status")
                        .expression_attribute_names("#ttl", "ttl")
                        .expression_attribute_values(":gsi1pk", Self::av_s("GAME"))
                        .expression_attribute_values(
                            ":gsi1sk",
                            Self::av_s(format!("complete#{}", ended_at.to_rfc3339())),
                        )
                        .expression_attribute_values(":id", Self::av_n(completion.game_id))
                        .expression_attribute_values(":server", Self::av_n(completion.server_id))
                        .expression_attribute_values(":game_type", Self::av_s(game_type_json))
                        .expression_attribute_values(":game_state", Self::av_s(state_json))
                        .expression_attribute_values(":status", Self::av_s("complete"))
                        .expression_attribute_values(":ended", Self::av_s(ended_at.to_rfc3339()))
                        .expression_attribute_values(
                            ":created",
                            Self::av_s(created_at.to_rfc3339()),
                        )
                        .expression_attribute_values(":mode", Self::av_s(game_mode))
                        .expression_attribute_values(
                            ":private",
                            Self::av_bool(completion.final_state.game_code.is_some()),
                        )
                        .expression_attribute_values(":runtime", Self::av_s(runtime_identity))
                        .expression_attribute_values(
                            ":revision",
                            Self::av_s(completion.revision.to_string()),
                        )
                        .expression_attribute_values(":ttl", Self::av_n(ttl));
                    if let Some(game_code) = &completion.final_state.game_code {
                        update =
                            update.expression_attribute_values(":game_code", Self::av_s(game_code));
                    }
                    vec![
                        TransactWriteItem::builder()
                            .update(
                                update
                                    .build()
                                    .context("Failed to build completed-game update")?,
                            )
                            .build(),
                    ]
                }
                CompletionEffect::AddXp {
                    user_id, amount, ..
                } => {
                    let (current_username, is_guest) =
                        self.completion_user_target(*user_id).await?;
                    let main_update = Update::builder()
                        .table_name(self.main_table())
                        .key("pk", Self::av_s(format!("USER#{user_id}")))
                        .key("sk", Self::av_s("META"))
                        .update_expression("ADD xp :delta")
                        .condition_expression(
                            "attribute_exists(pk) AND attribute_exists(sk) AND username=:username",
                        )
                        .expression_attribute_values(":delta", Self::av_n(amount))
                        .expression_attribute_values(":username", Self::av_s(&current_username))
                        .build()
                        .context("Failed to build idempotent XP update")?;
                    let mut mutations =
                        vec![TransactWriteItem::builder().update(main_update).build()];
                    if !is_guest {
                        let mirror_update = Update::builder()
                            .table_name(self.usernames_table())
                            .key("username", Self::av_s(current_username))
                            .update_expression("ADD xp :delta")
                            .condition_expression("attribute_exists(username) AND userId=:user")
                            .expression_attribute_values(":delta", Self::av_n(amount))
                            .expression_attribute_values(":user", Self::av_n(user_id))
                            .build()
                            .context("Failed to build idempotent XP mirror update")?;
                        mutations.push(TransactWriteItem::builder().update(mirror_update).build());
                    }
                    mutations
                }
                CompletionEffect::AddMmr {
                    user_id,
                    delta,
                    queue_mode,
                    ..
                } => {
                    let (current_username, is_guest) =
                        self.completion_user_target(*user_id).await?;
                    let field = match queue_mode {
                        common::QueueMode::Competitive => "rankedMmr",
                        common::QueueMode::Quickmatch => "casualMmr",
                    };
                    let main_update = Update::builder()
                        .table_name(self.main_table())
                        .key("pk", Self::av_s(format!("USER#{user_id}")))
                        .key("sk", Self::av_s("META"))
                        .update_expression(format!("ADD {field} :delta"))
                        .condition_expression(
                            "attribute_exists(pk) AND attribute_exists(sk) AND username=:username",
                        )
                        .expression_attribute_values(":delta", Self::av_n(delta))
                        .expression_attribute_values(":username", Self::av_s(&current_username))
                        .build()
                        .context("Failed to build idempotent MMR update")?;
                    let mut mutations =
                        vec![TransactWriteItem::builder().update(main_update).build()];
                    if !is_guest {
                        let mirror_update = Update::builder()
                            .table_name(self.usernames_table())
                            .key("username", Self::av_s(current_username))
                            .update_expression(format!("ADD {field} :delta"))
                            .condition_expression("attribute_exists(username) AND userId=:user")
                            .expression_attribute_values(":delta", Self::av_n(delta))
                            .expression_attribute_values(":user", Self::av_n(user_id))
                            .build()
                            .context("Failed to build idempotent MMR mirror update")?;
                        mutations.push(TransactWriteItem::builder().update(mirror_update).build());
                    }
                    mutations
                }
                CompletionEffect::UpdateRanking {
                    user_id,
                    username,
                    queue_mode,
                    game_type,
                    region,
                    season,
                    won,
                    ..
                } => {
                    let user_response = self
                        .client
                        .get_item()
                        .table_name(self.main_table())
                        .key("pk", Self::av_s(format!("USER#{user_id}")))
                        .key("sk", Self::av_s("META"))
                        .consistent_read(true)
                        .projection_expression("rankedMmr, casualMmr")
                        .send()
                        .await
                        .context("Failed to strongly read MMR for ranking effect")?;
                    let user_item = user_response.item.ok_or_else(|| {
                        anyhow!("user {user_id} disappeared before ranking effect")
                    })?;
                    let mmr_field = match queue_mode {
                        common::QueueMode::Competitive => "rankedMmr",
                        common::QueueMode::Quickmatch => "casualMmr",
                    };
                    let mmr = Self::extract_number(&user_item, mmr_field).unwrap_or(1000);

                    // Prevent a stale ranking read from committing after another
                    // game's MMR transaction. A failed condition causes this
                    // effect to re-read both user and ranking state.
                    let user_mmr_guard = ConditionCheck::builder()
                        .table_name(self.main_table())
                        .key("pk", Self::av_s(format!("USER#{user_id}")))
                        .key("sk", Self::av_s("META"))
                        .condition_expression(format!("{mmr_field}=:expected_mmr"))
                        .expression_attribute_values(":expected_mmr", Self::av_n(mmr))
                        .build()
                        .context("Failed to build ranking MMR consistency guard")?;

                    // Ranking is a projection of the MMR effect and must never be
                    // marked complete before that effect's atomic user/mirror
                    // transaction has succeeded.
                    let mmr_effect_id = format!("mmr:{user_id}");
                    let mmr_effect_guard =
                        self.completion_effect_dependency_guard(completion, &mmr_effect_id)?;

                    let queue = match queue_mode {
                        common::QueueMode::Competitive => "ranked",
                        common::QueueMode::Quickmatch => "casual",
                    };
                    let game_type_string = Self::game_type_to_string(game_type);
                    let pk = format!("RANKING#{queue}#{game_type_string}#{region}#{season}");
                    let inverted = 99_999_999 - mmr.clamp(0, 99_999_999);
                    let new_sk = format!("MMR#{inverted:08}#USER#{user_id}");
                    let existing = self
                        .get_user_ranking(*user_id as i32, queue_mode, game_type, region, *season)
                        .await?;
                    let (games, wins, losses) =
                        existing
                            .as_ref()
                            .map_or((1, i32::from(*won), i32::from(!*won)), |entry| {
                                (
                                    entry.games_played + 1,
                                    entry.wins + i32::from(*won),
                                    entry.losses + i32::from(!*won),
                                )
                            });
                    let now = DateTime::<Utc>::from_timestamp_millis(completion.ended_at_ms)
                        .ok_or_else(|| anyhow!("invalid completion timestamp"))?
                        .to_rfc3339();
                    let game_type_season = format!("{queue}#{game_type_string}#{season}");

                    let mut item = HashMap::new();
                    item.insert("pk".into(), Self::av_s(&pk));
                    item.insert("sk".into(), Self::av_s(&new_sk));
                    item.insert("gameTypeSeason".into(), Self::av_s(game_type_season));
                    item.insert("userId".into(), Self::av_n(user_id));
                    item.insert("username".into(), Self::av_s(username));
                    item.insert("mmr".into(), Self::av_n(mmr));
                    item.insert("gamesPlayed".into(), Self::av_n(games));
                    item.insert("wins".into(), Self::av_n(wins));
                    item.insert("losses".into(), Self::av_n(losses));
                    item.insert("region".into(), Self::av_s(region));
                    item.insert("queueMode".into(), Self::av_s(queue));
                    item.insert("gameType".into(), Self::av_s(game_type_string));
                    item.insert("season".into(), Self::av_n(season));
                    item.insert("updatedAt".into(), Self::av_s(&now));

                    let mut ranking_mutations = match existing {
                        None => {
                            let put = Put::builder()
                                .table_name(self.rankings_table())
                                .set_item(Some(item))
                                .condition_expression(
                                    "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                                )
                                .build()
                                .context("Failed to build first ranking effect")?;
                            vec![TransactWriteItem::builder().put(put).build()]
                        }
                        Some(entry) => {
                            let old_inverted = 99_999_999 - entry.mmr.clamp(0, 99_999_999);
                            let old_sk = format!("MMR#{old_inverted:08}#USER#{user_id}");
                            if old_sk == new_sk {
                                let update = Update::builder()
                                .table_name(self.rankings_table())
                                .key("pk", Self::av_s(&pk))
                                .key("sk", Self::av_s(&new_sk))
                                .update_expression(concat!(
                                    "SET username=:username, mmr=:new_mmr, gamesPlayed=:new_games, ",
                                    "wins=:new_wins, losses=:new_losses, updatedAt=:updated"
                                ))
                                .condition_expression(
                                    "userId=:user AND mmr=:old_mmr AND gamesPlayed=:old_games AND wins=:old_wins AND losses=:old_losses",
                                )
                                .expression_attribute_values(":username", Self::av_s(username))
                                .expression_attribute_values(":new_mmr", Self::av_n(mmr))
                                .expression_attribute_values(":new_games", Self::av_n(games))
                                .expression_attribute_values(":new_wins", Self::av_n(wins))
                                .expression_attribute_values(":new_losses", Self::av_n(losses))
                                .expression_attribute_values(":updated", Self::av_s(now))
                                .expression_attribute_values(":user", Self::av_n(user_id))
                                .expression_attribute_values(":old_mmr", Self::av_n(entry.mmr))
                                .expression_attribute_values(
                                    ":old_games",
                                    Self::av_n(entry.games_played),
                                )
                                .expression_attribute_values(":old_wins", Self::av_n(entry.wins))
                                .expression_attribute_values(
                                    ":old_losses",
                                    Self::av_n(entry.losses),
                                )
                                .build()
                                .context("Failed to build in-place ranking effect")?;
                                vec![TransactWriteItem::builder().update(update).build()]
                            } else {
                                let delete = Delete::builder()
                                .table_name(self.rankings_table())
                                .key("pk", Self::av_s(&pk))
                                .key("sk", Self::av_s(old_sk))
                                .condition_expression(
                                    "userId=:user AND mmr=:old_mmr AND gamesPlayed=:old_games AND wins=:old_wins AND losses=:old_losses",
                                )
                                .expression_attribute_values(":user", Self::av_n(user_id))
                                .expression_attribute_values(":old_mmr", Self::av_n(entry.mmr))
                                .expression_attribute_values(
                                    ":old_games",
                                    Self::av_n(entry.games_played),
                                )
                                .expression_attribute_values(":old_wins", Self::av_n(entry.wins))
                                .expression_attribute_values(
                                    ":old_losses",
                                    Self::av_n(entry.losses),
                                )
                                .build()
                                .context("Failed to build old-ranking delete")?;
                                let put = Put::builder()
                                    .table_name(self.rankings_table())
                                    .set_item(Some(item))
                                    .condition_expression(
                                        "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                                    )
                                    .build()
                                    .context("Failed to build moved ranking effect")?;
                                vec![
                                    TransactWriteItem::builder().delete(delete).build(),
                                    TransactWriteItem::builder().put(put).build(),
                                ]
                            }
                        }
                    };
                    ranking_mutations.insert(
                        0,
                        TransactWriteItem::builder()
                            .condition_check(user_mmr_guard)
                            .build(),
                    );
                    ranking_mutations.insert(1, mmr_effect_guard);
                    ranking_mutations
                }
                CompletionEffect::InsertHighScore {
                    user_id,
                    username,
                    score,
                    game_type,
                    region,
                    season,
                    ..
                } => {
                    let game_type_string = Self::game_type_to_string(game_type);
                    let inverted = 99_999_999_i64 - i64::from(*score);
                    let pk = format!("SCORE#{game_type_string}#{season}#{region}");
                    let sk = format!(
                        "SCORE#{:08}#GAME#{}#USER#{}",
                        inverted.max(0),
                        completion.game_id,
                        user_id
                    );
                    let timestamp = DateTime::<Utc>::from_timestamp_millis(completion.ended_at_ms)
                        .ok_or_else(|| anyhow!("invalid completion timestamp"))?
                        .to_rfc3339();
                    let put = Put::builder()
                        .table_name(self.high_scores_table())
                        .item("pk", Self::av_s(pk))
                        .item("sk", Self::av_s(sk))
                        .item("gameId", Self::av_s(completion.game_id.to_string()))
                        .item("userId", Self::av_s(user_id.to_string()))
                        .item("username", Self::av_s(username))
                        .item("score", Self::av_n(score))
                        .item("region", Self::av_s(region))
                        .item("gameType", Self::av_s(&game_type_string))
                        .item("season", Self::av_n(season))
                        .item(
                            "gameTypeSeason",
                            Self::av_s(format!("{game_type_string}#{season}")),
                        )
                        .item("timestamp", Self::av_s(timestamp))
                        .item(
                            "completionRevision",
                            Self::av_s(completion.revision.to_string()),
                        )
                        .condition_expression(
                            "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                        )
                        .build()
                        .context("Failed to build idempotent high-score effect")?;
                    vec![TransactWriteItem::builder().put(put).build()]
                }
            };

            match self
                .transact_completion_effect(completion, effect, mutations)
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) if attempt + 1 < max_attempts => {
                    // Ranking rows are sorted by MMR, so concurrent games for
                    // one user race a conditional delete/put. Re-read and
                    // rebuild the transaction until one observes the winner.
                    let exponent = attempt.min(6) as u32;
                    sleep(Duration::from_millis(1_u64 << exponent)).await;
                    debug!(
                        "Retrying completion ranking effect {} after concurrent mutation: {}",
                        effect.id(),
                        error
                    );
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("completion effect attempt loop always returns")
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

    async fn update_custom_lobby_game_id(&self, _lobby_id: i32, _game_id: i32) -> Result<()> {
        // Note: In real implementation, we'd need to query by lobby_id first to get the game_code
        // For now, this is simplified
        warn!("link_lobby_to_game: simplified implementation - would need to query by lobby_id");
        Ok(())
    }

    async fn get_custom_lobby_host(&self, _game_id: i32) -> Result<Option<i32>> {
        // Note: In real implementation, we'd need to query lobbies by game_id
        // For now, return None
        warn!("get_custom_lobby_host: simplified implementation - returning None");
        Ok(None)
    }

    async fn get_custom_lobby_by_code(&self, game_code: &str) -> Result<Option<CustomLobby>> {
        // Query the game code index table
        let _result = self
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
        season: Season,
        won: bool,
    ) -> Result<()> {
        // Ensure table exists
        self.create_rankings_table_if_not_exists().await?;

        let queue_mode_str = match queue_mode {
            common::QueueMode::Competitive => "ranked",
            common::QueueMode::Quickmatch => "casual",
        };

        let game_type_str = Self::game_type_to_string(game_type);
        let season_str = season.to_string();
        let game_type_season = format!("{}#{}#{}", queue_mode_str, game_type_str, season_str);

        // Pad MMR to 8 digits for sorting (99999999 - mmr for descending order)
        let inverted_mmr = 99999999 - mmr.clamp(0, 99999999);
        let padded_mmr = format!("{:08}", inverted_mmr);

        // Include season in PK for single-table design
        let pk = format!(
            "RANKING#{}#{}#{}#{}",
            queue_mode_str, game_type_str, region, season_str
        );
        let sk = format!("MMR#{}#USER#{}", padded_mmr, user_id);

        // Try to get existing ranking to calculate delta
        let existing = self
            .get_user_ranking(user_id, queue_mode, game_type, region, season)
            .await?;

        let (games_played, wins, losses, old_mmr) = match &existing {
            Some(entry) => {
                let new_wins = if won { entry.wins + 1 } else { entry.wins };
                let new_losses = if won { entry.losses } else { entry.losses + 1 };
                (
                    entry.games_played + 1,
                    new_wins,
                    new_losses,
                    Some(entry.mmr),
                )
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
        item.insert("gameTypeSeason".to_string(), Self::av_s(&game_type_season));
        item.insert("userId".to_string(), Self::av_n(user_id));
        item.insert("username".to_string(), Self::av_s(username));
        item.insert("mmr".to_string(), Self::av_n(mmr));
        item.insert("gamesPlayed".to_string(), Self::av_n(games_played));
        item.insert("wins".to_string(), Self::av_n(wins));
        item.insert("losses".to_string(), Self::av_n(losses));
        item.insert("region".to_string(), Self::av_s(region));
        item.insert("queueMode".to_string(), Self::av_s(queue_mode_str));
        item.insert("gameType".to_string(), Self::av_s(&game_type_str));
        item.insert("season".to_string(), Self::av_n(season));
        item.insert("updatedAt".to_string(), Self::av_s(now.to_rfc3339()));

        // Delete old entry if MMR changed (SK will be different)
        if let Some(prev_mmr) = old_mmr
            && prev_mmr != mmr
        {
            let old_inverted = 99999999 - prev_mmr.clamp(0, 99999999);
            let old_sk = format!("MMR#{:08}#USER#{}", old_inverted, user_id);

            self.client
                .delete_item()
                .table_name(self.rankings_table())
                .key("pk", Self::av_s(&pk))
                .key("sk", Self::av_s(&old_sk))
                .send()
                .await
                .ok(); // Ignore errors
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
        season: Season,
        limit: usize,
    ) -> Result<Vec<RankingEntry>> {
        // Ensure table exists
        self.create_rankings_table_if_not_exists().await?;

        let queue_mode_str = match queue_mode {
            common::QueueMode::Competitive => "ranked",
            common::QueueMode::Quickmatch => "casual",
        };

        let season_str = season.to_string();

        // Query by region and game_type if specified, otherwise scan with filters
        let items = if let Some(game_type_ref) = game_type {
            let game_type_str = Self::game_type_to_string(game_type_ref);

            if let Some(reg) = region {
                // Query specific region, game type, and season
                let pk = format!(
                    "RANKING#{}#{}#{}#{}",
                    queue_mode_str, game_type_str, reg, season_str
                );

                let response = self
                    .client
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
                // Prefer the GameTypeSeasonIndex to query all regions in a single partition
                let game_type_season =
                    format!("{}#{}#{}", queue_mode_str, game_type_str, season_str);
                let mut gsi_items: Vec<HashMap<String, AttributeValue>> = Vec::new();

                match self
                    .client
                    .query()
                    .table_name(self.rankings_table())
                    .index_name("GameTypeSeasonIndex")
                    .key_condition_expression("gameTypeSeason = :gts")
                    .expression_attribute_values(":gts", Self::av_s(&game_type_season))
                    .limit(limit as i32)
                    .send()
                    .await
                {
                    Ok(response) => {
                        gsi_items = response.items.unwrap_or_default();
                    }
                    Err(err) => {
                        warn!(
                            "Falling back to scan for global rankings (GameTypeSeasonIndex not available?): {:?}",
                            err
                        );
                    }
                }

                if !gsi_items.is_empty() {
                    gsi_items
                } else {
                    // Fallback: scan across all regions for the requested season
                    let pk_prefix = format!("RANKING#{}#{}#", queue_mode_str, game_type_str);
                    let mut items: Vec<HashMap<String, AttributeValue>> = Vec::new();
                    let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;
                    let target_items = limit.saturating_mul(3).max(limit + 5);

                    while items.len() < target_items {
                        let mut scan_builder = self
                            .client
                            .scan()
                            .table_name(self.rankings_table())
                            .filter_expression("begins_with(pk, :prefix) AND contains(pk, :season)")
                            .expression_attribute_values(":prefix", Self::av_s(&pk_prefix))
                            .expression_attribute_values(":season", Self::av_s(&season_str))
                            .limit((target_items - items.len()) as i32);

                        if let Some(ref lek) = last_evaluated_key {
                            scan_builder = scan_builder.set_exclusive_start_key(Some(lek.clone()));
                        }

                        let response = scan_builder
                            .send()
                            .await
                            .context("Failed to scan leaderboard")?;

                        if let Some(mut batch) = response.items {
                            items.append(&mut batch);
                        }

                        last_evaluated_key = response.last_evaluated_key;
                        if last_evaluated_key.is_none() {
                            break;
                        }
                    }

                    items
                }
            }
        } else {
            // Scan all game types and regions for a season
            let response = self
                .client
                .scan()
                .table_name(self.rankings_table())
                .filter_expression("begins_with(pk, :prefix)")
                .expression_attribute_values(
                    ":prefix",
                    Self::av_s(format!("RANKING#{}", queue_mode_str)),
                )
                .limit(limit as i32)
                .send()
                .await
                .context("Failed to scan leaderboard")?;

            response.items.unwrap_or_default()
        };

        // Parse results into RankingEntry
        let mut entries: Vec<RankingEntry> = items
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
                    game_type: Self::extract_string(&item, "gameType")
                        .unwrap_or_else(|| "unknown".to_string()),
                    season: Self::extract_number(&item, "season")
                        .map(|s| s as Season)
                        .unwrap_or(season),
                    updated_at: Self::extract_string(&item, "updatedAt")
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(Utc::now),
                })
            })
            .collect();

        // Sort by MMR descending (in case we scanned multiple regions)
        entries.sort_by_key(|e| std::cmp::Reverse(e.mmr));
        entries.truncate(limit);

        Ok(entries)
    }

    async fn get_user_ranking(
        &self,
        user_id: i32,
        queue_mode: &common::QueueMode,
        game_type: &common::GameType,
        region: &str,
        season: Season,
    ) -> Result<Option<RankingEntry>> {
        // Ensure table exists
        self.create_rankings_table_if_not_exists().await?;

        let queue_mode_str = match queue_mode {
            common::QueueMode::Competitive => "ranked",
            common::QueueMode::Quickmatch => "casual",
        };

        let game_type_str = Self::game_type_to_string(game_type);
        let pk = format!(
            "RANKING#{}#{}#{}#{}",
            queue_mode_str, game_type_str, region, season
        );

        // Query all rankings for this PK and filter in memory for the user
        // We can't use filter on sk since it's a key attribute
        let response = self
            .client
            .query()
            .table_name(self.rankings_table())
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", Self::av_s(&pk))
            .consistent_read(true)
            .send()
            .await
            .context("Failed to query rankings")?;

        let items = response.items.unwrap_or_default();

        // Filter in memory for the specific user
        let user_item = items
            .iter()
            .find(|item| Self::extract_number(item, "userId") == Some(user_id));

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
            queue_mode: Self::extract_string(item, "queueMode")
                .unwrap_or(queue_mode_str.to_string()),
            game_type: Self::extract_string(item, "gameType").unwrap_or(game_type_str.clone()),
            season: Self::extract_number(item, "season")
                .map(|s| s as Season)
                .unwrap_or(season),
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
        season: Season,
    ) -> Result<()> {
        let game_type_str = Self::game_type_to_string(game_type);

        // SK: SCORE#{inverted_score}#GAME#{game_id}
        // Invert score for descending order (99999999 - score)
        let inverted_score = 99999999 - score;
        let sk = format!("SCORE#{:08}#GAME#{}", inverted_score, game_id);

        let timestamp = Utc::now().to_rfc3339();

        // PK: SCORE#{game_type}#{season}#{region} (e.g., SCORE#solo#2025-S1#us-east-1)
        let pk = format!("SCORE#{}#{}#{}", game_type_str, season, region);
        let game_type_season = format!("{}#{}", game_type_str, season);

        debug!(
            "Inserting high score - table: {}, pk: {}, sk: {}, user: {}, score: {}, season: {}",
            self.high_scores_table(),
            pk,
            sk,
            username,
            score,
            season
        );

        self.client
            .put_item()
            .table_name(self.high_scores_table())
            .item("pk", Self::av_s(&pk))
            .item("sk", Self::av_s(&sk))
            .item("gameId", Self::av_s(game_id))
            .item("userId", Self::av_s(user_id.to_string()))
            .item("username", Self::av_s(username))
            .item("score", Self::av_n(score))
            .item("region", Self::av_s(region))
            .item("gameType", Self::av_s(&game_type_str))
            .item("season", Self::av_n(season))
            .item("gameTypeSeason", Self::av_s(&game_type_season))
            .item("timestamp", Self::av_s(&timestamp))
            .send()
            .await
            .context("Failed to insert high score")?;

        info!(
            "Inserted high score for game {} (user: {}, score: {})",
            game_id, username, score
        );
        Ok(())
    }

    async fn get_high_scores(
        &self,
        game_type: &common::GameType,
        region: Option<&str>,
        season: Season,
        limit: usize,
    ) -> Result<Vec<HighScoreEntry>> {
        let game_type_str = Self::game_type_to_string(game_type);
        let region_str = region.unwrap_or("global");
        let season_str = season.to_string();

        // If a specific region is requested, do a keyed query on that partition.
        if region.is_some() && region_str != "global" {
            let pk = format!("SCORE#{}#{}#{}", game_type_str, season_str, region_str);

            debug!(
                "Querying high scores - table: {}, pk: {}, season: {}, limit: {}",
                self.high_scores_table(),
                pk,
                season,
                limit
            );

            let response = self
                .client
                .query()
                .table_name(self.high_scores_table())
                .key_condition_expression("pk = :pk")
                .expression_attribute_values(":pk", Self::av_s(&pk))
                .limit(limit as i32)
                .send()
                .await
                .context("Failed to query high scores")?;

            let items = response.items.unwrap_or_default();
            debug!("Retrieved {} high score items from DynamoDB", items.len());

            let entries: Vec<HighScoreEntry> = items
                .into_iter()
                .filter_map(|item| {
                    let entry = HighScoreEntry {
                        game_id: Self::extract_string(&item, "gameId")?,
                        user_id: Self::extract_number(&item, "userId")?,
                        username: Self::extract_string(&item, "username")?,
                        score: Self::extract_number(&item, "score")?,
                        region: Self::extract_string(&item, "region")?,
                        game_type: Self::extract_string(&item, "gameType")?,
                        season: Self::extract_number(&item, "season")?.max(0) as Season,
                        timestamp: Self::extract_string(&item, "timestamp")
                            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                            .map(|dt| dt.with_timezone(&Utc))
                            .unwrap_or_else(Utc::now),
                    };
                    debug!(
                        "Parsed high score entry - user: {}, score: {}, game_id: {}",
                        entry.username, entry.score, entry.game_id
                    );
                    Some(entry)
                })
                .collect();

            debug!("Successfully parsed {} high score entries", entries.len());
            return Ok(entries);
        }

        // Global view: prefer the GameTypeSeasonIndex GSI for an ordered, single-partition query.
        let gsi_pk = format!("{}#{}", game_type_str, season_str);

        match self
            .client
            .query()
            .table_name(self.high_scores_table())
            .index_name("GameTypeSeasonIndex")
            .key_condition_expression("gameTypeSeason = :gts")
            .expression_attribute_values(":gts", Self::av_s(&gsi_pk))
            .limit(limit as i32)
            .send()
            .await
        {
            Ok(response) => {
                let items = response.items.unwrap_or_default();
                debug!(
                    "Retrieved {} high score items from GameTypeSeasonIndex for global view",
                    items.len()
                );

                let entries: Vec<HighScoreEntry> = items
                    .into_iter()
                    .filter_map(|item| {
                        let entry = HighScoreEntry {
                            game_id: Self::extract_string(&item, "gameId")?,
                            user_id: Self::extract_number(&item, "userId")?,
                            username: Self::extract_string(&item, "username")?,
                            score: Self::extract_number(&item, "score")?,
                            region: Self::extract_string(&item, "region")?,
                            game_type: Self::extract_string(&item, "gameType")?,
                            season: Self::extract_number(&item, "season")?.max(0) as Season,
                            timestamp: Self::extract_string(&item, "timestamp")
                                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                                .map(|dt| dt.with_timezone(&Utc))
                                .unwrap_or_else(Utc::now),
                        };
                        Some(entry)
                    })
                    .collect();

                debug!("Successfully parsed {} high score entries", entries.len());
                return Ok(entries);
            }
            Err(err) => {
                warn!(
                    "Falling back to scan for global high scores (GameTypeSeasonIndex not available?): {:?}",
                    err
                );
            }
        }

        // Fallback: scan across partitions filtered by game type + season, short-circuiting once we have enough.
        let pk_prefix = format!("SCORE#{}#{}#", game_type_str, season_str);
        let mut items: Vec<HashMap<String, AttributeValue>> = Vec::new();
        let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;
        // Read a little more than the requested limit to improve ordering accuracy before we sort.
        let target_items = limit.saturating_mul(3).max(limit + 5);

        while items.len() < target_items {
            let mut scan_builder = self
                .client
                .scan()
                .table_name(self.high_scores_table())
                .filter_expression("begins_with(pk, :pk_prefix)")
                .expression_attribute_values(":pk_prefix", Self::av_s(&pk_prefix))
                .limit((target_items - items.len()) as i32);

            if let Some(ref lek) = last_evaluated_key {
                scan_builder = scan_builder.set_exclusive_start_key(Some(lek.clone()));
            }

            let response = scan_builder
                .send()
                .await
                .context("Failed to scan high scores for global leaderboard")?;

            if let Some(mut batch) = response.items {
                items.append(&mut batch);
            }

            last_evaluated_key = response.last_evaluated_key;

            if last_evaluated_key.is_none() {
                break;
            }
        }

        debug!(
            "Global high score scan collected {} items (requested limit: {}, target read: {})",
            items.len(),
            limit,
            target_items
        );

        let mut entries: Vec<HighScoreEntry> = items
            .into_iter()
            .filter_map(|item| {
                let entry = HighScoreEntry {
                    game_id: Self::extract_string(&item, "gameId")?,
                    user_id: Self::extract_number(&item, "userId")?,
                    username: Self::extract_string(&item, "username")?,
                    score: Self::extract_number(&item, "score")?,
                    region: Self::extract_string(&item, "region")?,
                    game_type: Self::extract_string(&item, "gameType")?,
                    season: Self::extract_number(&item, "season")?.max(0) as Season,
                    timestamp: Self::extract_string(&item, "timestamp")
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(Utc::now),
                };
                Some(entry)
            })
            .collect();

        entries.sort_by_key(|e| std::cmp::Reverse(e.score));
        entries.truncate(limit);

        debug!(
            "Successfully parsed {} high score entries (fallback scan)",
            entries.len()
        );
        Ok(entries)
    }
}

// Private helper methods for rankings
impl DynamoDatabase {
    async fn create_rankings_table_if_not_exists(&self) -> Result<()> {
        let table_name = self.rankings_table();

        // Shared key schema definitions for the GameTypeSeasonIndex GSI
        let gsi_game_type_season_pk = KeySchemaElement::builder()
            .attribute_name("gameTypeSeason")
            .key_type(KeyType::Hash)
            .build()
            .context("Failed to build gameTypeSeason hash key for rankings")?;

        let gsi_game_type_season_sk = KeySchemaElement::builder()
            .attribute_name("sk")
            .key_type(KeyType::Range)
            .build()
            .context("Failed to build gameTypeSeason sort key for rankings")?;

        // Check if table exists, and add the cross-region GSI if missing
        match self
            .client
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
        {
            Ok(output) => {
                debug!("Rankings table {} already exists", table_name);

                let has_game_type_season_gsi = if let Some(table_desc) = output.table() {
                    let gsis = table_desc.global_secondary_indexes();
                    gsis.iter()
                        .any(|g| g.index_name.as_deref() == Some("GameTypeSeasonIndex"))
                } else {
                    false
                };

                if !has_game_type_season_gsi {
                    info!(
                        "Adding missing GameTypeSeasonIndex to existing rankings table: {}",
                        table_name
                    );

                    self.client
                        .update_table()
                        .table_name(&table_name)
                        .attribute_definitions(
                            AttributeDefinition::builder()
                                .attribute_name("gameTypeSeason")
                                .attribute_type(ScalarAttributeType::S)
                                .build()
                                .context("Failed to build gameTypeSeason attribute for rankings update")?,
                        )
                        .global_secondary_index_updates(
                            GlobalSecondaryIndexUpdate::builder()
                                .create(
                                    CreateGlobalSecondaryIndexAction::builder()
                                        .index_name("GameTypeSeasonIndex")
                                        .key_schema(gsi_game_type_season_pk.clone())
                                        .key_schema(gsi_game_type_season_sk.clone())
                                        .projection(
                                            Projection::builder()
                                                .projection_type(ProjectionType::All)
                                                .build(),
                                        )
                                        .build()
                                        .context("Failed to build rankings GameTypeSeasonIndex update action")?,
                                )
                                .build(),
                        )
                        .send()
                        .await
                        .context("Failed to add GameTypeSeasonIndex to existing rankings table")?;
                }

                return Ok(());
            }
            Err(_) => {
                info!("Creating rankings table: {}", table_name);
            }
        }

        // PK: RANKING#{queue_mode}#{game_type}#{region}#{season} (e.g., "RANKING#ranked#solo#us-east-1#2025-S1")
        // SK: MMR#{padded_mmr}#USER#{user_id} (e.g., "MMR#00001543#USER#1234")
        // GSI: GameTypeSeasonIndex with gameTypeSeason as PK and sk as SK for cross-region seasonal lookups
        // This schema allows:
        // - Querying top players by queue_mode + game_type + region + season (sorted by MMR descending)
        // - Querying top players across all regions for a queue/game_type/season via the GSI
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

        // Attribute for global seasonal lookups
        let gsi_game_type_season_attr = AttributeDefinition::builder()
            .attribute_name("gameTypeSeason")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .context("Failed to build gameTypeSeason attribute for rankings")?;

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

        // GSI for cross-region lookups by queue mode + game type + season
        let game_type_season_gsi = GlobalSecondaryIndex::builder()
            .index_name("GameTypeSeasonIndex")
            .key_schema(gsi_game_type_season_pk)
            .key_schema(gsi_game_type_season_sk)
            .projection(
                Projection::builder()
                    .projection_type(ProjectionType::All)
                    .build(),
            )
            .build()
            .context("Failed to build GameTypeSeasonIndex GSI for rankings")?;

        let result = self
            .client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(pk_attr)
            .attribute_definitions(sk_attr)
            .attribute_definitions(gsi_game_type_season_attr)
            .key_schema(pk_key)
            .key_schema(sk_key)
            .global_secondary_indexes(game_type_season_gsi)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        self.finish_table_creation(&table_name, result).await
    }

    async fn create_high_scores_table_if_not_exists(&self) -> Result<()> {
        let table_name = self.high_scores_table();

        // Shared key schema definitions for the GameTypeSeasonIndex GSI
        let gsi_game_type_season_pk = KeySchemaElement::builder()
            .attribute_name("gameTypeSeason")
            .key_type(KeyType::Hash)
            .build()
            .context("Failed to build gameTypeSeason hash key")?;

        let gsi_game_type_season_sk = KeySchemaElement::builder()
            .attribute_name("sk")
            .key_type(KeyType::Range)
            .build()
            .context("Failed to build gameTypeSeason sort key")?;

        // Check if table exists
        match self
            .client
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
        {
            Ok(output) => {
                debug!("High scores table {} already exists", table_name);

                let has_game_type_season_gsi = if let Some(table_desc) = output.table() {
                    let gsis = table_desc.global_secondary_indexes();
                    gsis.iter()
                        .any(|g| g.index_name.as_deref() == Some("GameTypeSeasonIndex"))
                } else {
                    false
                };

                if !has_game_type_season_gsi {
                    info!(
                        "Adding missing GameTypeSeasonIndex to existing high scores table: {}",
                        table_name
                    );

                    self.client
                        .update_table()
                        .table_name(&table_name)
                        .attribute_definitions(
                            AttributeDefinition::builder()
                                .attribute_name("gameTypeSeason")
                                .attribute_type(ScalarAttributeType::S)
                                .build()
                                .context("Failed to build gameTypeSeason attribute for update")?,
                        )
                        .global_secondary_index_updates(
                            GlobalSecondaryIndexUpdate::builder()
                                .create(
                                    CreateGlobalSecondaryIndexAction::builder()
                                        .index_name("GameTypeSeasonIndex")
                                        .key_schema(gsi_game_type_season_pk.clone())
                                        .key_schema(gsi_game_type_season_sk.clone())
                                        .projection(
                                            Projection::builder()
                                                .projection_type(ProjectionType::All)
                                                .build(),
                                        )
                                        .build()
                                        .context(
                                            "Failed to build GameTypeSeasonIndex update action",
                                        )?,
                                )
                                .build(),
                        )
                        .send()
                        .await
                        .context(
                            "Failed to add GameTypeSeasonIndex to existing high scores table",
                        )?;
                }

                return Ok(());
            }
            Err(_) => {
                info!("Creating high scores table: {}", table_name);
            }
        }

        // PK: SCORE#{game_type}#{season}#{region} (e.g., "SCORE#solo#2025-S1#us-east-1")
        // SK: SCORE#{inverted_score}#GAME#{game_id} (e.g., "SCORE#99998457#GAME#1234")
        // GSI: UserScoreIndex with userId as PK and sk as SK for user-specific lookups
        // GSI: GameTypeSeasonIndex with gameTypeSeason as PK and sk as SK for cross-region seasonal lookups
        // This schema allows:
        // - Querying top scores by game_type + season + region (sorted by score descending)
        // - Querying top scores by game_type + season across all regions via GSI
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

        let user_id_attr = AttributeDefinition::builder()
            .attribute_name("userId")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .context("Failed to build userId attribute")?;

        // GSI for global aggregation by game type + season
        let gsi_game_type_season_attr = AttributeDefinition::builder()
            .attribute_name("gameTypeSeason")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .context("Failed to build gameTypeSeason attribute")?;

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

        // GSI for querying by game type + season (global leaderboard)
        let game_type_season_gsi = GlobalSecondaryIndex::builder()
            .index_name("GameTypeSeasonIndex")
            .key_schema(gsi_game_type_season_pk.clone())
            .key_schema(gsi_game_type_season_sk.clone())
            .projection(
                Projection::builder()
                    .projection_type(ProjectionType::All)
                    .build(),
            )
            .build()
            .context("Failed to build GameTypeSeasonIndex GSI")?;

        let result = self
            .client
            .create_table()
            .table_name(&table_name)
            .attribute_definitions(pk_attr)
            .attribute_definitions(sk_attr)
            .attribute_definitions(user_id_attr)
            .attribute_definitions(gsi_game_type_season_attr)
            .key_schema(pk_key)
            .key_schema(sk_key)
            .global_secondary_indexes(gsi)
            .global_secondary_indexes(game_type_season_gsi)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        self.finish_table_creation(&table_name, result).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_dynamodb_standard_retry_policy_is_capped_at_five_attempts() {
        assert_eq!(
            dynamodb_retry_config().max_attempts(),
            DYNAMODB_RUNTIME_MAX_ATTEMPTS
        );
    }

    #[test]
    fn completed_game_retention_uses_configured_positive_days() {
        assert_eq!(
            DynamoDatabase::completed_game_retention_days(Some("45")),
            45
        );
    }

    #[test]
    fn completed_game_retention_rejects_invalid_or_non_positive_values() {
        for value in [None, Some(""), Some("invalid"), Some("0"), Some("-1")] {
            assert_eq!(
                DynamoDatabase::completed_game_retention_days(value),
                DEFAULT_COMPLETED_GAME_RETENTION_DAYS
            );
        }
    }

    #[test]
    fn game_from_item_reads_persisted_timestamps() {
        let created_at = "2026-07-17T10:00:00+00:00";
        let last_activity = "2026-07-17T10:05:00+00:00";
        let ended_at = "2026-07-17T10:06:00+00:00";
        let mut item = HashMap::new();
        item.insert("createdAt".to_string(), DynamoDatabase::av_s(created_at));
        item.insert(
            "lastActivity".to_string(),
            DynamoDatabase::av_s(last_activity),
        );
        item.insert("endedAt".to_string(), DynamoDatabase::av_s(ended_at));
        item.insert("status".to_string(), DynamoDatabase::av_s("complete"));
        item.insert(
            "gameState".to_string(),
            DynamoDatabase::av_s(r#"{"tick":42}"#),
        );

        let game = DynamoDatabase::game_from_item(123, &item).unwrap();

        assert_eq!(game.created_at.to_rfc3339(), created_at);
        assert_eq!(game.last_activity.to_rfc3339(), last_activity);
        assert_eq!(
            game.ended_at.map(|value| value.to_rfc3339()).as_deref(),
            Some(ended_at)
        );
        assert_eq!(game.game_state, Some(json!({ "tick": 42 })));
    }

    #[test]
    fn item_expiration_supports_dynamo_numbers_and_legacy_strings() {
        let mut numeric_item = HashMap::new();
        numeric_item.insert("ttl".to_string(), DynamoDatabase::av_n(100));
        assert!(DynamoDatabase::item_is_expired(&numeric_item, 100));
        assert!(!DynamoDatabase::item_is_expired(&numeric_item, 99));

        let mut string_item = HashMap::new();
        string_item.insert("ttl".to_string(), DynamoDatabase::av_s("100"));
        assert!(DynamoDatabase::item_is_expired(&string_item, 101));

        let item_without_ttl = HashMap::new();
        assert!(!DynamoDatabase::item_is_expired(&item_without_ttl, 101));
    }

    #[test]
    fn completion_fingerprint_is_independent_of_hash_map_order() {
        let mut left = HashMap::new();
        left.insert("b", 2);
        left.insert("a", 1);
        let mut right = HashMap::new();
        right.insert("a", 1);
        right.insert("b", 2);

        assert_eq!(
            DynamoDatabase::canonical_fingerprint(&left).unwrap(),
            DynamoDatabase::canonical_fingerprint(&right).unwrap()
        );
    }
}
