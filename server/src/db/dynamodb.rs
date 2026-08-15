use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::create_table::{CreateTableError, CreateTableOutput};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, ConditionCheck,
    CreateGlobalSecondaryIndexAction, Delete, GlobalSecondaryIndex, GlobalSecondaryIndexUpdate,
    KeySchemaElement, KeyType, Projection, ProjectionType, Put, ReturnValue, ScalarAttributeType,
    TableStatus, TimeToLiveSpecification, TimeToLiveStatus, TransactWriteItem, Update,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, time::Duration};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::models::*;
use super::{Database, SERVER_HEARTBEAT_FRESHNESS_SECONDS, ServerRegistration};
use crate::completion::{
    CompletionEffect, CompletionRecordV1, EffectApplyResult, MATCH_HISTORY_SCHEMA_VERSION,
    canonical_json_bytes, match_history_summary,
};
use crate::season::{Season, get_season_at};

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
const GUEST_UPGRADE_MAX_ATTEMPTS: usize = 8;
const CRAZYGAMES_IDENTITY_MAX_ATTEMPTS: usize = 8;
const DYNAMODB_RUNTIME_MAX_ATTEMPTS: u32 = 5;
const HISTORY_PAGE_DEFAULT_LIMIT: usize = 20;
const HISTORY_PAGE_MAX_LIMIT: usize = 50;
const PAGE_CURSOR_MAX_BYTES: usize = 2_048;
const HISTORY_GSI_PARTITION: &str = "MATCH_HISTORY";
const RUNTIME_CONFIG_PK: &str = "CONFIG#RUNTIME";
const RUNTIME_CONFIG_CURRENT_SK: &str = "CURRENT";
const RUNTIME_CONFIG_SCHEMA_VERSION_V1: u16 = 1;
const MAX_PRE_MATCH_AD_BREAK_USERS: usize = 4;
const MAX_DYNAMODB_CLIENT_REQUEST_TOKEN_BYTES: usize = 36;

const fn runtime_config_schema_version_v1() -> u16 {
    RUNTIME_CONFIG_SCHEMA_VERSION_V1
}

const fn default_ad_interval_minutes_v1() -> u16 {
    10
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct RuntimeAdsConfigV1 {
    // Kept only to decode schema-v1 rows. It must never authorize a pre-match
    // ad when the record is upconverted.
    post_match_enabled: bool,
    minimum_interval_minutes: u16,
}

impl Default for RuntimeAdsConfigV1 {
    fn default() -> Self {
        Self {
            post_match_enabled: false,
            minimum_interval_minutes: default_ad_interval_minutes_v1(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct RuntimeConfigV1 {
    announcement: RuntimeAnnouncementConfig,
    ads: RuntimeAdsConfigV1,
    history: RuntimeHistoryConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeConfigRecordV1 {
    #[serde(default = "runtime_config_schema_version_v1")]
    schema_version: u16,
    version: u64,
    config: RuntimeConfigV1,
    updated_by: Option<RuntimeConfigActor>,
    updated_at_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DynamoPageCursor {
    version: u8,
    scope: String,
    pk: String,
    sk: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gsi2pk: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gsi2sk: Option<String>,
}
const RECENT_COMPLETED_GAMES_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone)]
struct CrazyGamesIdentityRecord {
    user_id: i32,
    provider_user_id: String,
    username: String,
    avatar_url: String,
    profile_iat: i64,
}

#[derive(Clone, Copy)]
enum UserProgressMutation {
    Add(i32),
    Set(i32),
}

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
            season: Self::extract_number(item, "season")
                .and_then(|season| Season::try_from(season).ok()),
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
            news_eligible: Self::extract_bool(item, "newsEligible") == Some(true)
                && Self::extract_bool(item, "isPrivate") == Some(false)
                && Self::extract_string(item, "gameCode").is_none(),
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

    fn bounded_page_limit(limit: usize) -> usize {
        if limit == 0 {
            HISTORY_PAGE_DEFAULT_LIMIT
        } else {
            limit.min(HISTORY_PAGE_MAX_LIMIT)
        }
    }

    fn history_sort_key(ended_at_ms: i64, game_id: u32) -> Result<String> {
        let ended_at_ms = u64::try_from(ended_at_ms)
            .context("Match history timestamps must be non-negative epoch milliseconds")?;
        Ok(format!("HISTORY#{ended_at_ms:020}#GAME#{game_id:010}"))
    }

    fn retention_ttl_seconds(ended_at_ms: i64, retention_days: u16) -> Result<i64> {
        if ended_at_ms < 0 {
            return Err(anyhow!(
                "Retention timestamps must be non-negative epoch milliseconds"
            ));
        }
        let expiry_ms = ended_at_ms.saturating_add(
            i64::from(retention_days).saturating_mul(SECONDS_PER_DAY.saturating_mul(1_000)),
        );
        // DynamoDB TTL is second-granular. Round up so the millisecond-level
        // snapshot availability field never outlives the stored snapshot.
        Ok(expiry_ms.saturating_add(999) / 1_000)
    }

    fn encode_page_cursor(scope: &str, item: &HashMap<String, AttributeValue>) -> Result<String> {
        let cursor = DynamoPageCursor {
            version: 1,
            scope: scope.to_string(),
            pk: Self::extract_string(item, "pk")
                .ok_or_else(|| anyhow!("history item is missing pk"))?,
            sk: Self::extract_string(item, "sk")
                .ok_or_else(|| anyhow!("history item is missing sk"))?,
            gsi2pk: Self::extract_string(item, "gsi2pk"),
            gsi2sk: Self::extract_string(item, "gsi2sk"),
        };
        Ok(hex::encode(
            serde_json::to_vec(&cursor).context("Failed to serialize page cursor")?,
        ))
    }

    fn decode_page_cursor(
        raw: &str,
        expected_scope: &str,
        cursor_name: &str,
    ) -> Result<DynamoPageCursor> {
        let invalid = |detail: &str| anyhow!("invalid {cursor_name} cursor: {detail}");
        if raw.is_empty() || raw.len() > PAGE_CURSOR_MAX_BYTES {
            return Err(invalid("token length is invalid"));
        }
        let bytes = hex::decode(raw).map_err(|_| invalid("token encoding is invalid"))?;
        let cursor: DynamoPageCursor =
            serde_json::from_slice(&bytes).map_err(|_| invalid("token payload is invalid"))?;
        if cursor.version != 1 || cursor.scope != expected_scope {
            return Err(invalid("token scope is invalid"));
        }
        Ok(cursor)
    }

    fn cursor_key(cursor: &DynamoPageCursor) -> HashMap<String, AttributeValue> {
        let mut key = HashMap::from([
            ("pk".to_string(), Self::av_s(&cursor.pk)),
            ("sk".to_string(), Self::av_s(&cursor.sk)),
        ]);
        if let Some(gsi2pk) = &cursor.gsi2pk {
            key.insert("gsi2pk".to_string(), Self::av_s(gsi2pk));
        }
        if let Some(gsi2sk) = &cursor.gsi2sk {
            key.insert("gsi2sk".to_string(), Self::av_s(gsi2sk));
        }
        key
    }

    fn history_summary_from_item(
        item: &HashMap<String, AttributeValue>,
    ) -> Result<MatchHistorySummary> {
        let summary = Self::extract_string(item, "summaryJson")
            .ok_or_else(|| anyhow!("match history row is missing summaryJson"))?;
        let value: JsonValue = serde_json::from_str(&summary)
            .context("Match history row contains invalid summaryJson")?;
        let schema_version = value
            .get("schemaVersion")
            .and_then(JsonValue::as_u64)
            .and_then(|version| u16::try_from(version).ok())
            .ok_or_else(|| anyhow!("match history row has an invalid schemaVersion"))?;
        match schema_version {
            // Keep this dispatch explicit so a future schema can be upconverted
            // without changing or rewriting retained immutable version-1 rows.
            MATCH_HISTORY_SCHEMA_VERSION => serde_json::from_value(value)
                .context("Match history row contains an invalid version-1 summary"),
            version => Err(anyhow!(
                "match history row uses unsupported schemaVersion {version}"
            )),
        }
    }

    fn runtime_config_record_from_item(
        item: &HashMap<String, AttributeValue>,
    ) -> Result<RuntimeConfigRecord> {
        let record = Self::extract_string(item, "recordJson")
            .ok_or_else(|| anyhow!("runtime config row is missing recordJson"))?;
        let value: JsonValue =
            serde_json::from_str(&record).context("Runtime config row is corrupt")?;
        let schema_version = match value.get("schemaVersion") {
            Some(value) => value
                .as_u64()
                .and_then(|version| u16::try_from(version).ok())
                .ok_or_else(|| anyhow!("runtime config row has an invalid schemaVersion"))?,
            // Records written before schemaVersion was persisted have the v1
            // shape and must follow the same safe upconversion path.
            None => RUNTIME_CONFIG_SCHEMA_VERSION_V1,
        };
        let record = match schema_version {
            RUNTIME_CONFIG_SCHEMA_VERSION_V1 => {
                let legacy: RuntimeConfigRecordV1 = serde_json::from_value(value)
                    .context("Runtime config row contains an invalid version-1 record")?;
                if legacy.schema_version != RUNTIME_CONFIG_SCHEMA_VERSION_V1 {
                    return Err(anyhow!(
                        "runtime config row uses unsupported schemaVersion {}",
                        legacy.schema_version
                    ));
                }
                let RuntimeConfigV1 {
                    announcement,
                    ads: legacy_ads,
                    history,
                } = legacy.config;
                let RuntimeAdsConfigV1 {
                    post_match_enabled: _,
                    minimum_interval_minutes,
                } = legacy_ads;
                // The interval has equivalent cooldown meaning. The old
                // post-match switch intentionally does not enable any new
                // pre-match policy or distribution.
                let ads = RuntimeAdsConfig {
                    minimum_interval_minutes,
                    ..RuntimeAdsConfig::default()
                };
                RuntimeConfigRecord {
                    schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                    version: legacy.version,
                    config: RuntimeConfig {
                        announcement,
                        ads,
                        history,
                    },
                    updated_by: legacy.updated_by,
                    updated_at_ms: legacy.updated_at_ms,
                }
            }
            RUNTIME_CONFIG_SCHEMA_VERSION => serde_json::from_value(value)
                .context("Runtime config row contains an invalid version-2 record")?,
            version => {
                return Err(anyhow!(
                    "runtime config row uses unsupported schemaVersion {version}"
                ));
            }
        };
        record
            .config
            .validate()
            .map_err(|error| anyhow!("runtime config row is invalid: {error}"))?;
        Ok(record)
    }

    /// Resolve source privacy before completion persistence. A new runtime
    /// match can use its server-owned state as the fallback. Existing metadata
    /// must explicitly attest public visibility; an already-completed legacy
    /// row must carry the newer durable proof or fail closed.
    fn source_game_item_is_news_eligible(
        item: Option<&HashMap<String, AttributeValue>>,
        new_game_fallback: bool,
    ) -> bool {
        let Some(item) = item else {
            return new_game_fallback;
        };
        if Self::extract_string(item, "status").as_deref() == Some("complete") {
            return Self::extract_bool(item, "newsEligible") == Some(true)
                && Self::extract_bool(item, "isPrivate") == Some(false)
                && Self::extract_string(item, "gameCode").is_none();
        }
        Self::extract_bool(item, "isPrivate") == Some(false)
            && Self::extract_string(item, "gameCode").is_none()
    }

    /// Public attribution requires affirmative completion provenance. Missing
    /// legacy fields and non-terminal rows fail closed.
    fn completed_game_item_is_news_eligible(
        item: Option<&HashMap<String, AttributeValue>>,
    ) -> bool {
        item.is_some_and(|item| {
            Self::extract_bool(item, "newsEligible") == Some(true)
                && Self::extract_bool(item, "isPrivate") == Some(false)
                && Self::extract_string(item, "gameCode").is_none()
                && Self::extract_string(item, "status").as_deref() == Some("complete")
        })
    }

    fn new_game_state_is_news_eligible(game_state: &common::GameState) -> bool {
        !game_state.is_stress_test
            && game_state.game_code.is_none()
            && !matches!(game_state.game_type, common::GameType::Custom { .. })
    }

    async fn completed_game_is_news_eligible(&self, game_id: u32) -> Result<bool> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{game_id}")))
            .key("sk", Self::av_s("META"))
            .consistent_read(true)
            .projection_expression("isPrivate, gameCode, newsEligible, #status")
            .expression_attribute_names("#status", "status")
            .send()
            .await
            .context("Failed to read completed-game privacy provenance")?;

        Ok(Self::completed_game_item_is_news_eligible(
            response.item.as_ref(),
        ))
    }

    async fn source_game_is_news_eligible(
        &self,
        game_id: i32,
        new_game_fallback: bool,
    ) -> Result<bool> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{game_id}")))
            .key("sk", Self::av_s("META"))
            .consistent_read(true)
            .projection_expression("isPrivate, gameCode, newsEligible, #status")
            .expression_attribute_names("#status", "status")
            .send()
            .await
            .context("Failed to read source-game privacy metadata")?;

        Ok(Self::source_game_item_is_news_eligible(
            response.item.as_ref(),
            new_game_fallback,
        ))
    }

    fn recent_completed_games_page_limit(remaining: usize) -> Option<i32> {
        (remaining > 0).then(|| {
            i32::try_from(remaining.min(RECENT_COMPLETED_GAMES_PAGE_SIZE))
                .expect("recent completed-game page size fits in i32")
        })
    }

    fn recent_completed_games_from_items(
        items: Vec<HashMap<String, AttributeValue>>,
        now_epoch_seconds: i64,
    ) -> Result<Vec<Game>> {
        items
            .into_iter()
            .filter(|item| !Self::item_is_expired(item, now_epoch_seconds))
            .map(|item| {
                let game_id = Self::extract_number(&item, "id")
                    .context("Recent completed game is missing a valid id")?;
                Self::game_from_item(game_id, &item)
            })
            .collect()
    }

    fn append_recent_completed_games_from_items(
        games: &mut Vec<Game>,
        items: Vec<HashMap<String, AttributeValue>>,
        now_epoch_seconds: i64,
        limit: usize,
    ) -> Result<()> {
        let remaining = limit.saturating_sub(games.len());
        if remaining == 0 {
            return Ok(());
        }
        let mut accepted = Self::recent_completed_games_from_items(items, now_epoch_seconds)?;
        accepted.truncate(remaining);
        games.append(&mut accepted);
        Ok(())
    }

    fn high_score_entry_from_item(
        item: &HashMap<String, AttributeValue>,
    ) -> Option<HighScoreEntry> {
        let stored_season = Self::extract_number(item, "season")?;
        Some(HighScoreEntry {
            game_id: Self::extract_string(item, "gameId")?,
            user_id: Self::extract_number(item, "userId")?,
            username: Self::extract_string(item, "username")?,
            score: Self::extract_number(item, "score")?,
            region: Self::extract_string(item, "region")?,
            game_type: Self::extract_string(item, "gameType")?,
            season: Season::try_from(stored_season).ok()?,
            timestamp: Self::extract_string(item, "timestamp")
                .and_then(|timestamp| DateTime::parse_from_rfc3339(&timestamp).ok())
                .map(|timestamp| timestamp.with_timezone(&Utc))?,
            news_eligible: Self::extract_bool(item, "newsEligible") == Some(true),
        })
    }

    /// Sort key of a user's ranking *pointer* — the item that makes "what is
    /// this user's standing on this ladder?" a keyed read.
    ///
    /// The ladder rows are keyed `MMR#{inverted}#USER#{id}` so the partition
    /// sorts by rating, which means a user cannot be looked up by key at all.
    /// The pointer duplicates the row's fields under a second, user-addressed
    /// sort key. `"MMR#" < "USER#"`, so every pointer sorts after every ladder
    /// row and a top-N leaderboard query never reaches them.
    fn ranking_pointer_sk(user_id: i32) -> String {
        format!("USER#{}", user_id)
    }

    /// A pointer recording that a user has *no* row on this ladder.
    ///
    /// Without it, every unranked visitor to the leaderboard would re-run the
    /// legacy full-partition scan on each request, since a miss is otherwise
    /// indistinguishable from "not yet migrated".
    fn absent_ranking_pointer(pk: &str, user_id: i32) -> HashMap<String, AttributeValue> {
        // Deliberately carries no `userId`, so a tombstone can never parse as
        // a ranking even if the `absent` flag is ever dropped.
        HashMap::from([
            ("pk".to_string(), Self::av_s(pk)),
            (
                "sk".to_string(),
                Self::av_s(Self::ranking_pointer_sk(user_id)),
            ),
            ("absent".to_string(), Self::av_bool(true)),
            ("updatedAt".to_string(), Self::av_s(Utc::now().to_rfc3339())),
        ])
    }

    /// Store a pointer discovered by the legacy scan, without ever overwriting
    /// one that already exists.
    ///
    /// The condition is what makes this safe to run on a read path: a scan
    /// racing a concurrent `upsert_ranking` must not be able to roll the
    /// user's counters back to the values it happened to observe.
    async fn backfill_ranking_pointer(&self, item: HashMap<String, AttributeValue>) {
        let result = self
            .client
            .put_item()
            .table_name(self.rankings_table())
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;

        // A lost race is the expected outcome, not an error, and any other
        // failure only costs the next reader one more scan.
        if let Err(err) = result
            && err.code() != Some("ConditionalCheckFailedException")
        {
            debug!("Ranking pointer backfill did not apply: {:?}", err);
        }
    }

    fn user_ranking_from_items<'a>(
        items: impl IntoIterator<Item = &'a HashMap<String, AttributeValue>>,
        user_id: i32,
        queue_mode: &str,
        game_type: &str,
        region: &str,
        season: Season,
    ) -> Option<RankingEntry> {
        let item = items
            .into_iter()
            .find(|item| Self::extract_number(item, "userId") == Some(user_id))?;

        Some(RankingEntry {
            user_id: Self::extract_number(item, "userId").unwrap_or(user_id),
            username: Self::extract_string(item, "username").unwrap_or_default(),
            mmr: Self::extract_number(item, "mmr").unwrap_or(1000),
            games_played: Self::extract_number(item, "gamesPlayed").unwrap_or(0),
            wins: Self::extract_number(item, "wins").unwrap_or(0),
            losses: Self::extract_number(item, "losses").unwrap_or(0),
            region: Self::extract_string(item, "region").unwrap_or_else(|| region.to_string()),
            queue_mode: Self::extract_string(item, "queueMode")
                .unwrap_or_else(|| queue_mode.to_string()),
            game_type: Self::extract_string(item, "gameType")
                .unwrap_or_else(|| game_type.to_string()),
            season: Self::extract_number(item, "season")
                .map(|stored_season| stored_season as Season)
                .unwrap_or(season),
            updated_at: Self::extract_string(item, "updatedAt")
                .and_then(|timestamp| DateTime::parse_from_rfc3339(&timestamp).ok())
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .unwrap_or_else(Utc::now),
        })
    }

    /// Parse a leaderboard row only when its durable numeric season exactly
    /// matches the requested partition. This is a second line of defense for
    /// scan fallbacks and prevents Season 1 from admitting Season 10 rows.
    fn leaderboard_entry_from_item(
        item: &HashMap<String, AttributeValue>,
        requested_season: Season,
    ) -> Option<RankingEntry> {
        let stored_season = Self::extract_number(item, "season")?;
        let stored_season = Season::try_from(stored_season).ok()?;
        if stored_season != requested_season {
            return None;
        }

        Some(RankingEntry {
            user_id: Self::extract_number(item, "userId")?,
            username: Self::extract_string(item, "username")?,
            mmr: Self::extract_number(item, "mmr")?,
            games_played: Self::extract_number(item, "gamesPlayed")?,
            wins: Self::extract_number(item, "wins")?,
            losses: Self::extract_number(item, "losses")?,
            region: Self::extract_string(item, "region")?,
            queue_mode: Self::extract_string(item, "queueMode")?,
            game_type: Self::extract_string(item, "gameType")
                .unwrap_or_else(|| "unknown".to_string()),
            season: stored_season,
            updated_at: Self::extract_string(item, "updatedAt")
                .and_then(|timestamp| DateTime::parse_from_rfc3339(&timestamp).ok())
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .unwrap_or_else(Utc::now),
        })
    }

    fn unique_public_high_score_leader(
        ordered_head: &[Option<HighScoreEntry>],
    ) -> Option<HighScoreEntry> {
        let leader = ordered_head.first()?.as_ref()?;
        if !leader.news_eligible {
            return None;
        }
        if let Some(runner_up) = ordered_head.get(1) {
            let runner_up = runner_up.as_ref()?;
            if leader.score <= runner_up.score {
                return None;
            }
        }
        Some(leader.clone())
    }

    fn high_score_matches_sort_key(
        item: &HashMap<String, AttributeValue>,
        entry: &HighScoreEntry,
    ) -> bool {
        if !(0..=99_999_999).contains(&entry.score) {
            return false;
        }
        let inverted = 99_999_999_i64 - i64::from(entry.score);
        let expected_legacy = format!("SCORE#{inverted:08}#GAME#{}", entry.game_id);
        let expected_completion = format!("{expected_legacy}#USER#{}", entry.user_id);
        Self::extract_string(item, "sk")
            .is_some_and(|sort_key| sort_key == expected_legacy || sort_key == expected_completion)
    }

    fn legacy_high_score_source_item_is_news_eligible(
        item: Option<&HashMap<String, AttributeValue>>,
        entry: &HighScoreEntry,
    ) -> bool {
        let Some(item) = item else {
            return false;
        };
        if Self::extract_string(item, "status").as_deref() != Some("complete")
            || Self::extract_bool(item, "isPrivate") != Some(false)
            || Self::extract_string(item, "gameCode").is_some()
        {
            return false;
        }
        let Some(state) = Self::extract_string(item, "gameState")
            .and_then(|value| serde_json::from_str::<common::GameState>(&value).ok())
        else {
            return false;
        };
        if state.is_stress_test
            || state.game_code.is_some()
            || !matches!(state.game_type, common::GameType::Solo)
            || !matches!(state.status, common::GameStatus::Complete { .. })
        {
            return false;
        }
        let Ok(user_id) = u32::try_from(entry.user_id) else {
            return false;
        };
        let Some(player) = state.players.get(&user_id) else {
            return false;
        };
        state.scores.get(&player.snake_id).copied() == u32::try_from(entry.score).ok()
            && state.usernames.get(&user_id) == Some(&entry.username)
    }

    /// Upgrade an old unmarked score only while its retained source snapshot
    /// can still prove the exact public result. Missing/expired source games
    /// remain ineligible rather than turning uncertainty into a headline.
    async fn backfill_legacy_high_score_news_eligibility(
        &self,
        item: &HashMap<String, AttributeValue>,
        entry: &HighScoreEntry,
    ) -> Result<bool> {
        if item.contains_key("newsEligible") {
            return Ok(entry.news_eligible);
        }
        let Ok(game_id) = entry.game_id.parse::<i32>() else {
            return Ok(false);
        };
        let source = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("GAME#{game_id}")))
            .key("sk", Self::av_s("META"))
            .consistent_read(true)
            .projection_expression("isPrivate, gameCode, gameState, #status")
            .expression_attribute_names("#status", "status")
            .send()
            .await
            .context("Failed to verify a legacy high-score source")?;
        if !Self::legacy_high_score_source_item_is_news_eligible(source.item.as_ref(), entry) {
            return Ok(false);
        }

        let Some(pk) = Self::extract_string(item, "pk") else {
            return Ok(false);
        };
        let Some(sk) = Self::extract_string(item, "sk") else {
            return Ok(false);
        };
        let response = self
            .client
            .update_item()
            .table_name(self.high_scores_table())
            .key("pk", Self::av_s(pk))
            .key("sk", Self::av_s(sk))
            .update_expression("SET newsEligible = if_not_exists(newsEligible, :news_eligible)")
            .expression_attribute_values(":news_eligible", Self::av_bool(true))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .context("Failed to backfill legacy high-score provenance")?;

        Ok(response
            .attributes
            .as_ref()
            .and_then(|attributes| Self::extract_bool(attributes, "newsEligible"))
            == Some(true))
    }

    async fn query_global_news_high_score_snapshot(
        &self,
        game_type: &str,
        season: Season,
    ) -> Result<NewsHighScoreSnapshot> {
        let partition = format!("{game_type}#{season}");
        // No filter is applied: these must be the actual first two rows so a
        // private or malformed top row cannot promote the next public score.
        // One bounded read is enough to prove a unique leader and cannot walk
        // an append-only season partition looking for eligible rows.
        let response = self
            .client
            .query()
            .table_name(self.high_scores_table())
            .index_name("GameTypeSeasonIndex")
            .key_condition_expression("gameTypeSeason = :partition")
            .expression_attribute_values(":partition", Self::av_s(&partition))
            .scan_index_forward(true)
            .limit(2)
            .send()
            .await
            .context("Failed to query ordered global news high scores")?;
        let mut ordered_head = Vec::with_capacity(2);
        for (index, item) in response
            .items
            .unwrap_or_default()
            .into_iter()
            .take(2)
            .enumerate()
        {
            let mut parsed = Self::high_score_entry_from_item(&item).filter(|entry| {
                entry.game_type == game_type
                    && entry.season == season
                    && Self::high_score_matches_sort_key(&item, entry)
            });
            if index == 0
                && let Some(entry) = parsed.as_mut()
                && !entry.news_eligible
                && self
                    .backfill_legacy_high_score_news_eligibility(&item, entry)
                    .await?
            {
                entry.news_eligible = true;
            }
            ordered_head.push(parsed);
        }

        Ok(NewsHighScoreSnapshot {
            leader: Self::unique_public_high_score_leader(&ordered_head),
            coverage: NewsLeaderboardCoverage::OrderedGlobalIndex,
        })
    }

    fn validate_pre_match_ad_break_claim(
        break_id: &str,
        user_ids: &[u32],
        now_ms: i64,
        minimum_interval_ms: i64,
        policy_version: u64,
    ) -> Result<i64> {
        if break_id.is_empty()
            || break_id.len() > MAX_DYNAMODB_CLIENT_REQUEST_TOKEN_BYTES
            || break_id.chars().any(char::is_control)
        {
            return Err(anyhow!(
                "pre-match ad break ID must be 1 to {MAX_DYNAMODB_CLIENT_REQUEST_TOKEN_BYTES} bytes and contain no control characters"
            ));
        }
        if user_ids.is_empty() || user_ids.len() > MAX_PRE_MATCH_AD_BREAK_USERS {
            return Err(anyhow!(
                "pre-match ad break must target between 1 and {MAX_PRE_MATCH_AD_BREAK_USERS} users"
            ));
        }
        for (index, user_id) in user_ids.iter().enumerate() {
            if *user_id == 0 || *user_id > i32::MAX as u32 {
                return Err(anyhow!("pre-match ad break contains an invalid user ID"));
            }
            if user_ids[..index].contains(user_id) {
                return Err(anyhow!("pre-match ad break contains a duplicate user ID"));
            }
        }
        if now_ms < 0 {
            return Err(anyhow!("pre-match ad break timestamp must not be negative"));
        }
        if minimum_interval_ms <= 0 {
            return Err(anyhow!(
                "pre-match ad break minimum interval must be positive"
            ));
        }
        if policy_version == 0 {
            return Err(anyhow!(
                "pre-match ad break policy version must be positive"
            ));
        }
        now_ms
            .checked_sub(minimum_interval_ms)
            .ok_or_else(|| anyhow!("pre-match ad break cooldown cutoff overflow"))
    }

    fn transaction_cancellation_is_conditional(error: &TransactWriteItemsError) -> bool {
        match error {
            TransactWriteItemsError::TransactionCanceledException(cancelled) => cancelled
                .cancellation_reasons()
                .iter()
                .any(|reason| reason.code() == Some("ConditionalCheckFailed")),
            _ => false,
        }
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

    async fn completion_user_target(&self, user_id: u32) -> Result<(String, bool, bool)> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{user_id}")))
            .key("sk", Self::av_s("META"))
            .consistent_read(true)
            .projection_expression("username, isGuest, authProvider")
            .send()
            .await
            .context("Failed to read completion effect user")?;
        let item = response
            .item
            .ok_or_else(|| anyhow!("user {user_id} disappeared before completion effect"))?;
        let username = Self::extract_string(&item, "username")
            .ok_or_else(|| anyhow!("user {user_id} has no username"))?;
        let is_guest = Self::extract_bool(&item, "isGuest").unwrap_or(false);
        let uses_username_mirror = !is_guest
            && Self::extract_string(&item, "authProvider").as_deref() != Some("crazygames");
        Ok((username, is_guest, uses_username_mirror))
    }

    fn user_progress_value(user: &User, field: &str) -> Result<i32> {
        match field {
            "mmr" => Ok(user.mmr),
            "rankedMmr" => Ok(user.ranked_mmr),
            "casualMmr" => Ok(user.casual_mmr),
            "xp" => Ok(user.xp),
            _ => Err(anyhow!("Unsupported user progress field '{field}'")),
        }
    }

    /// Mutate canonical progress and its registered-user compatibility mirror
    /// in one transaction. The guest/account state guard makes an upgrade race
    /// retry with the correct mirror shape instead of skipping or doubling it.
    async fn mutate_user_progress(
        &self,
        user_id: i32,
        field: &str,
        mutation: UserProgressMutation,
    ) -> Result<i32> {
        for attempt in 0..GUEST_UPGRADE_MAX_ATTEMPTS {
            let user = self
                .get_user_by_id(user_id)
                .await?
                .ok_or_else(|| anyhow!("User not found"))?;
            let (update_expression, value) = match mutation {
                UserProgressMutation::Add(delta) => ("ADD #progress :value", delta),
                UserProgressMutation::Set(value) => ("SET #progress = :value", value),
            };

            let main_update = Update::builder()
                .table_name(self.main_table())
                .key("pk", Self::av_s(format!("USER#{user_id}")))
                .key("sk", Self::av_s("META"))
                .update_expression(update_expression)
                .condition_expression(concat!(
                    "attribute_exists(pk) AND attribute_exists(sk) AND ",
                    "username=:username AND isGuest=:is_guest"
                ))
                .expression_attribute_names("#progress", field)
                .expression_attribute_values(":value", Self::av_n(value))
                .expression_attribute_values(":username", Self::av_s(&user.username))
                .expression_attribute_values(":is_guest", Self::av_bool(user.is_guest))
                .build()
                .with_context(|| format!("Failed to build canonical {field} mutation"))?;
            let mut mutations = vec![TransactWriteItem::builder().update(main_update).build()];

            if !user.is_guest && user.auth_provider.as_deref() != Some("crazygames") {
                let mirror_update = Update::builder()
                    .table_name(self.usernames_table())
                    .key("username", Self::av_s(&user.username))
                    .update_expression(update_expression)
                    .condition_expression("attribute_exists(username) AND userId=:user_id")
                    .expression_attribute_names("#progress", field)
                    .expression_attribute_values(":value", Self::av_n(value))
                    .expression_attribute_values(":user_id", Self::av_n(user_id))
                    .build()
                    .with_context(|| format!("Failed to build mirrored {field} mutation"))?;
                mutations.push(TransactWriteItem::builder().update(mirror_update).build());
            }

            let result = self
                .client
                .transact_write_items()
                .client_request_token(uuid::Uuid::new_v4().to_string())
                .set_transact_items(Some(mutations))
                .send()
                .await;
            match result {
                Ok(_) => {
                    let current = self
                        .get_user_by_id(user_id)
                        .await?
                        .ok_or_else(|| anyhow!("User not found"))?;
                    return Self::user_progress_value(&current, field);
                }
                Err(error) if attempt + 1 < GUEST_UPGRADE_MAX_ATTEMPTS => {
                    let exponent = attempt.min(6) as u32;
                    sleep(Duration::from_millis(1_u64 << exponent)).await;
                    debug!(
                        "Retrying {} mutation for user {} after concurrent identity change: {}",
                        field, user_id, error
                    );
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("Failed to atomically mutate user {field}"));
                }
            }
        }

        unreachable!("user progress mutation attempt loop always returns")
    }

    fn crazygames_identity_pk(provider_user_id: &str) -> String {
        let digest = Sha256::digest(provider_user_id.as_bytes());
        format!("IDENTITY#CRAZYGAMES#{}", hex::encode(digest))
    }

    async fn get_crazygames_identity(
        &self,
        provider_user_id: &str,
    ) -> Result<Option<CrazyGamesIdentityRecord>> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key(
                "pk",
                Self::av_s(Self::crazygames_identity_pk(provider_user_id)),
            )
            .key("sk", Self::av_s("META"))
            .consistent_read(true)
            .send()
            .await
            .context("Failed to read CrazyGames identity mapping")?;

        let Some(item) = response.item else {
            return Ok(None);
        };
        let stored_provider_user_id = Self::extract_string(&item, "providerUserId")
            .ok_or_else(|| anyhow!("CrazyGames identity mapping is corrupt"))?;
        if stored_provider_user_id != provider_user_id {
            return Err(anyhow!(
                "CrazyGames identity hash collision or corrupt mapping"
            ));
        }
        Ok(Some(CrazyGamesIdentityRecord {
            user_id: Self::extract_number(&item, "userId")
                .ok_or_else(|| anyhow!("CrazyGames identity mapping is corrupt"))?,
            provider_user_id: stored_provider_user_id,
            username: Self::extract_string(&item, "username")
                .ok_or_else(|| anyhow!("CrazyGames identity mapping is corrupt"))?,
            avatar_url: Self::extract_string(&item, "profilePictureUrl")
                .ok_or_else(|| anyhow!("CrazyGames identity mapping is corrupt"))?,
            profile_iat: Self::extract_i64(&item, "profileIat")
                .ok_or_else(|| anyhow!("CrazyGames identity mapping is corrupt"))?,
        }))
    }

    async fn get_crazygames_preferences_with_version(
        &self,
        user_id: i32,
    ) -> Result<(CrazyGamesPreferences, i64)> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{user_id}")))
            .key("sk", Self::av_s("PREFERENCES#CRAZYGAMES"))
            .consistent_read(true)
            .send()
            .await
            .context("Failed to read CrazyGames preferences")?;

        let Some(item) = response.item else {
            return Ok((CrazyGamesPreferences::default(), 0));
        };
        let preferences = Self::extract_string(&item, "preferences")
            .ok_or_else(|| anyhow!("CrazyGames preferences are corrupt"))
            .and_then(|value| {
                serde_json::from_str(&value).context("CrazyGames preferences are corrupt")
            })?;
        let version = Self::extract_i64(&item, "version")
            .ok_or_else(|| anyhow!("CrazyGames preferences are corrupt"))?;
        Ok((preferences, version))
    }

    fn crazygames_preferences_put(
        &self,
        user_id: i32,
        preferences: &CrazyGamesPreferences,
        version: i64,
        expected_version: Option<i64>,
    ) -> Result<Put> {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(format!("USER#{user_id}")));
        item.insert("sk".to_string(), Self::av_s("PREFERENCES#CRAZYGAMES"));
        item.insert("schemaVersion".to_string(), Self::av_n(1));
        item.insert("version".to_string(), Self::av_n(version));
        item.insert(
            "preferences".to_string(),
            Self::av_s(
                serde_json::to_string(preferences)
                    .context("Failed to serialize CrazyGames preferences")?,
            ),
        );
        item.insert("updatedAt".to_string(), Self::av_s(Utc::now().to_rfc3339()));

        let mut put = Put::builder()
            .table_name(self.main_table())
            .set_item(Some(item));
        if let Some(expected_version) = expected_version {
            put = put
                .condition_expression("attribute_not_exists(pk) OR #version=:expected_version")
                .expression_attribute_names("#version", "version")
                .expression_attribute_values(":expected_version", Self::av_n(expected_version));
        }
        put.build()
            .context("Failed to build CrazyGames preferences write")
    }

    fn crazygames_identity_put(
        &self,
        profile: &CrazyGamesProfile,
        user_id: i32,
        now: DateTime<Utc>,
    ) -> Result<Put> {
        let mut item = HashMap::new();
        item.insert(
            "pk".to_string(),
            Self::av_s(Self::crazygames_identity_pk(&profile.provider_user_id)),
        );
        item.insert("sk".to_string(), Self::av_s("META"));
        item.insert("provider".to_string(), Self::av_s("crazygames"));
        item.insert(
            "providerUserId".to_string(),
            Self::av_s(&profile.provider_user_id),
        );
        item.insert("userId".to_string(), Self::av_n(user_id));
        item.insert("username".to_string(), Self::av_s(&profile.username));
        item.insert(
            "profilePictureUrl".to_string(),
            Self::av_s(&profile.avatar_url),
        );
        item.insert("profileIat".to_string(), Self::av_n(profile.profile_iat));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert(
            "lastAuthenticatedAt".to_string(),
            Self::av_s(now.to_rfc3339()),
        );

        Put::builder()
            .table_name(self.main_table())
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
            .build()
            .context("Failed to build CrazyGames identity mapping")
    }

    fn new_crazygames_user_put(
        &self,
        profile: &CrazyGamesProfile,
        user_id: i32,
        now: DateTime<Utc>,
    ) -> Result<Put> {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), Self::av_s(format!("USER#{user_id}")));
        item.insert("sk".to_string(), Self::av_s("META"));
        item.insert("gsi1pk".to_string(), Self::av_s("USER"));
        item.insert("gsi1sk".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("id".to_string(), Self::av_n(user_id));
        item.insert("username".to_string(), Self::av_s(&profile.username));
        item.insert("passwordHash".to_string(), Self::av_s(""));
        item.insert("mmr".to_string(), Self::av_n(1000));
        item.insert("rankedMmr".to_string(), Self::av_n(1000));
        item.insert("casualMmr".to_string(), Self::av_n(1000));
        item.insert("xp".to_string(), Self::av_n(0));
        item.insert("gamesPlayed".to_string(), Self::av_n(0));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("isGuest".to_string(), Self::av_bool(false));
        item.insert("isStressTest".to_string(), Self::av_bool(false));
        item.insert("authProvider".to_string(), Self::av_s("crazygames"));
        item.insert(
            "crazyGamesUserId".to_string(),
            Self::av_s(&profile.provider_user_id),
        );
        item.insert(
            "profilePictureUrl".to_string(),
            Self::av_s(&profile.avatar_url),
        );
        item.insert("profileIat".to_string(), Self::av_n(profile.profile_iat));

        Put::builder()
            .table_name(self.main_table())
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
            .build()
            .context("Failed to build CrazyGames user")
    }

    async fn update_crazygames_profile_if_newer(
        &self,
        current: &CrazyGamesIdentityRecord,
        profile: &CrazyGamesProfile,
    ) -> Result<CrazyGamesIdentityRecord> {
        let mut observed = current.clone();
        let mut last_error = None;
        for attempt in 0..CRAZYGAMES_IDENTITY_MAX_ATTEMPTS {
            if profile.profile_iat <= observed.profile_iat {
                return Ok(observed);
            }

            let identity_update = Update::builder()
                .table_name(self.main_table())
                .key(
                    "pk",
                    Self::av_s(Self::crazygames_identity_pk(&profile.provider_user_id)),
                )
                .key("sk", Self::av_s("META"))
                .update_expression(concat!(
                    "SET username=:username, profilePictureUrl=:avatar, ",
                    "profileIat=:profile_iat, lastAuthenticatedAt=:now"
                ))
                .condition_expression(concat!(
                    "attribute_exists(pk) AND providerUserId=:provider_user_id AND ",
                    "(attribute_not_exists(profileIat) OR profileIat<:profile_iat)"
                ))
                .expression_attribute_values(":username", Self::av_s(&profile.username))
                .expression_attribute_values(":avatar", Self::av_s(&profile.avatar_url))
                .expression_attribute_values(":profile_iat", Self::av_n(profile.profile_iat))
                .expression_attribute_values(
                    ":provider_user_id",
                    Self::av_s(&profile.provider_user_id),
                )
                .expression_attribute_values(":now", Self::av_s(Utc::now().to_rfc3339()))
                .build()
                .context("Failed to build CrazyGames identity profile update")?;
            let user_update = Update::builder()
                .table_name(self.main_table())
                .key("pk", Self::av_s(format!("USER#{}", observed.user_id)))
                .key("sk", Self::av_s("META"))
                .update_expression(
                    "SET username=:username, profilePictureUrl=:avatar, profileIat=:profile_iat",
                )
                .condition_expression(concat!(
                    "attribute_exists(pk) AND authProvider=:provider AND ",
                    "crazyGamesUserId=:provider_user_id AND ",
                    "(attribute_not_exists(profileIat) OR profileIat<:profile_iat)"
                ))
                .expression_attribute_values(":username", Self::av_s(&profile.username))
                .expression_attribute_values(":avatar", Self::av_s(&profile.avatar_url))
                .expression_attribute_values(":profile_iat", Self::av_n(profile.profile_iat))
                .expression_attribute_values(":provider", Self::av_s("crazygames"))
                .expression_attribute_values(
                    ":provider_user_id",
                    Self::av_s(&profile.provider_user_id),
                )
                .build()
                .context("Failed to build CrazyGames user profile update")?;

            match self
                .client
                .transact_write_items()
                .transact_items(TransactWriteItem::builder().update(identity_update).build())
                .transact_items(TransactWriteItem::builder().update(user_update).build())
                .send()
                .await
            {
                Ok(_) => {
                    return Ok(CrazyGamesIdentityRecord {
                        user_id: observed.user_id,
                        provider_user_id: profile.provider_user_id.clone(),
                        username: profile.username.clone(),
                        avatar_url: profile.avatar_url.clone(),
                        profile_iat: profile.profile_iat,
                    });
                }
                Err(error) => {
                    observed = self
                        .get_crazygames_identity(&profile.provider_user_id)
                        .await?
                        .ok_or_else(|| anyhow!("CrazyGames identity mapping disappeared"))?;
                    if observed.profile_iat >= profile.profile_iat {
                        return Ok(observed);
                    }
                    last_error =
                        Some(anyhow!(error).context("Failed to update CrazyGames profile"));
                    if attempt + 1 < CRAZYGAMES_IDENTITY_MAX_ATTEMPTS {
                        let exponent = attempt.min(6) as u32;
                        sleep(Duration::from_millis(1_u64 << exponent)).await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("Failed to update CrazyGames profile")))
    }

    async fn load_crazygames_account(
        &self,
        identity: CrazyGamesIdentityRecord,
        resolution: CrazyGamesAccountResolution,
    ) -> Result<CrazyGamesAccount> {
        let user = self
            .get_user_by_id(identity.user_id)
            .await?
            .ok_or_else(|| anyhow!("CrazyGames identity mapping points to a missing user"))?;
        if user.is_guest
            || user.auth_provider.as_deref() != Some("crazygames")
            || user.crazygames_user_id.as_deref() != Some(&identity.provider_user_id)
        {
            return Err(anyhow!("CrazyGames identity mapping is corrupt"));
        }
        let (preferences, _) = self
            .get_crazygames_preferences_with_version(identity.user_id)
            .await?;
        Ok(CrazyGamesAccount {
            user,
            profile: CrazyGamesProfile {
                provider_user_id: identity.provider_user_id,
                username: identity.username,
                avatar_url: identity.avatar_url,
                profile_iat: identity.profile_iat,
            },
            resolution,
            preferences,
        })
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
        username_item.insert("gamesPlayed".to_string(), Self::av_n(0));

        // This will fail if username already exists
        self.client
            .put_item()
            .table_name(self.usernames_table())
            .set_item(Some(username_item))
            .condition_expression("attribute_not_exists(username) OR attribute_not_exists(userId)")
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
        item.insert("gamesPlayed".to_string(), Self::av_n(0));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("isGuest".to_string(), Self::av_bool(false));
        item.insert("isStressTest".to_string(), Self::av_bool(false));

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
            games_played: 0,
            created_at: now,
            is_guest: false,
            guest_token: None,
            is_stress_test: false,
            auth_provider: None,
            crazygames_user_id: None,
            profile_picture_url: None,
            profile_iat: None,
            selected_skin: None,
        })
    }

    async fn create_guest_user(
        &self,
        nickname: &str,
        guest_token: &str,
        mmr: i32,
        is_stress_test: bool,
    ) -> Result<User> {
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
        item.insert("gamesPlayed".to_string(), Self::av_n(0));
        item.insert("createdAt".to_string(), Self::av_s(now.to_rfc3339()));
        item.insert("isGuest".to_string(), Self::av_bool(true));
        item.insert("isStressTest".to_string(), Self::av_bool(is_stress_test));
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
            games_played: 0,
            created_at: now,
            is_guest: true,
            guest_token: Some(guest_token.to_string()),
            is_stress_test,
            auth_provider: None,
            crazygames_user_id: None,
            profile_picture_url: None,
            profile_iat: None,
            selected_skin: None,
        })
    }

    async fn upgrade_guest_to_account(
        &self,
        user_id: i32,
        username: &str,
        password_hash: &str,
    ) -> Result<User> {
        for attempt in 0..GUEST_UPGRADE_MAX_ATTEMPTS {
            let guest = self
                .get_user_by_id(user_id)
                .await?
                .ok_or_else(|| anyhow!("Guest account not found"))?;

            if !guest.is_guest {
                return Err(anyhow!("Guest account has already been upgraded"));
            }
            if guest.is_stress_test {
                return Err(anyhow!("Stress-test guest accounts cannot be upgraded"));
            }

            // Keep a complete mirror during rolling deployments, even though
            // current readers use this table only as a username -> user ID
            // index and load canonical progress from the main user record.
            let mut username_item = HashMap::new();
            username_item.insert("username".to_string(), Self::av_s(username));
            username_item.insert("userId".to_string(), Self::av_n(user_id));
            username_item.insert("passwordHash".to_string(), Self::av_s(password_hash));
            username_item.insert("mmr".to_string(), Self::av_n(guest.mmr));
            username_item.insert("rankedMmr".to_string(), Self::av_n(guest.ranked_mmr));
            username_item.insert("casualMmr".to_string(), Self::av_n(guest.casual_mmr));
            username_item.insert("xp".to_string(), Self::av_n(guest.xp));
            username_item.insert("gamesPlayed".to_string(), Self::av_n(guest.games_played));

            let username_put = Put::builder()
                .table_name(self.usernames_table())
                .set_item(Some(username_item))
                .condition_expression(concat!(
                    "attribute_not_exists(username) OR ",
                    "attribute_not_exists(userId) OR userId=:user_id"
                ))
                .expression_attribute_values(":user_id", Self::av_n(user_id))
                .build()
                .context("Failed to build guest username reservation")?;

            // Progress fields participate in the condition, but not the SET.
            // If a game completes between the read and transaction, the
            // conditional cancellation forces a fresh snapshot and prevents a
            // stale username mirror from being published.
            let guest_update = Update::builder()
                .table_name(self.main_table())
                .key("pk", Self::av_s(format!("USER#{user_id}")))
                .key("sk", Self::av_s("META"))
                .update_expression(concat!(
                    "SET username=:username, passwordHash=:password_hash, ",
                    "isGuest=:not_guest, isStressTest=:not_stress ",
                    "REMOVE guestToken"
                ))
                .condition_expression(concat!(
                    "attribute_exists(pk) AND attribute_exists(sk) AND ",
                    "isGuest=:guest AND ",
                    "(attribute_not_exists(isStressTest) OR isStressTest=:not_stress) AND ",
                    "username=:old_username AND passwordHash=:old_password_hash AND ",
                    "mmr=:mmr AND rankedMmr=:ranked_mmr AND ",
                    "casualMmr=:casual_mmr AND xp=:xp AND ",
                    "(gamesPlayed=:games_played OR ",
                    "(attribute_not_exists(gamesPlayed) AND :games_played=:zero))"
                ))
                .expression_attribute_values(":username", Self::av_s(username))
                .expression_attribute_values(":password_hash", Self::av_s(password_hash))
                .expression_attribute_values(":not_guest", Self::av_bool(false))
                .expression_attribute_values(":not_stress", Self::av_bool(false))
                .expression_attribute_values(":guest", Self::av_bool(true))
                .expression_attribute_values(":old_username", Self::av_s(&guest.username))
                .expression_attribute_values(":old_password_hash", Self::av_s(&guest.password_hash))
                .expression_attribute_values(":mmr", Self::av_n(guest.mmr))
                .expression_attribute_values(":ranked_mmr", Self::av_n(guest.ranked_mmr))
                .expression_attribute_values(":casual_mmr", Self::av_n(guest.casual_mmr))
                .expression_attribute_values(":xp", Self::av_n(guest.xp))
                .expression_attribute_values(":games_played", Self::av_n(guest.games_played))
                .expression_attribute_values(":zero", Self::av_n(0))
                .build()
                .context("Failed to build in-place guest account upgrade")?;

            let result = self
                .client
                .transact_write_items()
                .client_request_token(uuid::Uuid::new_v4().to_string())
                .transact_items(TransactWriteItem::builder().put(username_put).build())
                .transact_items(TransactWriteItem::builder().update(guest_update).build())
                .send()
                .await;

            match result {
                Ok(_) => {
                    let mut upgraded = guest;
                    upgraded.username = username.to_string();
                    upgraded.password_hash = password_hash.to_string();
                    upgraded.is_guest = false;
                    upgraded.guest_token = None;
                    upgraded.is_stress_test = false;
                    info!(
                        "Upgraded guest user {} in place as account '{}'",
                        user_id, username
                    );
                    return Ok(upgraded);
                }
                Err(error) => {
                    // A response can be lost after DynamoDB commits. Recognize
                    // this exact attempt as success instead of marooning the
                    // browser with an invalidated guest token.
                    let current = self
                        .get_user_by_id(user_id)
                        .await?
                        .ok_or_else(|| anyhow!("Guest account not found"))?;
                    if !current.is_guest {
                        if current.username == username && current.password_hash == password_hash {
                            return Ok(current);
                        }
                        return Err(anyhow!("Guest account has already been upgraded"));
                    }

                    if let Some(owner) = self.get_user_by_username(username).await?
                        && owner.id != user_id
                    {
                        return Err(anyhow!("Username already exists"));
                    }

                    if attempt + 1 == GUEST_UPGRADE_MAX_ATTEMPTS {
                        return Err(error).context("Failed to atomically upgrade guest account");
                    }

                    let exponent = attempt.min(6) as u32;
                    sleep(Duration::from_millis(1_u64 << exponent)).await;
                    debug!(
                        "Retrying guest upgrade for user {} after concurrent mutation",
                        user_id
                    );
                }
            }
        }

        unreachable!("guest upgrade attempt loop always returns")
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
                    games_played: Self::extract_number(&item, "gamesPlayed").unwrap_or(0),
                    created_at: Self::extract_string(&item, "createdAt")
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(Utc::now),
                    is_guest: Self::extract_bool(&item, "isGuest").unwrap_or(false),
                    guest_token: Self::extract_string(&item, "guestToken"),
                    is_stress_test: Self::extract_bool(&item, "isStressTest").unwrap_or(false),
                    auth_provider: Self::extract_string(&item, "authProvider"),
                    crazygames_user_id: Self::extract_string(&item, "crazyGamesUserId"),
                    profile_picture_url: Self::extract_string(&item, "profilePictureUrl"),
                    profile_iat: Self::extract_i64(&item, "profileIat"),
                    selected_skin: Self::extract_string(&item, "selectedSkin"),
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
            .consistent_read(true)
            .send()
            .await
            .context("Failed to get user by username")?;

        match response.item {
            Some(item) => {
                let Some(user_id) = Self::extract_number(&item, "userId") else {
                    // Older progress writers could accidentally create a
                    // partial row for a guest nickname. Treat it as an
                    // unclaimed index entry so account creation can repair it.
                    warn!("Ignoring incomplete username index row for '{}'", username);
                    return Ok(None);
                };

                // The username table is the uniqueness/index boundary. The
                // main record remains canonical for credentials and progress,
                // avoiding stale mirror reads during an in-place guest claim.
                self.get_user_by_id(user_id).await
            }
            None => Ok(None),
        }
    }

    async fn update_user_mmr(&self, user_id: i32, mmr: i32) -> Result<()> {
        self.mutate_user_progress(user_id, "mmr", UserProgressMutation::Set(mmr))
            .await?;
        Ok(())
    }

    async fn update_guest_username(&self, user_id: i32, username: &str) -> Result<()> {
        self.client
            .update_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(format!("USER#{}", user_id)))
            .key("sk", Self::av_s("META"))
            .update_expression("SET username = :username")
            .condition_expression("attribute_exists(pk) AND isGuest = :guest")
            .expression_attribute_values(":username", Self::av_s(username))
            .expression_attribute_values(":guest", Self::av_bool(true))
            .send()
            .await
            .context("Failed to update guest username")?;

        Ok(())
    }

    async fn add_user_xp(&self, user_id: i32, xp_to_add: i32) -> Result<i32> {
        let new_xp = self
            .mutate_user_progress(user_id, "xp", UserProgressMutation::Add(xp_to_add))
            .await?;

        info!(
            "Added {} XP to user {} (new total: {})",
            xp_to_add, user_id, new_xp
        );
        Ok(new_xp)
    }

    async fn resolve_crazygames_account(
        &self,
        profile: &CrazyGamesProfile,
        guest_candidate_user_id: Option<i32>,
        guest_promotion: CrazyGamesGuestPromotion,
        initial_preferences: Option<&CrazyGamesPreferences>,
    ) -> Result<CrazyGamesAccountOutcome> {
        // Browser preferences are safe to import only when the caller proves
        // ownership of an eligible guest through its internal bearer token.
        // A newly created provider identity may be opening a browser last used
        // by another linked account, so it must start from its own empty
        // canonical snapshot instead of inheriting unscoped local state.
        let guest_preferences = initial_preferences.cloned().unwrap_or_default();
        let mut guest_candidate_user_id = match guest_promotion {
            CrazyGamesGuestPromotion::Check | CrazyGamesGuestPromotion::Allow => {
                guest_candidate_user_id
            }
            CrazyGamesGuestPromotion::Decline => None,
        };
        let mut last_error = None;

        for attempt in 0..CRAZYGAMES_IDENTITY_MAX_ATTEMPTS {
            if let Some(current) = self
                .get_crazygames_identity(&profile.provider_user_id)
                .await?
            {
                let current = self
                    .update_crazygames_profile_if_newer(&current, profile)
                    .await?;
                return Ok(CrazyGamesAccountOutcome::Resolved(Box::new(
                    self.load_crazygames_account(current, CrazyGamesAccountResolution::Returning)
                        .await?,
                )));
            }

            if let Some(candidate_id) = guest_candidate_user_id {
                let eligible = self
                    .get_user_by_id(candidate_id)
                    .await?
                    .is_some_and(|user| user.is_guest && !user.is_stress_test);
                if eligible {
                    if guest_promotion == CrazyGamesGuestPromotion::Check {
                        // Close the most useful race window before reporting
                        // consent: a concurrent first launch may have created
                        // the identity while we inspected the guest. Both
                        // reads are strongly consistent, and this branch does
                        // not write the guest, identity, or preferences.
                        if let Some(current) = self
                            .get_crazygames_identity(&profile.provider_user_id)
                            .await?
                        {
                            let current = self
                                .update_crazygames_profile_if_newer(&current, profile)
                                .await?;
                            return Ok(CrazyGamesAccountOutcome::Resolved(Box::new(
                                self.load_crazygames_account(
                                    current,
                                    CrazyGamesAccountResolution::Returning,
                                )
                                .await?,
                            )));
                        }
                        return Ok(CrazyGamesAccountOutcome::GuestLinkConsentRequired);
                    }

                    let now = Utc::now();
                    let identity_put = self.crazygames_identity_put(profile, candidate_id, now)?;
                    let user_update = Update::builder()
                        .table_name(self.main_table())
                        .key("pk", Self::av_s(format!("USER#{candidate_id}")))
                        .key("sk", Self::av_s("META"))
                        .update_expression(concat!(
                            "SET username=:username, isGuest=:not_guest, ",
                            "isStressTest=:not_stress, authProvider=:provider, ",
                            "crazyGamesUserId=:provider_user_id, ",
                            "profilePictureUrl=:avatar, profileIat=:profile_iat ",
                            "REMOVE guestToken"
                        ))
                        .condition_expression(concat!(
                            "attribute_exists(pk) AND attribute_exists(sk) AND ",
                            "isGuest=:guest AND ",
                            "(attribute_not_exists(isStressTest) OR isStressTest=:not_stress)"
                        ))
                        .expression_attribute_values(":username", Self::av_s(&profile.username))
                        .expression_attribute_values(":not_guest", Self::av_bool(false))
                        .expression_attribute_values(":not_stress", Self::av_bool(false))
                        .expression_attribute_values(":guest", Self::av_bool(true))
                        .expression_attribute_values(":provider", Self::av_s("crazygames"))
                        .expression_attribute_values(
                            ":provider_user_id",
                            Self::av_s(&profile.provider_user_id),
                        )
                        .expression_attribute_values(":avatar", Self::av_s(&profile.avatar_url))
                        .expression_attribute_values(
                            ":profile_iat",
                            Self::av_n(profile.profile_iat),
                        )
                        .build()
                        .context("Failed to build CrazyGames guest claim")?;
                    let preferences_put =
                        self.crazygames_preferences_put(candidate_id, &guest_preferences, 1, None)?;
                    let result = self
                        .client
                        .transact_write_items()
                        .client_request_token(uuid::Uuid::new_v4().to_string())
                        .transact_items(TransactWriteItem::builder().put(identity_put).build())
                        .transact_items(TransactWriteItem::builder().update(user_update).build())
                        .transact_items(TransactWriteItem::builder().put(preferences_put).build())
                        .send()
                        .await;
                    match result {
                        Ok(_) => {
                            let identity = self
                                .get_crazygames_identity(&profile.provider_user_id)
                                .await?
                                .ok_or_else(|| {
                                    anyhow!("CrazyGames identity claim committed without mapping")
                                })?;
                            info!(
                                "Claimed public guest {} for a verified CrazyGames identity",
                                candidate_id
                            );
                            return Ok(CrazyGamesAccountOutcome::Resolved(Box::new(
                                self.load_crazygames_account(
                                    identity,
                                    CrazyGamesAccountResolution::GuestClaimed,
                                )
                                .await?,
                            )));
                        }
                        Err(error) => {
                            if let Some(winner) = self
                                .get_crazygames_identity(&profile.provider_user_id)
                                .await?
                            {
                                let winner = self
                                    .update_crazygames_profile_if_newer(&winner, profile)
                                    .await?;
                                return Ok(CrazyGamesAccountOutcome::Resolved(Box::new(
                                    self.load_crazygames_account(
                                        winner,
                                        CrazyGamesAccountResolution::Returning,
                                    )
                                    .await?,
                                )));
                            }
                            if !self
                                .get_user_by_id(candidate_id)
                                .await?
                                .is_some_and(|user| user.is_guest && !user.is_stress_test)
                            {
                                // A concurrent password registration won. Never
                                // attach CrazyGames to that unrelated account.
                                guest_candidate_user_id = None;
                            }
                            last_error = Some(anyhow!(error).context(
                                "Failed to atomically claim guest for CrazyGames identity",
                            ));
                        }
                    }
                } else {
                    guest_candidate_user_id = None;
                }
            }

            if guest_candidate_user_id.is_none() {
                let user_id = self.generate_id_for_entity("USER").await?;
                let now = Utc::now();
                let identity_put = self.crazygames_identity_put(profile, user_id, now)?;
                let user_put = self.new_crazygames_user_put(profile, user_id, now)?;
                let preferences_put = self.crazygames_preferences_put(
                    user_id,
                    &CrazyGamesPreferences::default(),
                    1,
                    None,
                )?;
                let result = self
                    .client
                    .transact_write_items()
                    .client_request_token(uuid::Uuid::new_v4().to_string())
                    .transact_items(TransactWriteItem::builder().put(identity_put).build())
                    .transact_items(TransactWriteItem::builder().put(user_put).build())
                    .transact_items(TransactWriteItem::builder().put(preferences_put).build())
                    .send()
                    .await;
                match result {
                    Ok(_) => {
                        let identity = self
                            .get_crazygames_identity(&profile.provider_user_id)
                            .await?
                            .ok_or_else(|| {
                                anyhow!("CrazyGames account creation committed without mapping")
                            })?;
                        info!(
                            "Created user {} for a verified CrazyGames identity",
                            user_id
                        );
                        return Ok(CrazyGamesAccountOutcome::Resolved(Box::new(
                            self.load_crazygames_account(
                                identity,
                                CrazyGamesAccountResolution::Created,
                            )
                            .await?,
                        )));
                    }
                    Err(error) => {
                        if let Some(winner) = self
                            .get_crazygames_identity(&profile.provider_user_id)
                            .await?
                        {
                            let winner = self
                                .update_crazygames_profile_if_newer(&winner, profile)
                                .await?;
                            return Ok(CrazyGamesAccountOutcome::Resolved(Box::new(
                                self.load_crazygames_account(
                                    winner,
                                    CrazyGamesAccountResolution::Returning,
                                )
                                .await?,
                            )));
                        }
                        last_error = Some(
                            anyhow!(error)
                                .context("Failed to atomically create CrazyGames account"),
                        );
                    }
                }
            }

            if attempt + 1 < CRAZYGAMES_IDENTITY_MAX_ATTEMPTS {
                let exponent = attempt.min(6) as u32;
                sleep(Duration::from_millis(1_u64 << exponent)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Failed to resolve CrazyGames identity")))
    }

    async fn save_crazygames_preferences(
        &self,
        user_id: i32,
        preferences: &CrazyGamesPreferences,
    ) -> Result<CrazyGamesPreferences> {
        let user = self
            .get_user_by_id(user_id)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        if user.is_guest || user.auth_provider.as_deref() != Some("crazygames") {
            return Err(anyhow!("User is not linked to CrazyGames"));
        }

        let mut last_error = None;
        for attempt in 0..CRAZYGAMES_IDENTITY_MAX_ATTEMPTS {
            let (current, version) = self
                .get_crazygames_preferences_with_version(user_id)
                .await?;
            let merged = current.merge(preferences);
            let preference_put =
                self.crazygames_preferences_put(user_id, &merged, version + 1, Some(version))?;
            let user_check = ConditionCheck::builder()
                .table_name(self.main_table())
                .key("pk", Self::av_s(format!("USER#{user_id}")))
                .key("sk", Self::av_s("META"))
                .condition_expression(concat!(
                    "attribute_exists(pk) AND attribute_exists(sk) AND ",
                    "authProvider=:provider AND isGuest=:not_guest"
                ))
                .expression_attribute_values(":provider", Self::av_s("crazygames"))
                .expression_attribute_values(":not_guest", Self::av_bool(false))
                .build()
                .context("Failed to build CrazyGames preference owner check")?;
            match self
                .client
                .transact_write_items()
                .transact_items(
                    TransactWriteItem::builder()
                        .condition_check(user_check)
                        .build(),
                )
                .transact_items(TransactWriteItem::builder().put(preference_put).build())
                .send()
                .await
            {
                Ok(_) => return Ok(merged),
                Err(error) => {
                    let current_user = self.get_user_by_id(user_id).await?;
                    if !current_user.is_some_and(|user| {
                        !user.is_guest && user.auth_provider.as_deref() == Some("crazygames")
                    }) {
                        return Err(anyhow!("User is not linked to CrazyGames"));
                    }
                    last_error = Some(
                        anyhow!(error).context("Failed to atomically save CrazyGames preferences"),
                    );
                    if attempt + 1 < CRAZYGAMES_IDENTITY_MAX_ATTEMPTS {
                        let exponent = attempt.min(6) as u32;
                        sleep(Duration::from_millis(1_u64 << exponent)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Failed to save CrazyGames preferences")))
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

        let new_mmr = self
            .mutate_user_progress(user_id, mmr_field, UserProgressMutation::Add(mmr_delta))
            .await?;

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

    async fn get_recent_completed_games(&self, limit: usize) -> Result<Vec<Game>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut games = Vec::with_capacity(limit.min(RECENT_COMPLETED_GAMES_PAGE_SIZE));
        let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;
        let now_epoch_seconds = Utc::now().timestamp();

        while games.len() < limit {
            let remaining = limit - games.len();
            let page_limit = Self::recent_completed_games_page_limit(remaining)
                .expect("remaining recent-game count is positive");
            let mut request = self
                .client
                .query()
                .table_name(self.main_table())
                .index_name("GSI1")
                .key_condition_expression(
                    "gsi1pk = :game_partition AND begins_with(gsi1sk, :complete_prefix)",
                )
                .expression_attribute_values(":game_partition", Self::av_s("GAME"))
                .expression_attribute_values(":complete_prefix", Self::av_s("complete#"))
                .scan_index_forward(false)
                .limit(page_limit);
            if let Some(key) = &last_evaluated_key {
                request = request.set_exclusive_start_key(Some(key.clone()));
            }

            let response = request
                .send()
                .await
                .context("Failed to query recent completed games")?;
            let next_key = response.last_evaluated_key;
            Self::append_recent_completed_games_from_items(
                &mut games,
                response.items.unwrap_or_default(),
                now_epoch_seconds,
                limit,
            )?;

            let Some(next_key) = next_key else {
                break;
            };
            last_evaluated_key = Some(next_key);
        }

        Ok(games)
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
        let season = get_season_at(ended_at);
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
        let news_eligible = match self
            .source_game_is_news_eligible(
                game_id,
                Self::new_game_state_is_news_eligible(game_state),
            )
            .await
        {
            Ok(eligible) => eligible,
            Err(error) => {
                warn!(
                    game_id,
                    %error,
                    "Could not verify completed-game source for public news; failing closed"
                );
                false
            }
        };

        let mut update_expression = concat!(
            "SET gsi1pk = :gsi1pk, gsi1sk = :gsi1sk, id = :id, ",
            "serverId = :server_id, gameType = :game_type, gameState = :game_state, ",
            "#status = :status, endedAt = :ended_at, lastActivity = :last_activity, ",
            "createdAt = :created_at, gameMode = :game_mode, ",
            "isPrivate = if_not_exists(isPrivate, :is_private), ",
            "runtimeIdentity = :runtime_identity, season = :season, ",
            "newsEligible = :news_eligible, #ttl = :ttl"
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
            .expression_attribute_values(":season", Self::av_n(season))
            .expression_attribute_values(":runtime_identity", Self::av_s(runtime_identity))
            .expression_attribute_values(":news_eligible", Self::av_bool(news_eligible))
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

    async fn get_match_history(
        &self,
        user_id: i32,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<MatchHistoryPage> {
        let limit = Self::bounded_page_limit(limit);
        let target = limit.saturating_add(1);
        let scope = format!("history:user:{user_id}");
        let expected_pk = format!("USER#{user_id}");
        let mut exclusive_start_key = match cursor {
            Some(raw) => {
                let cursor = Self::decode_page_cursor(raw, &scope, "history")?;
                if cursor.pk != expected_pk
                    || !cursor.sk.starts_with("HISTORY#")
                    || cursor.gsi2pk.is_some()
                    || cursor.gsi2sk.is_some()
                {
                    return Err(anyhow!("invalid history cursor: token key is invalid"));
                }
                Some(Self::cursor_key(&cursor))
            }
            None => None,
        };
        let now = Utc::now().timestamp();
        let mut rows: Vec<(MatchHistorySummary, HashMap<String, AttributeValue>)> = Vec::new();

        while rows.len() < target {
            let remaining = target.saturating_sub(rows.len()).max(1);
            let mut query = self
                .client
                .query()
                .table_name(self.main_table())
                .key_condition_expression("pk=:pk AND begins_with(sk, :history)")
                .filter_expression("#ttl > :now")
                .expression_attribute_names("#ttl", "ttl")
                .expression_attribute_values(":pk", Self::av_s(&expected_pk))
                .expression_attribute_values(":history", Self::av_s("HISTORY#"))
                .expression_attribute_values(":now", Self::av_n(now))
                .projection_expression("pk, sk, summaryJson, #ttl")
                .consistent_read(true)
                .scan_index_forward(false)
                .limit(i32::try_from(remaining).unwrap_or(i32::MAX));
            if let Some(key) = exclusive_start_key.take() {
                query = query.set_exclusive_start_key(Some(key));
            }
            let response = query
                .send()
                .await
                .context("Failed to query player match history")?;
            for item in response.items.unwrap_or_default() {
                if !Self::item_is_expired(&item, now) {
                    match Self::history_summary_from_item(&item) {
                        Ok(summary) => rows.push((summary, item)),
                        Err(error) => warn!(
                            ?error,
                            pk = ?Self::extract_string(&item, "pk"),
                            sk = ?Self::extract_string(&item, "sk"),
                            "skipping unreadable player match history row"
                        ),
                    }
                }
            }
            exclusive_start_key = response.last_evaluated_key;
            if exclusive_start_key.is_none() {
                break;
            }
        }

        let next_cursor = if rows.len() > limit {
            Some(Self::encode_page_cursor(&scope, &rows[limit - 1].1)?)
        } else {
            None
        };
        rows.truncate(limit);
        Ok(MatchHistoryPage {
            entries: rows.into_iter().map(|(summary, _)| summary).collect(),
            next_cursor,
        })
    }

    async fn get_admin_match_history(
        &self,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<MatchHistoryPage> {
        let limit = Self::bounded_page_limit(limit);
        let target = limit.saturating_add(1);
        let scope = "history:admin";
        let mut exclusive_start_key = match cursor {
            Some(raw) => {
                let cursor = Self::decode_page_cursor(raw, scope, "history")?;
                if !cursor.pk.starts_with("GAME#")
                    || cursor.sk != "HISTORY"
                    || cursor.gsi2pk.as_deref() != Some(HISTORY_GSI_PARTITION)
                    || !cursor
                        .gsi2sk
                        .as_deref()
                        .is_some_and(|key| key.starts_with("HISTORY#"))
                {
                    return Err(anyhow!("invalid history cursor: token key is invalid"));
                }
                Some(Self::cursor_key(&cursor))
            }
            None => None,
        };
        let now = Utc::now().timestamp();
        let mut rows: Vec<(MatchHistorySummary, HashMap<String, AttributeValue>)> = Vec::new();

        while rows.len() < target {
            let remaining = target.saturating_sub(rows.len()).max(1);
            let mut query = self
                .client
                .query()
                .table_name(self.main_table())
                .index_name("GSI2")
                .key_condition_expression("gsi2pk=:pk")
                .filter_expression("#ttl > :now")
                .expression_attribute_names("#ttl", "ttl")
                .expression_attribute_values(":pk", Self::av_s(HISTORY_GSI_PARTITION))
                .expression_attribute_values(":now", Self::av_n(now))
                .projection_expression("pk, sk, gsi2pk, gsi2sk, summaryJson, #ttl")
                .scan_index_forward(false)
                .limit(i32::try_from(remaining).unwrap_or(i32::MAX));
            if let Some(key) = exclusive_start_key.take() {
                query = query.set_exclusive_start_key(Some(key));
            }
            let response = query
                .send()
                .await
                .context("Failed to query administrative match history")?;
            for item in response.items.unwrap_or_default() {
                if !Self::item_is_expired(&item, now) {
                    match Self::history_summary_from_item(&item) {
                        Ok(summary) => rows.push((summary, item)),
                        Err(error) => warn!(
                            ?error,
                            pk = ?Self::extract_string(&item, "pk"),
                            sk = ?Self::extract_string(&item, "sk"),
                            "skipping unreadable administrative match history row"
                        ),
                    }
                }
            }
            exclusive_start_key = response.last_evaluated_key;
            if exclusive_start_key.is_none() {
                break;
            }
        }

        let next_cursor = if rows.len() > limit {
            Some(Self::encode_page_cursor(scope, &rows[limit - 1].1)?)
        } else {
            None
        };
        rows.truncate(limit);
        Ok(MatchHistoryPage {
            entries: rows.into_iter().map(|(summary, _)| summary).collect(),
            next_cursor,
        })
    }

    async fn get_runtime_config(&self) -> Result<RuntimeConfigRecord> {
        let response = self
            .client
            .get_item()
            .table_name(self.main_table())
            .key("pk", Self::av_s(RUNTIME_CONFIG_PK))
            .key("sk", Self::av_s(RUNTIME_CONFIG_CURRENT_SK))
            .consistent_read(true)
            .projection_expression("recordJson")
            .send()
            .await
            .context("Failed to read runtime config")?;
        match response.item {
            Some(item) => Self::runtime_config_record_from_item(&item),
            None => Ok(RuntimeConfigRecord::default()),
        }
    }

    async fn update_runtime_config(
        &self,
        expected_version: u64,
        config: &RuntimeConfig,
        actor: &RuntimeConfigActor,
    ) -> Result<RuntimeConfigRecord> {
        config
            .validate()
            .map_err(|error| anyhow!("invalid runtime config: {error}"))?;
        let version = expected_version
            .checked_add(1)
            .ok_or_else(|| anyhow!("runtime config version overflow"))?;
        let record = RuntimeConfigRecord {
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            version,
            config: config.clone(),
            updated_by: Some(actor.clone()),
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        let record_json =
            serde_json::to_string(&record).context("Failed to serialize runtime config record")?;
        let config_json =
            serde_json::to_string(config).context("Failed to serialize runtime config")?;

        let mut current = Put::builder()
            .table_name(self.main_table())
            .item("pk", Self::av_s(RUNTIME_CONFIG_PK))
            .item("sk", Self::av_s(RUNTIME_CONFIG_CURRENT_SK))
            .item("version", Self::av_n(version))
            .item("configJson", Self::av_s(&config_json))
            .item("recordJson", Self::av_s(&record_json))
            .item("updatedAtMs", Self::av_n(record.updated_at_ms))
            .item("updatedByUserId", Self::av_n(actor.user_id))
            .item("updatedByUsername", Self::av_s(&actor.username));
        if expected_version == 0 {
            current = current
                .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)");
        } else {
            current = current
                .condition_expression("#version=:expected")
                .expression_attribute_names("#version", "version")
                .expression_attribute_values(":expected", Self::av_n(expected_version));
        }
        let current = current
            .build()
            .context("Failed to build runtime config update")?;

        let audit = Put::builder()
            .table_name(self.main_table())
            .item("pk", Self::av_s(RUNTIME_CONFIG_PK))
            .item("sk", Self::av_s(format!("AUDIT#{version:020}")))
            .item("version", Self::av_n(version))
            .item("configJson", Self::av_s(config_json))
            .item("recordJson", Self::av_s(record_json))
            .item("updatedAtMs", Self::av_n(record.updated_at_ms))
            .item("updatedByUserId", Self::av_n(actor.user_id))
            .item("updatedByUsername", Self::av_s(&actor.username))
            .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
            .build()
            .context("Failed to build runtime config audit write")?;

        let result = self
            .client
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().put(current).build())
            .transact_items(TransactWriteItem::builder().put(audit).build())
            .send()
            .await;
        match result {
            Ok(_) => Ok(record),
            Err(error) => {
                if let Ok(observed) = self.get_runtime_config().await {
                    // DynamoDB may commit a transaction even when the client
                    // loses the response. Treat that exact durable record as
                    // success so retry semantics remain idempotent.
                    if observed == record {
                        return Ok(observed);
                    }
                    if observed.version != expected_version {
                        return Err(anyhow!(
                            "runtime config version conflict: expected {}, current {}",
                            expected_version,
                            observed.version
                        ));
                    }
                }
                Err(error).context("Failed to atomically update runtime config")
            }
        }
    }

    async fn get_runtime_config_audit(
        &self,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<RuntimeConfigAuditPage> {
        let limit = Self::bounded_page_limit(limit);
        let target = limit.saturating_add(1);
        let scope = "config-audit";
        let mut exclusive_start_key = match cursor {
            Some(raw) => {
                let cursor = Self::decode_page_cursor(raw, scope, "config audit")?;
                if cursor.pk != RUNTIME_CONFIG_PK
                    || !cursor.sk.starts_with("AUDIT#")
                    || cursor.gsi2pk.is_some()
                    || cursor.gsi2sk.is_some()
                {
                    return Err(anyhow!("invalid config audit cursor: token key is invalid"));
                }
                Some(Self::cursor_key(&cursor))
            }
            None => None,
        };
        let mut rows: Vec<(RuntimeConfigRecord, HashMap<String, AttributeValue>)> = Vec::new();
        while rows.len() < target {
            let remaining = target.saturating_sub(rows.len()).max(1);
            let mut query = self
                .client
                .query()
                .table_name(self.main_table())
                .key_condition_expression("pk=:pk AND begins_with(sk, :audit)")
                .expression_attribute_values(":pk", Self::av_s(RUNTIME_CONFIG_PK))
                .expression_attribute_values(":audit", Self::av_s("AUDIT#"))
                .projection_expression("pk, sk, recordJson")
                .consistent_read(true)
                .scan_index_forward(false)
                .limit(i32::try_from(remaining).unwrap_or(i32::MAX));
            if let Some(key) = exclusive_start_key.take() {
                query = query.set_exclusive_start_key(Some(key));
            }
            let response = query
                .send()
                .await
                .context("Failed to query runtime config audit")?;
            for item in response.items.unwrap_or_default() {
                rows.push((Self::runtime_config_record_from_item(&item)?, item));
            }
            exclusive_start_key = response.last_evaluated_key;
            if exclusive_start_key.is_none() {
                break;
            }
        }

        let next_cursor = if rows.len() > limit {
            Some(Self::encode_page_cursor(scope, &rows[limit - 1].1)?)
        } else {
            None
        };
        rows.truncate(limit);
        Ok(RuntimeConfigAuditPage {
            entries: rows.into_iter().map(|(record, _)| record).collect(),
            next_cursor,
        })
    }

    async fn try_claim_pre_match_ad_break(
        &self,
        break_id: &str,
        user_ids: &[u32],
        now_ms: i64,
        minimum_interval_ms: i64,
        policy_version: u64,
    ) -> Result<bool> {
        let cutoff_ms = Self::validate_pre_match_ad_break_claim(
            break_id,
            user_ids,
            now_ms,
            minimum_interval_ms,
            policy_version,
        )?;
        let mut updates = Vec::with_capacity(user_ids.len());
        for user_id in user_ids {
            let update = Update::builder()
                .table_name(self.main_table())
                .key("pk", Self::av_s(format!("USER#{user_id}")))
                .key("sk", Self::av_s("AD#PRE_MATCH"))
                .update_expression(concat!(
                    "SET #last_break_at=:now, #break_id=:break_id, ",
                    "#policy_version=:policy_version"
                ))
                .condition_expression(concat!(
                    "attribute_not_exists(#last_break_at) OR ",
                    "#last_break_at<=:cutoff OR #break_id=:break_id"
                ))
                .expression_attribute_names("#last_break_at", "lastBreakAtMs")
                .expression_attribute_names("#break_id", "breakId")
                .expression_attribute_names("#policy_version", "policyVersion")
                .expression_attribute_values(":now", Self::av_n(now_ms))
                .expression_attribute_values(":cutoff", Self::av_n(cutoff_ms))
                .expression_attribute_values(":break_id", Self::av_s(break_id))
                .expression_attribute_values(":policy_version", Self::av_n(policy_version))
                .build()
                .context("Failed to build pre-match ad-break cooldown claim")?;
            updates.push(TransactWriteItem::builder().update(update).build());
        }

        match self
            .client
            .transact_write_items()
            .client_request_token(break_id)
            .set_transact_items(Some(updates))
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(Self::transaction_cancellation_is_conditional) =>
            {
                Ok(false)
            }
            Err(error) => Err(error).context("Failed to claim pre-match ad-break cooldown"),
        }
    }

    async fn apply_completion_effect(
        &self,
        completion: &CompletionRecordV1,
        effect: &CompletionEffect,
    ) -> Result<EffectApplyResult> {
        completion.validate_effect(effect)?;

        let max_attempts = if matches!(
            effect,
            CompletionEffect::AddXp { .. }
                | CompletionEffect::AddMmr { .. }
                | CompletionEffect::UpdateRanking { .. }
        ) {
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
                    let history_config = match self.get_runtime_config().await {
                        Ok(runtime_config) => runtime_config.config.history,
                        Err(error) => {
                            warn!(
                                ?error,
                                "failed to read runtime config for completed game; using default history retention"
                            );
                            RuntimeHistoryConfig::default()
                        }
                    };
                    let snapshot_ttl = Self::retention_ttl_seconds(
                        completion.ended_at_ms,
                        history_config.snapshot_retention_days,
                    )?;
                    let summary_ttl = Self::retention_ttl_seconds(
                        completion.ended_at_ms,
                        history_config.summary_retention_days,
                    )?;
                    let summary =
                        match_history_summary(completion, history_config.snapshot_retention_days)?;
                    let summary_json = serde_json::to_string(&summary)
                        .context("Failed to serialize immutable match history summary")?;
                    let history_sk =
                        Self::history_sort_key(completion.ended_at_ms, completion.game_id)?;
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
                    let news_eligible = match self
                        .source_game_is_news_eligible(
                            completion.game_id as i32,
                            Self::new_game_state_is_news_eligible(&completion.final_state),
                        )
                        .await
                    {
                        Ok(eligible) => eligible,
                        Err(error) => {
                            // Result persistence is mandatory; public news is
                            // optional and must fail closed under uncertainty.
                            warn!(
                                game_id = completion.game_id,
                                %error,
                                "Could not verify completion source for public news; failing closed"
                            );
                            false
                        }
                    };
                    let runtime_identity = Self::runtime_game_identity(
                        completion.game_id as i32,
                        &completion.final_state,
                    );

                    let mut expression = concat!(
                        "SET gsi1pk=:gsi1pk, gsi1sk=:gsi1sk, id=:id, serverId=:server, ",
                        "gameType=:game_type, gameState=:game_state, #status=:status, ",
                        "endedAt=:ended, lastActivity=:ended, createdAt=:created, ",
                        "gameMode=:mode, isPrivate=if_not_exists(isPrivate,:private), ",
                        "runtimeIdentity=:runtime, completionRevision=:revision, ",
                        "newsEligible=:news_eligible, #ttl=:ttl"
                    )
                    .to_string();
                    if completion.final_state.game_code.is_some() {
                        expression.push_str(", gameCode=:game_code");
                    }
                    if completion.season.is_some() {
                        expression.push_str(", season=:season");
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
                        .expression_attribute_values(":news_eligible", Self::av_bool(news_eligible))
                        .expression_attribute_values(":ttl", Self::av_n(snapshot_ttl));
                    if let Some(game_code) = &completion.final_state.game_code {
                        update =
                            update.expression_attribute_values(":game_code", Self::av_s(game_code));
                    }
                    if let Some(season) = completion.season {
                        update = update.expression_attribute_values(":season", Self::av_n(season));
                    }
                    let mut mutations = vec![
                        TransactWriteItem::builder()
                            .update(
                                update
                                    .build()
                                    .context("Failed to build completed-game update")?,
                            )
                            .build(),
                    ];

                    let canonical_history = Put::builder()
                        .table_name(self.main_table())
                        .item("pk", Self::av_s(format!("GAME#{}", completion.game_id)))
                        .item("sk", Self::av_s("HISTORY"))
                        .item("gsi2pk", Self::av_s(HISTORY_GSI_PARTITION))
                        .item("gsi2sk", Self::av_s(&history_sk))
                        .item("gameId", Self::av_n(completion.game_id))
                        .item("endedAtMs", Self::av_n(completion.ended_at_ms))
                        .item("summaryJson", Self::av_s(&summary_json))
                        .item(
                            "completionRevision",
                            Self::av_s(completion.revision.to_string()),
                        )
                        .item("ttl", Self::av_n(summary_ttl))
                        .condition_expression(
                            "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                        )
                        .build()
                        .context("Failed to build canonical match history write")?;
                    mutations.push(TransactWriteItem::builder().put(canonical_history).build());

                    if !completion.final_state.is_stress_test {
                        for player in &summary.players {
                            let user_history = Put::builder()
                                .table_name(self.main_table())
                                .item("pk", Self::av_s(format!("USER#{}", player.user_id)))
                                .item("sk", Self::av_s(&history_sk))
                                .item("gameId", Self::av_n(completion.game_id))
                                .item("endedAtMs", Self::av_n(completion.ended_at_ms))
                                .item("summaryJson", Self::av_s(&summary_json))
                                .item(
                                    "completionRevision",
                                    Self::av_s(completion.revision.to_string()),
                                )
                                .item("ttl", Self::av_n(summary_ttl))
                                .condition_expression(
                                    "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                                )
                                .build()
                                .context("Failed to build player match history write")?;
                            mutations.push(TransactWriteItem::builder().put(user_history).build());
                        }
                    }

                    // This durable v8+ completion counter is the advertisement
                    // eligibility source of truth. Keeping it in this replay-safe
                    // transaction makes each newly claimed completion count once.
                    for user_id in completion.final_state.players.keys() {
                        let progress_update = Update::builder()
                            .table_name(self.main_table())
                            .key("pk", Self::av_s(format!("USER#{user_id}")))
                            .key("sk", Self::av_s("META"))
                            .update_expression("ADD gamesPlayed :one")
                            .condition_expression("attribute_exists(pk) AND attribute_exists(sk)")
                            .expression_attribute_values(":one", Self::av_n(1))
                            .build()
                            .context("Failed to build games-played update")?;
                        mutations
                            .push(TransactWriteItem::builder().update(progress_update).build());
                    }
                    debug_assert!(mutations.len() + 2 <= 100);
                    mutations
                }
                CompletionEffect::AddXp {
                    user_id, amount, ..
                } => {
                    let (current_username, is_guest, uses_username_mirror) =
                        self.completion_user_target(*user_id).await?;
                    let main_update = Update::builder()
                        .table_name(self.main_table())
                        .key("pk", Self::av_s(format!("USER#{user_id}")))
                        .key("sk", Self::av_s("META"))
                        .update_expression("ADD xp :delta")
                        .condition_expression(concat!(
                            "attribute_exists(pk) AND attribute_exists(sk) AND ",
                            "username=:username AND isGuest=:is_guest"
                        ))
                        .expression_attribute_values(":delta", Self::av_n(amount))
                        .expression_attribute_values(":username", Self::av_s(&current_username))
                        .expression_attribute_values(":is_guest", Self::av_bool(is_guest))
                        .build()
                        .context("Failed to build idempotent XP update")?;
                    let mut mutations =
                        vec![TransactWriteItem::builder().update(main_update).build()];
                    if uses_username_mirror {
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
                    let (current_username, is_guest, uses_username_mirror) =
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
                        .condition_expression(concat!(
                            "attribute_exists(pk) AND attribute_exists(sk) AND ",
                            "username=:username AND isGuest=:is_guest"
                        ))
                        .expression_attribute_values(":delta", Self::av_n(delta))
                        .expression_attribute_values(":username", Self::av_s(&current_username))
                        .expression_attribute_values(":is_guest", Self::av_bool(is_guest))
                        .build()
                        .context("Failed to build idempotent MMR update")?;
                    let mut mutations =
                        vec![TransactWriteItem::builder().update(main_update).build()];
                    if uses_username_mirror {
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

                    // Same row under the user-addressed sort key, so
                    // `get_user_ranking` stays a keyed read. Written in this
                    // transaction rather than after it: the guards above make
                    // the ladder write conditional, and a pointer applied
                    // outside those guards could describe a state the ladder
                    // rejected. Unconditional, so it also clears any "no
                    // ranking here" tombstone a reader left behind.
                    let mut pointer = item.clone();
                    pointer.insert(
                        "sk".into(),
                        Self::av_s(Self::ranking_pointer_sk(*user_id as i32)),
                    );
                    // Ladder rows are the only rows the GSI should surface; a
                    // pointer in GameTypeSeasonIndex would double every global
                    // leaderboard.
                    pointer.remove("gameTypeSeason");
                    let pointer_write = Put::builder()
                        .table_name(self.rankings_table())
                        .set_item(Some(pointer))
                        .build()
                        .context("Failed to build ranking pointer effect")?;

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
                    ranking_mutations.push(TransactWriteItem::builder().put(pointer_write).build());
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
                    let news_eligible = match self
                        .completed_game_is_news_eligible(completion.game_id)
                        .await
                    {
                        Ok(eligible) => eligible,
                        Err(error) => {
                            warn!(
                                game_id = completion.game_id,
                                %error,
                                "Could not verify high-score source for public news; failing closed"
                            );
                            false
                        }
                    };
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
                        .item("newsEligible", Self::av_bool(news_eligible))
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
                    // Ranking rows and guest-to-account transitions both use
                    // conditional state. Re-read and rebuild until one attempt
                    // observes the winning MMR/account state.
                    let exponent = attempt.min(6) as u32;
                    sleep(Duration::from_millis(1_u64 << exponent)).await;
                    debug!(
                        "Retrying completion effect {} after concurrent mutation: {}",
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

        // The pointer is the same row under a user-addressed sort key. It is
        // what makes `get_user_ranking` a keyed read, including the one this
        // method just performed to derive the counters above.
        let mut pointer = item.clone();
        pointer.insert(
            "sk".to_string(),
            Self::av_s(Self::ranking_pointer_sk(user_id)),
        );
        // Ladder rows are the only rows the GSI should surface; a pointer in
        // GameTypeSeasonIndex would double every global leaderboard.
        pointer.remove("gameTypeSeason");

        let ladder_row = Put::builder()
            .table_name(self.rankings_table())
            .set_item(Some(item))
            .build()
            .context("Failed to build ranking row write")?;
        let pointer_row = Put::builder()
            .table_name(self.rankings_table())
            .set_item(Some(pointer))
            .build()
            .context("Failed to build ranking pointer write")?;

        // Row, pointer, and the retired row move together. Applied separately,
        // a crash between them would leave the pointer disagreeing with the
        // ladder about a user's counters, and the next match would replay the
        // increment off whichever one it happened to read.
        let mut mutations = vec![
            TransactWriteItem::builder().put(ladder_row).build(),
            TransactWriteItem::builder().put(pointer_row).build(),
        ];

        // Retire the previous entry when a new MMR gave it a different SK.
        if let Some(prev_mmr) = old_mmr
            && prev_mmr != mmr
        {
            let old_inverted = 99999999 - prev_mmr.clamp(0, 99999999);
            let old_sk = format!("MMR#{:08}#USER#{}", old_inverted, user_id);

            let retired = Delete::builder()
                .table_name(self.rankings_table())
                .key("pk", Self::av_s(&pk))
                .key("sk", Self::av_s(&old_sk))
                .build()
                .context("Failed to build retired ranking row delete")?;
            mutations.push(TransactWriteItem::builder().delete(retired).build());
        }

        self.client
            .transact_write_items()
            .set_transact_items(Some(mutations))
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

                // `begins_with` is load-bearing, not defensive: each user also
                // has a `USER#{id}` pointer in this partition, and a ladder
                // shorter than `limit` would otherwise page into the pointers
                // and list every player twice.
                let response = self
                    .client
                    .query()
                    .table_name(self.rankings_table())
                    .key_condition_expression("pk = :pk AND begins_with(sk, :mmr_prefix)")
                    .expression_attribute_values(":pk", Self::av_s(&pk))
                    .expression_attribute_values(":mmr_prefix", Self::av_s("MMR#"))
                    .limit(limit as i32)
                    .send()
                    .await
                    .context("Failed to query leaderboard")?;

                response.items.unwrap_or_default()
            } else {
                // Prefer the GameTypeSeasonIndex to query all regions in a single partition
                let game_type_season =
                    format!("{}#{}#{}", queue_mode_str, game_type_str, season_str);
                let gsi_items = match self
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
                        // A successful empty query is authoritative for a new
                        // season. Only an unavailable index justifies scanning.
                        Some(response.items.unwrap_or_default())
                    }
                    Err(err) => {
                        warn!(
                            "Falling back to scan for global rankings (GameTypeSeasonIndex not available?): {:?}",
                            err
                        );
                        None
                    }
                };

                if let Some(gsi_items) = gsi_items {
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
                            // `begins_with(sk, ...)` excludes the per-user
                            // `USER#{id}` pointers, which carry the same
                            // attributes and would otherwise list twice.
                            .filter_expression(concat!(
                                "begins_with(pk, :prefix) AND #season = :season ",
                                "AND begins_with(sk, :mmr_prefix)"
                            ))
                            .expression_attribute_names("#season", "season")
                            .expression_attribute_values(":prefix", Self::av_s(&pk_prefix))
                            .expression_attribute_values(":mmr_prefix", Self::av_s("MMR#"))
                            .expression_attribute_values(":season", Self::av_n(season))
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
            // Scan all game types and regions for a season. `begins_with(sk,
            // ...)` excludes the per-user `USER#{id}` pointers, which carry the
            // same attributes and would otherwise list twice.
            let response = self
                .client
                .scan()
                .table_name(self.rankings_table())
                .filter_expression(concat!(
                    "begins_with(pk, :prefix) AND #season = :season ",
                    "AND begins_with(sk, :mmr_prefix)"
                ))
                .expression_attribute_names("#season", "season")
                .expression_attribute_values(
                    ":prefix",
                    Self::av_s(format!("RANKING#{}#", queue_mode_str)),
                )
                .expression_attribute_values(":mmr_prefix", Self::av_s("MMR#"))
                .expression_attribute_values(":season", Self::av_n(season))
                .limit(limit as i32)
                .send()
                .await
                .context("Failed to scan leaderboard")?;

            response.items.unwrap_or_default()
        };

        // Parse results into RankingEntry
        let mut entries: Vec<RankingEntry> = items
            .into_iter()
            .filter_map(|item| Self::leaderboard_entry_from_item(&item, season))
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

        // Fast path: one keyed read of the user's pointer. This is the only
        // path that should ever execute in steady state.
        let pointer = self
            .client
            .get_item()
            .table_name(self.rankings_table())
            .key("pk", Self::av_s(&pk))
            .key("sk", Self::av_s(Self::ranking_pointer_sk(user_id)))
            // A single small item, so strong consistency costs ~1 RCU and
            // keeps the read-modify-write in `upsert_ranking` correct.
            .consistent_read(true)
            .send()
            .await
            .context("Failed to read ranking pointer")?;

        if let Some(item) = pointer.item {
            if Self::extract_bool(&item, "absent") == Some(true) {
                return Ok(None);
            }
            return Ok(Self::user_ranking_from_items(
                std::iter::once(&item),
                user_id,
                queue_mode_str,
                &game_type_str,
                region,
                season,
            ));
        }

        // Migration path, taken at most once per user per ladder: rows written
        // before pointers existed can only be found by walking the partition.
        // A ranking partition can exceed DynamoDB's 1 MiB response page, so
        // keep following LastEvaluatedKey until the user is found or the
        // partition is genuinely exhausted.
        let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut request = self
                .client
                .query()
                .table_name(self.rankings_table())
                .key_condition_expression("pk = :pk AND begins_with(sk, :mmr_prefix)")
                .filter_expression("#user_id = :user_id_number OR #user_id = :user_id_string")
                .expression_attribute_names("#user_id", "userId")
                .expression_attribute_values(":pk", Self::av_s(&pk))
                .expression_attribute_values(":mmr_prefix", Self::av_s("MMR#"))
                .expression_attribute_values(":user_id_number", Self::av_n(user_id))
                .expression_attribute_values(":user_id_string", Self::av_s(user_id.to_string()))
                .consistent_read(true);
            if let Some(key) = &last_evaluated_key {
                request = request.set_exclusive_start_key(Some(key.clone()));
            }

            let response = request.send().await.context("Failed to query rankings")?;
            if let Some(item) = response
                .items
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|item| Self::extract_number(item, "userId") == Some(user_id))
            {
                let entry = Self::user_ranking_from_items(
                    std::iter::once(item),
                    user_id,
                    queue_mode_str,
                    &game_type_str,
                    region,
                    season,
                );

                // Promote the row we just paid for, so no later request has to
                // repeat this scan.
                let mut promoted = item.clone();
                promoted.insert(
                    "sk".to_string(),
                    Self::av_s(Self::ranking_pointer_sk(user_id)),
                );
                self.backfill_ranking_pointer(promoted).await;

                return Ok(entry);
            }

            let Some(next_key) = response.last_evaluated_key else {
                // Record the absence too — otherwise every unranked player on
                // the leaderboard page re-scans this partition on every load.
                self.backfill_ranking_pointer(Self::absent_ranking_pointer(&pk, user_id))
                    .await;
                return Ok(None);
            };
            last_evaluated_key = Some(next_key);
        }
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
            // This legacy API cannot verify the source game's privacy.
            .item("newsEligible", Self::av_bool(false))
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
                        news_eligible: Self::extract_bool(&item, "newsEligible") == Some(true),
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
                            news_eligible: Self::extract_bool(&item, "newsEligible") == Some(true),
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
                    news_eligible: Self::extract_bool(&item, "newsEligible") == Some(true),
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

    async fn get_news_high_score_snapshot(
        &self,
        game_type: &common::GameType,
        season: Season,
    ) -> Result<NewsHighScoreSnapshot> {
        self.create_high_scores_table_if_not_exists().await?;
        let game_type = Self::game_type_to_string(game_type);
        match self
            .query_global_news_high_score_snapshot(&game_type, season)
            .await
        {
            Ok(snapshot) => Ok(snapshot),
            Err(error) => {
                warn!(
                    %error,
                    "Withholding Solo-leader claims and using a bounded sample because GameTypeSeasonIndex is unavailable"
                );
                Ok(NewsHighScoreSnapshot {
                    leader: None,
                    coverage: NewsLeaderboardCoverage::BoundedSample,
                })
            }
        }
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
        assert_eq!(game.season, None, "legacy rows have no proven season");
        assert!(!game.news_eligible, "legacy rows must fail closed");

        item.insert("season".to_string(), DynamoDatabase::av_n(7));
        item.insert("isPrivate".to_string(), DynamoDatabase::av_bool(false));
        item.insert("newsEligible".to_string(), DynamoDatabase::av_bool(true));
        let current = DynamoDatabase::game_from_item(123, &item).unwrap();
        assert_eq!(current.season, Some(7));
        assert!(current.news_eligible);

        item.insert("season".to_string(), DynamoDatabase::av_n(-1));
        assert_eq!(
            DynamoDatabase::game_from_item(123, &item).unwrap().season,
            None
        );
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
    fn history_sort_keys_order_epoch_millis_then_padded_game_id() {
        let first = DynamoDatabase::history_sort_key(1_000, 99).unwrap();
        let same_millisecond_later_game = DynamoDatabase::history_sort_key(1_000, 100).unwrap();
        let later_millisecond = DynamoDatabase::history_sort_key(1_001, 1).unwrap();

        assert_eq!(first, "HISTORY#00000000000000001000#GAME#0000000099");
        assert!(first < same_millisecond_later_game);
        assert!(same_millisecond_later_game < later_millisecond);
    }

    #[test]
    fn history_cursor_is_opaque_and_scope_bound() {
        let item = HashMap::from([
            ("pk".to_string(), DynamoDatabase::av_s("USER#7")),
            (
                "sk".to_string(),
                DynamoDatabase::av_s("HISTORY#00000000000000001000#GAME#0000000001"),
            ),
        ]);
        let encoded = DynamoDatabase::encode_page_cursor("history:user:7", &item).unwrap();
        assert!(!encoded.contains("USER#7"));
        let decoded =
            DynamoDatabase::decode_page_cursor(&encoded, "history:user:7", "history").unwrap();
        assert_eq!(decoded.pk, "USER#7");
        assert!(DynamoDatabase::decode_page_cursor(&encoded, "history:user:8", "history").is_err());
    }

    #[test]
    fn history_summary_reader_dispatches_by_schema_version() {
        let mut item = HashMap::from([(
            "summaryJson".to_string(),
            DynamoDatabase::av_s(
                serde_json::json!({
                    "schemaVersion": MATCH_HISTORY_SCHEMA_VERSION,
                    "gameId": 42,
                    "startedAtMs": 1_000,
                    "endedAtMs": 2_000,
                    "durationMs": 1_000,
                    "mode": "duel",
                    "modeLabel": "Duel",
                    "queueMode": "competitive",
                    "isPrivate": false,
                    "isStressTest": false,
                    "completedByInactivity": false,
                    "players": [],
                    "winnerUserIds": [],
                    "snapshotAvailableUntilMs": 3_000
                })
                .to_string(),
            ),
        )]);

        assert_eq!(
            DynamoDatabase::history_summary_from_item(&item)
                .unwrap()
                .game_id,
            42
        );

        item.insert(
            "summaryJson".to_string(),
            DynamoDatabase::av_s(r#"{"schemaVersion":2}"#),
        );
        assert!(
            DynamoDatabase::history_summary_from_item(&item)
                .unwrap_err()
                .to_string()
                .contains("unsupported schemaVersion 2")
        );
    }

    #[test]
    fn runtime_config_reader_rejects_future_schema_versions() {
        let mut value = serde_json::to_value(RuntimeConfigRecord::default()).unwrap();
        value["schemaVersion"] = serde_json::json!(RUNTIME_CONFIG_SCHEMA_VERSION + 1);
        let item = HashMap::from([(
            "recordJson".to_string(),
            DynamoDatabase::av_s(value.to_string()),
        )]);

        assert!(
            DynamoDatabase::runtime_config_record_from_item(&item)
                .unwrap_err()
                .to_string()
                .contains("unsupported schemaVersion")
        );
    }

    #[test]
    fn completed_game_news_provenance_requires_explicit_public_terminal_row() {
        let mut public = HashMap::new();
        public.insert("status".to_string(), DynamoDatabase::av_s("complete"));
        public.insert("isPrivate".to_string(), DynamoDatabase::av_bool(false));
        public.insert("newsEligible".to_string(), DynamoDatabase::av_bool(true));
        assert!(DynamoDatabase::completed_game_item_is_news_eligible(Some(
            &public
        )));

        let mut private = public.clone();
        private.insert("isPrivate".to_string(), DynamoDatabase::av_bool(true));
        assert!(!DynamoDatabase::completed_game_item_is_news_eligible(Some(
            &private
        )));

        let mut code_gated = public.clone();
        code_gated.insert("gameCode".to_string(), DynamoDatabase::av_s("SECRET"));
        assert!(!DynamoDatabase::completed_game_item_is_news_eligible(Some(
            &code_gated
        )));

        let mut legacy = public.clone();
        legacy.remove("newsEligible");
        assert!(!DynamoDatabase::completed_game_item_is_news_eligible(Some(
            &legacy
        )));

        let mut unfinished = public.clone();
        unfinished.insert("status".to_string(), DynamoDatabase::av_s("started"));
        assert!(!DynamoDatabase::completed_game_item_is_news_eligible(Some(
            &unfinished
        )));
        assert!(!DynamoDatabase::completed_game_item_is_news_eligible(None));
    }

    #[test]
    fn source_game_news_provenance_preserves_private_metadata_and_legacy_uncertainty() {
        assert!(DynamoDatabase::source_game_item_is_news_eligible(
            None, true
        ));
        assert!(!DynamoDatabase::source_game_item_is_news_eligible(
            None, false
        ));

        let mut waiting_public = HashMap::new();
        waiting_public.insert("status".to_string(), DynamoDatabase::av_s("waiting"));
        waiting_public.insert("isPrivate".to_string(), DynamoDatabase::av_bool(false));
        assert!(DynamoDatabase::source_game_item_is_news_eligible(
            Some(&waiting_public),
            false
        ));

        let mut waiting_private = waiting_public.clone();
        waiting_private.insert("isPrivate".to_string(), DynamoDatabase::av_bool(true));
        assert!(!DynamoDatabase::source_game_item_is_news_eligible(
            Some(&waiting_private),
            true
        ));

        let mut code_gated = waiting_public.clone();
        code_gated.insert("gameCode".to_string(), DynamoDatabase::av_s("SECRET"));
        assert!(!DynamoDatabase::source_game_item_is_news_eligible(
            Some(&code_gated),
            true
        ));

        let mut completed_legacy = waiting_public.clone();
        completed_legacy.insert("status".to_string(), DynamoDatabase::av_s("complete"));
        assert!(!DynamoDatabase::source_game_item_is_news_eligible(
            Some(&completed_legacy),
            true
        ));

        let mut completed_verified = completed_legacy.clone();
        completed_verified.insert("newsEligible".to_string(), DynamoDatabase::av_bool(true));
        assert!(DynamoDatabase::source_game_item_is_news_eligible(
            Some(&completed_verified),
            false
        ));
    }

    #[test]
    fn legacy_score_backfill_requires_an_exact_public_source_result() {
        let score_item = high_score_item("42", 2_000, false);
        let entry = DynamoDatabase::high_score_entry_from_item(&score_item).unwrap();
        let user_id = u32::try_from(entry.user_id).unwrap();
        let mut state = common::GameState::new(
            40,
            40,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(7),
            1,
        );
        let player = state
            .add_player(user_id, Some(entry.username.clone()))
            .unwrap();
        state.scores.insert(player.snake_id, 2_000);
        state.status = common::GameStatus::Complete {
            winning_snake_id: Some(player.snake_id),
        };

        let mut source = HashMap::new();
        source.insert("status".to_string(), DynamoDatabase::av_s("complete"));
        source.insert("isPrivate".to_string(), DynamoDatabase::av_bool(false));
        source.insert(
            "gameState".to_string(),
            DynamoDatabase::av_s(serde_json::to_string(&state).unwrap()),
        );
        assert!(
            DynamoDatabase::legacy_high_score_source_item_is_news_eligible(Some(&source), &entry)
        );

        source.insert("isPrivate".to_string(), DynamoDatabase::av_bool(true));
        assert!(
            !DynamoDatabase::legacy_high_score_source_item_is_news_eligible(Some(&source), &entry)
        );

        source.insert("isPrivate".to_string(), DynamoDatabase::av_bool(false));
        let mut wrong_score = entry;
        wrong_score.score += 1;
        assert!(
            !DynamoDatabase::legacy_high_score_source_item_is_news_eligible(
                Some(&source),
                &wrong_score
            )
        );
    }

    #[test]
    fn recent_completed_game_page_limit_is_bounded_without_capping_the_total() {
        assert_eq!(DynamoDatabase::recent_completed_games_page_limit(0), None);
        assert_eq!(
            DynamoDatabase::recent_completed_games_page_limit(1),
            Some(1)
        );
        assert_eq!(
            DynamoDatabase::recent_completed_games_page_limit(
                RECENT_COMPLETED_GAMES_PAGE_SIZE + 56
            ),
            Some(RECENT_COMPLETED_GAMES_PAGE_SIZE as i32)
        );
        assert_eq!(
            DynamoDatabase::recent_completed_games_page_limit(56),
            Some(56)
        );
    }

    #[test]
    fn recent_completed_games_exclude_expired_rows_and_preserve_query_order() {
        fn completed_item(
            id: i32,
            ended_at: &str,
            ttl: Option<i64>,
        ) -> HashMap<String, AttributeValue> {
            let mut item = HashMap::new();
            item.insert("id".to_string(), DynamoDatabase::av_n(id));
            item.insert("status".to_string(), DynamoDatabase::av_s("complete"));
            item.insert("endedAt".to_string(), DynamoDatabase::av_s(ended_at));
            if let Some(ttl) = ttl {
                item.insert("ttl".to_string(), DynamoDatabase::av_n(ttl));
            }
            item
        }

        let items = vec![
            completed_item(30, "2026-08-14T12:03:00+00:00", None),
            completed_item(20, "2026-08-14T12:02:00+00:00", Some(99)),
            completed_item(10, "2026-08-14T12:01:00+00:00", Some(101)),
        ];

        let games = DynamoDatabase::recent_completed_games_from_items(items, 100).unwrap();

        assert_eq!(
            games.iter().map(|game| game.id).collect::<Vec<_>>(),
            vec![30, 10]
        );
    }

    #[test]
    fn recent_completed_games_fill_the_accepted_limit_across_filtered_pages() {
        fn completed_item(id: i32, ttl: Option<i64>) -> HashMap<String, AttributeValue> {
            let mut item = HashMap::new();
            item.insert("id".to_string(), DynamoDatabase::av_n(id));
            item.insert("status".to_string(), DynamoDatabase::av_s("complete"));
            if let Some(ttl) = ttl {
                item.insert("ttl".to_string(), DynamoDatabase::av_n(ttl));
            }
            item
        }

        let mut games = Vec::new();
        DynamoDatabase::append_recent_completed_games_from_items(
            &mut games,
            vec![completed_item(30, None), completed_item(20, Some(99))],
            100,
            2,
        )
        .unwrap();
        assert_eq!(
            games.iter().map(|game| game.id).collect::<Vec<_>>(),
            vec![30]
        );

        DynamoDatabase::append_recent_completed_games_from_items(
            &mut games,
            vec![completed_item(10, None), completed_item(9, None)],
            100,
            2,
        )
        .unwrap();
        assert_eq!(
            games.iter().map(|game| game.id).collect::<Vec<_>>(),
            vec![30, 10]
        );
    }

    fn ranking_item(user_id: i32, mmr: i32) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::new();
        item.insert("userId".to_string(), DynamoDatabase::av_n(user_id));
        item.insert(
            "username".to_string(),
            DynamoDatabase::av_s(format!("player-{user_id}")),
        );
        item.insert("mmr".to_string(), DynamoDatabase::av_n(mmr));
        item.insert("gamesPlayed".to_string(), DynamoDatabase::av_n(20));
        item.insert("wins".to_string(), DynamoDatabase::av_n(12));
        item.insert("losses".to_string(), DynamoDatabase::av_n(8));
        item.insert("region".to_string(), DynamoDatabase::av_s("test"));
        item.insert("queueMode".to_string(), DynamoDatabase::av_s("ranked"));
        item.insert("gameType".to_string(), DynamoDatabase::av_s("duel"));
        item.insert("season".to_string(), DynamoDatabase::av_n(0));
        item.insert(
            "updatedAt".to_string(),
            DynamoDatabase::av_s("2026-08-14T12:00:00Z"),
        );
        item
    }

    #[test]
    fn leaderboard_rows_require_an_exact_numeric_season() {
        let mut season_one = ranking_item(1, 1_500);
        season_one.insert("season".to_string(), DynamoDatabase::av_n(1));
        assert!(DynamoDatabase::leaderboard_entry_from_item(&season_one, 1).is_some());

        let mut season_ten = ranking_item(10, 1_900);
        season_ten.insert("season".to_string(), DynamoDatabase::av_n(10));
        assert!(DynamoDatabase::leaderboard_entry_from_item(&season_ten, 1).is_none());

        let mut legacy = ranking_item(2, 1_400);
        legacy.remove("season");
        assert!(DynamoDatabase::leaderboard_entry_from_item(&legacy, 1).is_none());

        let mut invalid = ranking_item(3, 1_300);
        invalid.insert("season".to_string(), DynamoDatabase::av_n(-1));
        assert!(DynamoDatabase::leaderboard_entry_from_item(&invalid, 1).is_none());
    }

    fn high_score_item(
        game_id: &str,
        score: i32,
        news_eligible: bool,
    ) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::new();
        item.insert("gameId".to_string(), DynamoDatabase::av_s(game_id));
        item.insert("userId".to_string(), DynamoDatabase::av_n(score));
        item.insert(
            "username".to_string(),
            DynamoDatabase::av_s(format!("player-{score}")),
        );
        item.insert("score".to_string(), DynamoDatabase::av_n(score));
        let inverted = (99_999_999_i64 - i64::from(score)).max(0);
        item.insert(
            "sk".to_string(),
            DynamoDatabase::av_s(format!("SCORE#{inverted:08}#GAME#{game_id}")),
        );
        item.insert("region".to_string(), DynamoDatabase::av_s("test"));
        item.insert("gameType".to_string(), DynamoDatabase::av_s("solo"));
        item.insert("season".to_string(), DynamoDatabase::av_n(0));
        item.insert(
            "timestamp".to_string(),
            DynamoDatabase::av_s("2026-08-14T12:00:00Z"),
        );
        item.insert(
            "newsEligible".to_string(),
            DynamoDatabase::av_bool(news_eligible),
        );
        item
    }

    #[test]
    fn user_ranking_search_can_find_a_match_on_a_later_page() {
        let pages = [
            vec![ranking_item(1, 1_500), ranking_item(2, 1_400)],
            vec![ranking_item(42, 1_300)],
        ];

        let entry = pages.iter().find_map(|page| {
            DynamoDatabase::user_ranking_from_items(page, 42, "ranked", "duel", "test", 0)
        });

        assert_eq!(entry.map(|entry| entry.user_id), Some(42));
    }

    #[test]
    fn ordered_score_head_requires_a_public_strictly_greater_top() {
        let private_top =
            DynamoDatabase::high_score_entry_from_item(&high_score_item("private", 2_000, false));
        let public_runner_up =
            DynamoDatabase::high_score_entry_from_item(&high_score_item("public", 1_900, true));
        assert!(
            DynamoDatabase::unique_public_high_score_leader(&[
                private_top,
                public_runner_up.clone()
            ])
            .is_none()
        );

        let tied = DynamoDatabase::high_score_entry_from_item(&high_score_item("tie", 1_900, true));
        assert!(
            DynamoDatabase::unique_public_high_score_leader(&[public_runner_up, tied]).is_none()
        );

        let top = DynamoDatabase::high_score_entry_from_item(&high_score_item("top", 2_000, true));
        let runner_up =
            DynamoDatabase::high_score_entry_from_item(&high_score_item("runner", 1_900, true));
        assert_eq!(
            DynamoDatabase::unique_public_high_score_leader(&[top.clone(), runner_up.clone()])
                .map(|entry| entry.game_id),
            Some("top".to_string())
        );
        assert!(DynamoDatabase::unique_public_high_score_leader(&[runner_up, top]).is_none());
        assert!(DynamoDatabase::unique_public_high_score_leader(&[None]).is_none());
    }

    #[test]
    fn news_score_value_must_match_its_ordering_key() {
        let mut item = high_score_item("top", 2_000, true);
        let entry = DynamoDatabase::high_score_entry_from_item(&item).unwrap();
        assert!(DynamoDatabase::high_score_matches_sort_key(&item, &entry));

        item.insert(
            "sk".to_string(),
            DynamoDatabase::av_s("SCORE#99999999#GAME#top"),
        );
        assert!(!DynamoDatabase::high_score_matches_sort_key(&item, &entry));

        let mut wrong_game = high_score_item("top", 2_000, true);
        wrong_game.insert(
            "sk".to_string(),
            DynamoDatabase::av_s("SCORE#99997999#GAME#some-other-game"),
        );
        assert!(!DynamoDatabase::high_score_matches_sort_key(
            &wrong_game,
            &entry
        ));

        let mut out_of_range = entry;
        out_of_range.score = 100_000_000;
        assert!(!DynamoDatabase::high_score_matches_sort_key(
            &high_score_item("top", 2_000, true),
            &out_of_range
        ));
    }

    #[test]
    fn runtime_config_reader_safely_upconverts_version_one() {
        let item = HashMap::from([(
            "recordJson".to_string(),
            DynamoDatabase::av_s(
                serde_json::json!({
                    "schemaVersion": 1,
                    "version": 17,
                    "config": {
                        "announcement": {
                            "enabled": true,
                            "message": "Maintenance soon"
                        },
                        "ads": {
                            "postMatchEnabled": true,
                            "minimumIntervalMinutes": 22
                        },
                        "history": {
                            "snapshotRetentionDays": 45,
                            "summaryRetentionDays": 400
                        }
                    },
                    "updatedBy": {
                        "userId": 7,
                        "username": "operator"
                    },
                    "updatedAtMs": 123456
                })
                .to_string(),
            ),
        )]);

        let record = DynamoDatabase::runtime_config_record_from_item(&item).unwrap();

        assert_eq!(record.schema_version, RUNTIME_CONFIG_SCHEMA_VERSION);
        assert_eq!(record.version, 17);
        assert!(record.config.announcement.enabled);
        assert_eq!(record.config.announcement.message, "Maintenance soon");
        assert_eq!(record.config.history.snapshot_retention_days, 45);
        assert_eq!(record.config.history.summary_retention_days, 400);
        assert_eq!(record.config.ads.minimum_interval_minutes, 22);
        assert_eq!(record.config.ads.minimum_games_played, 1);
        assert!(!record.config.ads.enabled);
        assert!(!record.config.ads.distributions.web.enabled);
        assert!(!record.config.ads.distributions.crazygames.enabled);
        assert!(!record.config.ads.distributions.itch.enabled);
        assert_eq!(record.updated_by.unwrap().username, "operator");
        assert_eq!(record.updated_at_ms, 123456);
    }

    #[test]
    fn pre_match_ad_break_claim_validation_is_strict() {
        assert_eq!(
            DynamoDatabase::validate_pre_match_ad_break_claim(
                "12345678-1234-1234-1234-123456789012",
                &[1, 2, 3, 4],
                100_000,
                60_000,
                9,
            )
            .unwrap(),
            40_000
        );

        for invalid in [
            DynamoDatabase::validate_pre_match_ad_break_claim("", &[1], 100, 10, 1),
            DynamoDatabase::validate_pre_match_ad_break_claim(
                "12345678-1234-1234-1234-1234567890123",
                &[1],
                100,
                10,
                1,
            ),
            DynamoDatabase::validate_pre_match_ad_break_claim("break", &[], 100, 10, 1),
            DynamoDatabase::validate_pre_match_ad_break_claim(
                "break",
                &[1, 2, 3, 4, 5],
                100,
                10,
                1,
            ),
            DynamoDatabase::validate_pre_match_ad_break_claim("break", &[1, 1], 100, 10, 1),
            DynamoDatabase::validate_pre_match_ad_break_claim("break", &[0], 100, 10, 1),
            DynamoDatabase::validate_pre_match_ad_break_claim("break", &[1], -1, 10, 1),
            DynamoDatabase::validate_pre_match_ad_break_claim("break", &[1], 100, 0, 1),
            DynamoDatabase::validate_pre_match_ad_break_claim("break", &[1], 100, 10, 0),
        ] {
            assert!(invalid.is_err());
        }
    }

    #[test]
    fn only_conditional_transaction_cancellation_is_an_ineligible_claim() {
        use aws_sdk_dynamodb::types::CancellationReason;
        use aws_sdk_dynamodb::types::error::TransactionCanceledException;

        let cancelled = |code: &str| {
            TransactWriteItemsError::TransactionCanceledException(
                TransactionCanceledException::builder()
                    .cancellation_reasons(CancellationReason::builder().code(code).build())
                    .build(),
            )
        };

        assert!(DynamoDatabase::transaction_cancellation_is_conditional(
            &cancelled("ConditionalCheckFailed")
        ));
        assert!(!DynamoDatabase::transaction_cancellation_is_conditional(
            &cancelled("TransactionConflict")
        ));
    }

    #[test]
    fn news_scores_with_missing_timestamps_fail_closed() {
        let mut score = high_score_item("score", 2_000, true);
        score.remove("timestamp");
        assert!(DynamoDatabase::high_score_entry_from_item(&score).is_none());
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
