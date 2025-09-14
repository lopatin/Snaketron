// Query helper functions for DynamoDB operations
// This module can be expanded with more complex query patterns as needed

use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

pub fn pk_sk_key(pk: &str, sk: &str) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    key.insert("sk".to_string(), AttributeValue::S(sk.to_string()));
    key
}

pub fn single_key(key_name: &str, value: &str) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    key.insert(key_name.to_string(), AttributeValue::S(value.to_string()));
    key
}