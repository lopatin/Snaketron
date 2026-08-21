//! Reading the raw tier back, for the fold.
//!
//! The committer's whole view of S3 is here, and it is deliberately stateless:
//! it keeps no index of what it has seen and parses nothing out of a key. It
//! answers exactly one question — "every key in this dataset above this
//! bound, in order" — and every object is gzipped NDJSON, so the reader is the
//! writer's compressor inverted.
//!
//! What it deliberately does NOT do is decide what is new. Keys sort in the
//! order they were written only within one `dt=…/host=…` prefix
//! (`object_store::partition_prefix`), so a lexicographic bound cannot mean
//! "everything already folded" — that belief is what dropped an entire region.
//! The bound the committer passes is a retention window (`resume.rs`), and the
//! per-prefix marks do the skipping.

use std::io::Read;

use async_trait::async_trait;
use aws_sdk_s3::error::DisplayErrorContext;
use flate2::read::GzDecoder;
use snaketron_service_api::ServiceError;

use super::committer::SourceListing;
use super::object_store::dataset_prefix;

/// Ceiling on one object's decoded size.
///
/// The exporter caps a file at `BatchLimits::max_bytes_per_file` (32 MiB of
/// NDJSON by default), so this is 4x headroom for an operator raising that
/// limit. It exists because the reader must not trust the object: gzip's
/// expansion ratio is unbounded, and an unbounded `read_to_end` on a corrupt
/// or hostile member is how a small object becomes an OOM in the container
/// that is also running games.
pub const MAX_DECODED_BYTES: usize = 128 * 1024 * 1024;

/// Lists and reads the objects a regional exporter wrote.
pub struct S3SourceListing {
    client: aws_sdk_s3::Client,
    bucket: String,
    max_decoded_bytes: usize,
}

impl S3SourceListing {
    pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            max_decoded_bytes: MAX_DECODED_BYTES,
        }
    }

    /// Lowers the decode ceiling. Only tests need this: production wants the
    /// constant, and raising it past what the task can hold is not a knob
    /// worth exposing to an operator.
    #[cfg(test)]
    fn with_max_decoded_bytes(mut self, bytes: usize) -> Self {
        self.max_decoded_bytes = bytes;
        self
    }
}

#[async_trait]
impl SourceListing for S3SourceListing {
    async fn list_after(
        &self,
        dataset: &str,
        after: Option<&str>,
    ) -> Result<Vec<String>, ServiceError> {
        let prefix = dataset_prefix(dataset);
        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix);
        if let Some(after) = after {
            // `StartAfter` is the trait's contract verbatim — S3 returns keys
            // strictly greater than it, in lexicographic order. Filtering the
            // full listing client-side would be the same answer at the cost of
            // paging every object ever written, forever.
            request = request.start_after(after);
        }

        // Paginated, not a single call: one busy day exceeds the 1000-key page
        // limit, and a truncated page would leave the fold stuck at whatever
        // key the first page ended on — silently, since a short listing and a
        // quiet dataset look identical.
        let mut pages = request.into_paginator().send();
        let mut keys = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|error| {
                ServiceError::failed(format!(
                    "listing {prefix} after {} in {}: {}",
                    after.unwrap_or("(the beginning)"),
                    self.bucket,
                    DisplayErrorContext(&error),
                ))
            })?;
            keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|object| object.key().map(str::to_owned)),
            );
        }
        Ok(keys)
    }

    /// `dataset` is unused: a key from [`Self::list_after`] is already
    /// absolute, so joining a prefix onto it here would be a second, weaker
    /// statement of where the object lives — one that could disagree.
    async fn fetch(&self, _dataset: &str, key: &str) -> Result<String, ServiceError> {
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                ServiceError::failed(format!(
                    "fetching {key} from {}: {}",
                    self.bucket,
                    DisplayErrorContext(&error),
                ))
            })?;
        let body = object
            .body
            .collect()
            .await
            .map_err(|error| ServiceError::failed(format!("reading the body of {key}: {error}")))?
            .into_bytes();

        // One byte past the ceiling, so an object that lands exactly on it is
        // still admissible while anything larger is detected rather than
        // silently truncated into a half-line of NDJSON.
        let limit = (self.max_decoded_bytes as u64).saturating_add(1);
        let mut decoded = Vec::new();
        GzDecoder::new(body.as_ref())
            .take(limit)
            .read_to_end(&mut decoded)
            .map_err(|error| ServiceError::failed(format!("decompressing {key}: {error}")))?;
        if decoded.len() > self.max_decoded_bytes {
            return Err(ServiceError::failed(format!(
                "{key} decompresses past the {} byte ceiling",
                self.max_decoded_bytes
            )));
        }

        String::from_utf8(decoded)
            .map_err(|error| ServiceError::failed(format!("{key} is not valid UTF-8: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use aws_sdk_s3::config::retry::RetryConfig;
    use aws_sdk_s3::config::timeout::TimeoutConfig;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, StalledStreamProtectionConfig};
    use aws_smithy_runtime_api::client::http::{
        HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;

    use super::*;
    use crate::analytics::object_store::compress;

    const BUCKET: &str = "snaketron-analytics";
    const DATASET: &str = "game-events";

    /// One scripted HTTP answer: a status and a body.
    type CannedResponse = (u16, Vec<u8>);

    /// An S3 whose every answer is scripted, and which records what it was
    /// asked. The requests matter as much as the responses: `StartAfter` is a
    /// property of the REQUEST, and a listing that ignored the resume mark
    /// would still return exactly the right keys from a canned response.
    #[derive(Debug, Clone)]
    struct CannedS3 {
        responses: Arc<Mutex<VecDeque<CannedResponse>>>,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl HttpConnector for CannedS3 {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.requests.lock().unwrap().push(request.uri().to_owned());
            let (status, body) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("the test must script a response for every request");
            let response = HttpResponse::new(
                StatusCode::try_from(status).expect("a status code"),
                SdkBody::from(body),
            );
            HttpConnectorFuture::ready(Ok(response))
        }
    }

    fn listing(responses: Vec<CannedResponse>) -> (S3SourceListing, CannedS3) {
        let canned = CannedS3 {
            responses: Arc::new(Mutex::new(responses.into())),
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let connector = SharedHttpConnector::new(canned.clone());
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("ak", "sk", None, None, "test"))
            .http_client(http_client_fn(move |_, _| connector.clone()))
            // All three disabled so the runtime never waits: a canned response
            // is instant, so any delay here would be a test sitting in a
            // backoff rather than exercising the code.
            .retry_config(RetryConfig::disabled())
            .timeout_config(TimeoutConfig::disabled())
            .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
            .build();
        (
            S3SourceListing::new(aws_sdk_s3::Client::from_conf(config), BUCKET),
            canned,
        )
    }

    /// One `ListObjectsV2` page. `next` present means truncated, which is what
    /// makes the SDK paginator ask for another.
    fn page(keys: &[&str], next: Option<&str>) -> CannedResponse {
        let contents: String = keys
            .iter()
            .map(|key| format!("<Contents><Key>{key}</Key><Size>1</Size></Contents>"))
            .collect();
        let token = next
            .map(|token| format!("<NextContinuationToken>{token}</NextContinuationToken>"))
            .unwrap_or_default();
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{BUCKET}</Name><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys>\
             <IsTruncated>{}</IsTruncated>{token}{contents}</ListBucketResult>",
            keys.len(),
            next.is_some(),
        );
        (200, body.into_bytes())
    }

    fn key(date: &str, host: &str, cursor: &str) -> String {
        crate::analytics::object_store::object_key(DATASET, date, host, cursor, cursor, "hash")
    }

    /// A day of traffic exceeds the 1000-key page limit, so a listing that
    /// read only the first page would advance the fold to that page's last key
    /// and never come back for the rest.
    #[tokio::test]
    async fn every_page_of_a_truncated_listing_is_read() {
        let first = key("2026-08-19", "use1-1", "1");
        let second = key("2026-08-19", "use1-1", "2");
        let third = key("2026-08-20", "use1-1", "3");
        let (listing, canned) = listing(vec![
            page(&[&first, &second], Some("page-two")),
            page(&[&third], None),
        ]);

        let keys = listing.list_after(DATASET, None).await.expect("listing");
        assert_eq!(keys, vec![first, second, third]);

        let requests = canned.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2, "the second page must be requested");
        assert!(
            requests[1].contains("continuation-token=page-two"),
            "the second request must carry the token the first returned: {}",
            requests[1]
        );
    }

    /// The bound has to reach S3, not be applied after the fact: a
    /// client-side filter would page the whole dataset from the beginning on
    /// every tick and get slower forever.
    #[tokio::test]
    async fn the_bound_is_sent_as_start_after() {
        let mark = key("2026-08-19", "use1-1", "9");
        let (listing, canned) = listing(vec![page(&[], None)]);

        listing
            .list_after(DATASET, Some(&mark))
            .await
            .expect("listing");

        let requests = canned.requests.lock().unwrap().clone();
        let query = requests[0].clone();
        assert!(query.contains("start-after="), "{query}");
        assert!(
            percent_decode(&query).contains(&mark),
            "the exact mark must be the StartAfter value: {query}"
        );
        assert!(
            percent_decode(&query).contains(&format!("prefix={}", dataset_prefix(DATASET))),
            "the listing must be scoped to the dataset: {query}"
        );
    }

    /// No bound must mean no `StartAfter` — not a sentinel standing in for
    /// one, which is a key real objects could sort behind. The committer
    /// always passes a bound today, but the trait's `None` has to keep meaning
    /// "everything" or a future caller would silently lose the oldest keys.
    #[tokio::test]
    async fn a_first_run_lists_from_the_beginning() {
        let (listing, canned) = listing(vec![page(&[], None)]);
        listing.list_after(DATASET, None).await.expect("listing");
        assert!(
            !canned.requests.lock().unwrap()[0].contains("start-after"),
            "no resume mark means no StartAfter"
        );
    }

    /// The fold receives the exporter's own bytes back, so the reader must
    /// invert the writer's compressor rather than a hand-rolled one.
    #[tokio::test]
    async fn an_object_is_gunzipped_into_ndjson() {
        let rows = "{\"event_id\":\"a\"}\n{\"event_id\":\"b\"}\n";
        let (listing, _) = listing(vec![(200, compress(rows).expect("compress"))]);

        let fetched = listing
            .fetch(DATASET, &key("2026-08-19", "use1-1", "1"))
            .await
            .expect("fetch");
        assert_eq!(fetched, rows);
    }

    /// Gzip's expansion ratio is unbounded, so the decode must stop rather
    /// than allocate whatever the object claims.
    #[tokio::test]
    async fn an_object_that_decompresses_past_the_ceiling_is_refused() {
        let rows = "x".repeat(4096);
        let (listing, _) = listing(vec![(200, compress(&rows).expect("compress"))]);
        let listing = listing.with_max_decoded_bytes(64);

        let error = listing
            .fetch(
                DATASET,
                "raw/game-events/dt=2026-08-19/host=use1-1/big.json.gz",
            )
            .await
            .expect_err("an oversized object must not be returned");
        assert!(error.to_string().contains("64 byte ceiling"), "{error}");
    }

    /// The failure mode that matters most: an unreadable bucket must not be
    /// indistinguishable from a bucket with nothing new in it. An `Ok(vec![])`
    /// here would advance nothing, log nothing, and look exactly like a quiet
    /// hour — forever.
    #[tokio::test]
    async fn a_listing_failure_is_an_error_and_not_an_empty_list() {
        let denied = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
            .to_vec();
        let (listing, _) = listing(vec![(403, denied)]);

        let error = listing
            .list_after(DATASET, None)
            .await
            .expect_err("a refused listing must not read as an empty listing");
        let message = error.to_string();
        assert!(
            message.contains(dataset_prefix(DATASET).as_str()),
            "{message}"
        );
        assert!(message.contains("AccessDenied"), "{message}");
    }

    /// A page that fails partway through must fail the whole listing: keys
    /// from the pages that did arrive are a PREFIX of the truth, and folding
    /// them would advance the mark past everything the failed page held.
    #[tokio::test]
    async fn a_failure_on_a_later_page_fails_the_whole_listing() {
        let first = key("2026-08-19", "use1-1", "1");
        let (listing, _) = listing(vec![
            page(&[&first], Some("page-two")),
            (500, b"<Error><Code>InternalError</Code></Error>".to_vec()),
        ]);

        listing
            .list_after(DATASET, None)
            .await
            .expect_err("a mid-listing failure must not return a partial listing");
    }

    /// Percent-decodes just enough to compare a query string against a key.
    fn percent_decode(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut out = String::with_capacity(value.len());
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'%' if index + 2 < bytes.len() => {
                    let hex = &value[index + 1..index + 3];
                    match u8::from_str_radix(hex, 16) {
                        Ok(byte) => {
                            out.push(byte as char);
                            index += 3;
                        }
                        Err(_) => {
                            out.push('%');
                            index += 1;
                        }
                    }
                }
                byte => {
                    out.push(byte as char);
                    index += 1;
                }
            }
        }
        out
    }
}
