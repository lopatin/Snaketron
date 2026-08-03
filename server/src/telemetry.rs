//! Process-level OpenTelemetry metrics export.
//!
//! The application exports OTLP to a collector on the local task network. The
//! collector owns backend credentials, retry, batching, and cloud-resource
//! enrichment; the Rust process only knows its loopback OTLP endpoint.

use anyhow::{Context, Result, anyhow};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use std::env;
use std::time::Duration;

const DEFAULT_EXPORT_INTERVAL_MS: u64 = 5_000;
const MIN_EXPORT_INTERVAL_MS: u64 = 1_000;

/// Owns the SDK provider so shutdown can export the final measurements after
/// all supervised game-server workers have stopped.
#[derive(Debug, Default)]
pub struct MetricsGuard {
    provider: Option<SdkMeterProvider>,
}

impl MetricsGuard {
    pub fn is_enabled(&self) -> bool {
        self.provider.is_some()
    }

    /// Flushes and shuts down the provider off the async runtime worker.
    pub async fn shutdown(self) -> Result<()> {
        let Some(provider) = self.provider else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || provider.shutdown())
            .await
            .context("OpenTelemetry metrics shutdown task failed")?
            .map_err(|error| anyhow!(error))
            .context("OpenTelemetry metrics shutdown failed")
    }
}

/// Installs the global meter provider when a standard OTLP endpoint is set.
/// Without an endpoint, instrumentation remains a cheap no-op for tests and
/// local development.
pub fn init_metrics() -> Result<MetricsGuard> {
    let exporter_disabled =
        env::var("OTEL_METRICS_EXPORTER").is_ok_and(|value| value.eq_ignore_ascii_case("none"));
    let endpoint_configured = env::var_os("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_some()
        || env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some();

    if exporter_disabled || !endpoint_configured {
        crate::otel_metrics::init();
        return Ok(MetricsGuard::default());
    }

    let export_interval = export_interval_from_env()?;
    let exporter = MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_temporality(Temporality::Cumulative)
        .build()
        .context("failed to build OTLP metrics exporter")?;
    let reader = PeriodicReader::builder(exporter)
        .with_interval(export_interval)
        .build();

    let environment =
        env::var("SNAKETRON_ENVIRONMENT").unwrap_or_else(|_| "development".to_string());
    let service_name =
        env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "snaketron-server".to_string());
    let service_version =
        env::var("SNAKETRON_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());
    let instance_id =
        env::var("SNAKETRON_OTEL_INSTANCE_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

    let resource = metrics_resource(
        service_name,
        service_version,
        instance_id,
        environment,
        env::var("SNAKETRON_REGION").ok(),
        env::var("SNAKETRON_AWS_REGION").ok(),
    );
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();
    global::set_meter_provider(provider.clone());
    crate::otel_metrics::init();

    tracing::info!(
        export_interval_ms = export_interval.as_millis(),
        "OpenTelemetry metrics export enabled"
    );
    Ok(MetricsGuard {
        provider: Some(provider),
    })
}

fn metrics_resource(
    service_name: String,
    service_version: String,
    instance_id: String,
    environment: String,
    logical_region: Option<String>,
    aws_region: Option<String>,
) -> Resource {
    let mut attributes = vec![
        KeyValue::new("service.namespace", "snaketron"),
        KeyValue::new("service.version", service_version),
        KeyValue::new("service.instance.id", instance_id),
        KeyValue::new("deployment.environment.name", environment),
    ];
    if let Some(region) = logical_region {
        attributes.push(KeyValue::new("snaketron.region", region));
    }
    if let Some(region) = aws_region {
        attributes.extend([
            KeyValue::new("cloud.provider", "aws"),
            KeyValue::new("cloud.platform", "aws_ecs"),
            KeyValue::new("cloud.region", region),
        ]);
    }

    Resource::builder()
        .with_service_name(service_name)
        .with_attributes(attributes)
        .build()
}

fn export_interval_from_env() -> Result<Duration> {
    let milliseconds = match env::var("OTEL_METRIC_EXPORT_INTERVAL") {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("invalid OTEL_METRIC_EXPORT_INTERVAL {value:?}"))?,
        Err(env::VarError::NotPresent) => DEFAULT_EXPORT_INTERVAL_MS,
        Err(error) => return Err(error).context("failed to read OTEL_METRIC_EXPORT_INTERVAL"),
    };
    if milliseconds < MIN_EXPORT_INTERVAL_MS {
        anyhow::bail!("OTEL_METRIC_EXPORT_INTERVAL must be at least {MIN_EXPORT_INTERVAL_MS} ms");
    }
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::{Key, Value};

    #[test]
    fn default_export_interval_is_five_seconds() {
        // Environment mutation is unsafe in Rust 2024, so test the constants
        // that define the no-configuration contract directly.
        assert_eq!(DEFAULT_EXPORT_INTERVAL_MS, 5_000);
        assert_eq!(MIN_EXPORT_INTERVAL_MS, 1_000);
    }

    #[test]
    fn resource_uses_bounded_service_and_region_attributes() {
        let resource = metrics_resource(
            "snaketron-server".to_string(),
            "0123456789abcdef".to_string(),
            "task-process-id".to_string(),
            "prod".to_string(),
            Some("euw1".to_string()),
            Some("eu-west-1".to_string()),
        );
        let expected = [
            ("service.name", "snaketron-server"),
            ("service.namespace", "snaketron"),
            ("service.version", "0123456789abcdef"),
            ("service.instance.id", "task-process-id"),
            ("deployment.environment.name", "prod"),
            ("snaketron.region", "euw1"),
            ("cloud.provider", "aws"),
            ("cloud.platform", "aws_ecs"),
            ("cloud.region", "eu-west-1"),
        ];

        for (key, value) in expected {
            assert_eq!(
                resource.get(&Key::new(key)),
                Some(Value::String(value.to_string().into())),
                "wrong OpenTelemetry resource attribute {key}"
            );
        }
    }
}
