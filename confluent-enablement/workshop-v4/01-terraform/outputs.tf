output "kafka_bootstrap_servers" {
  description = "Kafka bootstrap endpoint (SASL_SSL:// prefix stripped)"
  value       = replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")
}

output "kafka_api_key" {
  description = "Kafka producer API key ID"
  value       = confluent_api_key.kafka_producer.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Kafka producer API key secret"
  value       = confluent_api_key.kafka_producer.secret
  sensitive   = true
}

output "schema_registry_url" {
  description = "Schema Registry REST endpoint"
  value       = data.confluent_schema_registry_cluster.main.rest_endpoint
}

output "schema_registry_api_key" {
  description = "Schema Registry API key ID"
  value       = confluent_api_key.schema_registry.id
  sensitive   = true
}

output "schema_registry_api_secret" {
  description = "Schema Registry API key secret"
  value       = confluent_api_key.schema_registry.secret
  sensitive   = true
}

output "environment_id" {
  description = "Confluent Cloud environment ID"
  value       = confluent_environment.main.id
}

output "cluster_id" {
  description = "Kafka cluster ID"
  value       = confluent_kafka_cluster.main.id
}

output "organization_id" {
  description = "Confluent Cloud organization ID"
  value       = data.confluent_organization.main.id
}

output "flink_rest_endpoint" {
  description = "Flink REST endpoint"
  value       = data.confluent_flink_region.main.rest_endpoint
}

output "flink_api_key" {
  description = "Flink API key ID"
  value       = confluent_api_key.flink.id
  sensitive   = true
}

output "flink_api_secret" {
  description = "Flink API key secret"
  value       = confluent_api_key.flink.secret
  sensitive   = true
}

output "flink_compute_pool_id" {
  description = "Flink compute pool ID"
  value       = confluent_flink_compute_pool.main.id
}

output "rtce_mcp_endpoint" {
  description = "RTCE MCP endpoint URL — use this in Module 5. NOTE: RTCE must be enabled manually in the Confluent Cloud UI first."
  value       = "https://mcp.${var.region}.${lower(var.cloud_provider)}.confluent.cloud/mcp/v1/context-engine/organizations/${data.confluent_organization.main.id}/environments/${confluent_environment.main.id}/kafka-clusters/${confluent_kafka_cluster.main.id}"
}
