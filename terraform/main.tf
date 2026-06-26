# ─────────────────────────────────────────────────────────────────────────────
# Data Sources
# ─────────────────────────────────────────────────────────────────────────────

data "confluent_organization" "main" {}

data "confluent_schema_registry_cluster" "main" {
  environment {
    id = confluent_environment.main.id
  }

  depends_on = [confluent_kafka_cluster.main]
}

data "confluent_flink_region" "main" {
  cloud  = var.cloud_provider
  region = var.region
}

# ─────────────────────────────────────────────────────────────────────────────
# Environment & Cluster
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_environment" "main" {
  display_name = var.environment_name

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "main" {
  display_name = var.cluster_name
  availability = "SINGLE_ZONE"
  cloud        = var.cloud_provider
  region       = var.region

  basic {}

  environment {
    id = confluent_environment.main.id
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Service Account
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_service_account" "app" {
  display_name = "fraud-detection-sa"
  description  = "Service account for the banking fraud detection pipeline"
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink Compute Pool
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_compute_pool" "main" {
  display_name = "fraud-detection-pool"
  cloud        = var.cloud_provider
  region       = var.region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.main.id
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# API Keys
# ─────────────────────────────────────────────────────────────────────────────

# Kafka producer API key → scoped to Kafka cluster
resource "confluent_api_key" "kafka_producer" {
  display_name = "fraud-detection-kafka-key"
  description  = "Kafka API key for the fraud detection producer"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.kafka_admin]
}

# Schema Registry API key → scoped to Schema Registry cluster
resource "confluent_api_key" "schema_registry" {
  display_name = "fraud-detection-sr-key"
  description  = "Schema Registry API key for the fraud detection pipeline"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.main.id
    api_version = data.confluent_schema_registry_cluster.main.api_version
    kind        = data.confluent_schema_registry_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.env_admin]
}

# Flink API key → scoped to Flink region (NOT the Kafka cluster)
resource "confluent_api_key" "flink" {
  display_name = "fraud-detection-flink-key"
  description  = "Flink API key for the fraud detection pipeline"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.flink_developer]
}

# ─────────────────────────────────────────────────────────────────────────────
# Role Bindings
# ─────────────────────────────────────────────────────────────────────────────

# CloudClusterAdmin → scoped to Kafka cluster (rbac_crn)
resource "confluent_role_binding" "kafka_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

# FlinkDeveloper → scoped to environment (resource_name)
resource "confluent_role_binding" "flink_developer" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.main.resource_name
}

# EnvironmentAdmin → scoped to environment (resource_name)
resource "confluent_role_binding" "env_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

# ─────────────────────────────────────────────────────────────────────────────
# RBAC Propagation Delay
# Allow 30 s for role bindings to propagate before Flink statements are submitted
# ─────────────────────────────────────────────────────────────────────────────

resource "time_sleep" "wait_for_rbac" {
  create_duration = "30s"

  depends_on = [
    confluent_role_binding.kafka_admin,
    confluent_role_binding.flink_developer,
    confluent_role_binding.env_admin,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink SQL — Source Table: banking_transactions
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "create_banking_transactions" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS banking_transactions (
      account_id       STRING,
      transaction_id   STRING,
      amount           DECIMAL(18, 2),
      transaction_type STRING,
      merchant_id      STRING,
      channel          STRING,
      transaction_time TIMESTAMP(3),
      WATERMARK FOR transaction_time AS transaction_time - INTERVAL '10' SECONDS
    ) DISTRIBUTED BY (account_id) INTO 4 BUCKETS
    WITH (
      'key.format'                          = 'json-registry',
      'value.format'                        = 'json-registry',
      'kafka.consumer.isolation-level'      = 'read-uncommitted'
    );
  SQL

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    time_sleep.wait_for_rbac,
    confluent_api_key.flink,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink SQL — Destination Table: fraud_alerts
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "create_fraud_alerts" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS fraud_alerts (
      account_id         STRING,
      window_start       TIMESTAMP(3),
      window_end         TIMESTAMP(3),
      transaction_count  BIGINT,
      total_amount       DECIMAL(18, 2),
      is_flagged         BOOLEAN,
      PRIMARY KEY (account_id, window_start) NOT ENFORCED
    ) WITH (
      'key.format'                          = 'json-registry',
      'value.format'                        = 'json-registry',
      'kafka.consumer.isolation-level'      = 'read-uncommitted'
    );
  SQL

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_flink_statement.create_banking_transactions,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink SQL — Aggregation Job: 5-minute windowed fraud detection
# Flags any account with more than 10 transactions in a 5-minute window.
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "fraud_detection_job" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-SQL
    INSERT INTO fraud_alerts
    SELECT
      account_id,
      window_start,
      window_end,
      COUNT(*)                  AS transaction_count,
      SUM(amount)               AS total_amount,
      COUNT(*) > 10             AS is_flagged
    FROM TABLE(
      TUMBLE(
        TABLE banking_transactions,
        DESCRIPTOR(transaction_time),
        INTERVAL '5' MINUTES
      )
    )
    GROUP BY account_id, window_start, window_end;
  SQL

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_flink_statement.create_fraud_alerts,
  ]
}
