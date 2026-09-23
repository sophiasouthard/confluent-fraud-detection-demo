# ─────────────────────────────────────────────────────────────────────────────
# workshop-v4 — Confluent Cloud Infrastructure
#
# What this creates:
#   • Confluent environment (ESSENTIALS Stream Governance for Schema Registry + RTCE)
#   • Kafka cluster (Standard tier — REQUIRED for RTCE)
#   • Service account + 3 role bindings (CloudClusterAdmin, FlinkDeveloper, EnvironmentAdmin)
#   • 30s time_sleep for RBAC propagation (prevents Flink permission errors on first apply)
#   • 3 API key pairs: Kafka producer, Schema Registry, Flink
#   • Flink compute pool (5 CFU, same region as cluster)
#   • 2 Kafka topics: player_events (4 partitions), player_risk_alerts (4 partitions)
#   • Flink SQL: DROP + CREATE player_events table
#   • Flink SQL: DROP + CREATE player_risk_alerts table (no PRIMARY KEY)
#   • Flink SQL: INSERT INTO risk aggregation job (TUMBLE on $rowtime, 1-min window)
#   • local_file: auto-writes 02-producer/.env with all credentials
#
# Key design decisions (learned from real failures in v3):
#   - DISTRIBUTED BY HASH(player_id) NOT DISTRIBUTED BY (player_id)
#     → HASH keeps player_id in value payload; DISTRIBUTED BY makes it a Kafka key
#       requiring key.format, which breaks the TUMBLE TVF column resolution.
#   - 'value.format' = 'json-registry' ONLY — no key.format declaration.
#   - PRIMARY KEY (player_id) NOT ENFORCED on player_events
#     → Required so Flink's TUMBLE TVF validator can resolve player_id in GROUP BY output.
#   - NO PRIMARY KEY on player_risk_alerts → append-only mode; avoids MT_UPSERT_NOT_SUPPORTED.
#   - DESCRIPTOR($rowtime) for TUMBLE → Confluent's built-in row-time attribute.
#     PROCTIME() is not supported in Confluent Cloud's Flink SQL dialect.
#   - GROUP BY window_start, window_end, player_id → window boundary columns FIRST.
#     Confluent Flink rejects other orderings.
#   - CAST(window_start AS TIMESTAMP(3)) in SELECT → matches sink table column type.
#
# RTCE: Must be enabled manually in the Confluent Cloud UI after apply.
#       See index.html Module 1b slide for steps.
# ─────────────────────────────────────────────────────────────────────────────

# ── Data Sources ──────────────────────────────────────────────────────────────

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

# ── Environment & Cluster ─────────────────────────────────────────────────────

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

  # Standard tier is REQUIRED for RTCE — do not downgrade to basic {}
  standard {}

  environment {
    id = confluent_environment.main.id
  }
}

# ── Service Account & Role Bindings ───────────────────────────────────────────

resource "confluent_service_account" "app" {
  display_name = "${var.environment_name}-sa"
  description  = "Service account for the player risk workshop pipeline"
}

resource "confluent_role_binding" "kafka_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

resource "confluent_role_binding" "flink_developer" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_role_binding" "env_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

# 30-second wait for RBAC to propagate before Flink statements are submitted.
# Without this, Flink statements fail with permissions errors on first apply.
resource "time_sleep" "wait_for_rbac" {
  create_duration = "30s"
  depends_on = [
    confluent_role_binding.kafka_admin,
    confluent_role_binding.flink_developer,
    confluent_role_binding.env_admin,
  ]
}

# ── API Keys ──────────────────────────────────────────────────────────────────

resource "confluent_api_key" "kafka_producer" {
  display_name = "${var.environment_name}-kafka-key"
  description  = "Kafka API key for the player events producer"

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

resource "confluent_api_key" "schema_registry" {
  display_name = "${var.environment_name}-sr-key"
  description  = "Schema Registry API key"

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

resource "confluent_api_key" "flink" {
  display_name = "${var.environment_name}-flink-key"
  description  = "Flink API key for the player risk pipeline"

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

# ── Flink Compute Pool ────────────────────────────────────────────────────────

resource "confluent_flink_compute_pool" "main" {
  display_name = "${var.environment_name}-pool"
  cloud        = var.cloud_provider
  region       = var.region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.main.id
  }
}

# ── Kafka Topics ──────────────────────────────────────────────────────────────

resource "confluent_kafka_topic" "player_events" {
  topic_name       = "player_events"
  partitions_count = 4

  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka_producer.id
    secret = confluent_api_key.kafka_producer.secret
  }

  depends_on = [confluent_api_key.kafka_producer]
}

resource "confluent_kafka_topic" "player_risk_alerts" {
  topic_name       = "player_risk_alerts"
  partitions_count = 4

  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.kafka_producer.id
    secret = confluent_api_key.kafka_producer.secret
  }

  depends_on = [confluent_api_key.kafka_producer]
}

# ── Flink SQL Statements ──────────────────────────────────────────────────────
#
# Pattern: DROP first, then CREATE IF NOT EXISTS.
# Reason: CREATE TABLE IF NOT EXISTS silently skips if a table already exists,
# keeping stale column definitions. DROP + CREATE is the only reliable approach.

resource "confluent_flink_statement" "drop_player_events" {
  organization { id = data.confluent_organization.main.id }
  environment  { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal    { id = confluent_service_account.app.id }

  statement = "DROP TABLE IF EXISTS player_events;"

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
    confluent_kafka_topic.player_events,
  ]
}

resource "confluent_flink_statement" "drop_player_risk_alerts" {
  organization { id = data.confluent_organization.main.id }
  environment  { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal    { id = confluent_service_account.app.id }

  statement = "DROP TABLE IF EXISTS player_risk_alerts;"

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
    confluent_kafka_topic.player_risk_alerts,
  ]
}

resource "confluent_flink_statement" "create_player_events" {
  organization { id = data.confluent_organization.main.id }
  environment  { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal    { id = confluent_service_account.app.id }

  # Design notes:
  #   DISTRIBUTED BY HASH(player_id)  — value-only partitioning; keeps player_id as a queryable column
  #   PRIMARY KEY (player_id) NOT ENFORCED  — required for Flink TUMBLE TVF to resolve player_id in GROUP BY
  #   'value.format' = 'json-registry'  — Schema Registry JSON; no key.format
  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS player_events (
      player_id   STRING,
      session_id  STRING,
      bet_id      STRING,
      amount      DECIMAL(18, 2),
      game_type   STRING,
      channel     STRING,
      device_id   STRING,
      event_time  STRING,
      PRIMARY KEY (player_id) NOT ENFORCED
    ) DISTRIBUTED BY HASH(player_id) INTO 4 BUCKETS
    WITH (
      'value.format' = 'json-registry'
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
    confluent_flink_statement.drop_player_events,
    confluent_kafka_topic.player_events,
    confluent_api_key.flink,
  ]
}

resource "confluent_flink_statement" "create_player_risk_alerts" {
  organization { id = data.confluent_organization.main.id }
  environment  { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal    { id = confluent_service_account.app.id }

  # Design notes:
  #   NO PRIMARY KEY  — append-only mode; PRIMARY KEY triggers MT_UPSERT_NOT_SUPPORTED
  #   window_start/end as TIMESTAMP(3)  — matches CAST in INSERT statement below
  #   DISTRIBUTED BY HASH(player_id)  — value-only partitioning
  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS player_risk_alerts (
      player_id      STRING,
      window_start   TIMESTAMP(3),
      window_end     TIMESTAMP(3),
      bet_count      BIGINT,
      total_wagered  DECIMAL(18, 2),
      avg_bet        DECIMAL(18, 2),
      is_flagged     BOOLEAN
    ) DISTRIBUTED BY HASH(player_id) INTO 4 BUCKETS
    WITH (
      'value.format' = 'json-registry'
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
    confluent_flink_statement.drop_player_risk_alerts,
    confluent_flink_statement.create_player_events,
    confluent_kafka_topic.player_risk_alerts,
  ]
}

resource "confluent_flink_statement" "player_risk_detection_job" {
  organization { id = data.confluent_organization.main.id }
  environment  { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal    { id = confluent_service_account.app.id }

  # Design notes:
  #   DESCRIPTOR($rowtime)  — Confluent's built-in row-time attribute; PROCTIME() is not supported
  #   GROUP BY window_start, window_end, player_id  — window boundaries MUST come first
  #   CAST(window_start AS TIMESTAMP(3))  — aligns with sink table column type
  statement = <<-SQL
    INSERT INTO player_risk_alerts
    SELECT
      player_id,
      CAST(window_start AS TIMESTAMP(3))      AS window_start,
      CAST(window_end   AS TIMESTAMP(3))      AS window_end,
      COUNT(*)                                AS bet_count,
      SUM(amount)                             AS total_wagered,
      AVG(amount)                             AS avg_bet,
      (COUNT(*) > 20 OR SUM(amount) > 10000)  AS is_flagged
    FROM TUMBLE(
      TABLE player_events,
      DESCRIPTOR($rowtime),
      INTERVAL '1' MINUTE
    )
    GROUP BY window_start, window_end, player_id;
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

  depends_on = [confluent_flink_statement.create_player_risk_alerts]
}

# ── Auto-generate producer .env ───────────────────────────────────────────────
# Writes all credentials to 02-producer/.env after apply so Module 2 works
# without any manual credential copying.

resource "local_file" "producer_env" {
  filename = "${path.module}/../02-producer/.env"
  content  = <<-EOT
KAFKA_BOOTSTRAP_SERVERS=${replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")}
KAFKA_API_KEY=${confluent_api_key.kafka_producer.id}
KAFKA_API_SECRET=${confluent_api_key.kafka_producer.secret}
SCHEMA_REGISTRY_URL=${data.confluent_schema_registry_cluster.main.rest_endpoint}
SCHEMA_REGISTRY_API_KEY=${confluent_api_key.schema_registry.id}
SCHEMA_REGISTRY_API_SECRET=${confluent_api_key.schema_registry.secret}
FLINK_REST_ENDPOINT=${data.confluent_flink_region.main.rest_endpoint}
FLINK_API_KEY=${confluent_api_key.flink.id}
FLINK_API_SECRET=${confluent_api_key.flink.secret}
FLINK_COMPUTE_POOL_ID=${confluent_flink_compute_pool.main.id}
EOT
}
