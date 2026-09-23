variable "api_key" {
  description = "Confluent Cloud API Key (Cloud-level, Global access)"
  type        = string
  sensitive   = true
}

variable "api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "environment_name" {
  description = "Name for your Confluent Cloud environment — make it unique (e.g. workshop-yourname)"
  type        = string
  default     = "workshop-lab"
}

variable "cluster_name" {
  description = "Name for the Kafka cluster"
  type        = string
  default     = "player-risk-cluster"
}

variable "region" {
  description = "AWS region — must support Confluent Flink (us-east-1 recommended)"
  type        = string
  default     = "us-east-1"
}

variable "cloud_provider" {
  description = "Cloud provider: AWS | GCP | AZURE"
  type        = string
  default     = "AWS"
}

variable "flink_max_cfu" {
  description = "Maximum Confluent Flink Units for the compute pool"
  type        = number
  default     = 5
}
