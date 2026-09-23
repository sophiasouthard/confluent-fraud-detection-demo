terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = ">= 2.10.0"
    }
    local = {
      source  = "hashicorp/local"
      version = ">= 2.0.0"
    }
    time = {
      source  = "hashicorp/time"
      version = ">= 0.9.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.api_key
  cloud_api_secret = var.api_secret
}
