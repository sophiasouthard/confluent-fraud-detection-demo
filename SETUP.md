# Setup Guide — Banking Fraud Detection

## Prerequisites

| Tool | Minimum Version | Notes |
|------|----------------|-------|
| Terraform | 1.0+ | `brew install terraform` |
| Python | 3.9+ | `brew install python` |
| Confluent Cloud account | — | [confluent.io](https://confluent.io) |
| Confluent Cloud API Key | Cloud-level | See below |

### Create a Confluent Cloud API Key

1. Log in to [Confluent Cloud](https://confluent.cloud)
2. Go to **Menu → Cloud API keys → Add key → Global access**
3. Copy the **Key** and **Secret** — you'll need both in step 2

---

## Step 1 — Clone & configure Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and fill in your credentials:

```hcl
api_key    = "YOUR_CONFLUENT_CLOUD_API_KEY"
api_secret = "YOUR_CONFLUENT_CLOUD_API_SECRET"

environment_name = "banking-fraud-env"   # change if desired
cluster_name     = "fraud-cluster"       # change if desired
region           = "us-east-1"           # must match a supported Flink region
cloud_provider   = "AWS"                 # AWS | GCP | AZURE
flink_max_cfu    = 5
```

> **Supported Flink regions:** check the
> [Confluent Flink region list](https://docs.confluent.io/cloud/current/flink/index.html#flink-sql-on-confluent-cloud)
> for valid `region` / `cloud_provider` combinations.

---

## Step 2 — Deploy infrastructure

```bash
cd terraform
terraform init
terraform validate
terraform plan   # review what will be created
terraform apply  # type "yes" to confirm
```

Terraform will:
- Create a Confluent Cloud environment and Kafka cluster
- Provision a Flink compute pool
- Create a service account with correct RBAC role bindings
- Issue three API key pairs (Kafka, Schema Registry, Flink)
- Submit three Flink SQL statements (source table, destination table, aggregation job)
- Auto-generate `python/.env` with all connection details

> **Duration:** ~3–5 minutes for the full apply including RBAC propagation delay.

---

## Step 3 — Verify Flink statements

In the Confluent Cloud UI:
1. Navigate to your environment → **Flink** → **Statements**
2. Confirm all three statements show status **Running** or **Completed**:
   - `create_banking_transactions` — source table
   - `create_fraud_alerts` — destination table
   - `fraud_detection_job` — continuous aggregation (stays **Running**)

---

## Step 4 — Install Python dependencies

```bash
cd python
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Step 5 — Generate window-aligned sample data

```bash
cd python
python generate_sample_data.py
```

This creates `python/sample-transactions.json` with timestamps aligned to the
next 5-minute window boundary. Expected output:

```
📅 Window start : HH:MM:10
📅 Window end   : HH:MM:10 (+ 5 min)
📅 Watermark    : HH:MM:10 (+ 6 min)

✅ Generated 26 transactions → python/sample-transactions.json

  ACC-001-SUSPECT:    12 transactions  🚨 WILL BE FLAGGED (>10)
  ACC-002-NORMAL:      3 transactions  ✅ Safe
  ACC-003-BORDERLINE: 10 transactions  ⚠️  AT THRESHOLD (=10)
```

> **Re-run this step** each time you test — timestamps must be in a future window.

---

## Step 6 — Produce transactions

```bash
cd python
python produce_messages.py
```

Expected output:
```
🔍 Fetching schemas from Schema Registry …
✅ Schemas retrieved successfully

📤 Producing 26 transactions to topic 'banking_transactions' …

✅ Delivered  partition=0  offset=0
✅ Delivered  partition=1  offset=0
…
─────────────────────────────────────────────────────
📊 Summary: 26 delivered, 0 failed
```

---

## Step 7 — Verify results

Wait ~30 seconds after the last event for the 5-minute window to close, then
run the verification queries in [`TESTING-APPROACH.md`](TESTING-APPROACH.md).

Use the **Confluent Cloud Flink SQL console** (or the Confluent CLI) to run them.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `terraform apply` fails with "region not supported" | Invalid region/cloud combo | Check supported Flink regions |
| `Could not retrieve schemas` | Flink hasn't registered schemas yet | Wait for Flink statements to reach **Running** status |
| `fraud_detection_job` stuck in **Pending** | Compute pool still provisioning | Wait 2–3 minutes and refresh |
| No rows in `fraud_alerts` | Window hasn't closed yet | Wait 30 s after last event, or check watermark event was delivered |
| Wrong counts | Events span two window boundaries | Re-run `generate_sample_data.py` to realign timestamps |
| `.env` file missing | Terraform apply failed or partial | Run `terraform apply` again or manually copy `.env.example` |

---

## Cleanup

To destroy all provisioned resources (avoids ongoing Confluent Cloud costs):

```bash
cd terraform
terraform destroy
```

> This also removes the Flink compute pool and all Kafka topics.
