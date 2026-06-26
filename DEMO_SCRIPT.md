# Fraud Detection Demo Script
### IBM Bob · Confluent Cloud · watsonx Orchestrate
**LIVE BUILD — Real-Time Fraud Detection Agent**

**Duration:** ~15 minutes  
**Audience:** Technical / business stakeholders

---

## Before You Start (Day-of Checklist)

- [ ] Confluent cluster `lkc-xqqxow1` is **running** (Standard tier)
- [ ] Flink job is **active**: Stream Processing → Statements shows a running statement
- [ ] `fraud_alerts` topic has **recent messages**: Topics → fraud_alerts → Messages
- [ ] watsonx Orchestrate open at `ptsenv` — `fraud_detection_agent` is deployed
- [ ] **Bob** (IBM AI Dev Platform) open and ready
- [ ] 4 browser tabs pre-loaded (see below)

---

## Browser Tab Setup

| Tab | Location | What it shows |
|-----|----------|---------------|
| **Tab 1** | IBM Bob | Where the environment was scaffolded from plain English |
| **Tab 2** | Confluent UI → `fraud_alerts` → **Messages** | Live records arriving from Flink |
| **Tab 3** | Confluent UI → **Stream Processing → Statements** | Running Flink job |
| **Tab 4** | watsonx Orchestrate chat — `fraud_detection_agent` | Where you run the live transaction |

---

## Slide 1 — Architecture (1 min)

**Show slide: "How Bob, Confluent Cloud & Orchestrate Work Together"**

Walk through left to right:

> *"IBM Bob is the AI development platform — it generated the Terraform, built the agent, handled the integration. Confluent Cloud is the streaming layer — real-time transaction events, no batch delay, no stale data. watsonx Orchestrate is the agent control plane — it receives the live event, scores the risk, and acts autonomously or escalates to a human."*

> *"Bob provisions the infrastructure, Bob deploys the agent, and live events flow from Confluent into Orchestrate in real time. Let me show you exactly how that works."*

---

## Slide 2 — The 3-Step Live Build

**Show slide: "Building a Real-Time Fraud Detection Agent"**

> *"Three steps. Bob prompts — the Confluent environment is scaffolded from a plain English description. Agent built — Orchestrate agent deployed, listening, scoring risk, deciding. Live transaction — a \$47K wire fires, the agent responds with a governed decision. Let's walk through all three."*

---

## Step 1 — Bob Prompts (3 min)
*"Confluent environment scaffolded from plain English description"*

**Switch to Tab 1 (Bob)**

> *"This is IBM Bob — an AI coding assistant built into the developer environment. Instead of writing Terraform by hand, I described what I needed in plain English."*

Show (or narrate) the Bob prompt that generated the infrastructure:

> *"I said: 'Build a real-time fraud detection pipeline on Confluent Cloud — Kafka topic for banking transactions, Flink job that aggregates into 5-minute windows, flags accounts with more than 10 transactions, outputs to a fraud_alerts topic.' Bob generated the complete Terraform configuration, the Flink SQL, and the Python producer."*

**Switch to Tab 3 (Flink Statements)**

> *"Here's what that produced — a live Flink SQL job running on Confluent Cloud right now. 5-minute tumbling windows, aggregating every transaction, flagging anomalies."*

**Switch to Tab 2 (fraud_alerts Messages)**

> *"And here's the output — `fraud_alerts`, live records arriving every few seconds. Each one is a windowed summary: account ID, transaction count, total amount, flagged or not."*

---

## Step 2 — Agent Built (2 min)
*"Orchestrate agent deployed — listens, scores risk, decides"*

**Switch to Tab 4 (Orchestrate)**

> *"Bob also generated and deployed the watsonx Orchestrate agent. It's connected directly to this Kafka stream via Confluent's Real-time Context Engine — a native MCP endpoint. The agent can query the live topic like a database, no Kafka client code, no ETL."*

> *"The agent has three decision tiers built in: high confidence fraud — freeze the account, send an SMS, escalate to a human analyst. Elevated risk — flag and email. Low risk — monitor only. All governed with a mandatory human-in-the-loop before any freeze action."*

---

## Step 3 — Live Transaction (10 min)
*"A \$47K wire fires — agent responds with a governed decision"*

### 3a — Prove the data is live (do this first — 2 min)

> *"Before we investigate any account, let me show you this is actually reading from the live Kafka stream — not a mock or a cached response."*

**Type in Tab 4:**
```
List all accounts currently flagged in the fraud_alerts stream and show me the most recent window for each
```

**Expand "Show Reasoning"** so the audience sees the `listTopics` → `getMetadata` → `queryData` calls and the raw JSON coming back from Confluent.

> *"You can see the agent calling the RTCE endpoint in real time — that's a live SQL query against the Kafka topic. The timestamps in those results are from the last few minutes."*

**Then type:**
```
What is the most recent fraud alert in the stream right now — show me the exact window_start and window_end timestamps
```

**While it responds** — switch to Tab 2 (Confluent Messages) and point to matching timestamps.

> *"Same timestamps. Same data. The agent is reading directly from the stream."*

---

### 3b — High confidence fraud (5 min)

**Type in Tab 4:**
```
Investigate account ACCOUNT-FRAUD-01 for fraud
```

**While agent is reasoning** — switch to Tab 2 and point to `ACCOUNT-FRAUD-01` records scrolling in.

**Expected response:**
- Risk Level: **HIGH**
- Transactions: 40–60 in 5-min window
- Total Amount: $47K–$120K
- Flagged by Flink: **Yes**
- Actions: `freeze_account` + `send_customer_alert` (SMS) + `escalate_to_human` (CRITICAL)
- Case ID: `CASE-XXXXXXXX`

> *"The agent queried the live stream, saw 50+ transactions totalling over \$47K flagged by Flink, and in one response: froze the account, sent an SMS to the customer, and opened a CRITICAL case for human review. Full audit trail, all autonomous."*

---

### 3c — Elevated risk (shows the tiered decision logic)

**Type:**
```
Run a fraud check on ACC-001-SUSPECT
```

**Expected response:**
- Risk Level: **ELEVATED**
- Transactions: ~12, is_flagged: Yes
- Actions: `flag_account` + `send_customer_alert` (EMAIL) + `escalate_to_human` (HIGH)
- **No freeze** — signal elevated but not conclusive

> *"Different account, different signal strength — the agent applied a different rule. Flagged, emailed, HIGH priority escalation. No freeze because it hasn't crossed the threshold. The governance logic is in the agent, not the application."*

---

### 3d — Clean account (completes the story)

**Type:**
```
What is the fraud status of ACC-002-NORMAL?
```

**Expected response:**
- Risk Level: **LOW**
- Transactions: 3, is_flagged: No
- Action: precautionary monitoring only
- No freeze, no alert

> *"And a clean account — precautionary flag, no action taken. The agent only escalates when the signal is real."*

---

## Closing (1 min)

**Return to slide 1**

> *"What you just saw: Bob described the infrastructure in plain English — Confluent provisioned, Flink running, agent deployed in minutes. The agent reads live Kafka data natively through Confluent's MCP endpoint. Every decision is governed — tiered logic, mandatory human escalation, full audit trail. This is what AI-native real-time automation looks like."*

---

## Backup Prompts

If ACCOUNT-FRAUD-01 is slow, use any of these — all have rich fraud data:
```
Investigate account ACCOUNT-FRAUD-03 for fraud
Investigate account ACCOUNT-FRAUD-04 for fraud
Investigate account ACCOUNT-FRAUD-05 for fraud
```

If RTCE times out, warm it up first:
```
List the available topics in the fraud detection stream
```
Then retry the investigation prompt.

---

## Q&A Cheat Sheet

| Question | Answer |
|----------|--------|
| *"Is this real data?"* | Yes — live Kafka topic, Flink job running right now, 7,000+ transactions produced today. |
| *"How does the agent read Kafka?"* | Confluent's Real-time Context Engine — a native MCP endpoint. The agent queries the live topic like SQL. |
| *"What stops the agent doing the wrong thing?"* | Tiered decision logic + mandatory human escalation before any freeze. Governance is in the agent instructions. |
| *"How long did Bob take to build this?"* | Terraform + Flink + agent wiring in one session. The infra deploys in ~5 minutes. |
| *"Can this work with our Kafka topics?"* | Any Confluent Cloud Standard+ topic with a schema can be exposed via RTCE. |
| *"What's the latency?"* | Flink windows close every 5 minutes. The agent queries in real time — seconds from question to decision. |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| AI Dev Platform | IBM Bob |
| Streaming | Confluent Cloud Standard (`lkc-xqqxow1`, AWS us-east-1) |
| Stream Processing | Apache Flink SQL (5-min tumbling window) |
| Topics | `banking_transactions` → `fraud_alerts` (6 partitions) |
| Agent → Kafka | Confluent RTCE (MCP streamable HTTP) |
| Agent Runtime | IBM watsonx Orchestrate (`ptsenv`) |
| LLM | `watsonx/meta-llama/llama-3-3-70b-instruct` |
| IaC | Terraform (`confluent` provider ≥ 2.68.0) |

---

*confluent-demo-1 · fraud_detection_agent · ptsenv*
