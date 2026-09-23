# workshop-v4 — Confluent Technical Enablement Workshop (Bob-First Edition)

## Real-Time Streaming with Kafka, Flink, Terraform & AI Agents

This is the **v4 prompt-driven edition** of the Confluent enablement workshop.
Instead of copy-pasting complex CLI commands, participants orchestrate the entire lifecycle using **IBM Bob** through structured natural language prompts.

---

## What's new in v4 vs v3?

| Aspect | v3 | v4 |
|---|---|---|
| **Slide order** | Module 3 (Flink explain) appeared *after* Module 2 (streaming) — too late | **Flink explained before Terraform applies it** — learners understand what they're deploying |
| **RTCE context** | Module 1b appeared with no explanation | Added "what is RTCE and why it matters for Module 5" callout |
| **Module descriptions** | "What Bob does" only | Added "What you'll understand after this" to every module |
| **Flink SQL files** | Used PROCTIME() (not supported in Confluent Cloud) | Corrected to `$rowtime` throughout |
| **Producer schema** | Original schema caused BACKWARD compatibility errors | Schema aligned with Flink/Connect-registered format from day one |
| **verify.sh** | Checked for orchestrate CLI but gave wrong install command | Fixed to `pip install ibm-watsonx-orchestrate` |

---

## Workshop Slide Order (v4)

```
Slide 1   Title & overview
Slide 2   Concepts: Terraform, Kafka, Flink, Confluent (2-min primer)
Slide 3   Business scenario & end-to-end architecture
Slide 4   ★ NEW: Flink SQL deep-dive — understand the detection job BEFORE it deploys
Slide 5   Setup: Cloud accounts & API key creation
Slide 6   Setup: Verify environment with Bob (prereqs + venv)
Slide 7   Module 1: Terraform provisioning (now learners know what Terraform builds)
Slide 8   Module 1b: Enable RTCE in UI (with context: "this powers Module 5")
Slide 9   Module 2: Start event streaming (Flink is already running — feed it data)
Slide 10  Module 4: Configure Bob MCP to query live Kafka
Slide 11  Module 4b: Chat with your live stream
Slide 12  Module 5: Deploy watsonx Orchestrate autonomous risk agent
Slide 13  Teardown
```

---

## Quick Start

1. Open [`workshop-v4/index.html`](index.html) in your browser — it is the slide deck guide.
2. Open this `workshop-v4/` directory as your workspace in **IBM Bob**.
3. Follow each slide by copying the **🟣 Prompt IBM Bob** text into your Bob chat.

---

## Directory Layout

```
workshop-v4/
├── index.html                        ← Reordered prompt-driven slide guide (13 slides)
├── README.md                         ← This file
│
├── 00-prereqs/
│   └── verify.sh                     ← Dependency verification script
│
├── 01-terraform/
│   ├── providers.tf
│   ├── variables.tf
│   ├── main.tf                       ← Kafka + Flink + Schema Registry IaC (v4 fixes applied)
│   ├── outputs.tf
│   └── tfvars.example
│
├── 02-producer/
│   ├── produce_player_events.py      ← Schema aligned with Flink/Connect registered format
│   ├── generate_sample_data.py
│   ├── requirements.txt
│   └── .env.example
│
├── 03-flink/
│   ├── 01_create_player_events.sql   ← Uses $rowtime, corrected comments
│   ├── 02_create_player_risk_alerts.sql
│   └── 03_player_risk_detection_job.sql
│
├── 04-mcp-bob/
│   ├── config.yaml.example
│   └── bob-mcp-settings.example.json
│
└── 05-rtce-agent/
    ├── player_risk_agent.agent.yaml
    └── player_risk_toolkit/
        ├── player_risk_actions.py
        └── requirements.txt
```

---

## Key Technical Gotchas (learned from v3)

These are all documented in the source files but summarised here for facilitators:

1. **`DISTRIBUTED BY HASH(player_id)` not `DISTRIBUTED BY (player_id)`** — the latter creates a Kafka key column that breaks TUMBLE TVF column resolution.
2. **`$rowtime` not `PROCTIME()`** — `PROCTIME()` is not supported in Confluent Cloud's Flink SQL dialect.
3. **`GROUP BY window_start, window_end, player_id`** — window boundary columns must come first; other orderings fail.
4. **No `PRIMARY KEY` on `player_risk_alerts`** — PRIMARY KEY activates the upsert connector which returns `MT_UPSERT_NOT_SUPPORTED`.
5. **Producer schema must match the Flink/Connect-registered schema** — nullable `oneOf` fields, `player_id` as message key (not value field), no `required` array.
6. **Standard cluster tier required** — Basic clusters do not support RTCE.
