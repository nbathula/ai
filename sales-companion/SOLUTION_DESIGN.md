# Sales Companion — Solution Design

**Platform**: Databricks + AWS  
**Version**: 1.0  
**Status**: Draft  

---

## 1. Executive Summary

Sales Companion is an AI-powered assistant embedded in the sales workflow, enabling sales reps, managers, and revenue leaders to query pipeline health, customer health, revenue metrics (ACV, ARR), and contract details using natural language.

It combines two retrieval patterns:
- **Text-to-SQL** for structured metrics (Snowflake → Delta Lake)
- **Vector Search RAG** for unstructured documents (contracts, proposals, call transcripts)

Real-time Salesforce events flow through **AWS MSK (Kafka)** into **Delta Live Tables**, keeping pipeline data fresh without batch lag.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│   Salesforce (CRM)    Snowflake (Metrics)    Databricks Volumes     │
│   Accounts · Opps     ACV · ARR · NRR        Contracts · Proposals  │
│   Contacts · Tasks    Customer Health         Call Transcripts       │
└────────┬──────────────────────┬───────────────────────┬─────────────┘
         │                      │                       │
         ▼ Real-time            ▼ Batch                 ▼ Auto Loader
┌────────────────┐   ┌──────────────────┐   ┌─────────────────────────┐
│  AWS MSK       │   │  Snowflake       │   │  Parse + Chunk + Embed  │
│  (Kafka)       │   │  Connector /     │   │  (Databricks Workflows) │
│  SF Platform   │   │  JDBC Sync       │   │                         │
│  Events / CDC  │   │                  │   │                         │
└────────┬───────┘   └────────┬─────────┘   └──────────┬──────────────┘
         │                    │                         │
         ▼                    ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG / DELTA LAKE                       │
│                                                                     │
│  sales.realtime.*          sales.metrics.*     sales.documents.*    │
│  pipeline_events           arr_snapshot        chunks (vector)      │
│  opportunity_changes       acv_by_segment      contracts            │
│  health_score_updates      customer_health     proposals            │
│                            forecast                                 │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────┐
│                     HYBRID RETRIEVAL LAYER                          │
│                                                                     │
│      Text-to-SQL                      Vector Search                 │
│   (Structured metrics)           (Unstructured documents)           │
│   Databricks SQL                  Databricks Vector Search          │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────┐
│                      AGENT ORCHESTRATION                            │
│                    (LangGraph on Databricks)                        │
│                                                                     │
│  Router → Pipeline  Customer   Revenue    Contract   Forecast       │
│           Agent     Health     Metrics    Review     Agent          │
│                     Agent      Agent      Agent                     │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────┐
│                    DATABRICKS MODEL SERVING                         │
│           Claude 3.5 Sonnet (LLM) + BGE-large (Embeddings)         │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────┐
│                         FRONTEND                                    │
│              Sales Companion Web App (React)                        │
│         + Salesforce Lightning Component (embedded)                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Sources & Ingestion

### 3.1 Salesforce (CRM) — Real-time + Batch

**Real-time path (Kafka):**
- Enable Salesforce Change Data Capture (CDC) on: Opportunity, Account, Contact, Task, Contract
- CDC events published to **AWS MSK** (Managed Kafka) via MuleSoft / Salesforce Platform Events connector
- **Delta Live Tables** consume Kafka topic using Structured Streaming
- Landing zone: `sales.realtime.*` Delta tables, updated within seconds

**Batch path (fallback + historical):**
- Databricks Auto Loader pulls from Salesforce REST API daily
- Full historical sync for reporting and model training
- Landing zone: `sales.crm.*`

**Key Salesforce objects:**

| Object | Fields | Use |
|---|---|---|
| Opportunity | Amount, Stage, CloseDate, Probability, OwnerId | Pipeline health, forecast |
| Account | Name, Industry, ARR, HealthScore, CSM | Customer health |
| Contract | StartDate, EndDate, Value, Status | Contract review |
| Activity/Task | Subject, Description, ActivityDate | Engagement signals |
| Product | Name, ACV, MRR | Revenue metrics |

---

### 3.2 Snowflake (Structured Metrics) — Batch Sync

- Connect via **Databricks Snowflake Connector** (native)
- Scheduled sync every 4 hours into Delta Lake
- Snowflake stays as system of record; Delta Lake is the query layer for Sales Companion

**Key Snowflake tables synced:**

| Table | Metrics | Target Delta Table |
|---|---|---|
| `revenue.arr_snapshot` | ARR, MRR, churn ARR, expansion ARR | `sales.metrics.arr` |
| `revenue.acv_by_deal` | ACV, TCV per opportunity | `sales.metrics.acv` |
| `customers.health_scores` | Health score, NPS, CSAT, risk tier | `sales.metrics.health` |
| `finance.forecast` | Committed, best case, pipeline | `sales.metrics.forecast` |
| `customers.usage_metrics` | DAU, feature adoption, login frequency | `sales.metrics.usage` |

---

### 3.3 Databricks Volumes (Unstructured) — Auto Loader

Documents land in Unity Catalog Volumes and are processed by an ingestion pipeline:

| Document Type | Source | Volume Path |
|---|---|---|
| Contracts | Salesforce Files / manual upload | `/volumes/sales/docs/contracts/` |
| Proposals | Google Drive sync | `/volumes/sales/docs/proposals/` |
| Call transcripts | Gong / Zoom export | `/volumes/sales/docs/transcripts/` |
| Email threads | Export from inbox | `/volumes/sales/docs/emails/` |
| Competitive intel | Manual upload | `/volumes/sales/docs/competitive/` |

**Processing pipeline (Databricks Workflows):**
```
New file in Volume
    ↓
Parse (PDF → text, DOCX → text, MP3 → Whisper transcription)
    ↓
Classify (contract / proposal / transcript / email / competitive)
    ↓
Extract metadata (account_id, opportunity_id, date, owner)
    ↓
Chunk (512 tokens, 50-token overlap)
    ↓
Embed (BGE-large-en-v1.5 via Databricks Model Serving)
    ↓
Write to Delta table + sync to Vector Search index
```

---

## 4. Real-Time Streaming (Kafka / AWS MSK)

### Architecture

```
Salesforce                 AWS MSK                  Databricks
──────────                 ────────                 ──────────
Platform Events    →    sf.opportunity.changes  →   Delta Live Tables
Change Data CDC    →    sf.account.changes      →   sales.realtime.*
                   →    sf.contract.changes     →
                   →    sf.health.updates       →
```

### Kafka Topics

| Topic | Source | Consumer | Latency |
|---|---|---|---|
| `sf.opportunity.changes` | Salesforce Opportunity CDC | Delta Live Tables | < 30s |
| `sf.account.changes` | Salesforce Account CDC | Delta Live Tables | < 30s |
| `sf.health.score.updates` | Customer health service | Delta Live Tables | < 30s |
| `sf.contract.events` | Salesforce Contract CDC | Delta Live Tables | < 30s |

### Delta Live Tables (Streaming)

```python
import dlt
from pyspark.sql.functions import from_json, col

@dlt.table(name="pipeline_events")
def pipeline_events():
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", MSK_BOOTSTRAP)
            .option("subscribe", "sf.opportunity.changes")
            .load()
            .select(from_json(col("value").cast("string"), opportunity_schema).alias("data"))
            .select("data.*")
    )

@dlt.table(name="pipeline_current")
@dlt.expect("valid_amount", "Amount > 0")
def pipeline_current():
    return dlt.read_stream("pipeline_events").groupBy("OpportunityId").latest()
```

### Why Kafka Here

- Salesforce API has rate limits — Kafka decouples ingestion from consumption
- Pipeline health queries need data fresher than 4-hour batch cycles
- Stage change events trigger immediate re-calculation of forecast
- Health score degradations surface in Sales Companion within 30 seconds

---

## 5. Storage Layer (Unity Catalog / Delta Lake)

### Catalog Structure

```
unity_catalog/
└── sales/
    ├── realtime/          ← Kafka-fed, near real-time
    │   ├── pipeline_events
    │   ├── opportunity_current
    │   └── health_score_updates
    ├── crm/               ← Salesforce batch sync
    │   ├── opportunities
    │   ├── accounts
    │   ├── contacts
    │   └── contracts
    ├── metrics/           ← Snowflake sync
    │   ├── arr
    │   ├── acv
    │   ├── health
    │   ├── forecast
    │   └── usage
    └── documents/         ← Vector Search
        ├── chunks          (text chunks + embeddings)
        ├── metadata        (doc-level metadata)
        └── vector_index    (Databricks Vector Search)
```

### Key Delta Table: `sales.realtime.opportunity_current`

```sql
CREATE TABLE sales.realtime.opportunity_current (
  opportunity_id    STRING,
  account_id        STRING,
  owner_id          STRING,
  name              STRING,
  amount            DECIMAL(18,2),
  acv               DECIMAL(18,2),
  stage             STRING,
  close_date        DATE,
  probability       INTEGER,
  forecast_category STRING,  -- Commit / Best Case / Pipeline / Omit
  last_activity     TIMESTAMP,
  days_in_stage     INTEGER,
  health_score      INTEGER,
  updated_at        TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

---

## 6. Hybrid Retrieval Layer

This is the core of Sales Companion — questions can be about numbers OR documents, and the agent must route correctly.

### Query Classification

```
User: "What is Acme Corp's current ARR and when does their contract expire?"
         ↓
Router classifies:
  → ARR question        → Text-to-SQL  → sales.metrics.arr
  → Contract question   → Vector Search → sales.documents.chunks
         ↓
Merge both results → Claude generates unified answer
```

### Path 1 — Text-to-SQL (Structured Metrics)

```python
# Agent generates SQL from natural language
sql_agent_prompt = """
You are a SQL expert for a B2B SaaS sales team.
Tables available:
  - sales.realtime.opportunity_current  (live pipeline)
  - sales.metrics.arr                   (ARR by account)
  - sales.metrics.acv                   (ACV by deal)
  - sales.metrics.health                (customer health scores)
  - sales.metrics.forecast              (sales forecast)
  - sales.crm.accounts                  (account details)

Generate only valid Databricks SQL.
Always include account_name in results for context.
Never return more than 100 rows without a LIMIT.
"""

# Example generated SQL
"""
SELECT a.name, m.arr, m.arr_growth_pct, m.risk_tier
FROM sales.metrics.arr m
JOIN sales.crm.accounts a ON m.account_id = a.id
WHERE m.risk_tier = 'High'
ORDER BY m.arr DESC
LIMIT 20
"""
```

### Path 2 — Vector Search (Unstructured Documents)

```python
from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()
index = client.get_index("sales_vs_endpoint", "sales.documents.vector_index")

results = index.similarity_search(
    query_text="Acme Corp contract renewal terms and pricing",
    columns=["chunk_id", "content", "doc_type", "account_id", "doc_date"],
    filters={"account_id": "acme-corp-001"},  # scope to account
    num_results=5,
)
```

### Path 3 — Merged Response

When both paths are needed, results are combined and passed to Claude:
```
Context (structured):
  Acme Corp ARR: $240,000 | Health Score: 62 (At Risk) | Renewal: 2026-09-30

Context (unstructured):
  [Contract excerpt]: "...renewal pricing at 5% uplift unless..."
  [Call transcript]: "...CFO mentioned budget freeze in Q3..."

Question: What is Acme's renewal risk?

→ Claude synthesizes both into a coherent answer with citations
```

---

## 7. Agent Design (LangGraph)

### Agent Router

```
User Query
    ↓
Intent Classifier
    ├── "pipeline health / forecast"     → Pipeline Agent
    ├── "customer health / risk"         → Customer Health Agent
    ├── "ACV / ARR / revenue metrics"   → Revenue Metrics Agent
    ├── "contract / proposal / document" → Document Agent
    ├── "rep performance / activity"     → Sales Performance Agent
    └── "mixed / unclear"               → Multi-Agent (parallel)
```

### Agent 1 — Pipeline Health Agent
```
Tools: SQL query (sales.realtime.opportunity_current)
Answers:
  "What's in my pipeline for Q3?"
  "Which deals are at risk of slipping?"
  "What's the average days in stage for Enterprise deals?"
  "Show me all deals closing this month above $50K"
```

### Agent 2 — Customer Health Agent
```
Tools: SQL (sales.metrics.health) + Vector Search (transcripts, emails)
Answers:
  "Which accounts are at risk of churn?"
  "What is Acme Corp's health score and why?"
  "Show me all red accounts in the Financial Services segment"
  "What did the last QBR with Globex say about their satisfaction?"
```

### Agent 3 — Revenue Metrics Agent
```
Tools: SQL (sales.metrics.arr, acv, forecast)
Answers:
  "What is our current ARR?"
  "What's the ARR by segment?"
  "What's the ACV of deals closed this quarter?"
  "What is our net revenue retention this month?"
  "Show me expansion ARR vs new logo ARR split"
```

### Agent 4 — Document Agent
```
Tools: Vector Search (contracts, proposals, transcripts)
Answers:
  "Summarize the MSA with TechCorp"
  "What are the termination clauses in Acme's contract?"
  "Show me the proposal we sent to Globex last quarter"
  "What did the prospect say about pricing on the last call?"
```

### Agent 5 — Sales Performance Agent
```
Tools: SQL (sales.crm.activities, opportunities, accounts)
Answers:
  "How many calls did the team make this week?"
  "Which reps are behind quota?"
  "What's the win rate by industry vertical?"
  "Show me deal velocity by stage"
```

---

## 8. AWS Services Used

| Service | Role |
|---|---|
| **AWS MSK** | Managed Kafka — real-time Salesforce event streaming |
| **AWS S3** | Raw document landing zone before Unity Catalog Volumes |
| **AWS Secrets Manager** | Salesforce API keys, Snowflake credentials, MSK certs |
| **AWS IAM** | Cross-service auth between MSK and Databricks |
| **AWS CloudWatch** | MSK monitoring, consumer lag alerting |

---

## 9. Databricks Services Used

| Service | Role |
|---|---|
| **Unity Catalog** | Data governance, access control per user role |
| **Delta Lake** | Unified storage for structured + semi-structured data |
| **Delta Live Tables** | Streaming ingestion from Kafka → Delta |
| **Databricks Vector Search** | Semantic search on document chunks |
| **Databricks Model Serving** | LLM endpoint (Claude) + embedding model (BGE-large) |
| **Databricks SQL** | Query execution for Text-to-SQL agent |
| **Databricks Workflows** | Document ingestion pipeline orchestration |
| **MLflow** | Prompt versioning, agent evaluation, experiment tracking |
| **Auto Loader** | File ingestion from Volumes |
| **Lakehouse Monitoring** | Answer quality drift, retrieval latency tracking |

---

## 10. Security & Access Control

### User Roles

| Role | Access |
|---|---|
| Sales Rep | Own opportunities + accounts + public docs |
| Sales Manager | Full team pipeline + all accounts in region |
| RevOps / Analyst | All metrics, no privileged contracts |
| Revenue Leader / CRO | Full access to all data |

### Unity Catalog Enforcement
- Row-level security on `opportunity_current` by `owner_id` for reps
- Column masking on sensitive financial fields for non-finance roles
- Vector Search index filtered by `account_owner_id` at query time

### Data Classification
```
GREEN  — All sales users: pipeline metrics, account health, public docs
YELLOW — Manager+: full team metrics, all account data
RED    — RevOps+: ARR, forecast, financial metrics, contracts
```

---

## 11. Implementation Roadmap

```
Phase 1 — Foundation (Month 1-2)
  ✦ Kafka (AWS MSK) setup + Salesforce CDC connectors
  ✦ Delta Live Tables for real-time Salesforce ingestion
  ✦ Snowflake → Delta Lake batch sync
  ✦ Unity Catalog schema setup (sales.realtime / metrics / crm)
  ✦ Basic Pipeline Health Agent + Revenue Metrics Agent
  ✦ Internal testing with RevOps team

Phase 2 — Document RAG (Month 2-3)
  ✦ Databricks Volumes ingestion pipeline
  ✦ Vector Search index for contracts + proposals
  ✦ Document Agent live
  ✦ Customer Health Agent (SQL + Vector merged)

Phase 3 — Sales Companion App (Month 3-4)
  ✦ React web app (standalone)
  ✦ Salesforce Lightning Component (embedded in SF sidebar)
  ✦ Role-based access (rep vs manager vs leader views)
  ✦ Conversation history per user

Phase 4 — Intelligence Layer (Month 4-5)
  ✦ Call transcript ingestion (Gong/Zoom)
  ✦ Deal risk scoring model (ML)
  ✦ Proactive alerts ("Deal X hasn't had activity in 14 days")
  ✦ Forecast accuracy tracking vs actuals

Phase 5 — Hardening (Month 5-6)
  ✦ Answer quality eval framework (MLflow)
  ✦ Lakehouse Monitoring for query latency + drift
  ✦ Kafka consumer lag alerting
  ✦ Load testing (100 concurrent users)
```

---

## 12. Example Conversations

**Pipeline Health**
```
Rep:   "What deals am I closing this month?"
Agent: "You have 4 deals closing in June totalling $187K ACV:
        1. Acme Corp — $80K — Negotiation — 85% probability
        2. TechFlow — $52K — Proposal — 60% probability
        3. Globex — $35K — Commit — 95% probability
        4. Initech — $20K — Verbal Commit — 90% probability"
```

**Customer Health + Document**
```
Manager: "What's the renewal risk for Acme Corp?"
Agent:   "Acme Corp health score is 62 (At Risk). Key signals:
          - Feature adoption dropped 23% in last 30 days [source: usage_metrics]
          - CFO mentioned budget freeze on 2026-05-12 call [source: transcript]
          - Contract renews 2026-09-30, 5% uplift clause [source: contract_v3.pdf]
          Recommended action: Schedule executive sponsor call before July."
```

**Revenue Metrics**
```
CRO:   "What's our ARR growth this quarter vs last?"
Agent: "ARR grew from $12.4M to $14.1M this quarter (+13.7%).
        New logo ARR: +$980K | Expansion: +$720K | Churn: -$40K
        NRR this quarter: 106% [source: arr_snapshot 2026-Q2]"
```

---

## 13. Open Questions

1. Is Gong or Zoom used for call recording — determines transcript ingestion approach
2. What Salesforce edition is in use — affects CDC and Platform Events availability
3. Is there an existing MSK cluster or does one need to be created?
4. Should the Salesforce Lightning Component be in Phase 1 or Phase 3?
5. What is the data retention policy for call transcripts and emails?
