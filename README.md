# Sales Companion

AI-powered sales assistant for B2B SaaS teams. Ask natural language questions about pipeline health, customer health, revenue metrics, and contracts. Built on Databricks + AWS.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Local Development](#local-development)
5. [Deployment](#deployment)
6. [Configuration Reference](#configuration-reference)
7. [Runbook](#runbook)
8. [How It Works](#how-it-works)
9. [Enhancements](#enhancements)

---

## Architecture

```
Salesforce CDC → AWS MSK (Kafka) → Delta Live Tables → sales.realtime.*
Snowflake Metrics                → Snowflake Sync   → sales.metrics.*
Databricks Volumes (docs)        → Auto Loader      → sales.documents.*

                    ┌─────────────────────────────┐
                    │       Agent Router           │
                    │  (LangGraph on Databricks)   │
                    └──────────┬──────────────────┘
                               │
             ┌─────────────────┼─────────────────┐
             ▼                                   ▼
   Pipeline Health Agent              Customer Health Agent
   ├── Text-to-SQL                    ├── Text-to-SQL
   │   (sales.realtime/metrics)       │   (sales.metrics.health/nrr)
   └── Vector Search                  └── Vector Search
       (proposals, transcripts)           (contracts, renewals)

                    ┌─────────────────────────────┐
                    │       FastAPI + React UI     │
                    │  Monitoring on every call    │
                    │  Eval gate before deploy     │
                    └─────────────────────────────┘
```

**Key technology choices:**

| Concern | Choice | Why |
|---|---|---|
| Real-time data | AWS MSK + Kafka Connect | Salesforce CDC → Delta in <30s without polling |
| Structured queries | Text-to-SQL (Claude) | Natural language over Delta Lake metrics tables |
| Document queries | Databricks Vector Search | Semantic search over chunked contracts/proposals |
| LLM | Claude 3.5 Sonnet (Databricks endpoint) | Best instruction-following, grounded responses |
| Embeddings | BGE-large-en-v1.5 (Databricks endpoint) | Strong domain-neutral embeddings, fast |
| Agent framework | LangGraph | Stateful multi-step routing with conditional edges |
| Monitoring | Custom decorator + Delta tables | All 4 signals on every call from day 1 |

---

## Project Structure

```
sales-companion/
├── agents/
│   ├── llm.py                    # Claude 3.5 Sonnet client
│   ├── router.py                 # Query → Pipeline or Customer agent
│   ├── pipeline_health.py        # Pipeline / forecast / deal agent
│   ├── customer_health.py        # Account health / NRR / renewal agent
│   ├── monitoring/
│   │   ├── monitor.py            # @monitor decorator (latency, groundedness, cost)
│   │   └── groundedness.py       # Claude Haiku judge for groundedness scoring
│   └── tools/
│       ├── text_to_sql.py        # NL → SQL → execute (DML blocked)
│       └── vector_search.py      # BGE embed → Databricks Vector Search
├── ingestion/
│   ├── snowflake_sync.py         # Snowflake → sales.metrics.* (hourly)
│   ├── document_pipeline.py      # Volumes → parse → chunk → embed → Vector Search
│   └── kafka/
│       ├── dlt_pipeline.py       # Delta Live Tables (Bronze/Silver/Gold CDC)
│       ├── topics.py             # MSK topic creation
│       ├── salesforce_connector.json   # Kafka Connect Opportunity CDC
│       └── account_connector.json      # Kafka Connect Account CDC
├── serving/
│   ├── main.py                   # FastAPI: POST /query, POST /feedback, GET /health
│   ├── auth.py                   # X-API-Key middleware
│   └── models.py                 # Pydantic request/response models
├── frontend/
│   ├── src/
│   │   ├── App.tsx               # Root — session management, message state
│   │   ├── api/client.ts         # Axios client → FastAPI
│   │   ├── types.ts              # TypeScript types
│   │   └── components/
│   │       ├── MessageBubble.tsx # Chat message with confidence badge + SQL drawer
│   │       ├── QueryInput.tsx    # Textarea + suggestion chips
│   │       ├── ConfidenceBadge.tsx
│   │       ├── FeedbackBar.tsx   # Thumbs up/down → POST /feedback
│   │       └── SqlDrawer.tsx     # Collapsible generated SQL panel
│   ├── package.json
│   └── vite.config.ts            # Proxies /api → FastAPI on :8000
├── eval/
│   ├── golden_dataset.json       # 12 golden questions across both agents
│   └── run_eval.py               # Eval gate: exits 1 on failure (blocks deploy)
├── infra/
│   ├── databricks/
│   │   ├── setup_catalog.py      # Unity Catalog schemas + Delta tables + Volumes
│   │   ├── workflow_sprint1.json # Databricks Workflow: setup → sync → ingest
│   │   └── dlt_pipeline_config.json  # DLT pipeline (continuous, Kafka)
│   └── terraform/
│       ├── msk.tf                # AWS MSK cluster + MSK Connect + Salesforce plugin
│       └── frontend.tf           # S3 + CloudFront for React hosting
├── scripts/
│   ├── deploy_serving.py         # MLflow pyfunc → Unity Catalog → Databricks endpoint
│   └── deploy_frontend.sh        # npm build → S3 sync → CloudFront invalidation
├── tests/
│   ├── test_text_to_sql.py       # SQL safety guard unit tests
│   ├── test_router.py            # Keyword routing unit tests
│   └── test_serving.py           # FastAPI endpoint tests (mocked agent)
├── .github/workflows/
│   ├── ci.yml                    # PR: lint + unit tests + frontend build
│   └── deploy.yml                # Push to main: eval gate → deploy serving + frontend
├── pyproject.toml
├── Makefile
└── SOLUTION_DESIGN.md
```

---

## Prerequisites

**Databricks workspace (Unity Catalog enabled)**
- Databricks Runtime 14.3+ (Spark 3.5)
- Unity Catalog with admin permissions to create a `sales` catalog
- Two Model Serving endpoints provisioned:
  - `bge-large-en` — BGE-large-en-v1.5 embedding model
  - `claude-3-5-sonnet` — Claude 3.5 Sonnet via external model
- Vector Search endpoint: `sales-companion-vs`

**AWS account**
- VPC with private subnets in 3 AZs (for MSK)
- IAM permissions to create MSK clusters and MSK Connect connectors
- S3 permissions for plugin uploads and frontend hosting

**Salesforce**
- Salesforce edition with Change Data Capture enabled (Enterprise or above)
- Connected App with OAuth 2.0 credentials
- PushTopics created for `Opportunity` and `Account` objects

**Local dev**
- Python 3.11+
- Node.js 20+
- AWS CLI configured
- Databricks CLI configured (`databricks configure`)

---

## Local Development

### 1. Clone and install

```bash
git clone <repo>
cd sales-companion
pip install -e ".[dev]"
cd frontend && npm install && cd ..
```

### 2. Set environment variables

```bash
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...
export ANTHROPIC_API_KEY=sk-ant-...
export VS_ENDPOINT=sales-companion-vs
export EMBED_ENDPOINT=bge-large-en
export SALES_COMPANION_API_KEYS=dev-key
```

### 3. Run tests

```bash
make test
# or
pytest tests/ -v
```

### 4. Start the backend

```bash
make dev-backend
# FastAPI starts at http://localhost:8000
# Docs at http://localhost:8000/docs
```

### 5. Start the frontend

```bash
# In a separate terminal:
cd frontend
cp .env.example .env.local   # set VITE_API_KEY=dev-key
make dev-frontend
# React starts at http://localhost:3000
```

---

## Deployment

### Step 1 — Provision infrastructure (one-time)

```bash
cd infra/terraform

# Set variables
cat > terraform.tfvars <<EOF
aws_region                   = "us-east-1"
environment                  = "prod"
vpc_id                       = "vpc-xxxxxxxx"
private_subnet_ids           = ["subnet-aaa", "subnet-bbb", "subnet-ccc"]
databricks_security_group_id = "sg-xxxxxxxx"
salesforce_instance_url      = "https://yourorg.my.salesforce.com"
salesforce_username          = "your-sf-user@company.com"
salesforce_password          = "..."
salesforce_security_token    = "..."
salesforce_consumer_key      = "..."
salesforce_consumer_secret   = "..."
EOF

terraform init
terraform apply
```

Copy the outputs — you will need them:
- `msk_bootstrap_brokers_tls` → Databricks secret
- `cloudfront_distribution_id` → GitHub secret
- `frontend_s3_bucket` → GitHub secret

### Step 2 — Create Databricks secret scope

```bash
databricks secrets create-scope sales-companion

# Add each secret:
databricks secrets put-secret sales-companion databricks-host   --string-value "$DATABRICKS_HOST"
databricks secrets put-secret sales-companion databricks-token  --string-value "$DATABRICKS_TOKEN"
databricks secrets put-secret sales-companion anthropic-api-key --string-value "$ANTHROPIC_API_KEY"
databricks secrets put-secret sales-companion msk-bootstrap-servers --string-value "<from terraform output>"
databricks secrets put-secret sales-companion snowflake-account --string-value "<your-account>"
databricks secrets put-secret sales-companion snowflake-user    --string-value "<your-user>"
databricks secrets put-secret sales-companion snowflake-password --string-value "<your-password>"
databricks secrets put-secret sales-companion api-keys          --string-value "prod-key-1,prod-key-2"
```

### Step 3 — Run Unity Catalog setup (one-time)

Upload `infra/databricks/setup_catalog.py` to Databricks and run it as a notebook or job. This creates:
- Catalog `sales` with schemas `realtime`, `metrics`, `documents`, `monitoring`
- All Delta tables and Volumes

```bash
# Or via Databricks CLI:
databricks jobs run-now --job-id <setup-job-id>
```

### Step 4 — Start the DLT pipeline (continuous)

Import `infra/databricks/dlt_pipeline_config.json` in Databricks → Workflows → Delta Live Tables → Create Pipeline. Start it in continuous mode.

### Step 5 — Import and run the Sprint 1 Workflow

Import `infra/databricks/workflow_sprint1.json` in Databricks → Workflows. Run it once to trigger:
1. Snowflake sync (all 5 metrics tables)
2. Document ingestion from Volumes

Set the Snowflake sync task on an hourly schedule after the first run.

### Step 6 — Deploy via GitHub Actions

Add these secrets to your GitHub repo under Settings → Secrets → Actions:

```
DATABRICKS_HOST
DATABRICKS_TOKEN
ANTHROPIC_API_KEY
VS_ENDPOINT
EMBED_ENDPOINT
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
CLOUDFRONT_DISTRIBUTION_ID
FRONTEND_API_KEY
```

Push to `main`. The deploy pipeline will:
1. Run unit tests
2. Run the eval gate against live Databricks (blocks on failure)
3. Deploy the serving endpoint + React frontend in parallel

### Step 7 — Verify

```bash
# Check serving endpoint
curl -X POST $DATABRICKS_HOST/serving-endpoints/sales-companion-api/invocations \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"query": "What is our Q2 pipeline?", "user_id": "test"}]}'

# Check frontend
open https://<cloudfront-domain>
```

---

## Configuration Reference

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABRICKS_HOST` | Yes | Workspace URL, e.g. `https://abc.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Yes | Personal access token or service principal token |
| `ANTHROPIC_API_KEY` | Yes | Used by groundedness scorer (Claude Haiku) |
| `VS_ENDPOINT` | Yes | Vector Search endpoint name, default `sales-companion-vs` |
| `EMBED_ENDPOINT` | Yes | Embedding model endpoint name, default `bge-large-en` |
| `CLAUDE_ENDPOINT` | No | LLM serving endpoint name, default `claude-3-5-sonnet` |
| `SALES_COMPANION_API_KEYS` | Yes | Comma-separated API keys for the FastAPI layer |
| `MSK_BOOTSTRAP_SERVERS` | Yes (DLT + topics.py) | MSK broker URLs from Terraform output |
| `SNOWFLAKE_ACCOUNT` | Yes (sync) | Snowflake account identifier |
| `SNOWFLAKE_USER` | Yes (sync) | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Yes (sync) | Snowflake password |
| `AGENT_VERSION` | No | Version tag for traces, default `0.1.0` |
| `PORT` | No | FastAPI port, default `8000` |

### Monitoring Thresholds

Defined in `eval/run_eval.py`. Change here to tighten or relax the eval gate:

```python
PASS_THRESHOLDS = {
    "groundedness": 0.85,    # % of claims supported by retrieved context
    "no_bad_phrases": 1.0,   # 100% — safety questions must never generate DML
    "no_unsafe_sql": 1.0,    # 100% — DELETE/DROP/TRUNCATE must always be blocked
}
```

### Databricks Secret Scope

All secrets live in scope `sales-companion`. The key names used in code:

| Secret key | Used by |
|---|---|
| `databricks-host` | deploy scripts, DLT config |
| `databricks-token` | serving layer, vector search, embed |
| `anthropic-api-key` | groundedness scorer |
| `msk-bootstrap-servers` | DLT pipeline |
| `snowflake-account/user/password` | Snowflake sync |
| `api-keys` | FastAPI auth middleware |

---

## Runbook

### The frontend is down

1. Check CloudFront: `aws cloudfront get-distribution --id $DISTRIBUTION_ID`
2. Check S3 bucket has `index.html`: `aws s3 ls s3://sales-companion-frontend-prod/`
3. Re-deploy: `./scripts/deploy_frontend.sh prod`

### Agents are giving stale data

1. Check Kafka consumer lag in CloudWatch → MSK → Consumer groups
2. Check DLT pipeline state in Databricks → Workflows → Delta Live Tables
3. If DLT is stopped, restart it — it is stateful and will resume from last checkpoint
4. Check Snowflake sync last run: `SELECT MAX(synced_at) FROM sales.metrics.arr`

### Groundedness score is dropping

1. Query `sales.monitoring.groundedness_issues` for recent unsupported claims:
   ```sql
   SELECT query, unsupported_claim, ts
   FROM sales.monitoring.groundedness_issues
   WHERE ts > NOW() - INTERVAL 24 HOURS
   ORDER BY ts DESC
   ```
2. Check if Vector Search index is out of sync — trigger a manual sync via the Databricks Vector Search UI
3. If it's a SQL path issue, check `sales.monitoring.sql_log` for query failures

### High latency complaints

Query the monitoring table to find slow calls:
```sql
SELECT agent_type, query, total_latency_ms, retrieval_latency_ms, llm_latency_ms
FROM sales.monitoring.agent_traces
WHERE ts > NOW() - INTERVAL 1 HOUR
  AND total_latency_ms > 5000
ORDER BY total_latency_ms DESC
LIMIT 20
```

Common causes: Vector Search cold start (first call after idle), MSK rebalancing, Databricks serving cluster scaling.

### Eval gate is failing the CI deploy

1. Check the eval results artifact uploaded in GitHub Actions
2. Run locally against live Databricks to reproduce: `make eval`
3. Check `sales.monitoring.eval_runs` for the failing run:
   ```sql
   SELECT * FROM sales.monitoring.eval_runs ORDER BY ts DESC LIMIT 5
   ```
4. If a golden question changed meaning (e.g., a column was renamed), update `eval/golden_dataset.json`
5. If groundedness dropped, check if the Vector Search index needs a sync

### Adding a new golden question to the eval set

Edit `eval/golden_dataset.json` and add an entry following this pattern:

```json
{
  "id": "pipeline-006",
  "agent": "pipeline_health",
  "query": "Your question here",
  "expected_contains": ["keyword1", "keyword2"],
  "expected_sql_keywords": ["COLUMN_NAME", "TABLE"],
  "min_groundedness": 0.85
}
```

Run `make eval` locally before merging.

---

## How It Works

### A query end to end

```
1. User types "Which accounts are at risk of churning?"
   → React (App.tsx) calls POST /api/query with session_id

2. FastAPI (serving/main.py) validates X-API-Key, calls router.route()

3. Router (agents/router.py) keyword-matches "churn" → CUSTOMER
   → calls customer_health.run(query)

4. @monitor decorator starts timer, captures session context

5. Customer Health Agent (LangGraph graph):
   a. classify node: "churn" keyword → SQL
   b. sql node: text_to_sql.run() → Claude generates SQL → executes on Spark
      SELECT account_id, health_score, risk_tier, arr ...
      WHERE risk_tier IN ('Yellow','Red') OR health_score < 60
   c. build_context: formats rows as LLM-readable table
   d. generate node: Claude synthesizes response with Key Takeaway

6. @monitor decorator:
   - scores groundedness via Claude Haiku judge
   - writes trace to sales.monitoring.agent_traces (background thread)
   - logs SQL to sales.monitoring.sql_log
   - attaches trace_id, groundedness_score, cost to result

7. FastAPI returns QueryResponse with confidence, groundedness, SQL, trace_id

8. React renders MessageBubble:
   - Markdown-rendered response
   - Color-coded confidence + groundedness badges
   - Latency + cost in metadata row
   - "View SQL" collapsible drawer
   - Thumbs up/down feedback bar
```

### Monitoring signals on every call

| Signal | Source | Table |
|---|---|---|
| Latency (total, retrieval, LLM) | `time.perf_counter()` in decorator | `monitoring.agent_traces` |
| Groundedness score | Claude Haiku judge vs retrieved context | `monitoring.agent_traces` + `monitoring.groundedness_issues` |
| Confidence score | Agent calculates based on data completeness | `monitoring.agent_traces` |
| Token cost | Input + output tokens × Anthropic pricing | `monitoring.agent_traces` |
| User feedback | Thumbs up/down from UI | `monitoring.user_feedback` |
| SQL accuracy | Every generated SQL logged with success/error | `monitoring.sql_log` |
| Eval history | Pre-release gate results | `monitoring.eval_runs` |

---

## Enhancements

Planned improvements across monitoring, agents, and user experience. Prioritized by impact.

---

### 1. Feedback Flywheel — Close the Loop on Thumbs Down

**Current state:** Users can give thumbs up/down via the UI. Ratings are stored in `sales.monitoring.user_feedback` and linked to traces via `trace_id`. But the feedback stops there — it is not yet used to improve the system.

**Enhancement:** Build a feedback review workflow that converts bad responses into new golden dataset questions:

```
User gives 👎
      ↓
sales.monitoring.user_feedback (rating=1)
      ↓ (weekly review — Data/AI team)
Join with agent_traces to get: query + response + groundedness_score
      ↓
Human reviews: Was the response wrong? Hallucinated? Missing data?
      ↓
If confirmed bad → add to eval/golden_dataset.json as a new test case
      ↓
Tighter eval gate → next deploy must pass this case
      ↓
Model/retrieval improved → groundedness rises → fewer thumbs down
```

This flywheel is the primary mechanism for improving answer quality over time. Without it, the eval golden set becomes stale and stops catching real failure modes.

**Query to start the weekly review:**
```sql
SELECT
  t.query,
  t.response,
  t.agent_type,
  t.groundedness_score,
  t.confidence_score,
  t.generated_sql,
  f.comment,
  f.ts AS feedback_ts
FROM sales.monitoring.user_feedback f
JOIN sales.monitoring.agent_traces t USING (trace_id)
WHERE f.rating = 1                              -- thumbs down only
  AND f.ts > NOW() - INTERVAL 7 DAYS
ORDER BY t.groundedness_score ASC;             -- worst groundedness first
```

---

### 2. Databricks Lakehouse Monitoring — Automated Drift Alerts

**Current state:** Monitoring data is written to Delta tables but there are no automated alerts. The ops team must query manually to detect problems.

**Enhancement:** Wire Databricks Lakehouse Monitoring to watch `sales.monitoring.agent_traces` and alert when key metrics drift:

| Metric | Alert Condition |
|---|---|
| `groundedness_score` | 7-day rolling average drops below 0.80 |
| `total_latency_ms` | P95 exceeds 5,000ms over a 1-hour window |
| `confidence_score` | Average drops below 0.70 |
| `estimated_cost_usd` | Daily total exceeds budget threshold |
| SQL failure rate | `executed_successfully = false` rate exceeds 10% |

This removes the need for manual monitoring and surfaces regressions before users notice them.

---

### 3. `/metrics` Endpoint — Live Observability for Ops

**Current state:** There is no programmatic way to check agent health without querying Delta directly.

**Enhancement:** Add a `GET /metrics` endpoint to `serving/main.py` that returns a live snapshot:

```json
{
  "period": "last_1h",
  "total_calls": 142,
  "avg_latency_ms": 1820,
  "p95_latency_ms": 4100,
  "avg_groundedness": 0.89,
  "avg_confidence": 0.87,
  "sql_failure_rate": 0.02,
  "thumbs_up_rate": 0.81,
  "estimated_cost_usd": 0.74
}
```

This allows the ops team to scrape the endpoint in CloudWatch or Grafana without needing Databricks access.

---

### 4. Kafka Consumer Lag Alerting

**Current state:** The DLT pipeline reads from MSK but there is no alert if the pipeline falls behind (e.g., DLT stops, MSK rebalances, Salesforce sends a spike of events).

**Enhancement:** Add a CloudWatch alarm on the `kafka.consumer.group.lag` metric for the DLT consumer group. Alert the Data/AI team if lag exceeds 10,000 messages or if the consumer group goes inactive for more than 5 minutes. This ensures real-time data freshness SLA (<30s) is visibly broken when violated.

---

### 5. Missing Agents

**Current state:** The solution design called for 5 agents. Only 2 are built (Pipeline Health, Customer Health). Three are missing:

| Agent | What it answers | Retrieval |
|---|---|---|
| **Revenue Metrics Agent** | ARR by segment, NRR trend, expansion vs churn split, MRR movement | Text-to-SQL (`sales.metrics.arr`, `nrr`) |
| **Document Agent** | Summarize a contract, pull a proposal, search call transcripts | Vector Search only |
| **Sales Performance Agent** | Rep activity counts, win rate by rep/vertical, quota attainment per rep | Text-to-SQL (`sales.realtime.*`, `sales.crm.*`) |

Currently, these question types fall through to the Pipeline Health agent via Text-to-SQL, which partially works but uses the wrong system prompt and routing context.

---

### 6. Conversation History — Stateful Follow-Up Questions

**Current state:** Every query is stateless. If a user asks "Which accounts are at risk?" and follows up with "And what are their renewal dates?" — the second query has no memory of the first.

**Enhancement:** Store conversation turns in a session store (Redis or Delta table keyed by `session_id`) and inject the last N turns into the LLM context window. This enables natural follow-up questions without the user having to repeat context.

```python
# In agents/router.py
history = load_session_history(session_id, last_n=4)
messages = history + [{"role": "user", "content": query}]
```

---

### 7. Row-Level Security — Reps See Only Their Deals

**Current state:** Unity Catalog schemas and tables are created, but row-level security has not been applied. Any authenticated user can query any deal or account.

**Enhancement:** Apply Databricks row filters on `sales.realtime.opportunity_current` so that sales reps only see their own opportunities, while managers see their team and leaders see everything:

```sql
-- Row filter applied at Unity Catalog level
CREATE ROW FILTER sales_rep_filter ON sales.realtime.opportunity_current
AS (owner_id) -> is_account_group_member('sales-leaders')
               OR owner_id = current_user();
```

This is enforced at the data layer — not the application layer — so it cannot be bypassed regardless of how the query is formed.

---

### Priority Order

| # | Enhancement | Effort | Impact |
|---|---|---|---|
| 1 | Feedback flywheel (thumbs down → golden dataset) | Low | High — directly improves answer quality |
| 2 | Row-level security | Medium | High — required before broad user rollout |
| 3 | Conversation history | Medium | High — core UX expectation |
| 4 | Missing agents (Revenue Metrics, Document, Sales Performance) | High | High — covers question types that currently fall through |
| 5 | Lakehouse Monitoring drift alerts | Low | Medium — operational hygiene |
| 6 | `/metrics` endpoint | Low | Medium — ops observability |
| 7 | Kafka consumer lag alerting | Low | Medium — data freshness SLA enforcement |
