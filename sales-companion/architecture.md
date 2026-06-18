# Sales Companion — Architecture Diagrams

---

## 1. System Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                              DATA SOURCES                                       ║
╠══════════════════════╦═══════════════════════╦══════════════════════════════════╣
║   SALESFORCE (CRM)   ║  SNOWFLAKE (Metrics)  ║   DATABRICKS VOLUMES (Docs)     ║
║                      ║                       ║                                  ║
║  Opportunities       ║  ARR / MRR            ║  Contracts   (.pdf, .docx)      ║
║  Accounts            ║  ACV by deal          ║  Proposals   (.pdf, .docx)      ║
║  Contacts            ║  Customer Health      ║  Transcripts (.txt)             ║
║  Contracts           ║  Forecast             ║  Emails      (.txt)             ║
║  Activities          ║  NRR                  ║                                  ║
╚══════════╤═══════════╩═══════════╤═══════════╩════════════════╤═════════════════╝
           │ Change Data Capture   │ Snowflake Connector        │ Auto Loader
           │ (Salesforce CDC)      │ (hourly batch sync)        │ (triggered on new file)
           ▼                       ▼                            ▼
╔══════════════════╗   ╔═══════════════════════╗   ╔═══════════════════════════════╗
║   AWS MSK        ║   ║  Snowflake Sync Job   ║   ║  Document Ingestion Pipeline ║
║   (Kafka)        ║   ║  ingestion/           ║   ║  ingestion/document_pipeline  ║
║                  ║   ║  snowflake_sync.py    ║   ║                               ║
║  Topics:         ║   ║                       ║   ║  Parse  → PyMuPDF / python-  ║
║  sf.opp.cdc      ║   ║  ARR, ACV, Health,    ║   ║         docx                 ║
║  sf.account.cdc  ║   ║  Forecast, NRR        ║   ║  Chunk  → 512 tok / 50 ovlp  ║
║  sf.contact.cdc  ║   ║                       ║   ║  Embed  → BGE-large-en-v1.5  ║
║  dlq.*           ║   ║                       ║   ║                               ║
╚══════════╤═══════╝   ╚═══════════╤═══════════╝   ╚══════════════╤════════════════╝
           │ Kafka Connect          │                              │
           │ Salesforce Source      │                              │
           ▼                       ▼                              ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                    UNITY CATALOG  (catalog: sales)                              ║
╠═══════════════════╦═══════════════════╦══════════════════════════════════════════╣
║ sales.realtime.*  ║  sales.metrics.*  ║  sales.documents.*                      ║
║                   ║                   ║                                          ║
║ opportunity_      ║  arr              ║  chunks          ← embeddings here       ║
║   current         ║  acv              ║  metadata                                ║
║ account_          ║  health           ║  Vector Index    ← synced from chunks    ║
║   current         ║  forecast         ║                                          ║
║ dlq_events        ║  nrr              ║  Volumes:                                ║
║                   ║                   ║  /contracts /proposals                   ║
║ (fed by DLT,      ║  (fed by hourly   ║  /transcripts /emails                   ║
║  APPLY CHANGES)   ║   Snowflake sync) ║                                          ║
╠═══════════════════╩═══════════════════╩══════════════════════════════════════════╣
║ sales.monitoring.*                                                               ║
║  agent_traces · user_feedback · groundedness_issues · eval_runs · sql_log       ║
╚════════════════════════════════════════╤═════════════════════════════════════════╝
                                         │
╔════════════════════════════════════════▼═════════════════════════════════════════╗
║                        HYBRID RETRIEVAL LAYER                                   ║
╠══════════════════════════════════╦══════════════════════════════════════════════╣
║        TEXT-TO-SQL               ║          VECTOR SEARCH                       ║
║   agents/tools/text_to_sql.py   ║    agents/tools/vector_search.py             ║
║                                  ║                                              ║
║  NL query                        ║  NL query                                    ║
║    → Claude generates SQL        ║    → BGE embeds query                        ║
║    → Safety check (block DML)    ║    → Similarity search on chunks index       ║
║    → Spark executes on Delta     ║    → Returns ranked chunks + metadata        ║
║    → Rows → context text         ║    → Context text for LLM                   ║
║                                  ║                                              ║
║  Tables available:               ║  Index: sales.documents.chunks_index         ║
║  sales.realtime.*                ║  Filters: doc_type, account_id               ║
║  sales.metrics.*                 ║  num_results: 6–8 chunks                     ║
╚══════════════════════════════════╩══════════════════════════════════════════════╝
                                         │
╔════════════════════════════════════════▼═════════════════════════════════════════╗
║                       AGENT ORCHESTRATION  (LangGraph)                          ║
║                                                                                  ║
║   User Query                                                                     ║
║       │                                                                          ║
║       ▼                                                                          ║
║   ┌──────────────────────────────────────────────────────────────────────┐       ║
║   │                    ROUTER  (agents/router.py)                        │       ║
║   │  Fast keyword match → PIPELINE / CUSTOMER / AMBIGUOUS               │       ║
║   │  LLM fallback for ambiguous queries                                  │       ║
║   └────────┬─────────────────────────────────────────────┬──────────────┘       ║
║            │                                             │                       ║
║            ▼                                             ▼                       ║
║   ┌────────────────────────┐               ┌─────────────────────────┐          ║
║   │  PIPELINE HEALTH AGENT │               │  CUSTOMER HEALTH AGENT  │          ║
║   │  pipeline_health.py    │               │  customer_health.py     │          ║
║   │                        │               │                         │          ║
║   │  LangGraph nodes:      │               │  LangGraph nodes:       │          ║
║   │  classify              │               │  classify (+ shortcuts) │          ║
║   │  → sql                 │               │  → sql                  │          ║
║   │  → vector_search       │               │  → vector_search        │          ║
║   │  → sql_and_search      │               │  → sql_and_search       │          ║
║   │  → build_context       │               │  → build_context        │          ║
║   │  → generate            │               │  → generate             │          ║
║   │                        │               │                         │          ║
║   │  Handles:              │               │  Handles:               │          ║
║   │  Pipeline totals       │               │  Health scores          │          ║
║   │  Deal stages           │               │  Churn risk             │          ║
║   │  Forecast vs quota     │               │  NRR / retention        │          ║
║   │  Win rate              │               │  Renewal dates          │          ║
║   │  ACV / close dates     │               │  Contract terms         │          ║
║   └────────────────────────┘               └─────────────────────────┘          ║
║                                                                                  ║
║   Every agent call wrapped with @monitor decorator:                              ║
║   latency · groundedness · confidence · token cost → sales.monitoring.*         ║
╚════════════════════════════════════════╤═════════════════════════════════════════╝
                                         │
                      ┌──────────────────┴──────────────────┐
                      ▼                                      ▼
╔═════════════════════════════╗    ╔══════════════════════════════════════════╗
║   DATABRICKS MODEL SERVING  ║    ║           CI / CD PIPELINE               ║
║                             ║    ║                                          ║
║  LLM endpoint:              ║    ║  Push to main                            ║
║  claude-3-5-sonnet          ║    ║    → Unit tests (pytest)                 ║
║  (Claude 3.5 Sonnet)        ║    ║    → Eval gate (run_eval.py)             ║
║                             ║    ║       blocks on groundedness < 0.85      ║
║  Embedding endpoint:        ║    ║    → Deploy serving endpoint             ║
║  bge-large-en               ║    ║    → Deploy React frontend               ║
║  (BGE-large-en-v1.5)        ║    ║       S3 + CloudFront                    ║
║                             ║    ║                                          ║
║  FastAPI app served via     ║    ║  scripts/deploy_serving.py               ║
║  MLflow pyfunc model        ║    ║  scripts/deploy_frontend.sh              ║
╚═════════════════════════════╝    ╚══════════════════════════════════════════╝
                      │
                      ▼
╔═════════════════════════════════════════════════════════════════════════════════╗
║                         REACT WEB APP  (frontend/)                             ║
║                         Served via CloudFront + S3                             ║
║                                                                                ║
║  ┌──────────────────────────────────────────────────────────────────────────┐  ║
║  │  Sales Companion                                           [S]            │  ║
║  │  Pipeline & Customer Health Agent                                        │  ║
║  ├──────────────────────────────────────────────────────────────────────────┤  ║
║  │                                                                          │  ║
║  │   Pipeline Health Agent                                                  │  ║
║  │  ┌─────────────────────────────────────────────────────┐                │  ║
║  │  │ Your Q2 pipeline totals $14.2M across 63 open deals. │               │  ║
║  │  │                                                       │               │  ║
║  │  │ • Commit:     $4.1M (11 deals)                        │               │  ║
║  │  │ • Best Case:  $5.8M (22 deals)                        │               │  ║
║  │  │ • Pipeline:   $4.3M (30 deals)                        │               │  ║
║  │  │                                                       │               │  ║
║  │  │ Key Takeaway: Commit coverage is 82% of quota.        │               │  ║
║  │  └─────────────────────────────────────────────────────┘                │  ║
║  │  [Confidence: 91%]  [Grounded: 88%]  1.4s  $0.0012                     │  ║
║  │  [View SQL ▼]   Helpful? 👍 👎                                          │  ║
║  │                                                                          │  ║
║  │  ┌──────────────────────────────────────────────────────────────────┐   │  ║
║  │  │ What is our Q2 pipeline?                          Enter to send  │   │  ║
║  │  └──────────────────────────────────────────────────────────────────┘   │  ║
║  └──────────────────────────────────────────────────────────────────────────┘  ║
╚═════════════════════════════════════════════════════════════════════════════════╝
```

---

## 2. Data Flow — Query End-to-End

```
User types query
      │
      ▼
React App (App.tsx)
  POST /api/query
  { query, session_id, user_id }
      │
      ▼ (proxied by Vite → FastAPI :8000)
FastAPI (serving/main.py)
  Validate X-API-Key
      │
      ▼
Router (agents/router.py)
  Keyword match → PIPELINE or CUSTOMER
  LLM classify  → AMBIGUOUS (both)
      │
      ├──── PIPELINE ────────────────────────┐
      │                                      │
      ▼                                      ▼
Pipeline Health Agent               Customer Health Agent
(LangGraph)                         (LangGraph)
      │                                      │
  [classify node]                       [classify node]
  SQL / DOCS / HYBRID               SQL / DOCS / HYBRID
      │                                      │
  ┌───┴───┐                          ┌───────┴──────┐
  │       │                          │              │
  ▼       ▼                          ▼              ▼
SQL    Vector                     SQL          Vector
Tool   Search                     Tool         Search
  │       │                          │              │
  ▼       ▼                          ▼              ▼
Spark  BGE embed                  Spark         BGE embed
SQL    → VS index                 SQL           → VS index
exec   similarity                 exec          similarity
       search                                   search
  │       │                          │              │
  └───┬───┘                          └──────┬───────┘
      │                                     │
  [build_context node]               [build_context node]
  Merge SQL rows +                   Merge SQL rows +
  doc chunks into                    doc chunks into
  context text                       context text
      │                                     │
  [generate node]                    [generate node]
  Claude 3.5 Sonnet                  Claude 3.5 Sonnet
  RESPONSE_SYSTEM prompt             RESPONSE_SYSTEM prompt
  + context                          + context
      │                                     │
      └──────────────┬──────────────────────┘
                     │
              @monitor decorator
              ├── total_latency_ms
              ├── retrieval_latency_ms
              ├── llm_latency_ms
              ├── groundedness (Claude Haiku judge)
              ├── confidence_score
              └── estimated_cost_usd
                     │
              Delta write (background thread)
              sales.monitoring.agent_traces
                     │
                     ▼
              FastAPI response
              { response, confidence, groundedness,
                trace_id, sql, latency, cost }
                     │
                     ▼
              React renders MessageBubble
              ├── Markdown response
              ├── Confidence badge (green/amber/red)
              ├── Groundedness badge
              ├── Latency + cost
              ├── [View SQL] drawer
              └── 👍 👎 feedback bar
```

---

## 3. Streaming Data Flow — Salesforce CDC

```
Salesforce Org
  │
  │  Change Data Capture enabled on:
  │  Opportunity, Account, Contact, Activity
  │
  ▼
Kafka Connect (MSK Connect)
  Salesforce Source Connector
  salesforce_connector.json
  account_connector.json
  │
  │  Publishes JSON events:
  │  { Id, StageName, Amount, _EventType: "UPDATE", ... }
  │
  ▼
AWS MSK (3-broker cluster, 3 AZs)
  Topics:
  ├── salesforce.opportunity.cdc  (6 partitions, 7-day retention)
  ├── salesforce.account.cdc      (6 partitions, 7-day retention)
  ├── salesforce.contact.cdc      (3 partitions)
  ├── salesforce.activity.cdc     (6 partitions, 3-day retention)
  └── sales-companion.dlq.*       (dead-letter queues, 30-day retention)
  │
  ▼
Delta Live Tables (dlt_pipeline.py)
  Continuous streaming pipeline
  │
  ├── [Bronze] opportunity_cdc_raw
  │   Raw JSON from Kafka, no transformation
  │
  ├── [Silver] opportunity_cdc_parsed
  │   Parse JSON → typed columns
  │   DQ: opportunity_id IS NOT NULL
  │   DQ: event_type IN ('CREATE','UPDATE','DELETE')
  │   Bad rows dropped
  │
  └── [Gold] opportunity_current
      APPLY CHANGES (SCD Type 1 upsert)
      Keyed on opportunity_id
      Sequenced by updated_at
      DELETE events → soft delete
      Change Data Feed enabled
      │
      ▼
  sales.realtime.opportunity_current
  (always-fresh, <30s from Salesforce)
```

---

## 4. CI/CD Pipeline

```
Developer push to main branch
         │
         ▼
┌────────────────────┐
│   GitHub Actions   │
│   deploy.yml       │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Job 1: Unit Tests │  ← pytest tests/ -v
│  (ubuntu-latest)   │  ← No Databricks needed
└────────┬───────────┘
         │ Pass
         ▼
┌────────────────────────────────────────┐
│  Job 2: Eval Gate                      │  ← LIVE Databricks
│  (ubuntu-latest)                       │
│                                        │
│  python eval/run_eval.py --agent all  │
│                                        │
│  Runs 12 golden questions:             │
│  ├── Pipeline health (5 questions)     │
│  ├── Customer health (5 questions)     │
│  └── Safety / refuse (2 questions)    │
│                                        │
│  Thresholds:                           │
│  ├── groundedness ≥ 0.85              │
│  ├── no DML in SQL ≥ 1.00             │
│  └── no bad phrases ≥ 1.00            │
│                                        │
│  Logs to:                              │
│  ├── MLflow experiment                 │
│  └── sales.monitoring.eval_runs       │
│                                        │
│  sys.exit(1) on failure → BLOCKED     │
└────────┬───────────────────────────────┘
         │ Pass
         │
    ┌────┴────┐
    │         │ (parallel)
    ▼         ▼
┌─────────┐ ┌──────────────────────┐
│ Job 3   │ │ Job 4                │
│ Deploy  │ │ Deploy Frontend      │
│ Serving │ │                      │
│         │ │ npm ci               │
│ MLflow  │ │ npm run build        │
│ pyfunc  │ │ aws s3 sync → S3     │
│ model   │ │ CloudFront invalidate│
│    ↓    │ │                      │
│ Unity   │ │ React app live at:   │
│ Catalog │ │ https://cf-domain    │
│    ↓    │ │                      │
│ Update  │ └──────────────────────┘
│ serving │
│ endpoint│
│    ↓    │
│ Smoke   │
│ test    │
└─────────┘
```
