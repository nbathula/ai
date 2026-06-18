# AI Projects

Two production AI projects built for real-world use.

---

## Projects

### [sales-companion/](sales-companion/)

A real-time Sales Companion agent that answers sales rep questions during live calls using a RAG pipeline over product docs, CRM data, and deal history.

**Stack:** Databricks · AWS MSK (Kafka) · LangChain · React · Terraform

See [sales-companion/README.md](sales-companion/README.md) for setup and architecture.

---

### [job-agent/](job-agent/)

A local AI agent that searches Google Jobs daily for senior data/AI leadership roles, scores each match against a candidate profile using Claude Opus, and delivers a ranked email digest with a CSV export.

**Stack:** n8n (Docker) · SerpAPI · OpenRouter (Claude Opus 4) · Gmail SMTP

See [job-agent/README.md](job-agent/README.md) for setup and architecture.
