# Job Search Agent

A local AI agent that searches for senior data/AI leadership roles daily, scores each match against your profile using Claude Opus, and sends a morning email digest with a ranked shortlist. Results are also exported to a local CSV file for tracking.

**Stack:** n8n (Docker) · SerpAPI · OpenRouter (Claude Opus 4) · Gmail SMTP

---

## What it does

Runs every weekday at 9am:

1. Searches Google Jobs across 12 role-specific queries (VP Data, Director AI, Head of Data Engineering, etc.)
2. Deduplicates across all search results
3. Scores each job 1–10 against your profile using Claude Opus
4. Filters jobs that score 5 or above
5. Sends an HTML email digest ranked by score
6. Writes all qualifying jobs to `output/jobs_YYYY-MM-DD.csv`

---

## Setup

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- [SerpAPI account](https://serpapi.com) — 100 free searches/month (sufficient for daily runs)
- [OpenRouter account](https://openrouter.ai) — pay-per-use, ~$0.02 per job scored
- Gmail account with an App Password for SMTP

### 1. Configure environment

```bash
cd job-agent
cp .env.example .env
```

Edit `.env`:

```
SERPAPI_KEY=your-serpapi-key
OPENROUTER_API_KEY=sk-or-your-key
GMAIL_USER=you@gmail.com
NOTIFY_EMAIL=you@gmail.com
```

**Gmail App Password** (required — standard password won't work with SMTP):
Google Account → Security → 2-Step Verification → App Passwords → create one named "n8n"

### 2. Start n8n

```bash
./scripts/setup.sh
```

Or manually:

```bash
docker compose up -d
```

### 3. Configure credentials in n8n

Open `http://localhost:5678`, create your account, then:

Settings → Credentials → Add credential → **SMTP**

| Field    | Value              |
|----------|--------------------|
| Host     | smtp.gmail.com     |
| Port     | 465                |
| SSL      | on                 |
| User     | your Gmail address |
| Password | your App Password  |

### 4. Import the workflow

Workflows → Import from File → `workflows/job_search_agent.json`

After import, open the **Send Email Digest** node and reselect the SMTP credential (credentials detach on import).

### 5. Test and activate

Click **Test Workflow** to run immediately. Check your inbox and `output/` for the CSV.

Toggle **Active** in the top-right to enable daily 9am runs (Mon–Fri).

---

## Customising your search

Edit the **Load User Profile** Code node in the workflow to update:

- `search_queries` — the 12 Google Jobs search strings
- `experience_summary` — fed to Claude for scoring
- `key_skills` — also used in scoring prompt
- `salary_minimum` — sets floor for scoring
- `disqualifiers` — strings that flag a job as not relevant

Alternatively, edit `profile/user_profile.json` as a reference (the node reads from its inline code, not this file at runtime — but keeping both in sync makes future updates easier).

### Change the score threshold

In the **Score >= 5?** IF node, change `5` to `7` once you've confirmed the pipeline is working.

---

## Output

**Email:** HTML digest ranked by score, showing top 30 results. Sent to `NOTIFY_EMAIL`.

**CSV:** `output/jobs_YYYY-MM-DD.csv` with columns:

| Column | Description |
|--------|-------------|
| title | Job title |
| company | Company name |
| location | Location or Remote |
| salary | Salary range if listed |
| score | Claude's 1–10 fit score |
| score_reason | One-sentence explanation |
| url | Direct link to apply |
| posted | Days/weeks since posted |
| is_disqualified | true if Claude flagged a disqualifier |

---

## Cost

| Item | Cost |
|------|------|
| SerpAPI | Free (100 searches/month), $50/month for 5K |
| OpenRouter — Claude Opus scoring | ~$0.02 per job |
| Typical daily run (50–100 jobs) | ~$1–2 |

---

## Managing n8n

```bash
# Stop
docker compose down

# Start
docker compose up -d

# View logs
docker compose logs -f n8n

# Full reset (deletes workflows and credentials)
docker compose down -v
```

Workflows are stored in the `n8n_data` Docker volume. Export backups from the n8n UI: Workflows → ⋯ → Export.

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full node-by-node breakdown and data flow diagram.
