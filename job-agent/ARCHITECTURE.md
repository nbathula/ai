# Job Search Agent — Architecture

## Overview

The agent is an n8n workflow running in Docker. It uses a linear pipeline with one fan-out branch: the Merge node pattern re-attaches job data lost after an HTTP Request node, and the Aggregate node collects all qualifying jobs before building the email.

## Data Flow

```
Daily Trigger (9am Mon-Fri)
         |
         v
Load User Profile (Code)
  Saves profile object to workflow staticData so it
  survives across HTTP Request nodes that replace item data.
         |
         v
Build Search Queries (Code)
  Emits 12 items, one per search query string.
         |
         v (12 items in parallel)
Search Jobs — SerpAPI (HTTP GET)
  GET https://serpapi.com/search.json
  engine=google_jobs, chips=date_posted:week
  Returns jobs_results[] per query.
         |
         v (12 API responses)
Parse Job Listings (Code)
  Flattens all 12 responses into individual job items.
  Deduplicates by title+company key.
  Normalises fields: title, company, location, salary,
  posted, url, snippet.
         |
         v (N unique jobs)
Prepare Score Request (Code) ──────────────────────┐
  Reads profile from staticData.                    | (job data, input 0)
  Builds Claude API JSON body per job using         |
  JSON.stringify to avoid special-char issues.      |
  Passes body in _score_body field.                 |
         |                                          |
         v                                          |
Score Job — Claude Opus (HTTP POST)                 |
  POST https://openrouter.ai/api/v1/chat/completions|
  model: anthropic/claude-opus-4-5                  |
  Sends pre-built body as raw content-type.         |
  Response REPLACES item data (n8n behaviour).      |
         |                                          |
         v (Claude response, input 1)               |
         └──────────────────> Merge Job and Score <─┘
                               typeVersion 1, mergeByIndex
                               Pairs job data (input 0) with
                               Claude response (input 1) by position.
                               Both field sets available downstream.
                                        |
                                        v
                               Parse Score (Code)
                                 Extracts JSON from choices[0].message.content
                                 Fields: score (int), reason (str),
                                         is_disqualified (bool)
                                 Logs each job score to execution log.
                                        |
                                        v
                               Score >= 5? (IF node, loose validation)
                                  TRUE |          | FALSE
                                       |          v (dropped)
                                       v
                               Collect Matches (Aggregate)
                                 Gathers all passing items into
                                 { jobs: [...] } on a single output item.
                                        |
                          ┌─────────────┴──────────────┐
                          v                            v
                 Build Email Digest (Code)        Build CSV (Code)
                   Unwraps Aggregate's            Converts jobs[] to
                   item structure.                CSV string via Buffer.
                   Sorts by score desc.           Returns binary data.
                   Caps at top 30.                       |
                   Builds HTML email.                    v
                          |                      Write CSV File
                          v                      (Write Binary File node)
                 Send Email Digest               Writes to
                 (emailSend v1, SMTP)            /home/node/output/
                   Sends HTML body               jobs_YYYY-MM-DD.csv
                   via smtp.gmail.com:465        (mounted to ./output/)
```

## Key Design Decisions

### Merge node pattern
n8n's HTTP Request node replaces the current item's data entirely with the API response. To keep job fields (title, company, etc.) alongside Claude's response, `Prepare Score Request` fans out to both `Score Job` AND `Merge Job and Score` (as input 0). `Score Job` feeds its output as input 1. The Merge node (typeVersion 1, mergeByIndex) pairs them back by position.

### staticData for profile
Job data is passed item-by-item through HTTP Request nodes that overwrite item data. The user profile is written once to `$getWorkflowStaticData('global')` and read back in any Code node that needs it, regardless of what HTTP requests happened in between.

### Pre-built JSON body
Claude scoring requests use `JSON.stringify()` in a Code node (`Prepare Score Request`) rather than building the JSON inline in the HTTP Request node. This prevents special characters in job descriptions (quotes, newlines, backticks) from breaking the request body.

### Aggregate + unwrap
The Aggregate node's `aggregateAllItemData` mode wraps each item with a `json` key internally. Build Email Digest and Build CSV both apply `rawJobs.map(j => j.json || j)` to handle this transparently.

### emailSend typeVersion 1
Only typeVersion 1 of n8n's emailSend node has a dedicated `html` parameter that maps to the `text/html` MIME part. typeVersion 2 only has a `message` (plain text) field.

## Node Reference

| # | Node | Type | Purpose |
|---|------|------|---------|
| 1 | Daily Trigger | scheduleTrigger | Cron: 9am Mon-Fri |
| 2 | Load User Profile | Code | Stores profile in staticData |
| 3 | Build Search Queries | Code | Emits 12 query items |
| 4 | Search Jobs | HTTP GET | SerpAPI Google Jobs |
| 5 | Parse Job Listings | Code | Flatten + deduplicate |
| 6a | Prepare Score Request | Code | Build Claude API body |
| 6b | Score Job | HTTP POST | OpenRouter Claude Opus |
| 6c | Merge Job and Score | Merge v1 | Re-attach job data |
| 7 | Parse Score | Code | Extract score from JSON |
| 8 | Score >= 5? | IF | Filter threshold |
| 9 | Collect Matches | Aggregate | Gather passing jobs |
| 10 | Build Email Digest | Code | Build HTML email |
| 11 | Send Email Digest | emailSend v1 | Gmail SMTP |
| 12 | Build CSV | Code | Convert to CSV binary |
| 13 | Write CSV File | writeBinaryFile | Save to ./output/ |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SERPAPI_KEY` | SerpAPI key for Google Jobs search |
| `OPENROUTER_API_KEY` | OpenRouter key for Claude Opus access |
| `GMAIL_USER` | Gmail address n8n sends from |
| `NOTIFY_EMAIL` | Destination for the daily digest |
| `N8N_BLOCK_ENV_ACCESS_IN_NODE` | Must be `false` to allow `$env.KEY` in Code nodes |
