#!/usr/bin/env bash
set -euo pipefail

echo "==> Job Search Agent — Local Setup"
echo ""

# 1. Copy .env
if [ ! -f .env ]; then
  cp .env.example .env
  echo "  Created .env — fill in your API keys before continuing"
  echo ""
  echo "  SERPAPI_KEY        -> https://serpapi.com (100 free searches/month)"
  echo "  OPENROUTER_API_KEY -> https://openrouter.ai/keys"
  echo "  GMAIL_USER         -> your Gmail address"
  echo "  NOTIFY_EMAIL       -> where the daily digest goes"
  echo ""
  echo "  Edit .env, then re-run this script."
  exit 0
fi

# 2. Check Docker is running
if ! docker info > /dev/null 2>&1; then
  echo "  Docker is not running. Start Docker Desktop and retry."
  exit 1
fi
echo "  Docker is running"

# 3. Create output directory for CSV exports
mkdir -p output

# 4. Start n8n
echo ""
echo "==> Starting n8n..."
docker compose up -d

echo ""
echo "==> Waiting for n8n to be ready..."
until curl -s http://localhost:5678/healthz > /dev/null 2>&1; do
  sleep 2
  printf "."
done
echo ""

echo ""
echo "n8n is running at http://localhost:5678"
echo ""
echo "Next steps:"
echo "  1. Open http://localhost:5678 and create your n8n account"
echo "  2. Settings -> Credentials -> Add credential -> SMTP"
echo "     Host: smtp.gmail.com  Port: 465  SSL: on"
echo "     User: your Gmail  Password: Gmail App Password"
echo "     (Google Account -> Security -> 2-Step Verification -> App Passwords)"
echo "  3. Workflows -> Import from File -> workflows/job_search_agent.json"
echo "  4. Open Send Email Digest node -> re-select the SMTP credential"
echo "  5. Test Workflow to run now, or toggle Active for daily 9am runs"
echo "  6. CSV output appears in ./output/jobs_YYYY-MM-DD.csv after each run"
