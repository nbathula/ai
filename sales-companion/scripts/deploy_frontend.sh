#!/usr/bin/env bash
# Sprint 5 — Build React frontend and deploy to S3 + CloudFront
# Usage: ./scripts/deploy_frontend.sh [environment]
set -euo pipefail

ENV="${1:-prod}"
BUCKET="sales-companion-frontend-${ENV}"
DISTRIBUTION_ID="${CLOUDFRONT_DISTRIBUTION_ID:?CLOUDFRONT_DISTRIBUTION_ID env var required}"
API_KEY="${VITE_API_KEY:?VITE_API_KEY env var required}"

echo "==> Building frontend (env=${ENV})"
cd "$(dirname "$0")/../frontend"

# Write .env for this build
cat > .env.production.local <<EOF
VITE_API_KEY=${API_KEY}
EOF

npm ci --silent
npm run build

echo "==> Syncing to s3://${BUCKET}"
aws s3 sync dist/ "s3://${BUCKET}/" \
  --delete \
  --cache-control "public, max-age=31536000, immutable" \
  --exclude "index.html"

# index.html must not be cached (browsers need fresh version on deploy)
aws s3 cp dist/index.html "s3://${BUCKET}/index.html" \
  --cache-control "no-cache, no-store, must-revalidate"

echo "==> Invalidating CloudFront cache"
aws cloudfront create-invalidation \
  --distribution-id "${DISTRIBUTION_ID}" \
  --paths "/*" \
  --query "Invalidation.Id" \
  --output text

echo "✅ Frontend deployed: https://$(aws cloudfront get-distribution \
  --id "${DISTRIBUTION_ID}" \
  --query "Distribution.DomainName" \
  --output text)"

# Clean up
rm -f .env.production.local
