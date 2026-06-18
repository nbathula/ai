.PHONY: install dev-backend dev-frontend test lint eval deploy-serving deploy-frontend

install:
	pip install -e ".[dev]"
	cd frontend && npm install

dev-backend:
	DATABRICKS_HOST=$$DATABRICKS_HOST \
	DATABRICKS_TOKEN=$$DATABRICKS_TOKEN \
	ANTHROPIC_API_KEY=$$ANTHROPIC_API_KEY \
	SALES_COMPANION_API_KEYS=dev-key \
	ENV=dev \
	uvicorn serving.main:app --reload --port 8000

dev-frontend:
	cd frontend && npm run dev

test:
	pytest tests/ -v --tb=short

lint:
	ruff check agents/ serving/ ingestion/ eval/ tests/
	ruff format --check agents/ serving/ ingestion/ eval/ tests/

eval:
	python eval/run_eval.py --agent all

eval-pipeline:
	python eval/run_eval.py --agent pipeline_health

eval-customer:
	python eval/run_eval.py --agent customer_health

deploy-serving:
	python scripts/deploy_serving.py --endpoint sales-companion-api

deploy-frontend:
	./scripts/deploy_frontend.sh prod

fmt:
	ruff format agents/ serving/ ingestion/ eval/ tests/
