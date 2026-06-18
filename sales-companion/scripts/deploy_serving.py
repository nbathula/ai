"""
Sprint 5 — Deploy FastAPI app to Databricks Model Serving.
Packages the app as an MLflow pyfunc model and creates/updates
a Databricks serving endpoint.

Usage:
  python scripts/deploy_serving.py --endpoint sales-companion-api --version 0.1.0
"""

import argparse
import os
import sys
import time
from pathlib import Path

import mlflow
import mlflow.pyfunc
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    TrafficConfig,
    Route,
)

ROOT = Path(__file__).parent.parent
CATALOG = "sales"
MODEL_NAME = f"{CATALOG}.serving.sales_companion_api"


class SalesCompanionModel(mlflow.pyfunc.PythonModel):
    """MLflow pyfunc wrapper that boots the FastAPI app and handles requests."""

    def load_context(self, context):
        import sys
        sys.path.insert(0, str(ROOT))

    def predict(self, context, model_input):
        import pandas as pd
        from agents.router import route

        results = []
        for _, row in model_input.iterrows():
            try:
                result = route(
                    query=row["query"],
                    session_id=row.get("session_id", "api"),
                    user_id=row.get("user_id", "anonymous"),
                )
                results.append({
                    "response": result["response"],
                    "confidence_score": result.get("confidence_score", 0.0),
                    "groundedness_score": result.get("groundedness_score", 0.0),
                    "agent_type": result.get("agent_type", ""),
                    "trace_id": result.get("trace_id", ""),
                    "estimated_cost_usd": result.get("estimated_cost_usd", 0.0),
                })
            except Exception as e:
                results.append({
                    "response": f"Error: {e}",
                    "confidence_score": 0.0,
                    "groundedness_score": 0.0,
                    "agent_type": "error",
                    "trace_id": "",
                    "estimated_cost_usd": 0.0,
                })

        return pd.DataFrame(results)


def log_model(version: str) -> str:
    """Log the model to Unity Catalog and return the model URI."""
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment("/sales-companion/serving")

    with mlflow.start_run(run_name=f"deploy-{version}") as run:
        mlflow.log_param("version", version)

        model_info = mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=SalesCompanionModel(),
            artifacts={"app_root": str(ROOT)},
            pip_requirements=[
                f"sales-companion=={version}",
                "databricks-sdk>=0.20.0",
                "databricks-vectorsearch>=0.22",
                "langgraph>=0.0.30",
                "anthropic>=0.20.0",
                "fastapi>=0.109.0",
                "uvicorn>=0.27.0",
            ],
            registered_model_name=MODEL_NAME,
        )

    print(f"  ✓ Model logged: {model_info.model_uri}")
    return model_info.model_uri


def get_latest_model_version() -> str:
    from mlflow import MlflowClient
    client = MlflowClient(registry_uri="databricks-uc")
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    latest = max(versions, key=lambda v: int(v.version))
    return latest.version


def deploy_endpoint(endpoint_name: str, version: str):
    """Create or update the Databricks Model Serving endpoint."""
    w = WorkspaceClient()
    model_version = get_latest_model_version()

    config = EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                name=f"sales-companion-v{version.replace('.', '-')}",
                entity_name=MODEL_NAME,
                entity_version=model_version,
                workload_size="Small",        # 0–4 concurrent requests
                scale_to_zero_enabled=False,  # always warm for <3s cold start
                environment_vars={
                    "DATABRICKS_HOST": "{{secrets/sales-companion/databricks-host}}",
                    "DATABRICKS_TOKEN": "{{secrets/sales-companion/databricks-token}}",
                    "ANTHROPIC_API_KEY": "{{secrets/sales-companion/anthropic-api-key}}",
                    "VS_ENDPOINT": "sales-companion-vs",
                    "EMBED_ENDPOINT": "bge-large-en",
                    "AGENT_VERSION": version,
                },
            )
        ],
        traffic_config=TrafficConfig(
            routes=[Route(served_model_name=f"sales-companion-v{version.replace('.', '-')}", traffic_percentage=100)]
        ),
        auto_capture_config={
            "catalog_name": CATALOG,
            "schema_name": "monitoring",
            "table_name_prefix": "serving_inference",
            "enabled": True,
        },
    )

    existing = None
    try:
        existing = w.serving_endpoints.get(endpoint_name)
    except Exception:
        pass

    if existing:
        print(f"  Updating endpoint: {endpoint_name}")
        w.serving_endpoints.update_config(name=endpoint_name, served_entities=config.served_entities)
    else:
        print(f"  Creating endpoint: {endpoint_name}")
        w.serving_endpoints.create(name=endpoint_name, config=config)

    # Poll until ready
    print("  Waiting for endpoint to be ready", end="", flush=True)
    for _ in range(60):
        ep = w.serving_endpoints.get(endpoint_name)
        state = ep.state.config_update if ep.state else None
        if str(state) in ("NOT_UPDATING", "None"):
            print(" ✓")
            break
        print(".", end="", flush=True)
        time.sleep(10)
    else:
        print("\n  ⚠ Timed out waiting — check Databricks UI")

    ep = w.serving_endpoints.get(endpoint_name)
    print(f"\n✅ Endpoint ready: {endpoint_name}")
    print(f"   Model version : {model_version}")
    print(f"   State         : {ep.state}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="sales-companion-api")
    parser.add_argument("--version", default=os.environ.get("AGENT_VERSION", "0.1.0"))
    parser.add_argument("--skip-log", action="store_true", help="Skip model logging (use latest version)")
    args = parser.parse_args()

    if not args.skip_log:
        print(f"Logging model v{args.version} to Unity Catalog...")
        log_model(args.version)

    print(f"\nDeploying endpoint: {args.endpoint}")
    deploy_endpoint(args.endpoint, args.version)


if __name__ == "__main__":
    main()
