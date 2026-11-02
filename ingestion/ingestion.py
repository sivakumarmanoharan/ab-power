# Databricks notebook source
# Ingestion runner: pulls each AESO feed and lands the raw JSON in the Volume.
# One file per dataset per run, named with a UTC timestamp so nothing is overwritten.

# COMMAND ----------

import json
import os
from datetime import date, datetime, timedelta, timezone

from aeso_api_client import AESOClient, ENDPOINTS

LANDING_ROOT = "/Volumes/ab_power/landing/raw_files/api"

# COMMAND ----------

api_key = dbutils.secrets.get(scope="ab_power", key="aeso_api_key")
client = AESOClient(api_key)

run_ts = datetime.now(timezone.utc)
run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")

# Re-pull yesterday as well as today: AESO revises recent hours,
# and the overlap catches late corrections. Silver deduplicates.
today = date.today()
start, end = today - timedelta(days=1), today

# COMMAND ----------

results = {}
for dataset, (_, mode) in ENDPOINTS.items():
    try:
        payload = client.fetch(dataset, start, end) if mode == "range" else client.fetch(dataset)

        folder = f"{LANDING_ROOT}/{dataset}"
        os.makedirs(folder, exist_ok=True)
        path = f"{folder}/{dataset}_{run_id}.json"

        with open(path, "w") as f:
            json.dump(payload, f)

        results[dataset] = f"OK -> {path}"
    except Exception as exc:
        results[dataset] = f"FAILED: {exc}"

for dataset, status in results.items():
    print(f"{dataset:12s} {status}")

# COMMAND ----------

# Fail the run loudly if any feed failed, so the job shows red and alerts fire.
failed = [d for d, s in results.items() if s.startswith("FAILED")]
if failed:
    raise RuntimeError(f"Ingestion failed for: {failed}")