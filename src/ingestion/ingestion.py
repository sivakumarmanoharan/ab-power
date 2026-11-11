# Ingestion runner: pulls each AESO feed and lands the raw JSON in the Volume.
# One file per dataset per run, named with a UTC timestamp so nothing is overwritten.
# Runs as a Python script task in the ab_power_ingest job (serverless).

import json
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from databricks.sdk.runtime import dbutils


def _script_dir() -> str:
    """Folder containing this script. On serverless jobs the script runs via exec(),
    so __file__ is undefined and the path is taken from sys.argv[0] instead."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        return os.path.dirname(os.path.abspath(sys.argv[0]))


# Make sibling modules importable when run as a job script
sys.path.insert(0, _script_dir())

from aeso_api_client import AESOClient, ENDPOINTS

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="ab_power")
args, _ = parser.parse_known_args()

LANDING_ROOT = f"/Volumes/{args.catalog}/landing/raw_files/api"

api_key = dbutils.secrets.get(scope="ab_power", key="aeso_api_key")
client = AESOClient(api_key)

# File names use UTC so they sort correctly and never repeat across DST changes.
run_ts = datetime.now(timezone.utc)
run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")

# AESO's startDate/endDate are Alberta calendar dates. Use Alberta's date, not UTC's:
# from 17:00/18:00 local time onward, UTC is already on tomorrow's date.
# Re-pull yesterday as well as today: AESO revises recent hours. Silver deduplicates.
alberta_today = datetime.now(ZoneInfo("America/Edmonton")).date()
start, end = alberta_today - timedelta(days=1), alberta_today

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

# Fail the run loudly if any feed failed, so the job shows red, retries, and alerts fire.
failed = [d for d, s in results.items() if s.startswith("FAILED")]
if failed:
    raise RuntimeError(f"Ingestion failed for: {failed}")