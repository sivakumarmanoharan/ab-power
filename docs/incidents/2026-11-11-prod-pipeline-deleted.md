# Incident: production pipeline deleted during migration to Asset Bundles

| | |
|---|---|
| **Date** | 2026-11-11 |
| **Severity** | High: production pipeline unavailable; SCD2 history at risk |
| **Status** | Resolved, no data lost |
| **Affected** | `ab_power_market_pipeline_new` (production), job `ab_power_pipeline_refresh` |
| **Not affected** | Landing Volume (raw files), job `ab_power_ingest` (kept running throughout) |
| **Author** | Sivakumar Manoharan |

---

## Summary

While moving the project's pipeline and jobs from UI-managed resources to a Databricks Asset Bundle, the production pipeline `ab_power_market_pipeline_new` was deleted. Its streaming tables and materialized views in `ab_power` were orphaned but not dropped. A new production pipeline was deployed from the bundle, every table was moved to it with its data, the original jobs were bound to the bundle, and a duplicate job created by an earlier deploy was removed. No data was lost, including the SCD2 asset history that cannot be rebuilt from source.

## Impact

- **Production pipeline:** unavailable until recovery; no Bronze, Silver or Gold refreshes ran in that window.
- **Data:** none lost. All `ab_power` tables survived in the pipeline's restoration period and were moved intact.
- **SCD2 history** (`silver.dim_asset`, `dim_participant`, `bridge_asset_participant`): preserved. Verified after recovery: 3,756 asset rows, `MIN(__START_AT)` = 2026-11-04 01:09:35 UTC, the original history start rather than the recovery date.
- **Ingestion:** unaffected. `ab_power_ingest` kept landing hourly files, which the recovered pipeline processed on its first run.
- **Freshness:** Silver and Gold were stale for the duration of the outage; caught up on the first run after recovery.

## Timeline (2026-11-11)

1. Asset Bundle added with `dev` and `prod` targets; dev deployed.
2. The pipeline's bundle **resource key** was changed between dev deploys. The bundle treated the new key as a new resource and created a second dev pipeline; the first one still owned the `ab_power_dev` tables.
3. Dev runs failed with *"table is already managed by pipeline f2c7813d-…"*. The leftover dev pipeline was deleted, but its tables stayed tied to it during the restoration period. Dev data was disposable, so the dev schemas were dropped and recreated.
4. A `bundle deploy -t prod` ran before the existing production resources were bound, creating a **duplicate** refresh job (`1061616564188839`).
5. During a reset of the bundle state, the **production pipeline was deleted**.
6. Detection: the production pipeline no longer appeared under Jobs & Pipelines.
7. Assessment: raw files intact; tables still present in `ab_power`; ingestion job still running. Decision: recover by moving tables, not by rebuilding.
8. Recovery executed (below). Production pipeline ran successfully on all tables.
9. Verification of SCD2 history completed.

## Root cause

**Direct cause:** a destructive bundle operation (reset/destroy) ran while production resources were in an inconsistent state: some resources unbound, a duplicate created by an earlier deploy, and the pipeline's resource key changed during the same session. *(Author: record the exact command that deleted the pipeline here.)*

**Contributing causes:**
- The resource key was not fixed before the first deploy. Changing a key makes the bundle delete one resource and create another.
- `prod` was deployed before every existing production resource was bound, so the bundle created duplicates instead of adopting the originals.
- `bundle summary -t prod` was not checked immediately before the destructive command.
- The production pipeline had no deletion guard.

## Recovery

1. Confirmed which assets survived: landing Volume intact; all tables listed in `ab_power.bronze`, `silver`, `gold`; `silver.dim_asset` queryable.
2. Deployed the bundle's prod target to create the new production pipeline. **Did not run it** (it would only fail on table ownership).
3. Moved every table to the new pipeline, SCD2 tables first:
   ```sql
   ALTER STREAMING TABLE ab_power.silver.dim_asset
     SET TBLPROPERTIES('pipelines.pipelineId' = '<new_pipeline_id>');
   -- repeated for every streaming table (ALTER STREAMING TABLE)
   -- and every Gold table (ALTER MATERIALIZED VIEW)
   ```
4. Fixed the jobs:
   ```bash
   databricks bundle deployment unbind ab_power_pipeline_refresh -t prod
   databricks bundle deployment bind   ab_power_pipeline_refresh 662238249862227 -t prod
   databricks bundle deployment bind   ab_power_ingest           342771815899687 -t prod
   databricks jobs delete 1061616564188839        # duplicate created by the early prod deploy
   databricks bundle deploy -t prod               # repoints the refresh job at the new pipeline
   ```
5. Ran the production pipeline normally, **not** as a full refresh. All tables updated.

## Verification

| Check | Result |
|---|---|
| All Bronze, Silver and Gold tables updated by the new pipeline | ✅ |
| `silver.dim_asset` row count | 3,756 (matches registered assets before the incident) |
| `MIN(__START_AT)` on `silver.dim_asset` | 2026-11-04 01:09:35 UTC: original history, not rebuilt |
| `bundle summary -t prod` | one pipeline; jobs show original IDs 342771815899687 and 662238249862227 |
| Duplicate job removed | ✅ |

## Prevention

| Action | Status |
|---|---|
| Fix the pipeline's resource key (`ab_power_market_pipeline_new`) and never change it | Done |
| Bind every existing production resource **before** the first `deploy -t prod` | Done |
| Add `lifecycle: prevent_destroy: true` to the production pipeline in `resources/pipeline.yml` | To do |
| Run `bundle summary -t <target>` before any `destroy` or `unbind`; read the destroy list before confirming | Adopted as practice |
| `dev` is the bundle's default target, so a command without `-t` can never change production | In place |
| SCD2 tables protected from full refresh (`pipelines.reset.allowed = false`, ADR-013) | In place |
| Recovery procedure documented (this file, ADR-019) | Done |

## Lessons

- **A table belongs to exactly one pipeline.** Deleting or recreating a pipeline orphans its tables; they can be moved to a new pipeline with `pipelines.pipelineId`, keeping their data.
- **Raw data outside the pipeline is the safety net.** Because the landing Volume is not owned by any pipeline, every Bronze, Silver fact and Gold table could have been rebuilt. Only SCD2 history was truly at risk.
- **Bundle resource keys are identity.** The display name can change freely; the key cannot.
- **Adopt, don't recreate.** Existing production resources are brought under a bundle with `bind`, never by deploying a fresh copy.
