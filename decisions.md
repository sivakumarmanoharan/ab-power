# Architecture Decision Records

Each record states the context, the decision, and its consequences, including the trade-offs accepted.

---

## ADR-001: Standard Unity Catalog catalog on Default Storage

**Context.** The workspace runs on Databricks Free Edition, which has no metastore root storage and does not allow attaching your own cloud storage. Creating a catalog without a location failed. Databricks also offers Lakebase Postgres catalogs.

**Decision.** Create `ab_power` as a **Standard** catalog using **Default Storage**, through the UI. Schemas and the landing Volume are created with SQL and inherit the catalog's storage.

**Consequences.** Delta tables, Volumes, Auto Loader and declarative pipelines all work as designed. Lakebase was rejected because it serves OLTP workloads (low-latency row reads and writes for applications), not analytical pipelines. In a company setting, a platform team would normally provide a managed location on the organization's own cloud storage instead of Default Storage.

---

## ADR-002: Least-privilege access set at schema level

**Context.** The project should show a realistic governance model, not just owner access.

**Decision.** Two groups. `ab_power_analysts` can read the `gold` schema only. `ab_power_engineers` can build in `bronze`, `silver` and `gold`, and read and write the landing Volume. `landing` holds files only, so engineers get no table privileges there.

**Consequences.** Grants are made on schemas, so every future table inherits them with no further grants. Analysts never see raw or intermediate data. Volume access needs its own grants, because schema privileges cover tables, not Volumes.

---

## ADR-003: Land raw API responses as timestamped files

**Context.** The pipeline needs a replayable record of exactly what AESO returned.

**Decision.** `run_ingestion.py` writes each API response unchanged to `landing/raw_files/api/<feed>/<feed>_<UTC run timestamp>.json`. Date-range feeds re-pull **yesterday** as well as today on every run. The API key lives in a Databricks secret scope.

**Consequences.** Files are never overwritten, and each one is processed exactly once downstream. The overlap catches AESO's revisions to recent hours, at the cost of duplicates that Silver removes (ADR-009). The run fails loudly if any feed fails, so a scheduled job shows the failure.

---

## ADR-004: Bronze stores one row per file, exactly as received

**Context.** Bronze can either keep whole responses or explode them into one row per record.

**Decision.** Bronze keeps **one row per landed file**, with the payload unparsed, plus `_ingested_at`, `_source_file`, `_file_modified_at` and `_batch_id`. Each feed is read by Auto Loader with schema inference, `addNewColumns` schema evolution, and a `_rescued_data` column.

**Consequences.** Each feed nests its records differently, so exploding at Bronze would require per-feed parsing logic. Keeping Bronze raw means one generic function serves all feeds, and Silver can always be rebuilt from Bronze if its parsing logic was wrong, without going back to the API. The cost is that Bronze is awkward to query directly, which is acceptable because analysts have no access to it.

---

## ADR-005: Normalize column names at Bronze

**Context.** AESO wraps records in keys containing spaces, such as `Actual Forecast Report`. Delta rejects these names by default.

**Decision.** Recursively rename every field, including nested struct, array and map fields, by replacing invalid characters with underscores and lowercasing. The alternative was Delta column mapping, which keeps the original names.

**Consequences.** Every layer uses clean snake_case names and needs no backticks, except for `return`, which is a reserved SQL word. Bronze is no longer byte-for-byte identical to the source, which is a small, deterministic change. Known limitation: two source keys that differ only by case or spacing would clean to the same name and collide. This has not occurred with this API.

---

## ADR-006: Streaming tables for Bronze and Silver, materialized views for Gold

**Context.** Lakeflow offers both streaming tables and materialized views.

**Decision.** Bronze and Silver are **streaming tables**. Gold will be **materialized views**.

**Consequences.** Streaming tables process each input exactly once: Auto Loader tracks landed files, and AUTO CDC requires a streaming table as its target. Gold aggregates span all of history and must stay correct when upstream hours are revised, which is what materialized views guarantee. "Streaming" here describes how data is processed, not how often: the pipeline is triggered and stops after each run.

---

## ADR-007: UTC as the time key; local time as `TIMESTAMP_NTZ`

**Context.** AESO publishes timestamps in both UTC and Mountain Prevailing Time (MPT). On the November fall-back date, one local hour occurs twice.

**Decision.** All hourly tables are keyed on `begin_datetime_utc`. MPT is kept as `TIMESTAMP_NTZ`, a wall-clock value with no timezone, and is never used as a key. The pipeline session timezone is pinned to UTC.

**Consequences.** Data for November 1, 2026 confirmed it: one local hour appears twice in `silver.pool_price` as two distinct UTC hours, and both are preserved. Keying on MPT would have silently merged two different hours. A plain `TIMESTAMP` for MPT would have been converted to UTC by Spark, and a string would have lost date functions. `TIMESTAMP_NTZ` requires the `delta.feature.timestampNtz` table feature, which is enabled in each table's properties.

---

## ADR-008: Tiered data quality expectations

**Context.** Not every bad value means the same thing, and dropping data silently is as harmful as letting bad data through.

**Decision.** Every expectation has a deliberately chosen severity:

| Severity | Used when | Examples |
|---|---|---|
| **Fail** | The run is untrustworthy | Unparseable timestamp; settled price dated in the future |
| **Drop** | The row doesn't belong yet | Forecast-only hour with no settled price (it returns once the hour settles) |
| **Warn** | Possible but suspicious; keep it visible | Price outside $0–$999.99/MWh; load outside 5,000–15,000 MW; forecast miss over 10%; an hour more than 3 hours old with no actual load; generation above capability |

**Consequences.** The thresholds are wide on purpose. Alberta's load typically runs about 8,000–12,000 MW, so 5,000–15,000 catches unit and parsing errors without flagging a real cold-snap peak. Negative net generation is never dropped, because battery storage assets are legitimately negative while charging. Expectation metrics are visible per rule in the pipeline UI.

---

## ADR-009: AUTO CDC to deduplicate and apply revisions

**Context.** The overlapping ingestion window (ADR-003) delivers the same hour several times, sometimes with revised values.

**Decision.** Each Silver fact is written by an `AUTO CDC` flow as SCD Type 1, keyed on the business key and ordered by `_batch_ts`, the ingestion run time parsed from the file name. Numeric fields use `try_cast`, because AESO sends numbers as strings that are sometimes empty, and serverless runs in ANSI mode, where a plain cast would fail the run.

**Consequences.** One current row per hour, and the most recent pull always wins. Ordering by the file-name timestamp rather than the processing time is deterministic even when several files are processed in the same pipeline update.

---

## ADR-010: SCD Type 2 assets from full snapshots

**Context.** The asset list returns the complete registry on every pull, with no change feed.

**Decision.** Use `AUTO CDC FROM SNAPSHOT` with SCD Type 2. The snapshot views select **business columns only**.

**Consequences.** Each run compares the latest snapshot with the previous one and records `__START_AT` and `__END_AT` for each version. Including metadata such as `_batch_id` would make every snapshot look like a change and create a new version of every asset on every run. History starts from the first pull and cannot be backfilled from earlier dates.

---

## ADR-011: Asset-participant relationship as a normalized bridge

**Context.** The first SCD2 run failed: asset **NX01** appeared twice in one snapshot, identical except for the pool participant (CNOOC Marketing Canada and ENMAX Balzac LP).

**Decision.** The participant is a many-to-many relationship, not an asset attribute. The model is split into three SCD Type 2 tables:

- `dim_asset`: one row per asset, asset attributes only.
- `dim_participant`: one row per participant.
- `bridge_asset_participant`: asset and participant **IDs only**.

**Consequences.** Each fact lives in exactly one place. A participant rename creates one new version in `dim_participant` instead of one per asset in the bridge. Generation joins to `dim_asset` only, so it is never double-counted. Reporting generation by participant requires a business rule for shared assets like NX01, to be decided in Gold. Gold will expose a denormalized view so analysts don't write the three-way join themselves.

---

## ADR-012: Choosing the current participant name

**Context.** `dim_participant` then failed: participant **VPC2** had two names in one snapshot. Profiling found **32** participant IDs with multiple names. Most were corporate history rather than typos: Nexen → CNOOC, ATCO Power → Heartland Generation, EnCana → Ovintiv, Husky and Renaissance → Cenovus, Pengrowth → Strathcona, B.C. Power Exchange → Powerex, Power Pool of Alberta → AESO. The rest were spelling variants such as "Ltd" versus "Ltd.". Retired assets keep the name from when they retired, and **1,579 of 3,756** registered assets (about 43%) are retired.

**Decision.** For each participant ID, the name used on **non-retired** assets wins, then the name used on the most assets, then alphabetical order. Suspended and Inactive assets count as current, because they remain registered to their current participant.

**Consequences.** "Most frequent name wins" was rejected because a participant with many retired assets would get its historic name. Validation against known company history confirmed the rule: APC, NXI, PCES, PGCP, PPOA, PWX and RENA all resolve to their current names. Historic names that predate the first pull are not recoverable, but future renames are captured by SCD2.

---

## ADR-013: SCD2 tables protected from full refresh

**Context.** Fact tables can be rebuilt from Bronze at any time. SCD2 history cannot: it exists only because snapshots were compared over time.

**Decision.** `dim_asset`, `dim_participant` and `bridge_asset_participant` set `pipelines.reset.allowed = false`.

**Consequences.** A "Full refresh all" rebuilds every other table and skips these. Changing their schema now requires deliberately and temporarily lifting the protection, which makes the cost of that change visible.

---

## Known limitations

- **Generation is a point-in-time snapshot.** Its time series is only as dense as the ingestion schedule, so ingestion must run hourly for hourly renewable-share analysis. Historical generation comes from the CSD backfill.
- **SCD2 history begins at the first pull.** Changes before that date are not recoverable from the API.
- **Name normalization collisions** are theoretically possible (ADR-005).
