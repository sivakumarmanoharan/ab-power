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

**Decision.** `ingestion.py` writes each API response unchanged to `landing/raw_files/api/<feed>/<feed>_<UTC run timestamp>.json`. Date-range feeds re-pull **yesterday** as well as today on every run. The API key lives in a Databricks secret scope.

Two different clocks are used deliberately:

- **File names use UTC**, so they sort correctly and never repeat or skip when local time rules change.
- **API date parameters use Alberta's calendar date** (`America/Edmonton`). AESO's `startDate` and `endDate` are Alberta dates. Serverless compute runs in UTC, and from 6 p.m. Alberta time onward UTC is already on the next day, so using the server's date would have requested a date that doesn't exist yet in Alberta on every evening run.

**Consequences.** Files are never overwritten, and each one is processed exactly once downstream. The overlap catches AESO's revisions to recent hours, at the cost of duplicates that Silver removes (ADR-009). The run fails loudly if any feed fails, so the scheduled job shows the failure, retries, and alerts. The date bug would not have appeared in daytime manual runs; it only surfaces once ingestion runs around the clock.

The script runs as a **Python script task** on serverless. Two consequences of that environment: `dbutils` must be imported explicitly (`databricks.sdk.runtime`), and `__file__` is undefined because the script is executed via `exec()`, so the script locates its own folder from `sys.argv[0]` to import its sibling API client.

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

**Decision.** Bronze and Silver are **streaming tables**. Gold tables are **materialized views**, created by reading Silver with batch reads (`spark.read`).

**Consequences.** Streaming tables process each input exactly once: Auto Loader tracks landed files, and AUTO CDC requires a streaming table as its target. Gold aggregates span all of history and must stay correct when upstream hours are revised, which is what materialized views guarantee. "Streaming" here describes how data is processed, not how often: the pipeline is triggered and stops after each run.

---

## ADR-007: UTC as the time key; local time derived, never a key

**Context.** AESO publishes timestamps in both UTC and Alberta local time (MPT). Local time is not a stable identifier: under daylight saving, one autumn hour repeats and one spring hour is skipped, and the rules themselves are set by law and can change.

**Decision.** All hourly tables are keyed on `begin_datetime_utc`. Silver keeps AESO's local time as `TIMESTAMP_NTZ`, a wall-clock value with no time zone, for analysis only. Gold derives local time from the UTC key using the `America/Edmonton` time zone, so the two can never disagree. The pipeline session time zone is pinned to UTC.

**Consequences.** The design was tested by a real rule change. Alberta adopted permanent daylight time in 2026, so clocks did not fall back on November 1, 2026, and the province now stays at UTC−6 year-round. A Gold acceptance test expected a repeated 1 a.m. hour that day and failed. Comparing against AESO's own local timestamps showed a constant six-hour offset: the data was correct and the test's assumption was out of date. Because keys are UTC, no table or code needed to change.

The conversion relies on the platform's time zone database being current; an outdated database would silently apply the old rules. Local time is therefore validated against the source's own local timestamps, not just computed. Historical data from before 2026 still contains real fall-back and spring-forward hours (for example, November 2, 2025), which UTC keys handle without duplicates.

A plain `TIMESTAMP` for local time would have been converted to UTC by Spark, and a string would have lost date functions. `TIMESTAMP_NTZ` requires the `delta.feature.timestampNtz` table feature, enabled in each table's properties.

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

**Consequences.** Each fact lives in exactly one place. A participant rename creates one new version in `dim_participant` instead of one per asset in the bridge. Generation joins to `dim_asset` only, so it is never double-counted. Reporting generation by participant requires a business rule for shared assets like NX01, to be decided in Gold. Gold will expose `dim_asset_current`, a denormalized table, so analysts don't write the three-way join themselves.

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

## ADR-014: Decoupled ingestion and pipeline schedules

**Context.** Generation is a point-in-time snapshot, so it must be captured hourly. Running the full pipeline hourly is expensive, and Free Edition caps daily serverless usage.

**Decision.** Two jobs instead of one:

| Job | Schedule (UTC) | Task | Retries |
|---|---|---|---|
| `ab_power_ingest` | every hour at :05 (`0 5 * * * ?`) | Python script `ingestion.py` | 2, 5 min apart |
| `ab_power_pipeline_refresh` | every 6 hours at :20 (`0 20 0/6 * * ?`) | Pipeline `ab_power_market_pipeline` (Bronze → Silver → Gold), no full refresh | 1, 10 min apart |

Both schedules use **UTC**, so changes to local time rules never skip or repeat a run. Both notify by email on failure. The pipeline runs at :20 so it never starts while that hour's ingestion is still writing files.

**Consequences.** The landing Volume decouples the two jobs: Auto Loader processes every file landed since the last pipeline run, so no data is lost between refreshes. Silver and Gold are up to 6 hours behind the source, which is acceptable for analytical use. Asset registry changes are timestamped at pipeline-run precision, because the snapshot flow reads only the latest asset snapshot per run. Registry changes are rare, so this is an acceptable trade-off for the cost saving.

---

## ADR-015: Spike definition: relative threshold with an absolute floor

**Context.** Question 1 asks when price spikes happen. A fixed threshold such as $500/MWh measures how expensive an hour is, not how unusual: it fires constantly in high-price periods and misses dramatic jumps in low-price periods. A purely relative threshold has the opposite weakness: when prices are very low, small moves look large (with an $8 average, 3× is only $24).

**Decision.** An hour is a **spike** when `pool_price > 3 × rolling_30day_avg` **and** `pool_price > $100/MWh`. AESO's own rolling 30-day average is the baseline. The multiplier and floor are named constants (`SPIKE_MULTIPLIER`, `SPIKE_FLOOR`) at the top of `gold.py` and appear in the table comment in Unity Catalog. An hour with no 30-day average is treated as **not a spike** (`False`, not `NULL`), so downstream logic such as "previous hour was not a spike" behaves predictably.

**Consequences.** The rule adapts to the price level of each period and ignores immaterial moves. Both thresholds are judgement calls; changing them changes every downstream result, which is why they live in one place (ADR-016).

---

## ADR-016: `fact_hourly_market` as the single source of the spike rule

**Context.** The first spike-events table joined price and load and calculated the spike rule itself. Upcoming tables (supply analysis, wind-drop analysis) need the same hourly view. Each recalculating it would define the spike rule in several places that could silently drift apart.

**Decision.** `gold.fact_hourly_market` holds one row per UTC hour with price, load, forecast error, local-time attributes and `is_spike`. It **LEFT JOINs** load onto price, so every price hour is kept even when load is missing. `agg_price_spike_events` and every later table read `is_spike` from it rather than recalculating.

**Consequences.** The spike rule exists in exactly one place, and the pipeline graph shows a chain (Silver → `fact_hourly_market` → `agg_price_spike_events`) instead of parallel copies. Acceptance tests were based on facts already verified: row count equals Silver's hour count, no duplicate hours, local time matches AESO's own local timestamps, and the known spike hours of November 1, 2026 are flagged.

---

## ADR-017: Spike events derived with gaps-and-islands

**Context.** The source contains spike *hours* but no spike *events*. Three consecutive spike hours are one event, not three.

**Decision.** Events are derived in layers: flag each hour; compare it with the previous hour (`LAG`); mark an event start where a spike hour follows a normal hour, is the first hour, or follows a **gap in the data** (previous hour not exactly one hour earlier); number events with a running count of starts; then group. Each event reports start, end (last hour's start + 1 hour), duration, peak and average price, average and peak load, and average forecast error.

**Consequences.** The gap check prevents two spikes either side of a missing hour from merging into one event. Working on continuous UTC timestamps rather than calendar dates keeps events that cross midnight UTC intact. `event_id` is a sequence number recalculated on every refresh, so it shifts if an earlier hour is revised; **`event_start_utc` is the stable identifier** and is what other tables and dashboards should reference.

---

## Known limitations

- **Generation is a point-in-time snapshot.** Its time series is only as dense as the ingestion schedule (hourly, ADR-014) and starts from the first scheduled run. Earlier history comes from the CSD backfill.
- **SCD2 history begins at the first pull.** Changes before that date are not recoverable from the API.
- **Asset changes have 6-hour precision** (ADR-014).
- **Name normalization collisions** are theoretically possible (ADR-005).
- **Local time depends on the platform's time zone database** being current (ADR-007).
- **Spike causes cannot yet be confirmed** for events before generation data begins; the planned supply tables and historical backfill address this.
