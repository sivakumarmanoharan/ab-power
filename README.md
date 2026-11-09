# Alberta Power Market Lakehouse

A governed lakehouse on Databricks that ingests live data from the Alberta Electric System Operator (AESO), models it through a medallion architecture with declarative pipelines and data quality contracts, and tracks the province's generating assets and market participants as SCD Type 2 history.

**Stack:** Databricks (Free Edition, serverless) · Unity Catalog · Auto Loader · Lakeflow Declarative Pipelines · AUTO CDC · Lakeflow Jobs · Delta Lake · Python · SQL

![Pipeline graph](docs/images/04_pipeline_graph.png)

---

## Business questions

The Gold layer exists to answer these four questions. Everything upstream serves them.

1. When do pool price spikes happen, and what are demand and wind output doing at those moments?
2. What share of each hour's generation is renewable, and how is it trending month over month?
3. What is each generator's capacity factor, and which assets underperform their maximum capability?
4. How does a sharp drop in wind output in one hour affect price in the following hours?

---

## Architecture

```mermaid
flowchart LR
    A[AESO Public API] -->|ingestion.py<br/>hourly job| B[(UC Volume<br/>landing/raw_files)]
    C[AESO historical<br/>CSD files] -->|backfill| B
    B -->|Auto Loader| D[Bronze<br/>raw responses]
    D -->|expectations +<br/>AUTO CDC<br/>6-hourly job| E[Silver<br/>facts + SCD2 dims]
    E -->|materialized views| F[Gold<br/>business answers]
    F --> G[AI/BI dashboard<br/>+ Genie]
```

| Layer | Contents | Table type |
|---|---|---|
| **Landing** | Raw JSON from the API, one file per feed per run, named with a UTC run timestamp | UC Volume |
| **Bronze** | One row per landed file, payload exactly as received, plus ingestion metadata (`_ingested_at`, `_source_file`, `_batch_id`) | Streaming tables |
| **Silver** | Typed, validated, deduplicated facts; normalized SCD Type 2 asset model | Streaming tables (AUTO CDC) |
| **Gold** | Business-ready tables answering the four questions | Materialized views |

### Silver data model

| Table | Grain | Notes |
|---|---|---|
| `pool_price` | one row per settlement hour | Latest AESO revision wins |
| `alberta_internal_load` | one row per hour | Actual and forecast load (MW) |
| `generation_snapshot` | one row per asset per AESO update | Net generation, capability, fuel type |
| `dim_asset` | one row per asset version | SCD Type 2 |
| `dim_participant` | one row per participant version | SCD Type 2 |
| `bridge_asset_participant` | one row per asset–participant pair version | SCD Type 2, keys only |

### Gold data model

| Table | Grain | Answers | Status |
|---|---|---|---|
| `fact_hourly_market` | one row per UTC hour | Price, load, forecast error, local-time attributes, spike flag. The only place the spike rule is defined. | ✅ Built |
| `agg_price_spike_events` | one row per spike event | Q1: when spikes happen and what demand was doing | ✅ Built |
| `fact_hourly_generation_by_fuel` | one row per hour per fuel type | Base for Q2 | Planned |
| `fact_hourly_supply` | one row per hour | Wind, solar, gas, storage, supply cushion; Q1 causes, Q2, Q4 | Planned |
| `agg_generator_performance` | one row per asset per month | Q3: capacity factor, ranked within fuel type | Planned |
| `agg_wind_drop_price_response` | one row per sharp wind drop | Q4 | Planned |
| `dim_asset_current` | one row per asset | Current asset, owner and participants in one table for analysts | Planned |

A **spike hour** is one where the price is more than **3× AESO's rolling 30-day average** and above **$100/MWh**. The relative condition adapts to high- and low-price periods; the floor stops small moves in very cheap periods from counting. A **spike event** is a run of consecutive spike hours, derived with a gaps-and-islands pattern, because the source data contains no events.

---

## Data sources

All data is public and published by AESO.

- **AESO Public API** (`apimgw.aeso.ca`): pool price, Alberta Internal Load (actual and forecast), current supply and demand by asset, and the asset registry. A free key is available from the AESO developer portal.
- **AESO historical CSD generation files**: per-unit historical generation, used as a one-time backfill.

---

## Engineering highlights

### Data quality as contracts
Every Silver feed has expectations at three deliberate severities:

- **Fail** when the data is untrustworthy (unparseable timestamps, a settled price dated in the future).
- **Drop** rows that don't belong yet (forecast-only hours with no settled price).
- **Warn** on data that is possible but suspicious, so it stays visible instead of hidden (prices outside Alberta's $0–$999.99/MWh bounds, load outside 5,000–15,000 MW, forecast misses over 10%).

![Data quality metrics](docs/images/06_data_quality_expectations.png)

### Time zones
AESO publishes both UTC and Alberta local time. Silver is keyed on UTC, and local time is stored as `TIMESTAMP_NTZ` for analysis, never as a key. Gold derives local time from the UTC key with the `America/Edmonton` time zone, so the two can never disagree.

This mattered sooner than expected. Alberta moved to permanent daylight time in 2026, so clocks did not fall back on November 1, 2026, and the province now stays at UTC−6 year-round. A Gold acceptance test written on the old rules expected a repeated 1 a.m. hour and failed. Checking it against AESO's own local timestamps showed the data was right and the assumption was out of date. Because every table is keyed on UTC, the rule change needed no code change. Historical data from before 2026 still contains real daylight-saving transitions, which the same design handles without duplicates.

![UTC to local time across Nov 1, 2026](docs/images/07_timezone_check.png)

### Findings from real data
Two duplicate-key failures in the SCD2 flows exposed facts about the source that shaped the model:

- **One asset, several participants.** Asset NX01 is offered by two pool participants. Participant is a many-to-many relationship, not an asset attribute, so it was modeled as a bridge table.
- **Corporate history inside one snapshot.** 32 participant IDs carried more than one name, mostly from mergers and renames (Nexen → CNOOC, ATCO Power → Heartland Generation, EnCana → Ovintiv). Retired assets keep their historic names, and about 43% of registered assets are retired. The current name is chosen from non-retired assets by a deterministic rule.

![NX01 through the bridge table](docs/images/08_nx01_bridge.png)

Full reasoning for every design choice is in [docs/decisions.md](docs/decisions.md).

---

## First results

**Sunday, November 1, 2026, 4 p.m. to 9 p.m. Alberta time:** a 5-hour price spike, peaking at **$553.43/MWh** and averaging **$323.50**, several times the normal price level.

| Measure | Value |
|---|---|
| Average load during the event | 11,045 MW |
| Peak load | 11,109 MW |
| Demand forecast error | −31 MW (about 0.3% below forecast) |

Demand was normal and forecast almost exactly, so this spike was **not demand-driven**. Supply-side causes are likely: the event spans sunset, when solar output falls to zero, and evening demand rises. Generation data does not yet cover this date, so wind and supply conditions cannot be confirmed for this event. The planned `fact_hourly_supply` table and the historical backfill address this.

---

## Orchestration

Two Lakeflow Jobs, decoupled by the landing Volume:

| Job | Schedule (UTC) | Does |
|---|---|---|
| `ab_power_ingest` | every hour at :05 | Pulls all four AESO feeds and lands raw JSON |
| `ab_power_pipeline_refresh` | every 6 hours at :20 | Runs Bronze → Silver → Gold over everything landed since the last run |

Ingestion is cheap and runs hourly so no generation snapshot is missed. The pipeline is the expensive step and runs less often without losing data, because Auto Loader picks up every file landed in between. Schedules use UTC, so changes to local time rules never skip or repeat a run. Both jobs retry automatically and alert by email on failure.

![Ingestion job run history](docs/images/11_job_runs.png)

---

## Repository structure

```
├── sql/
│   ├── 00_catalog_setup.sql      # schemas and landing Volume (catalog created via UI with Default Storage)
│   └── 01_grants.sql             # least-privilege access model
├── src/
│   ├── ingestion/
│   │   ├── aeso_api_client.py    # API client with retries and backoff
│   │   └── ingestion.py          # job script: lands raw JSON in the Volume
│   └── pipelines/
│       ├── bronze.py             # Auto Loader streaming tables
│       ├── silver.py             # expectations, AUTO CDC, SCD2 model
│       └── gold.py               # materialized views answering the business questions
├── docs/
│   ├── decisions.md              # architecture decision records
│   └── images/                   # screenshots used in this README
└── README.md
```

---

## Governance

Unity Catalog catalog `ab_power` with schemas `landing`, `bronze`, `silver` and `gold`. Access follows least privilege:

- **Analysts** can read the Gold layer only.
- **Engineers** can build and maintain all layers and read and write the landing Volume.

Grants are set at schema level, so every future table inherits them. See [sql/01_grants.sql](sql/01_grants.sql).

![Unity Catalog lineage](docs/images/10_lineage.png)

---

## How to run

1. Create a Databricks workspace (Free Edition works) and a catalog named `ab_power` using Default Storage.
2. Run `sql/00_catalog_setup.sql` and `sql/01_grants.sql` on a SQL warehouse.
3. Register for an AESO API key and store it in a secret scope:
   ```bash
   databricks secrets create-scope ab_power
   databricks secrets put-secret ab_power aeso_api_key --string-value "<your key>"
   ```
4. Create a Lakeflow Declarative Pipeline with source folder `src/pipelines/`, default catalog `ab_power`, serverless compute, and the configuration `spark.sql.session.timeZone = UTC`.
5. Create the two jobs described under [Orchestration](#orchestration):
   - `ab_power_ingest`: Python script task on `src/ingestion/ingestion.py`, serverless, cron `0 5 * * * ?` (UTC), 2 retries.
   - `ab_power_pipeline_refresh`: Pipeline task on the pipeline above, cron `0 20 0/6 * * ?` (UTC), 1 retry.
6. Click **Run now** on the ingestion job, then on the pipeline job, to load the first data without waiting for the schedule.

---

## Roadmap

- [x] Unity Catalog setup and least-privilege grants
- [x] API ingestion into a UC Volume
- [x] Bronze: incremental Auto Loader ingestion
- [x] Silver: expectations, AUTO CDC deduplication, normalized SCD2 asset model
- [x] Scheduled jobs: hourly ingestion, 6-hourly pipeline refresh
- [ ] Gold: materialized views for the four business questions
  - [x] `fact_hourly_market` and `agg_price_spike_events` (Q1)
  - [ ] `dim_asset_current`
  - [ ] `Supply and generation tables` (Q1 causes, Q2)
  - [ ] `agg_generator_performance` (Q3)
  - [ ] `agg_wind_drop_price_response` (Q4)
- [ ] Historical backfill: price and load from the API; generation from CSD files
- [ ] Delta deep dive: time travel, `RESTORE`, Change Data Feed
- [ ] Cost and reliability monitoring with system tables
- [ ] CI/CD with Databricks Asset Bundles and GitHub Actions
- [ ] AI/BI dashboard and Genie space

---

## Author

**Sivakumar Manoharan**, Data Engineer, Calgary, AB
[LinkedIn](https://www.linkedin.com/in/sivakumarmano/) · [GitHub](https://github.com/sivakumarmanoharan)
