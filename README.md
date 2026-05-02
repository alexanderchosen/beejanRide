# beejanRide — dbt Analytics Engineering Project

> A production-grade dbt project modelling trip, driver, rider, and payment data for a fast-growing UK mobility startup operating across 5 cities. Built on dbt-core 1.11.6, BigQuery, and Airbyte.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Entity Relationship Diagram](#entity-relationship-diagram)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [Layer-by-Layer Breakdown](#layer-by-layer-breakdown)
- [Business Metric Definitions](#business-metric-definitions)
- [Data Quality & Testing](#data-quality--testing)
- [Materialisation Strategy](#materialisation-strategy)
- [Design Decisions](#design-decisions)
- [Tradeoffs](#tradeoffs)
- [dbt Docs & Lineage](#dbt-docs--lineage)
- [Getting Started](#getting-started)
- [Sample Analytical Queries](#sample-analytical-queries)
- [Future Improvements](#future-improvements)

---

## Project Overview

beejanRide is a UK mobility startup running ride-hailing operations across 5 cities. The project covers the full analytics engineering lifecycle: source freshness monitoring, staging, business logic computation, a star schema core, domain-specific mart models, and SCD Type 2 historical tracking of driver records.

**What this project answers:**
- Which cities and drivers are generating the most revenue?
- Where are trips completing without a successful payment?
- Which riders have the highest lifetime value, and how do we retain them?
- How have driver statuses and vehicle assignments changed over time?
- Where are payment integrity issues concentrated?

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| dbt-core | 1.11.6 | Transformation framework |
| dbt-bigquery | 1.11.0 | BigQuery adapter |
| dbt-utils | 0.9.7 | Generic test helpers |
| Google BigQuery | — | Data warehouse |
| Airbyte | — | CDC ingestion from PostgreSQL |
| PostgreSQL | — | Production source database |

---

## Architecture

The project follows dbt's recommended layered architecture. Each layer has a single, clearly defined responsibility. Models only reference layers at the same level or below — never upward.

> **architecture_diagram.png**
>
> `![Architecture Diagram](docs/images/Architectural_Diagram.drawio.png)`


## Entity Relationship Diagram

The star schema centres on `fct_trips` as the primary fact table. `fct_payments` is a subordinate fact linked to trips. Three dimensions (`dim_cities`, `dim_drivers`, `dim_riders`) provide all descriptive context.

> **ERD**
>
> `![ERD](docs/images/ERD.drawio.png)`

**Key relationships:**
- `dim_cities` → `dim_drivers` (one city, many drivers)
- `dim_cities` → `fct_trips` (one city, many trips)
- `dim_drivers` → `fct_trips` (one driver, many trips)
- `dim_riders` → `fct_trips` (one rider, many trips)
- `fct_trips` → `fct_payments` (one trip, many payment events)
- `dim_drivers` → `drivers_snapshot` (one driver, many historical records)

---

## Data Flow

> **DATA FLOW DIAGRAM**
>
> `![Data Flow](docs/images/DATA_FLOW.drawio.png)`

The data flow is deliberately linear. Each layer acts as a quality checkpoint before the next one builds.

1. **Source freshness** is checked before anything runs. If `trips_raw` is more than 2 hours old, the pipeline errors and stops. This prevents `driver_metrics` and `rider_metrics` from building on stale `trips_metrics` data.

2. **Staging** materialises as views — no storage cost, always reflects the latest source data. Type casting and column renaming only.

3. **Intermediate** is where business logic lives. `trips_metrics` runs as an incremental merge on `trip_id`. Since trips can change status after the fact (e.g. a payment failure surfacing on a previously completed trip), merge is the correct strategy — not append.

4. **Core and marts** build only after all tests pass. Incremental facts benefit from the quality gates catching upstream issues before they propagate.

---

---

## Layer-by-Layer Breakdown

### Staging

One model per source table. The contract is simple: clean up the raw data, cast types correctly, rename columns to a consistent convention, and stop. Nothing else happens here.

| Model | Source Table | Notes |
|---|---|---|
| `stg_cities` | `cities_raw` | 5 rows — one per city |
| `stg_drivers` | `drivers_raw` | `vehicle_id` cast to STRING |
| `stg_driver_status_events` | `driver_status_events_raw` | Used to compute online hours in intermediate |
| `stg_payments` | `payments_raw` | Raw payment events, no deduplication at this layer |
| `stg_riders` | `riders_raw` | `is_referred_rider` derived from `referral_code IS NOT NULL` |
| `stg_trips` | `trips_raw` | `vehicle_id` cast to STRING |

All staging models materialise as **views**. No storage cost, always up to date.

---

### Intermediate

This is the engine room. All business logic lives here and only here. The three models have distinct responsibilities:

**`trips_metrics`** — the most important model in the project. One row per trip, enriched with:
- `net_revenue` — computed via the `calculate_net_revenue` macro (`amount - fee`)
- Fraud flags: `duplicate_payment_flag`, `missing_payment_flag`, `invalid_payment_amount_flag`, `failed_payment_on_completed_trip_flag`, `extreme_surge_flag`
- `trip_duration_minutes` — computed via the `calculate_duration_minutes` macro
- `payment_status` — joined from `stg_payments` via LEFT JOIN (null-safe throughout)

Materialised as **incremental (merge on `trip_id`)** because trips can change status after the fact — a payment failure can surface hours after a trip completed.

**`driver_metrics`** — one row per driver, aggregated across all their trips. Includes lifetime trips, cancellation rates, net revenue earned, online hours (computed using `LEAD()` on status events), productivity score, and payment integrity signals. Materialised as a **full table** because aggregate correctness requires a complete pass — incremental aggregations risk stale counts when historical trips update.

**`rider_metrics`** — one row per rider including all riders with zero trips (via LEFT JOIN). Lifetime value, average spend, completion rate, most frequent city. Materialised as a **full table** for the same reason as `driver_metrics`.

---

### Core (Star Schema)

The core layer implements a standard star schema. Dimension tables carry descriptive attributes. Fact tables carry measures and foreign keys only — no dimension attributes are embedded in facts.

**Dimensions:**
- `dim_cities` — 5 rows, adds `days_since_launch`
- `dim_drivers` — all drivers LEFT JOIN `driver_metrics`, adds `driver_tier` (Bronze/Silver/Gold/Platinum based on lifetime trips)
- `dim_riders` — all riders LEFT JOIN `rider_metrics`, adds `rider_tier` and `ltv_segment`

**Facts:**
- `fact_trips` — one row per trip, FK keys only, pre-extracted date parts for partitioning. Incremental merge on `trip_id`.
- `fact_payments` — one row per payment event, not deduplicated. Preserves raw payment history for reliability and fraud investigation. Incremental merge on `payment_id`.

Note: `net_revenue` does not appear on `fact_payments`. Revenue is a trip-grain measure. Adding it to the payment fact would be the wrong grain and would mislead any analyst joining the two.

---

### Marts

Mart models are pre-aggregated, business-domain-specific, and designed to be queried directly by dashboards without any further joins.

**Finance (`tag: finance`)**
- `mart_daily_revenue` — daily revenue by city. Incremental merge on `[trip_date, city_id]`. The right grain for time-series dashboards.
- `mart_city_profitability` — all-time profitability per city including `revenue_per_day_since_launch`. Full table — only 5 cities, rankings require full history.

**Operations (`tag: operations`)**
- `mart_driver_leaderboard` — city-level driver rankings by net revenue, using `RANK()`. Full table — rankings require complete population.
- `mart_rider_ltv` — rider lifetime value with `PERCENT_RANK()` for LTV percentile and segment assignment. Full table — `PERCENT_RANK()` requires full population.

**Fraud (`tag: fraud`)**
- `mart_payment_reliability` — joins all fraud signals from `fact_trips` and `fact_payments` with dimensional context. Full table — fraud investigation needs complete history.
- `mart_fraud_monitoring` — incremental, filters only flagged trips, adds `total_flags` triage score. Designed as an operational queue for the fraud team.

---

### Snapshot

**`drivers_snapshot`** — SCD Type 2 snapshot of `stg_drivers` using the `timestamp` strategy on `updated_at`. Tracks changes to driver status, vehicle assignment, and rating over time. `invalidate_hard_deletes: true` closes records when a driver is deleted from the source.

Point-in-time join pattern:
```sql
WHERE dbt_valid_from <= event_time
  AND (dbt_valid_to > event_time OR dbt_valid_to IS NULL)
```

---

### Macros

| Macro | Purpose |
|---|---|
| `calculate_net_revenue(amount, fee)` | Centralises `amount - fee`. Net revenue can be negative when `fee > amount` — this is intentional and preserved as a fraud signal. |
| `calculate_duration_minutes(start_ts, end_ts)` | Centralises `TIMESTAMP_DIFF` logic for trip duration. |
| `duplicate_payment_flag(trip_id)` | Returns 1 if a `trip_id` appears more than once in `stg_payments`. |

All three macros exist for a single reason: the same logic should be written once. If the definition of `net_revenue` changes, it changes in one place.

---

## Business Metric Definitions

These definitions are the source of truth. They are also documented inline in schema yml files so they appear in the dbt docs site.

| Metric | Definition | Model |
|---|---|---|
| `net_revenue` | `amount - fee`. Can be negative when processing fee exceeds trip amount — preserved as fraud signal. | `trips_metrics`, `fct_trips` |
| `rider_lifetime_value` | Sum of `net_revenue` across all completed trips for a rider. | `rider_metrics`, `dim_riders` |
| `driver_lifetime_trips` | Count of all trips assigned to a driver regardless of status. | `driver_metrics`, `dim_drivers` |
| `total_online_hours` | Computed from `stg_driver_status_events` using `LEAD()` to pair online/offline events. Measures actual time available, not shift length. | `driver_metrics` |
| `revenue_per_day_since_launch` | `total_net_revenue / days_since_launch`. Normalises revenue across cities that launched at different times. | `mart_city_profitability` |
| `driver_level` | Bronze (< 50 trips) / Silver (50–199) / Gold (200–499) / Platinum (500+). Based on `driver_lifetime_trips`. | `dim_drivers` |
| `rider_level` | Standard / Regular (5+ trips) / Frequent (20+ trips) / VIP (50+ trips or top 10% LTV). | `dim_riders` |
| `ltv_percentile` | `PERCENT_RANK()` of rider LTV across all riders. Requires full population — this is why `mart_rider_ltv` is a full table refresh. | `mart_rider_ltv` |
| `total_flags` | Sum of all fraud flag columns on a trip. Acts as a triage score in `mart_fraud_monitoring`. | `mart_fraud_monitoring` |

---

## Data Quality & Testing

### Generic Tests (column-level)

Applied across all layers via schema yml files.

| Test | Purpose |
|---|---|
| `not_null` | Ensures required columns are always populated |
| `unique` | Enforces primary key integrity on all grain columns |
| `relationships` | Validates foreign key integrity across all FK columns |
| `accepted_values` | Restricts categorical columns to known valid values |

### Custom Tests (model-level, singular)

Three custom singular tests run on `trips_metrics` after the intermediate layer builds. They live in the `tests/` folder as plain SQL files. The convention is: the query returns the rows that violate the rule. Zero rows = pass.

**`no_negative_revenue`**
```sql
select * from {{ ref('trips_metrics') }}
where net_revenue < 0
  and invalid_payment_amount_flag = 0
```
Why the flag condition? Negative `net_revenue` on `invalid_payment_amount_flag = 1` trips is intentional — it is preserved as a fraud signal. This test only catches *unexpected* negative revenue.

**`trip_duration_positive`**
```sql
select * from {{ ref('trips_metrics') }}
where status = 'completed'
  and trip_duration_minutes <= 0
```
Non-completed trips are excluded — cancelled and no-show trips legitimately have zero duration.

**`completed_trip_must_have_successful_payment`**
```sql
select * from {{ ref('trips_metrics') }}
where status = 'completed'
  and coalesce(payment_status, 'missing') != 'success'
```
`COALESCE` makes this null-safe. A completed trip with a missing payment record fails the same as one with an explicit failed payment.

### Source Freshness

`trips_raw` has a 2-hour error threshold. If Airbyte hasn't landed new data within 2 hours, the pipeline stops before any dbt models run.

```yaml
config:
  loaded_at_field: updated_at
  freshness:
    warn_after: {count: 1, period: hour}
    error_after: {count: 2, period: hour}
```

### Recency Guard

A `dbt_utils.recency` test on `trips_metrics` ensures it has been updated within 2 hours before `driver_metrics` and `rider_metrics` consume it. This prevents those two full-table models from building on a stale incremental base.

---

## Materialisation Strategy

| Model | Materialisation | Why |
|---|---|---|
| All staging | view | No storage cost, always reflects latest source |
| `trips_metrics` | incremental (merge) | High volume; trips change status retrospectively |
| `driver_metrics` | table | Aggregate correctness; incremental aggregations risk stale counts |
| `rider_metrics` | table | Full LEFT JOIN on all riders required |
| `dim_*` | table | Full population always needed for FK integrity |
| `fct_trips` | incremental (merge) | High volume; status changes need overwrite not append |
| `fct_payments` | incremental (merge) | Append-style, merge guards against Airbyte duplicates |
| `daily_revenue_dashboard` | incremental (merge) | Append by date; retrospective updates possible |
| `city_level_profitability` | table | Only 5 cities; rankings need full history |
| `driver_leaderboard` | table | `RANK()` requires full population |
| `rider_ltv_analysis` | table | `PERCENT_RANK()` requires full population |
| `payment_rel_report` | table | Fraud investigation needs complete history |
| `fraud_monitor` | incremental (merge) | Event-based; retrospective flags possible |

**Full refresh when needed:**
```bash
dbt build --full-refresh --select trips_metrics
```

---

## Design Decisions

### Why all business logic lives in intermediate

The intermediate layer is the only place business logic exists. `driver_metrics` and `rider_metrics` both pull from `trips_metrics` rather than reaching back to staging directly. This means `net_revenue` and fraud flags are computed exactly once. If the definition changes, it changes in one model and propagates everywhere. Bypassing `trips_metrics` and re-implementing the same logic in two places would risk silent inconsistency — the worst kind of data quality issue.

### Why there is no standalone `int_payments` model

Payment logic (deduplication, the `duplicate_payment_flag` macro) lives inside `trips_metrics` CTEs because payments only make sense in the context of a trip. A standalone `int_payments` model would only be justified if multiple downstream consumers needed the same payment logic independently. Right now they don't — everything flows through `trips_metrics`.

### Why `fact_payments` is not deduplicated

`fact_payments` preserves raw payment events, including duplicates. Deduplication at the fact table level would destroy the evidence trail that the fraud team needs. The `duplicate_payment_flag` column identifies duplicates without removing them. Analysts querying revenue should use `fact_trips`, not `fact_payments`.

### Why `net_revenue` is not floored at zero

Negative `net_revenue` (when processing fee exceeds trip amount) is deliberately preserved. It is a fraud signal, not a data error. Flooring it at zero would hide the signal from `payment_rel_report` and `fraud_monitor`, which is the opposite of what we want.

### Star schema: FK keys only in facts

Fact tables carry foreign keys and measures only. No dimension attributes are embedded in facts. This prevents denormalisation and ensures that when a dimension attribute changes (e.g. a driver's tier is upgraded), the fact table doesn't need to be updated — the join to the dimension gives the current value automatically.

### SCD Type 2: timestamp strategy

`drivers_snapshot` uses the `timestamp` strategy on `updated_at`. This means dbt compares the `updated_at` timestamp on each run against the last known `updated_at` for that driver — if it has advanced, a new snapshot row is created. The alternative is the `check` strategy with explicit `check_cols` which compares column values directly. The `timestamp` strategy is simpler but depends on `updated_at` being reliably maintained by Airbyte. If that assumption ever becomes unreliable, switching to `check` strategy is the mitigation.

---

## Tradeoffs

### Incremental vs full refresh

Incremental models (merge strategy) are faster and cheaper but introduce complexity. A failed run can leave the incremental state inconsistent. The mitigation is `dbt build --fail-fast` in CI so a failing upstream model stops the build before downstream incrementals process partial data. When in doubt, `dbt build --full-refresh` is always available as a reset.

### trips_metrics as incremental dependency

`driver_metrics` and `rider_metrics` are full table refreshes that aggregate from `trips_metrics`, which is incremental. If `trips_metrics` ever falls behind, the aggregation models will silently under-count. The `dbt_utils.recency` guard on `trips_metrics` is the safeguard — it blocks `driver_metrics` and `rider_metrics` from building if `trips_metrics` is stale.

### Timestamp snapshot strategy vs check strategy

The `timestamp` strategy for `drivers_snapshot` is clean and fast but depends entirely on Airbyte maintaining `updated_at` correctly. If a source system update occurs without touching `updated_at`, the snapshot will miss it. This is an acceptable tradeoff for now — `check` strategy with explicit `check_cols` is the documented fallback if this becomes a concern.

### Schema naming with target prefix

dbt appends the `+schema` value to the target schema with an underscore by default. If your BigQuery target schema is `beejanride`, datasets will be named `beejanride_staging`, `beejanride_intermediate`, etc. If you want exact schema names without the prefix, a custom `generate_schema_name` macro is needed. This is a deliberate dbt convention and the prefix approach is kept here for simplicity.

### No `int_payments` model

As noted in design decisions, payment logic lives inside `trips_metrics`. The tradeoff is that if a future use case needs payment logic independently of trips (e.g. a subscription payment model), refactoring will be needed. The current design optimises for simplicity over extensibility at this stage.

---

## dbt Docs & Lineage

All models, columns, and business metrics are documented in schema yml files across every layer. The dbt docs site surfaces this documentation alongside the interactive lineage graph.

**Generate and serve the docs site:**
```bash
dbt docs generate
dbt docs serve
```

This opens a local web server at `http://localhost:8080`. The lineage graph shows the full DAG from sources through staging, intermediate, core, and marts.


> `![dbt Lineage Graph](docs/images/dbt-lineage-graph.png)`


## Getting Started

### Prerequisites

- dbt-core 1.11.6
- dbt-bigquery 1.11.0
- A BigQuery project with the `beejanride` source schema populated via Airbyte
- A `profiles.yml` configured for BigQuery authentication

### Installation

```bash
# Clone the repo
git clone https://github.com/alexanderchosen/beejanRide.git
cd beejanRide

# Install dbt packages
dbt deps

# Verify connection
dbt debug
```

### Running the Project

```bash
# Check source freshness first
dbt source freshness

# Run and test everything
dbt build

# Run a specific layer
dbt build --select staging
dbt build --select intermediate
dbt build --select marts

# Run by domain tag
dbt build --select tag:finance
dbt build --select tag:fraud

# Full refresh an incremental model
dbt build --full-refresh --select trips_metrics

# Run the snapshot
dbt snapshot

# Generate docs
dbt docs generate && dbt docs serve
```

### Running Tests Only

```bash
# All tests
dbt test

# Custom tests only
dbt test --select negative_revenue trip_duration successful_payment_trip

# Tests on a specific model
dbt test --select trips_metrics
```

---

## Sample Analytical Queries

These queries demonstrate the kind of insights the mart layer is designed to answer. All of them hit a single pre-aggregated table — no runtime joins needed.

### 1. Revenue trend by city over the last 30 days

```sql
SELECT
    city_id,
    trip_date,
    total_net_revenue,
    total_trips,
    avg_revenue_per_trip
FROM `beejanride_finance.mart_daily_revenue`
WHERE trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY city_id, trip_date
```

---

### 2. Which city is the most profitable, adjusted for time since launch?

```sql
SELECT
    city_name,
    total_net_revenue,
    days_since_launch,
    revenue_per_day_since_launch,
    total_trips
FROM `beejanride_finance.mart_city_profitability`
ORDER BY revenue_per_day_since_launch DESC
```

---
### 1. City Level Profitability

```sql
SELECT *
FROM `beejanride-488715.beejanride_488715_beejanRide_dataset_finance.city_level_profitability`
```

### 2. Daily revenue dashboard by trip_id

```sql
SELECT *
FROM `beejanride-488715.beejanride_488715_beejanRide_dataset_finance.daily_revenue_dashboard`
ORDER BY trip_date
```

### 3. Fraud monitor - Driver under monitoring

```sql
SELECT driver_id
FROM `beejanride-488715.beejanride_488715_beejanRide_dataset_fraud.fraud_monitor`
```

### 4. Net revenue grouped by city_id, driver_id

```sql
SELECT net_revenue, driver_id, city_id
FROM `beejanride-488715.beejanride_488715_beejanRide_dataset_fraud.payment_rel_report`
GROUP BY net_revenue, city_id, driver_id
```

### 5. Driver rating on leaderboard showing city_name and rating in descending order

```sql
SELECT driver_id, rating, driver_level, city_name
FROM `beejanride-488715.beejanride_488715_beejanRide_dataset_operations.driver_leaderboard`
ORDER BY rating DESC

```

### 6. Rider LTV Analysis

```sql
SELECT rider_id, rider_level, rider_lifetime_value
FROM `beejanride-488715.beejanride_488715_beejanRide_dataset_operations.rider_ltv_analysis`
GROUP BY rider_id, rider_level, rider_lifetime_value
ORDER BY rider_lifetime_value DESC
```

---

### 7. How has a driver's status changed over time? (SCD Type 2 query)

```sql
SELECT
    driver_id,
    driver_status,
    vehicle_id,
    rating,
    dbt_valid_from,
    dbt_valid_to
FROM `beejanride_snapshots.drivers_snapshot`
WHERE driver_id = 'D001'
ORDER BY dbt_valid_from
```

---

## Future Improvements

These are things we would build next if this were a production system continuing to grow.

**Testing & Observability**
- Add `dbt_utils.equal_rowcount` between `stg_trips` and `trips_metrics` to catch silent row drops in the incremental

**Modelling**
- Build a `dim_vehicles` dimension — vehicle data currently lives as an attribute on `dim_drivers`, which prevents independent vehicle-level analysis
- Extend `drivers_snapshot` to cover riders — rider preferences and account status also change over time
- Add a `mart_referral_attribution` model — `is_referred_rider` exists in `dim_riders` but referral programme effectiveness is not yet measured

**Infrastructure**
- Evaluate moving `driver_metrics` and `rider_metrics` to incremental once volume justifies it — using windowed aggregation patterns rather than full scans

**Documentation**
- Add `meta: owner:` blocks to staging and intermediate schema yml files (currently only on marts)
- Write exposures yml to document which dashboards consume which mart models — this makes the lineage graph show BI tools as downstream nodes

---

*Built with dbt-core 1.11.6 · BigQuery · Airbyte · dbt-utils 0.9.7*


---
---

---

## Orchestration with Apache Airflow for BeejanRide Project

BeejanRide now runs end-to-end automatically. Apache Airflow orchestrates the complete ELT pipeline — triggering Airbyte Cloud to sync raw data into BigQuery, running all dbt transformation layers in order, enforcing data quality gates, and sending email alerts on success or failure. Nothing runs manually in production.

---

### Orchestration Tech Stack

| Tool | Version | Role |
|---|---|---|
| Apache Airflow | 3.1.8 | Pipeline orchestration and scheduling |
| apache-airflow-providers-airbyte | 5.4.1 | Airbyte Cloud integration |
| Airbyte Cloud | — | Scheduled CDC sync (set to Manual, triggered by Airflow) |
| Python | 3.12 | DAG runtime |

---

### Updated Architecture

The architecture now includes an orchestration layer sitting above the entire ELT stack. Airflow is the single entry point — nothing runs unless Airflow triggers it.


```
Apache Airflow Scheduler (every 2 hours)
              │
              ▼
    ┌─────────────────────┐
    │  Airbyte Cloud Sync │  PostgreSQL → BigQuery (CDC)
    └─────────────────────┘
              │
              ▼
    ┌─────────────────────┐
    │  dbt source         │  trips_raw freshness check (< 2 hrs)
    │  freshness check    │
    └─────────────────────┘
              │
              ▼
    ┌─────────────────────┐
    │  dbt build staging  │  6 views + column tests
    └─────────────────────┘
              │
              ▼
    ┌─────────────────────┐
    │  dbt build          │  trips_metrics (incremental)
    │  intermediate       │  driver_metrics, rider_metrics (table)
    └─────────────────────┘
              │
              ▼
    ┌─────────────────────┐
    │  dbt test           │  Quality gate — custom tests + recency guard
    │  intermediate       │  Pipeline stops here if any test fails
    └─────────────────────┘
              │
              ▼
    ┌─────────────────────┐
    │  dbt build core     │  dim_* (table) + fct_* (incremental)
    └─────────────────────┘
              │
       ┌──────┼──────┐
       ▼      ▼      ▼         ← parallel execution
   Finance  Ops   Fraud
       └──────┼──────┘
              │
              ▼
    ┌─────────────────────┐
    │  dbt snapshot       │  SCD Type 2 — drivers_snapshot
    └─────────────────────┘
              │
              ▼
    ┌─────────────────────┐
    │  Email notification │  Success alert
    └─────────────────────┘
```

---

### DAG Structure

The project contains two DAGs:

**`beejanride_elt_pipeline`** — the production pipeline. Runs automatically every 2 hours via cron schedule `0 */2 * * *`. This schedule is aligned deliberately with the 2-hour source freshness error threshold on `trips_raw` and the `dbt_utils.recency` guard on `trips_metrics` — a successful pipeline run always satisfies both guards.

**`beejanride_backfill`** — a manual repair DAG with schedule set to daily. Skips Airbyte (raw data already in BigQuery) and the snapshot (SCD Type 2 cannot replay history).

---

### Pipeline Folder Structure

```
beejanRide/
└── airflow/
    └── dags/
        ├── beejanride_elt_dag.py        ← production ELT pipeline
        └── beejanride_backfill_dag.py   ← manual backfill DAG
```

---

### Airflow Setup

#### Prerequisites

- Python 3.12
- A dedicated virtual environment for Airflow (separate from the dbt environment)
- Airflow home directory: `~/airflow`

---

### Connections Required

Set up the following connections in **Admin → Connections → + Add**:

**Airbyte Cloud connection:**

```
Conn Id:       airbyte_cloud_conn
Conn Type:     Airbyte
Server URL:    https://api.airbyte.com/v1/
Client ID:     your-client-id      (from Airbyte Cloud → Settings → Applications)
Client Secret: your-client-secret
Token URL:     v1/applications/token
```

> Note: In Airflow 3, the Client ID and Client Secret fields map to the Login and Password fields in the connection form due to a known display issue. The provider reads them correctly regardless of the label shown.

**SMTP Email connection:**

```
Conn Id:   smtp_coonect
Conn Type: SMTP
Host:      smtp.gmail.com
Port:      465
Login:     your@gmail.com
Password:  your-gmail-app-password
```

---

### Variables Required

Set up the following in **Admin → Variables → + Add**:

| Key | Value |
|---|---|
| `airbyte_connection_id` | Your Airbyte connection UUID (from the Airbyte Cloud connection URL) |

**Finding your Airbyte connection UUID:** log into `cloud.airbyte.com` → Connections → click your connection → copy the UUID from the browser URL between `/connections/` and `/status`.

---

### Before Running

**Set your Airbyte Cloud sync schedule to Manual.** In Airbyte Cloud: Connections → your connection → Settings → Schedule → Manual. This ensures Airflow is the sole trigger for all syncs. If left on an automatic schedule, Airbyte will sync independently of Airflow causing concurrent writes to BigQuery.

**Update the constants at the top of each DAG file:**

```python
AIRBYTE_CONNECTION_ID = "your-uuid-here"
ALERT_EMAIL           = "your@gmail.com"
DBT_PROJECT_DIR       = "/home/yourname/projects/beejanRide"
DBT_PROFILES_DIR      = "/home/yourname/.dbt"
```

**Verify the DAGs parse correctly:**

```bash
source ~/venvs/airflow-env/bin/activate
airflow dags list
# Both beejanride_elt_pipeline and beejanride_backfill should appear
```

---

### Running the Pipeline

```bash
# Trigger a manual run of the production pipeline
airflow dags trigger beejanride_elt_pipeline

# Trigger a single backfill run via the UI
# DAGs → beejanride_backfill → Trigger DAG

# Trigger a backfill across a date range via CLI
airflow dags backfill beejanride_backfill \
    --start-date 2025-04-30 \
    --end-date   2025-05-02
```

---

### Task Dependency Design

Each task in the production DAG represents a distinct pipeline phase with a single responsibility. The separation is intentional — when a task fails, the Airflow UI immediately identifies which phase failed without requiring log inspection.

| Task | Phase | Purpose |
|---|---|---|
| `trigger_airbyte_sync` | Ingestion | Calls Airbyte Cloud API to start sync |
| `wait_for_airbyte_sync` | Ingestion | Polls until sync completes (mode=reschedule) |
| `dbt_source_freshness` | Freshness | Fails pipeline if trips_raw > 2 hours old |
| `dbt_build_staging` | Transformation | 6 staging views + column tests |
| `dbt_build_intermediate` | Transformation | Business logic layer (incremental + tables) |
| `dbt_test_intermediate` | Quality Gate | Custom tests + recency guard — blocks marts if failed |
| `dbt_build_core` | Transformation | Star schema dims and facts |
| `dbt_build_finance` | Transformation | Finance mart models (parallel) |
| `dbt_build_operations` | Transformation | Operations mart models (parallel) |
| `dbt_build_fraud` | Transformation | Fraud mart models (parallel) |
| `dbt_run_snapshot` | Snapshot | SCD Type 2 on drivers |
| `email_success_notification` | Alerting | Success email when pipeline completes |

The three mart tasks (`dbt_build_finance`, `dbt_build_operations`, `dbt_build_fraud`) run in parallel after `dbt_build_core` completes. This is the key performance optimisation — parallel execution reduces total mart build time significantly compared to running them sequentially.

The `dbt_test_intermediate` task acts as a hard quality gate. If any of the three custom tests or the recency guard fails, all downstream tasks (`dbt_build_core` and everything below it) are skipped. Stale mart data is preferable to corrupt mart data reaching dashboards.

---

### Failure Handling and Retries

Every task is configured with the following defaults:

```python
"retries":           2,
"retry_delay":       timedelta(minutes=5),
"email_on_failure":  True,
"execution_timeout": timedelta(minutes=30),
```

When a task fails after exhausting all retries, Airflow automatically sends a failure email containing the DAG name, task name, execution date, and a direct link to the task logs. The `execution_timeout` of 30 minutes acts as a hard kill — if a task hangs (for example, a BigQuery query that never returns), Airflow kills the task after 30 minutes rather than allowing it to hold a worker slot indefinitely.

> `![Failed DAG run](docs/images/failed_dag_run.png)`

> `![Failure email notification](docs/images/failure_email.png)`

---

### Monitoring

The Airflow UI provides the primary monitoring interface:

- **Grid view** — run history with colour-coded task states across all DAG runs
- **Graph view** — live task dependency graph showing current run state
- **Task logs** — full stdout and stderr output for every task execution
- **Email alerts** — automatic failure emails and explicit success notification via `EmailOperator`

> `![DAGs list](docs/images/airflow_dags_list.png)`

> `![Successful DAG run](docs/images/successful_dag_run.png)`

---

### Backfill

The `beejanride_backfill` DAG is designed for reprocessing historical data when something has gone wrong — a bug fixed in a dbt model, a late-arriving Airbyte sync, or a new mart model that needs historical data populated.

It skips Airbyte because the raw data is already in BigQuery. It excludes the snapshot because dbt SCD Type 2 snapshots record current state at execution time and cannot replay historical row states for a given date range.

**Trigger a backfill across a date range:**

```bash
airflow dags backfill beejanride_backfill \
    --start-date 2025-04-30 \
    --end-date   2025-05-02
```

This creates one DAG run per day in the range. With `max_active_runs=1`, runs execute sequentially — each date completes before the next begins.

> `![Backfill execution](docs/images/backfill_execution.png)`

---

### How Idempotency is Maintained

Idempotency means the pipeline can be run multiple times over the same data and always produce the same result — no duplicates, no data corruption, no side effects from reruns.

Four mechanisms work together to guarantee this:

**1. `max_active_runs=1`**
Only one DAG run can be active at any time. If a new scheduled run triggers while the previous one is still executing, it queues and waits. This prevents two concurrent runs from writing to the same BigQuery incremental tables simultaneously.

**2. `catchup=False` on the production DAG**
If the scheduler misses a scheduled run (for example, the server was down), Airflow does not automatically replay all missed runs when it comes back up. Only the next scheduled run executes. This prevents unexpected historical reprocessing.

**3. dbt MERGE strategy on incremental models**
All incremental models (`trips_metrics`, `fct_trips`, `fct_payments`, `mart_daily_revenue`, `mart_fraud_monitoring`) use a MERGE strategy on their primary key. Re-running the pipeline over data that was already processed updates existing rows rather than inserting duplicates. The same trip processed twice produces one row, not two.

**4. dbt CREATE OR REPLACE on table and view models**
All dimension tables, full-refresh intermediate models, and staging views use `CREATE OR REPLACE`. These are fully idempotent regardless of how many times they run — the result is always the same complete table.

The backfill DAG intentionally sets `catchup=True` — this is the one controlled exception where historical replay is deliberate and expected. Even here, the MERGE and CREATE OR REPLACE strategies ensure reprocessing the same date range multiple times is safe.

---

*Orchestration built with Apache Airflow 3.1.8 · Airbyte Cloud · dbt-core 1.11.6 · BigQuery*
