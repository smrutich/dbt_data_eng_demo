# 🍕 Pizza Place Sales — dbt Project

> A data transformation project built with dbt + Snowflake, transforming raw pizza sales data into an analytics-ready star schema using the Medallion Architecture (Bronze → Silver → Gold).

---

## Table of Contents

- [Project Overview](#project-overview)
- [Dataset](#dataset)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Layers](#layers)
  - [Bronze (Staging)](#bronze-staging)
  - [Silver (Intermediate)](#silver-intermediate)
  - [Gold (Final)](#gold-final)
- [Star Schema](#star-schema)
- [Testing](#testing)
- [Macros](#macros)
- [Seeds](#seeds)
- [Packages](#packages)
- [Documentation](#documentation)
- [How to Run](#how-to-run)

---

## Project Overview

This project applies analytics engineering best practices to the Maven Analytics Pizza Place Sales dataset. Raw transactional data loaded into Snowflake is transformed through three medallion layers into a dimensional model (star schema) ready for BI consumption.

**Business questions this project answers:**
- Which pizzas generate the most revenue?
- When is the restaurant busiest (day of week, hour of day)?
- Which ingredients appear most frequently across orders?
- How does revenue trend across quarters?
- What is the average order value by pizza category?

---

## Dataset

**Source:** [Maven Analytics — Pizza Place Sales](https://mavenanalytics.io/data-playground/pizza-place-sales)

A year's worth of sales (2015) from a fictitious pizza restaurant (Plato's Pizza). Loaded as raw tables into Snowflake.

| Table | Rows | Description |
|---|---|---|
| `ORDERS` | 21,350 | One row per order with date and time |
| `ORDER_DETAILS` | 48,620 | One row per pizza per order |
| `PIZZAS` | 96 | Pizza SKUs with size and price |
| `PIZZA_TYPES` | 32 | Pizza types with name, category, ingredients |

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **dbt Core 1.10** | Transformation framework |
| **Snowflake** | Cloud data warehouse |
| **dbt_utils** | Surrogate key generation, combo uniqueness tests |
| **dbt_expectations** | Advanced data quality assertions |

---

## Project Structure

```
pizza_dbt/
├── dbt_project.yml               # Project configuration
├── packages.yml                  # dbt package dependencies
├── profiles.yml                  # Snowflake connection (local only)
├── README.md                     # This file
│
├── models/
│   ├── staging/                  # Bronze layer
│   │   ├── _sources.yml          # Source definitions + tests
│   │   ├── stg_orders.sql
│   │   ├── stg_order_details.sql
│   │   ├── stg_pizza.sql
│   │   └── stg_pizza_types.sql
│   │
│   ├── intermediate/             # Silver layer
│   │   ├── _intermediate.yml     # Silver model docs + tests
│   │   ├── int_orders.sql
│   │   ├── int_order_details.sql
│   │   ├── int_pizza.sql
│   │   ├── int_pizza_types.sql
│   │   └── int_pizza_ingredients.sql
│   │
│   └── final/                    # Gold layer
│       ├── _final.yml            # Gold model docs + tests
│       ├── dim_date.sql
│       ├── dim_pizza.sql
│       ├── dim_ingredients.sql
│       ├── lookup_pizza_ingredients.sql
│       └── fact_orders.sql
│
├── macros/
│   ├── audit_columns.sql         # Reusable audit column macro
│
├── seeds/
│   └── pizza_size_labels.csv     # Size code lookup table
│
└── tests/
    ├── assert_positive_quantity.sql
    ├── assert_positive_price.sql
    ├── assert_valid_order_date_range.sql
    ├── assert_fact_orders_match_pizza_dim.sql
```

---

## Architecture

```
Snowflake Raw Tables (ORDERS, ORDER_DETAILS, PIZZAS, PIZZA_TYPES)
        │
        │  source()
        ▼
┌─────────────────────────────────┐
│         BRONZE (staging/)       │  Materialization: table / incremental
│  stg_orders                     │  Purpose: Raw capture + audit columns
│  stg_order_details              │  No transformations applied
│  stg_pizza                      │
│  stg_pizza_types                │
└─────────────────────────────────┘
        │
        │  ref()
        ▼
┌─────────────────────────────────┐
│       SILVER (intermediate/)    │  Materialization: view
│  int_orders                     │  Purpose: Type casting, standardization,
│  int_order_details              │  deduplication, null filtering
│  int_pizza                      │  No business logic
│  int_pizza_types                │
│  int_pizza_ingredients          │
└─────────────────────────────────┘
        │
        │  ref()
        ▼
┌─────────────────────────────────┐
│         GOLD (final/)           │  Materialization: table / incremental
│  dim_date                       │  Purpose: Business logic, surrogate keys,
│  dim_pizza                      │  dimensional modeling, aggregations
│  dim_ingredients                │
│  lookup_pizza_ingredients       │
│  fact_orders                    │
└─────────────────────────────────┘
```

---

## Layers

### Bronze (Staging)

**Purpose:** Capture source data with minimal transformation. Adds audit columns for lineage tracking. No business logic or type casting applied.

**Materialization:** `table` for static lookups (pizzas, pizza_types), `incremental` for transactional tables (orders, order_details).

**Audit columns added to every model:**

| Column | Description |
|---|---|
| `SRC_SYS` | Source system identifier — always `pizza_place` |
| `CRT_TS` | Timestamp when dbt first loaded this row |
| `UPD_TS` | Timestamp when dbt last processed this row |

**Incremental strategy:** New rows identified via `ORDER_DETAILS_ID > max(ORDER_DETAILS_ID)` and `DATE > max(DATE)` for orders.

**Source freshness:** Configured in `_sources.yml`:
- Warn after 24 hours
- Error after 48 hours

---

### Silver (Intermediate)

**Purpose:** Data cleansing, type casting, naming standardization, and deduplication. No business logic applied — that belongs in Gold.

**Materialization:** `view` — always reflects the latest Bronze data.

**Transformations applied:**
- All columns explicitly cast to correct data types (`::number`, `::varchar`, `::date`, `::time`, `::float`)
- Column names standardized to `snake_case`
- Columns trimmed of whitespace with `TRIM()`
- Null filtering on primary keys (`WHERE x IS NOT NULL`)
- Deduplication via `QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY UPD_TS DESC) = 1`
- Audit columns passed through from Bronze

**Special model — `int_pizza_ingredients`:**
Explodes the comma-separated ingredients string into one row per ingredient per pizza type using Snowflake's `LATERAL FLATTEN`. This enables ingredient-level analysis in Gold.

---

### Gold (Final)

**Purpose:** Business transformations, dimensional modeling, surrogate key generation, and business rule application.

**Materialization:** `table` for dimensions, `incremental` (merge strategy) for `fact_orders`.

**Key design decisions:**
- Surrogate keys generated using `dbt_utils.generate_surrogate_key()` from business keys — no joins to dimensions to retrieve keys
- Fact table contains no joins to dimensions — surrogate keys generated directly from business keys already on the record
- Business logic (price tiers, quantity validation, size label standardization) applied here, not in Silver
- `dim_pizza` merges `int_pizza` and `int_pizza_types` — no reason to expose two separate pizza dimensions to BI users

---

## Star Schema

```
                        dim_date
                      (date_sk PK)
                           │
                        date_sk (FK)
                           │
dim_pizza ─────────── fact_orders ─────────── (future: dim_time)
(pizza_sk PK)         (order_detail_sk PK)
(pizza_type_sk)        pizza_sk (FK)
     │                 date_sk (FK)
     │                 order_detail_id
  pizza_type_sk (FK)   order_id
     │                 pizza_id
     ▼                 quantity
lookup_pizza_ingredients  gross_amount
(pizza_ingredient_sk PK)
 pizza_type_sk (FK)
 ingredient_sk (FK)
     │
     ▼
dim_ingredients
(ingredient_sk PK)
```

| Model | Type | Grain | Rows |
|---|---|---|---|
| `fact_orders` | Fact | One row per order line item | 48,620 |
| `dim_pizza` | Dimension | One row per pizza SKU | 96 |
| `dim_date` | Dimension | One row per calendar date | 365 |
| `dim_ingredients` | Dimension | One row per unique ingredient | ~65 |
| `lookup_pizza_ingredients` | Bridge | One row per pizza type + ingredient | ~350 |

---

## Testing

### Generic Tests (YAML)

All tests configured with `severity: error` — tests fail hard, no warnings.

| Layer | Tests Applied |
|---|---|
| Bronze | Source freshness (`warn_after: 24h`, `error_after: 48h`) |
| Silver | `unique`, `not_null`, `accepted_values`, `relationships` |
| Gold | `relationships`, `dbt_utils.unique_combination_of_columns` |

**Silver test count:** 45 tests, all passing.
**Gold test count:** 30 tests, all passing.

### Singular Tests (Custom Business Logic)

Located in `tests/` — return failing rows if business rules are violated:

| Test | Business Rule |
|---|---|
| `assert_positive_quantity` | No order line item can have quantity ≤ 0 |
| `assert_positive_price` | No pizza can have price ≤ 0 |
| `assert_fact_orders_match_dim_pizza` | No orphaned fact records — every pizza_sk must exist in dim_pizza |

Run all tests:
```bash
# All tests
dbt test

# By layer
dbt test --select staging
dbt test --select intermediate
dbt test --select final

# Singular tests only
dbt test --select test_type:singular

# Generic tests only
dbt test --select test_type:generic
```

---

## Macros

Located in `macros/`. Follow DRY principle — eliminates repeated SQL patterns.

### `audit_columns(src_sys='pizza_place')`

Generates standard audit columns (`SRC_SYS`, `CRT_TS`, `UPD_TS`) with consistent naming and types across all layers.

```sql
-- Usage
{{ audit_columns() }}
{{ audit_columns(src_sys='date_generator') }}

-- Compiles to:
'pizza_place'                     as SRC_SYS,
current_timestamp()::timestamp_ntz as CRT_TS,
current_timestamp()::timestamp_ntz as UPD_TS
```

---

## Seeds

Located in `seeds/`. Small static CSV files loaded into Snowflake as lookup tables.

### `pizza_size_labels.csv`

Enriches pizza size codes with descriptive labels and serving suggestions. Referenced in `dim_pizza` via `{{ ref('pizza_size_labels') }}`.

| Column | Description |
|---|---|
| `size_code` | Raw size code from source (S, M, L, XL, XXL) |
| `size_label` | Human readable label (Small, Medium, etc.) |
| `size_description` | Marketing description |
| `serves_min` | Minimum people served |
| `serves_max` | Maximum people served |
| `sort_order` | Display order for BI tools |

```bash
# Load seeds
dbt seed
```

---

## Packages

Defined in `packages.yml`. Install with `dbt deps`.

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

| Package | Usage |
|---|---|
| `dbt_utils` | `generate_surrogate_key()`, `unique_combination_of_columns` test |

---