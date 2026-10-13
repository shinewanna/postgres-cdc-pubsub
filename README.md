# pg-cdc-pubsub

A small, reusable test project for **capturing table changes in Postgres with a
trigger** (change-data-capture through an outbox table). It mirrors the V1 → V2
offer sync (Shortcut sc-10850): the `products` table has exactly the V1 columns,
and the install SQL is the exact file that goes to V1.

```
change products ──trigger (1 insert)──▶ products_change_log   ("which product changed")
                                              │
                                              └── later: worker (Cloud Scheduler + function) ──▶ Pub/Sub
```

This first part covers the **table + trigger** only. The worker and Pub/Sub come later.

## Files

| File | What | When |
|---|---|---|
| `sql/00_one_go_test.sql` | **everything in one go**: recreates `products` + test rows, installs the trigger, runs all scenarios, shows PASS / FAIL | the quick full test |
| `sql/01_products_table.sql` | `products` (same columns + indexes as V1) + 6 test rows | step-by-step mode |
| `sql/02_install_change_log.sql` | `products_change_log`, `products_change_dead_letter`, trigger (= V1 `001_install.sql`) | step-by-step mode |
| `sql/03_uninstall_change_log.sql` | removes the trigger, keeps the tables (= V1 `002_uninstall.sql`) | to test uninstall |
| `sql/04_scenarios.sql` | 21 scenarios, each using a real V1 writer's SQL, + PASS / FAIL table | after 01 + 02 |
| `scripts/build_one_go.sh` | rebuilds `00` from 01 + 02 + 04; with the cloud-functions path it first refreshes 02 / 03 | after changing any SQL |

Every script checks it is connected to the database **`cdc_pubsub_test`**. On any other database
(e.g. V1 production) it makes the DBeaver session **read-only** and stops with an error — so even if
the error is ignored ("Ignore All"), nothing is written. Reconnect to clear the read-only mode.

Test rows: seller **100** (Thailand, ids 1-3), seller **2010** (Thailand, ids 4-5), seller **3010** (Korea, id 6).

## Names

| Where | Value |
|---|---|
| GCP project (display name) | `PG CDC PubSub` |
| Cloud SQL instance | `pg-cdc-pubsub-db` (PostgreSQL 17) |
| Database | `cdc_pubsub_test` |
| Pub/Sub topic (later) | `product-changes` |

## Setup (once)

1. **Cloud SQL** (GCP console): on your Postgres 17 instance → **Databases** → **Create database** →
   `cdc_pubsub_test`.
2. **DBeaver**: new PostgreSQL connection → host = the instance's **Public IP**, port `5432`,
   database `cdc_pubsub_test`, user `postgres` + password. (Your IP must be in the instance's
   *Authorized networks*.)
3. **DBeaver setting** (important — the trigger function contains blank lines):
   *Window → Preferences → Editors → SQL Editor → SQL Processing* →
   **"Blank line is statement delimiter" = Never**.

## Test everything in one go

1. In DBeaver open `sql/00_one_go_test.sql` on the `cdc_pubsub_test` connection.
2. Press **Alt+X** (*Execute SQL Script* — not Ctrl+Enter).
3. Look at the **last two result tabs**:
   - every scenario with `expected`, `actual` and `PASS` / `FAIL`;
   - the totals — expected: **PASS 23**.

Scenario S20 renames the log table on purpose, so DBeaver shows one WARNING
(`products_change_log_capture: could not log …`) — that is the test passing: the
product write is still saved.

You can run it as often as you like; it starts from scratch every time.

## Step by step (to try your own changes)

1. Run `01_products_table.sql` (fresh table + test rows).
2. Run `02_install_change_log.sql` (install).
3. Change products yourself, then look at the log:
   ```sql
   UPDATE products SET stock = 3 WHERE id_product = 1;          -- logged
   UPDATE products SET price = 3424 WHERE id_product = 1;       -- logged
   UPDATE products SET updated_at = now() WHERE id_product = 1; -- NOT logged
   DELETE FROM products WHERE id_product = 3;                   -- logged as DELETE
   SELECT * FROM products_change_log ORDER BY id;
   ```
4. Run `03_uninstall_change_log.sql`, change a product again — nothing new is logged.
5. Optional: run `04_scenarios.sql` after a fresh 01 + 02 for the automatic checks.

## What gets logged

`products_change_log` — one row per change; it records **which** product changed, not the values:

| Column | Example |
|---|---|
| `id` | 12 (order of the changes) |
| `id_product` | 501 |
| `operation` | `INSERT` / `UPDATE` / `DELETE` |
| `id_seller` | 100 |
| `venture` | th |
| `changed_at` | 10:00:01 |

An `UPDATE` is logged only when one of these changes: `stock, price, discounted_price, active, dot,
sku_seller, sku_manufacturer, id_seller, country_of_origin_short, sell_only_by_pack`.

## Keeping it the same as V1

The install / uninstall files are copies of `cloud-functions/sql/v2_sync/001_install.sql` and
`002_uninstall.sql`. After those change:

```bash
scripts/build_one_go.sh ~/App/cloud-functions
```

## Cost

Cloud SQL is not in GCP's free tier (the free-trial credit covers it). **Stop** the instance when
you are not testing (*SQL → instance → Stop*) and **delete** it when you are done.
