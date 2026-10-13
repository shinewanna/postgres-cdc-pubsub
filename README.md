# pg-cdc-pubsub

A small, reusable test project for **capturing table changes in Postgres with a
trigger** (change-data-capture through an outbox table). It mirrors the V1 → V2
offer sync (Shortcut sc-10850): the `products` table has exactly the V1 columns,
and the install SQL is the exact file that goes to V1.

```
change products ──trigger (1 insert)──▶ products_change_log   ("which product changed")
                                              │  Cloud Scheduler (every minute)
                                              ▼
                                   Cloud Run function poll-change-log
                                   (reads the current products row, builds the message)
                                              │  publish (ordering key = product id)
                                              ▼
                                   Pub/Sub topic product-changes ──▶ subscription product-changes-inspect
```

## Files

| File | What | When |
|---|---|---|
| `sql/00_one_go_test.sql` | **everything in one go**: recreates `products` + test rows, installs the trigger, runs all scenarios, shows PASS / FAIL | the quick full test |
| `sql/01_products_table.sql` | `products` (same columns + indexes as V1) + 6 test rows | step-by-step mode |
| `sql/02_install_change_log.sql` | `products_change_log`, `products_change_dead_letter`, trigger (= V1 `001_install.sql`) | step-by-step mode |
| `sql/03_uninstall_change_log.sql` | removes the trigger, keeps the tables (= V1 `002_uninstall.sql`) | to test uninstall |
| `sql/04_scenarios.sql` | 21 scenarios, each using a real V1 writer's SQL, + PASS / FAIL table | after 01 + 02 |
| `scripts/build_one_go.sh` | rebuilds `00` from 01 + 02 + 04; with the cloud-functions path it first refreshes 02 / 03 | after changing any SQL |
| `function/main.py`, `function/requirements.txt` | the Cloud Run function that drains the log into Pub/Sub | deploy (see below) |
| `function/test_main.py`, `scripts/test_function_locally.sh` | local tests of the function: throwaway Postgres + fake Pub/Sub | before deploying |

Every script checks it is connected to the database **`cdc_pubsub_test`**. On any other database
(e.g. V1 production) it makes the DBeaver session **read-only** and stops with an error — so even if
the error is ignored ("Ignore All"), nothing is written. Reconnect to clear the read-only mode.

Test rows: seller **100** (Thailand, ids 1-3), seller **2010** (Thailand, ids 4-5), seller **3010** (Korea, id 6).

## Names

| Where | Value |
|---|---|
| GCP project (display name) | `PG CDC PubSub` |
| Cloud SQL instance | `pg-cdc-pubsub-db` (PostgreSQL 18, `us-east1`) |
| Database | `cdc_pubsub_test` |
| Pub/Sub topic | `product-changes` |
| Pub/Sub subscription | `product-changes-inspect` (pull, message ordering on) |
| Cloud Run function | `poll-change-log` (`us-east1`, entry point `poll_change_log`) |
| Service account | `cdc-poller` |
| Cloud Scheduler job | `poll-change-log-every-minute` |

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
| `sent` | `false` until the function published it, then `true` |
| `sent_at` | when it was published |

An `UPDATE` is logged only when one of these changes: `stock, price, discounted_price, active, dot,
sku_seller, sku_manufacturer, id_seller, country_of_origin_short, sell_only_by_pack`.

## Keeping it the same as V1

The install / uninstall files started as copies of `cloud-functions/sql/v2_sync/001_install.sql` and
`002_uninstall.sql`. This project's `02` **adds the `sent` / `sent_at` flag** (published rows are kept
instead of deleted), which V1's file does not have yet — so the refresh below refuses to run until
the V1 file has the flag too. Once it has:

```bash
scripts/build_one_go.sh ~/App/cloud-functions
```

## The function: drain the log into Pub/Sub

`function/main.py` (Cloud Run function, Python, HTTP). Per run, until the log is empty or ~50 s:
claim 500 log rows → drop sellers that are not enabled → collapse by product and read the current
`products` row → build the message (ex-tax prices: TH ÷ 1.07, KR ÷ 1.10; `is_active` rules) →
publish with ordering key = product id → mark the log rows **`sent = true`** (they are kept as a
record). A publish failure leaves them unsent for the next run; a row that can never become a message
goes to `products_change_dead_letter`. Rows of sellers that are not enabled are deleted (never sent).

Message body (one per product; `sequence` = latest log id, so a receiver can ignore older ones):

```json
{"v1_id_product": 1, "sku_manufacturer": "245/45r18", "warehouse_id": "100", "dot_group": "0824",
 "country_of_origin": "TH", "base_price": 3000.0, "promo_price": null, "tax_included": false,
 "stock": 3, "is_active": true, "v1_sku_seller": "A-1", "sequence": 12}
```

| Environment variable | Value |
|---|---|
| `DB_HOST` | `/cloudsql/<connection name>` (the instance's *Connection name*) |
| `DB_NAME` / `DB_USER` | `cdc_pubsub_test` / `postgres` |
| `DB_PASSWORD` | set in the console only — **never commit it** |
| `PUBSUB_TOPIC` | `projects/<project id>/topics/product-changes` |
| `PUBSUB_ENDPOINT` | `us-east1-pubsub.googleapis.com:443` (default) |
| `ENABLED_SELLERS` | `100` (and/or `ENABLED_VENTURES=th,kr`); empty → the run does nothing |
| `PUBLISH` | `true` to publish; anything else only logs the messages |

### Test it locally first

```bash
scripts/test_function_locally.sh
```

Expected: `12 passed, 0 failed` (throwaway Postgres + fake Pub/Sub; nothing touches GCP).

### Deploy (GCP console)

1. **Service account** — *IAM & Admin → Service accounts → Create*: `cdc-poller`, then grant it
   **Cloud SQL Client** and **Pub/Sub Publisher** (*IAM → Grant access*).
2. **Function** — *Cloud Run → Write a function*: service name `poll-change-log`, region `us-east1`,
   runtime **Python 3.12**, *Require authentication*. Under *Containers / Variables & secrets* add the
   environment variables above; under *Cloud SQL connections* add `pg-cdc-pubsub-db`; under
   *Security* pick the `cdc-poller` service account. **Create**.
3. **Code** — in the inline editor replace `main.py` and `requirements.txt` with the files from
   `function/`, set the *function entry point* to `poll_change_log`, **Save and redeploy**.
4. **Scheduler** — *Cloud Scheduler → Create job*: `poll-change-log-every-minute`, region
   `us-east1`, frequency `* * * * *`, target HTTP **GET** to the function URL, auth header
   **OIDC token** with the `cdc-poller` service account (give it **Cloud Run Invoker** on the
   function). Use **Force run** to test without waiting.
5. **Check** — change a seller-100 product in DBeaver → Force run → the function's *Logs* show
   `run: {... 'published': 1 ...}` → *Pub/Sub → product-changes-inspect → Messages → Pull* shows
   the message; the row in `products_change_log` now has `sent = true`.

## Cost

Cloud SQL is not in GCP's free tier (the free-trial credit covers it). **Stop** the instance when
you are not testing (*SQL → instance → Stop*) and **delete** it when you are done.
