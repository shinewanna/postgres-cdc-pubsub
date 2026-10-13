"""
Cloud Run function (2nd gen, HTTP): drains `products_change_log` into Pub/Sub.

Called by Cloud Scheduler every minute. Per run, until the log is empty or ~50 s:
  1. claim up to 500 log rows (oldest first, FOR UPDATE SKIP LOCKED);
  2. drop rows of sellers / ventures that are not enabled;
  3. collapse by id_product and read the CURRENT products rows;
  4. build one message per product (ex-tax prices, is_active rules, sequence);
     rows that can never become a message go to products_change_dead_letter;
  5. publish (ordering key = v1_id_product); success -> mark the log rows
     sent = true (they are kept as a record), failure -> leave them unsent for
     the next run. Rows of sellers that are not enabled, and rows moved to the
     dead letter, are deleted -- they are never sent.

Settings (environment variables -- the password is set in the console, never in git):
  DB_HOST           /cloudsql/<project>:<region>:<instance>  (or an IP for local runs)
  DB_NAME           cdc_pubsub_test
  DB_USER           postgres
  DB_PASSWORD       ...
  PUBSUB_TOPIC      projects/<project>/topics/product-changes
  PUBSUB_ENDPOINT   us-east1-pubsub.googleapis.com:443 (same region as the topic's publisher)
  ENABLED_SELLERS   e.g. 100        } at least one must be set,
  ENABLED_VENTURES  e.g. th,kr      } otherwise the run does nothing
  PUBLISH           true to publish; anything else = only log the messages
"""
import json
import logging
import os
import time
from decimal import Decimal, ROUND_HALF_UP

import functions_framework
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('poll_change_log')

PUBLISH = os.environ.get('PUBLISH', 'false').lower() == 'true'
ENABLED_SELLERS = {int(s) for s in os.environ.get('ENABLED_SELLERS', '').split(',') if s.strip()}
ENABLED_VENTURES = {v.strip().lower() for v in os.environ.get('ENABLED_VENTURES', '').split(',') if v.strip()}
PUBSUB_TOPIC = os.environ.get('PUBSUB_TOPIC', '')
PUBSUB_ENDPOINT = os.environ.get('PUBSUB_ENDPOINT', 'us-east1-pubsub.googleapis.com:443')

BATCH_SIZE = 500
RUN_SECONDS = 50
OLDEST_ROW_ALERT_SECONDS = 600
TAX_RATES = {'th': Decimal('0.07'), 'kr': Decimal('0.10')}
WRONG_MAPPING_MARKER = '-wrong_mapping'

SQL_CLAIM = """
    SELECT id, id_product, operation, id_seller, venture
    FROM products_change_log
    WHERE NOT sent
    ORDER BY id
    LIMIT %(limit)s
    FOR UPDATE SKIP LOCKED
"""
SQL_GET_PRODUCTS = """
    SELECT id_product, id_seller, venture, sku_manufacturer, sku_seller, dot, country_of_origin_short,
           price, discounted_price, stock, active, sell_only_by_pack
    FROM products
    WHERE id_product = ANY(%(ids)s)
"""
SQL_MARK_SENT = "UPDATE products_change_log SET sent = true, sent_at = now() WHERE id = ANY(%(ids)s)"
SQL_DELETE_LOG_ROWS = "DELETE FROM products_change_log WHERE id = ANY(%(ids)s)"
SQL_DEAD_LETTER = """
    INSERT INTO products_change_dead_letter (id_product, payload, error, attempts)
    VALUES (%(id_product)s, %(payload)s, %(error)s, 1)
"""
SQL_OLDEST_ROW_AGE = "SELECT EXTRACT(EPOCH FROM now() - min(changed_at)) AS age FROM products_change_log WHERE NOT sent"

_publisher = None


# ------------------------------------------------------------------ mapping ---

class Unmappable(Exception):
    """ The row can never be turned into a message (missing data, unknown venture). """


def ex_tax(amount, venture):
    rate = TAX_RATES.get((venture or '').lower())
    if rate is None:
        raise Unmappable(f"no tax rate for venture {venture!r}")
    return float((Decimal(str(amount)) / (1 + rate)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def to_offer(row):
    """ The CURRENT products row -> the offer sent to the target system. """
    if not row.get('sku_manufacturer'):
        raise Unmappable('missing sku_manufacturer')
    price = row.get('price')
    if price is None or price <= 0:
        raise Unmappable(f"invalid price {price!r}")
    sku_seller = row.get('sku_seller') or ''
    stock = max(int(row.get('stock') or 0), 0)
    discounted = row.get('discounted_price')
    country = row.get('country_of_origin_short')
    return {
        'v1_id_product': row['id_product'],
        'sku_manufacturer': row['sku_manufacturer'],
        'warehouse_id': str(row['id_seller']),
        'dot_group': row.get('dot'),
        'country_of_origin': country.upper() if country else None,
        'base_price': ex_tax(price, row['venture']),
        'promo_price': ex_tax(discounted, row['venture']) if discounted and discounted > 0 else None,
        'tax_included': False,
        'stock': stock,
        'is_active': (bool(row.get('active')) and stock > 0 and not row.get('sell_only_by_pack')
                      and WRONG_MAPPING_MARKER not in sku_seller),
        'v1_sku_seller': row.get('sku_seller'),
    }


def to_deactivation(id_product, id_seller):
    """ The row was deleted: the target deactivates the offer of this v1_id_product. """
    return {'v1_id_product': id_product, 'warehouse_id': str(id_seller), 'stock': 0, 'is_active': False}


# ------------------------------------------------------------------ helpers ---

def connect():
    return psycopg2.connect(
        host=os.environ['DB_HOST'], dbname=os.environ.get('DB_NAME', 'cdc_pubsub_test'),
        user=os.environ.get('DB_USER', 'postgres'), password=os.environ.get('DB_PASSWORD', ''),
        port=int(os.environ.get('DB_PORT', '5432')), connect_timeout=10,
    )


def publisher():
    global _publisher
    if _publisher is None:
        from google.cloud import pubsub_v1
        _publisher = pubsub_v1.PublisherClient(
            publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True),
            client_options={'api_endpoint': PUBSUB_ENDPOINT},
        )
    return _publisher


def is_enabled(venture, id_seller):
    return (venture or '').lower() in ENABLED_VENTURES or id_seller in ENABLED_SELLERS


def publish(messages):
    """ messages = [(offer, sequence, venture)]. Raises if any publish fails. """
    if not PUBSUB_TOPIC:
        raise RuntimeError('PUBSUB_TOPIC is not set')
    client = publisher()
    futures = []
    for offer, sequence, venture in messages:
        key = str(offer['v1_id_product'])
        data = json.dumps({**offer, 'sequence': sequence}).encode()
        futures.append((key, client.publish(
            PUBSUB_TOPIC, data, ordering_key=key, v1_id_product=key, venture=(venture or '').lower(),
            type='offer' if 'base_price' in offer else 'deactivation')))
    failed = None
    for key, future in futures:
        try:
            future.result(timeout=60)
        except Exception as e:  # noqa: BLE001 - keep checking the rest, then resume the key
            failed = failed or e
            client.resume_publish(PUBSUB_TOPIC, key)
    if failed:
        raise failed


# -------------------------------------------------------------------- batch ---

def process_batch(conn):
    summary = {'claimed': 0, 'dropped': 0, 'published': 0, 'logged': 0, 'retry': 0, 'dead_lettered': 0}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_CLAIM, {'limit': BATCH_SIZE})
        claimed = cur.fetchall()
        summary['claimed'] = len(claimed)
        if not claimed:
            conn.commit()
            return summary

        # 1. not enabled -> dropped (deleted: never meant to be sent)
        to_delete = [r['id'] for r in claimed if not is_enabled(r['venture'], r['id_seller'])]
        enabled = [r for r in claimed if is_enabled(r['venture'], r['id_seller'])]
        summary['dropped'] = len(to_delete)

        # 2. collapse by product (latest operation wins), read current rows
        by_product = {}
        for row in enabled:
            entry = by_product.setdefault(row['id_product'], {'log_ids': [], 'last': row})
            entry['log_ids'].append(row['id'])
            entry['last'] = row
        products = {}
        if by_product:
            cur.execute(SQL_GET_PRODUCTS, {'ids': list(by_product)})
            products = {p['id_product']: p for p in cur.fetchall()}

        # 3. build messages; rows that can never be sent -> dead letter
        messages, message_log_ids = [], []
        for id_product, entry in by_product.items():
            last, product = entry['last'], products.get(id_product)
            if last['operation'] == 'DELETE' or product is None:
                offer, venture = to_deactivation(id_product, last['id_seller']), last['venture']
            else:
                try:
                    offer, venture = to_offer(product), product['venture']
                except Unmappable as e:
                    log.error("id_product %s moved to products_change_dead_letter: %s", id_product, e)
                    cur.execute(SQL_DEAD_LETTER, {'id_product': id_product, 'error': str(e),
                                                  'payload': json.dumps(product, default=str)})
                    to_delete.extend(entry['log_ids'])
                    summary['dead_lettered'] += 1
                    continue
            messages.append((offer, last['id'], venture))   # sequence = latest log id
            message_log_ids.extend(entry['log_ids'])

        # 4. publish, or only log (rows stay unsent)
        if messages and not PUBLISH:
            log.info("NOT PUBLISHED (PUBLISH=false): %s messages: %s", len(messages),
                     json.dumps([{**o, 'sequence': s} for o, s, _ in messages]))
            summary['logged'] = len(messages)
        elif messages:
            try:
                publish(messages)
                cur.execute(SQL_MARK_SENT, {'ids': message_log_ids})
                summary['published'] = len(messages)
            except Exception as e:  # noqa: BLE001 - keep the rows, retry next run
                log.warning("publish failed, %s messages stay unsent for the next run: %s", len(messages), e)
                summary['retry'] = len(messages)

        if to_delete:
            cur.execute(SQL_DELETE_LOG_ROWS, {'ids': to_delete})
    conn.commit()
    return summary


# --------------------------------------------------------------- entry point ---

@functions_framework.http
def poll_change_log(request):
    totals = {'publish': PUBLISH, 'claimed': 0, 'dropped': 0, 'published': 0, 'logged': 0, 'retry': 0,
              'dead_lettered': 0}
    if not ENABLED_SELLERS and not ENABLED_VENTURES:
        log.warning("no seller or venture enabled (ENABLED_SELLERS / ENABLED_VENTURES): nothing to do")
        return totals

    conn = connect()
    try:
        deadline = time.monotonic() + RUN_SECONDS
        while True:
            batch = process_batch(conn)
            for key, value in batch.items():
                totals[key] += value
            # log-only keeps the rows, so a second batch would claim the same ones
            if not PUBLISH or batch['claimed'] < BATCH_SIZE or batch['retry'] or time.monotonic() > deadline:
                break
        with conn.cursor() as cur:
            cur.execute(SQL_OLDEST_ROW_AGE)
            age = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    totals['oldest_row_age_seconds'] = int(age) if age is not None else 0
    log.info("run: %s", totals)
    if age is not None and age > OLDEST_ROW_ALERT_SECONDS:
        log.error("lagging: the oldest unsent change is %s s old", int(age))
    return totals
