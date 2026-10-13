"""
Local tests for main.py: a throwaway Postgres (products + trigger from sql/) and
a fake Pub/Sub publisher. Run through scripts/test_function_locally.sh.
"""
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

DSN = os.environ['LOCAL_TEST_DSN']
if urlparse(DSN).hostname not in ('127.0.0.1', 'localhost'):
    sys.exit('Refusing to run: LOCAL_TEST_DSN must point to localhost')

SQL_DIR = Path(__file__).resolve().parent.parent / 'sql'
url = urlparse(DSN)
os.environ.update({
    'DB_HOST': url.hostname, 'DB_PORT': str(url.port), 'DB_NAME': url.path.lstrip('/'),
    'DB_USER': url.username, 'DB_PASSWORD': '', 'PUBSUB_TOPIC': 'projects/test/topics/product-changes',
})
sys.path.insert(0, str(Path(__file__).resolve().parent))
import main  # noqa: E402


# ------------------------------------------------------------- fake Pub/Sub ---

class FakeFuture:
    def __init__(self, error=None):
        self.error = error

    def result(self, timeout=None):
        if self.error:
            raise self.error
        return 'message-id'


class FakePublisher:
    def __init__(self):
        self.messages = []        # (topic, body, ordering_key, attributes)
        self.fail = False
        self.resumed = []

    def publish(self, topic, data, ordering_key='', **attributes):
        if self.fail:
            return FakeFuture(RuntimeError('503 unavailable'))
        self.messages.append((topic, json.loads(data), ordering_key, attributes))
        return FakeFuture()

    def resume_publish(self, topic, ordering_key):
        self.resumed.append(ordering_key)


fake = FakePublisher()
main._publisher = fake


# ------------------------------------------------------------------ helpers ---

db = psycopg2.connect(DSN)
db.autocommit = True


def q(sql, params=None):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else None


def psql(path):
    subprocess.run(['psql', DSN, '-q', '-v', 'ON_ERROR_STOP=1', '-f', str(path)],
                   check=True, capture_output=True, text=True)


def unsent():
    return q("SELECT count(*) AS n FROM products_change_log WHERE NOT sent")[0]['n']


def sent():
    return q("SELECT count(*) AS n FROM products_change_log WHERE sent AND sent_at IS NOT NULL")[0]['n']


def total():
    return q("SELECT count(*) AS n FROM products_change_log")[0]['n']


def bodies():
    return [body for _, body, _, _ in fake.messages]


def run(publish=True, sellers=(100, 2010, 3010), ventures=()):
    main.PUBLISH = publish
    main.ENABLED_SELLERS = set(sellers)
    main.ENABLED_VENTURES = set(ventures)
    return main.poll_change_log(None)


def reset():
    psql(SQL_DIR / '01_products_table.sql')      # products + 6 test rows (ids 1-6)
    q("DROP TABLE IF EXISTS products_change_log, products_change_dead_letter CASCADE")
    psql(SQL_DIR / '02_install_change_log.sql')
    fake.__init__()


results = []


def test(fn):
    reset()
    try:
        fn()
        results.append((fn.__name__, None))
        print(f"PASS  {fn.__name__}")
    except Exception as e:  # noqa: BLE001 - report every failure and keep going
        results.append((fn.__name__, e))
        print(f"FAIL  {fn.__name__}: {type(e).__name__}: {e}")
    return fn


def expect(actual, expected, what):
    assert actual == expected, f"{what}: expected {expected!r}, got {actual!r}"


# ------------------------------------------------------------------- tests ---

@test
def nothing_enabled_does_nothing():
    q("UPDATE products SET stock = 1 WHERE id_product = 1")
    body = run(sellers=())
    expect((body['claimed'], unsent(), fake.messages), (0, 1, []), 'claimed, unsent rows, messages')


@test
def log_only_keeps_rows_and_publishes_nothing():
    q("UPDATE products SET stock = 3 WHERE id_product = 1")
    body = run(publish=False)
    expect((body['logged'], body['published'], unsent(), sent(), fake.messages), (1, 0, 1, 0, []),
           'logged, published, unsent, sent, messages')


@test
def publishes_current_values_and_marks_rows_sent():
    q("UPDATE products SET stock = 3 WHERE id_product = 1")
    body = run()
    expect((body['published'], unsent(), sent()), (1, 0, 1), 'published, unsent, sent (row kept)')
    topic, msg, key, attributes = fake.messages[0]
    expect((topic, key), ('projects/test/topics/product-changes', '1'), 'topic, ordering key')
    expect(attributes, {'v1_id_product': '1', 'venture': 'th', 'type': 'offer'}, 'attributes')
    expect({k: msg[k] for k in ('v1_id_product', 'warehouse_id', 'base_price', 'stock', 'is_active',
                                'tax_included', 'country_of_origin')},
           {'v1_id_product': 1, 'warehouse_id': '100', 'base_price': 3000.0, 'stock': 3, 'is_active': True,
            'tax_included': False, 'country_of_origin': 'TH'}, 'message body (3210 / 1.07 = 3000)')


@test
def korea_price_ex_10_percent_tax():
    q("UPDATE products SET stock = 4 WHERE id_product = 6")
    run()
    expect(bodies()[0]['base_price'], 30000.0, 'kr base_price (33000 / 1.10)')


@test
def not_enabled_seller_is_dropped():
    q("UPDATE products SET stock = 1 WHERE id_product IN (1, 4)")   # seller 100 and seller 2010
    body = run(sellers=(100,))
    expect((body['dropped'], [b['v1_id_product'] for b in bodies()], unsent(), sent(), total()), (1, [1], 0, 1, 1),
           'dropped, published ids, unsent, sent, total (dropped row deleted)')


@test
def several_changes_become_one_message_with_latest_sequence():
    for stock in (7, 6, 5):
        q("UPDATE products SET stock = %s WHERE id_product = 1", (stock,))
    last_id = q("SELECT max(id) AS m FROM products_change_log")[0]['m']
    body = run()
    expect((body['claimed'], body['published']), (3, 1), 'claimed rows, messages')
    expect((bodies()[0]['stock'], bodies()[0]['sequence']), (5, last_id), 'current stock, sequence')
    expect((unsent(), sent()), (0, 3), 'all three rows marked sent')


@test
def deleted_product_is_deactivated():
    q("DELETE FROM products WHERE id_product = 5")
    run()
    topic, msg, key, attributes = fake.messages[0]
    expect({k: v for k, v in msg.items() if k != 'sequence'},
           {'v1_id_product': 5, 'warehouse_id': '2010', 'stock': 0, 'is_active': False}, 'deactivation')
    expect(attributes['type'], 'deactivation', 'type')


@test
def unmappable_row_goes_to_dead_letter():
    q("UPDATE products SET price = 0 WHERE id_product = 2")
    body = run()
    expect((body['dead_lettered'], body['published'], total()), (1, 0, 0), 'dead lettered, published, log rows')
    letter = q("SELECT id_product, error FROM products_change_dead_letter")[0]
    expect((letter['id_product'], letter['error']), (2, 'invalid price 0.0'), 'dead letter row')


@test
def publish_failure_keeps_rows_never_dead_letters():
    q("UPDATE products SET stock = 2 WHERE id_product = 1")
    fake.fail = True
    body = run()
    expect((body['retry'], unsent(), sent(), q("SELECT count(*) AS n FROM products_change_dead_letter")[0]['n']),
           (1, 1, 0, 0), 'retry, unsent, sent, dead letters')
    expect(fake.resumed, ['1'], 'ordering key resumed')
    fake.fail = False
    expect((run()['published'], unsent(), sent()), (1, 0, 1), 'published on the next run')


@test
def run_loops_over_batches():
    q("INSERT INTO products (id_seller, venture, sku_manufacturer, sku, dot, stock, price, active) "
      "SELECT 100, 'th', 'sku-' || g, 'sku-' || g, '0824', 10, 1070, true FROM generate_series(1, 1200) g")
    body = run()
    expect((body['claimed'], body['published'], unsent(), sent()), (1200, 1200, 0, 1200),
           'claimed, published, unsent, sent')


@test
def sent_rows_are_never_claimed_again():
    q("UPDATE products SET stock = 3 WHERE id_product = 1")
    run()
    q("UPDATE products SET stock = 2 WHERE id_product = 1")
    body = run()
    expect((body['claimed'], len(fake.messages), unsent(), sent()), (1, 2, 0, 2),
           'second run claims only the new change')


@test
def install_upgrades_a_log_without_the_sent_flag():
    q("ALTER TABLE products_change_log DROP COLUMN sent, DROP COLUMN sent_at")
    q("INSERT INTO products_change_log (id_product, operation, id_seller, venture) VALUES (1, 'UPDATE', 100, 'th')")
    psql(SQL_DIR / '02_install_change_log.sql')
    expect((unsent(), sent()), (1, 0), 'existing row kept, now unsent')
    run()
    expect((unsent(), sent()), (0, 1), 'published after the upgrade')


failed = [name for name, error in results if error]
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
sys.exit(1 if failed else 0)
