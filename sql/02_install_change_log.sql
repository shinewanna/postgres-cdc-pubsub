-- sc-10850 -- V1 -> V2 offer mirror: products_change_log + capture trigger on products.
--
-- Every relevant change to `products` is queued in products_change_log,
-- whichever writer made it (seller feeds, order path, admin routes, scripts).
-- The mirror poller (sc-10825) drains the log; rows are deleted once V2
-- accepted them, so the log is a queue, not a history table.
--
-- The log only says WHICH product changed. The poller reads the current
-- products row and sends it with v1_id_product = id_product; V2 finds the
-- offer to update through its offer_v1_mapping table, so a reprice or DOT
-- change updates the same offer in place.
--
-- Run ONCE on V1 Postgres (PostgreSQL 17), as the OWNER of `products` (postgres).
-- Tested locally on PostgreSQL 18; nothing used here is newer than PostgreSQL 11.
--
-- Safe for V1:
--   * The trigger does exactly one INSERT: no lookups, no HTTP, nothing
--     computed. All filtering and mapping live in the poller.
--   * SECURITY DEFINER: every V1 writer role can log rows without new grants.
--   * Any error inside the trigger is downgraded to a WARNING, so the V1 write
--     that fired it always commits (the backfill, sc-10826, repairs a gap).
--   * lock_timeout: CREATE TRIGGER briefly locks `products`; if it cannot get
--     the lock within 3s it gives up instead of queueing behind checkout.
--
-- Undo: 002_uninstall.sql

BEGIN;

SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS products_change_log (
    id          bigserial   PRIMARY KEY,               -- capture order
    id_product  integer     NOT NULL,
    operation   text        NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    -- copied from the row (the deleted row for DELETE): lets the poller route a
    -- delete to the right venture credential and filter sellers without a join
    id_seller   integer,
    venture     text,
    changed_at  timestamptz NOT NULL DEFAULT now()     -- lag monitoring only, never a read cursor
);

-- Rows the poller isolated as undeliverable (poison-row guard, sc-10825).
CREATE TABLE IF NOT EXISTS products_change_dead_letter (
    id          bigserial   PRIMARY KEY,
    id_product  integer     NOT NULL,
    payload     jsonb,
    error       text,
    attempts    integer     NOT NULL DEFAULT 0,
    parked_at   timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE products_change_log IS
    'V1 -> V2 offer mirror queue (sc-10850): one row per relevant products change, written by the '
    'products_change_log_capture trigger, deleted by the mirror poller (sc-10825) once V2 accepted it.';
COMMENT ON COLUMN products_change_log.id IS 'Capture order.';
COMMENT ON COLUMN products_change_log.id_product IS 'The products row that changed.';
COMMENT ON COLUMN products_change_log.operation IS 'INSERT, UPDATE or DELETE on products.';
COMMENT ON COLUMN products_change_log.id_seller IS 'Seller of the row (of the deleted row for DELETE).';
COMMENT ON COLUMN products_change_log.venture IS 'Venture of the row (of the deleted row for DELETE); picks the V2 credential.';
COMMENT ON COLUMN products_change_log.changed_at IS 'When the change was captured; lag monitoring only, never a read cursor.';

COMMENT ON TABLE products_change_dead_letter IS
    'Changes the mirror poller (sc-10825) could not deliver to V2 after repeated attempts; waits for a person.';
COMMENT ON COLUMN products_change_dead_letter.id_product IS 'The products row that could not be sent.';
COMMENT ON COLUMN products_change_dead_letter.payload IS 'The V2 offer the poller tried to send.';
COMMENT ON COLUMN products_change_dead_letter.error IS 'Error returned by V2 (or raised while sending).';
COMMENT ON COLUMN products_change_dead_letter.attempts IS 'How many times it was tried before being parked.';
COMMENT ON COLUMN products_change_dead_letter.parked_at IS 'When it was moved here.';

CREATE OR REPLACE FUNCTION products_change_log_capture() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
-- pg_temp last: a temporary table must never shadow the queue (it is searched
-- FIRST when not listed); the INSERT below is schema-qualified as well.
SET search_path = public, pg_temp
AS $$
DECLARE
    rec  record;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        -- UPDATE OF <columns> also fires when a column is only in the SET list;
        -- skip writes that changed none of the mirrored fields.
        IF  NEW.stock                   IS NOT DISTINCT FROM OLD.stock
        AND NEW.price                   IS NOT DISTINCT FROM OLD.price
        AND NEW.discounted_price        IS NOT DISTINCT FROM OLD.discounted_price
        AND NEW.active                  IS NOT DISTINCT FROM OLD.active
        AND NEW.dot                     IS NOT DISTINCT FROM OLD.dot
        AND NEW.sku_seller              IS NOT DISTINCT FROM OLD.sku_seller
        AND NEW.sku_manufacturer        IS NOT DISTINCT FROM OLD.sku_manufacturer
        AND NEW.id_seller               IS NOT DISTINCT FROM OLD.id_seller
        AND NEW.country_of_origin_short IS NOT DISTINCT FROM OLD.country_of_origin_short
        AND NEW.sell_only_by_pack       IS NOT DISTINCT FROM OLD.sell_only_by_pack
        THEN
            RETURN NULL;
        END IF;
    END IF;

    BEGIN
        IF TG_OP = 'DELETE' THEN
            rec := OLD;
        ELSE
            rec := NEW;
        END IF;

        INSERT INTO public.products_change_log (id_product, operation, id_seller, venture)
        VALUES (rec.id_product, TG_OP, rec.id_seller, rec.venture);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'products_change_log_capture: could not log id_product % (%): % [%]',
            rec.id_product, TG_OP, SQLERRM, SQLSTATE;
    END;

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS products_change_log_capture ON products;

CREATE TRIGGER products_change_log_capture
    AFTER INSERT OR DELETE OR UPDATE OF
        stock, price, discounted_price, active, dot, sku_seller, sku_manufacturer,
        id_seller, country_of_origin_short, sell_only_by_pack
    ON products
    FOR EACH ROW
    EXECUTE FUNCTION products_change_log_capture();

COMMIT;
