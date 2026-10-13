-- pg-cdc-pubsub: the example table, identical to V1 `public.products`
-- (DDL taken from the V1 database, PostgreSQL 17.10, Oct 2026) + test rows.
--
-- Run in DBeaver against the database `cdc_pubsub_test` (Alt+X).
-- It DROPS and recreates `products`, so it refuses to run anywhere else.

-- Safety guard. On any other database (e.g. V1 production) it first makes this
-- session READ-ONLY, then stops with an error -- so even if the error is ignored
-- ("Ignore All" in DBeaver), every following write fails. Reconnect to clear it.
DO $$
BEGIN
    IF current_database() <> 'cdc_pubsub_test' THEN
        PERFORM set_config('default_transaction_read_only', 'on', false);
    END IF;
END $$;
DO $$
BEGIN
    IF current_database() <> 'cdc_pubsub_test' THEN
        RAISE EXCEPTION 'Wrong database %: this script only runs on cdc_pubsub_test. The session is now read-only.',
            current_database();
    END IF;
END $$;

DROP TABLE IF EXISTS public.products CASCADE;

CREATE TABLE public.products (
	id_product serial4 NOT NULL,
	sku_manufacturer varchar(100) NULL,
	sku varchar(100) NULL,
	id_seller int4 NULL,
	price float8 NULL,
	dot varchar(100) NULL,
	stock int4 NULL,
	created_at timestamptz NULL,
	updated_at timestamptz NULL,
	active bool NULL,
	discounted_price float8 NULL,
	discount_level float8 NULL,
	country_of_origin_short varchar(100) NULL,
	country_of_origin_long varchar(100) NULL,
	sku_seller varchar(100) NULL,
	venture varchar(100) NULL,
	id_promo int4 NULL,
	price_bucket varchar(500) NULL,
	min_price_bucket float8 NULL,
	sell_only_by_pack bool NULL,
	price_b_quik float8 NULL
);
CREATE INDEX idx_products_active ON public.products USING btree (active);
CREATE INDEX idx_products_country_of_origin_long ON public.products USING btree (country_of_origin_long);
CREATE INDEX idx_products_country_of_origin_short ON public.products USING btree (country_of_origin_short);
CREATE INDEX idx_products_dot ON public.products USING btree (dot);
CREATE INDEX idx_products_id_promo ON public.products USING btree (id_promo);
CREATE INDEX idx_products_id_seller ON public.products USING btree (id_seller);
CREATE INDEX idx_products_sku ON public.products USING btree (sku);
CREATE UNIQUE INDEX idx_products_sku_bis ON public.products USING btree (sku);
CREATE INDEX idx_products_sku_manufacturer ON public.products USING btree (sku_manufacturer);
CREATE INDEX idx_products_sku_seller ON public.products USING btree (sku_seller);
CREATE INDEX idx_products_venture ON public.products USING btree (venture);

-- Test rows (made up). id_product 1-6 are used by the scenarios.
-- Seller 100 = the pilot seller (Thailand), seller 2010 = another TH seller, seller 3010 = Korea.
INSERT INTO public.products
    (sku_manufacturer, sku, id_seller, price, dot, stock, created_at, updated_at, active,
     discounted_price, discount_level, country_of_origin_short, country_of_origin_long,
     sku_seller, venture, sell_only_by_pack)
VALUES
    ('245/45r18', '100_245/45r18_0824', 100, 3210, '0824',  8, now(), now(), true,  NULL, NULL, 'th', 'thailand', 'A-1', 'th', false),
    ('225/40r18', '100_225/40r18_1225', 100, 2996, '1225',  4, now(), now(), true,  2889,    1, 'jp', 'japan',    'A-2', 'th', false),
    ('195/65r15', '100_195/65r15_0925', 100, 1605, '0925', 12, now(), now(), true,  NULL, NULL, 'th', 'thailand', 'A-3', 'th', false),
    ('205/55r16', '2010_205/55r16_0824', 2010, 2140, '0824', 6, now(), now(), true, NULL, NULL, 'th', 'thailand', 'B-1', 'th', false),
    ('215/60r16', '2010_215/60r16_1024', 2010, 2354, '1024', 3, now(), now(), true, NULL, NULL, 'cn', 'china',    'B-2', 'th', false),
    ('235/55r19', '3010_235/55r19_0325', 3010, 33000, '0325', 9, now(), now(), true, NULL, NULL, 'kr', 'korea',   'K-1', 'kr', false);

SELECT * FROM public.products ORDER BY id_product;
