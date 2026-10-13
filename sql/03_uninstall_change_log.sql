-- sc-10850 -- removes the products_change_log capture trigger.
-- V1 writes are unaffected before and after. The tables are kept so a pending
-- backlog is not lost; drop them by hand once the poller has drained them.

BEGIN;

SET LOCAL lock_timeout = '3s';

DROP TRIGGER IF EXISTS products_change_log_capture ON products;
DROP FUNCTION IF EXISTS products_change_log_capture();

-- DROP TABLE IF EXISTS products_change_log;
-- DROP TABLE IF EXISTS products_change_dead_letter;

COMMIT;
