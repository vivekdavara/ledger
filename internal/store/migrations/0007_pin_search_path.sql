-- Pin every ledger function's search_path (found in review on day 1).
--
-- The trigger functions name their tables without a schema, so Postgres resolved the names through the calling
-- session's search_path. There pg_temp comes first unless it is listed, and by default any role may create a
-- temporary table. A session-private table named accounts could then take the posting trigger's reads and
-- writes while the real account stayed put. Each function now resolves names in the ledger's own schema, with
-- pg_temp last. TestATempTableCannotStandInForTheLedgersTables reproduces it, and
-- TestEveryFunctionPinsItsSearchPath makes any later function do the same.

DO $$
DECLARE
    fn regprocedure;
BEGIN
    FOR fn IN
        SELECT p.oid::regprocedure FROM pg_proc p
         WHERE p.pronamespace = current_schema()::regnamespace AND p.proname LIKE 'ledger\_%'
    LOOP
        EXECUTE format('ALTER FUNCTION %s SET search_path = %I, pg_temp', fn, current_schema());
    END LOOP;
END
$$;
