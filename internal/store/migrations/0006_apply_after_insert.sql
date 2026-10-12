-- Move a posting's balance change to after the row is inserted (found in review on day 1).
--
-- 0002 moved the account's balance in a BEFORE INSERT trigger. But Postgres runs BEFORE triggers first and only
-- then decides whether to insert the row: INSERT ... ON CONFLICT DO NOTHING on a posting that conflicts (or
-- DO UPDATE ... WHERE false) skips the row, and the balance change made before stays. The balance moved with
-- no posting recorded: money from nothing. TestASkippedPostingMovesNoBalance reproduces it.
--
-- Now the BEFORE trigger only locks the account row and works out balance_after and account_version, and an
-- AFTER trigger, which runs only for rows that were inserted, writes them to the account. The overdraft CHECK
-- runs on that write, still under the row lock taken before. One cost: two postings to the same account in one
-- INSERT statement (from different entries) now both read the same version and the second fails on
-- postings_account_version_unique, so a client posts one entry per statement. That fails closed.

DROP TRIGGER postings_apply ON postings;

CREATE FUNCTION ledger_prepare_posting() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    current_balance bigint;
    current_version bigint;
BEGIN
    SELECT balance, version INTO current_balance, current_version
      FROM accounts WHERE id = NEW.account_id AND currency = NEW.currency
       FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'account % does not exist or is not in %', NEW.account_id, NEW.currency
            USING ERRCODE = 'foreign_key_violation', CONSTRAINT = 'postings_account_currency_fkey';
    END IF;
    NEW.balance_after := current_balance + NEW.amount; -- past bigint this raises 22003, it never wraps
    NEW.account_version := current_version + 1;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION ledger_apply_posting() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE accounts SET balance = NEW.balance_after, version = NEW.account_version
     WHERE id = NEW.account_id AND version = NEW.account_version - 1;
    IF NOT FOUND THEN
        -- Unreachable while the row lock from ledger_prepare_posting is held; kept so a gap can never be written.
        RAISE EXCEPTION 'account % moved while posting % was being applied', NEW.account_id, NEW.id
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN NULL;
END
$$;

CREATE TRIGGER postings_prepare BEFORE INSERT ON postings
    FOR EACH ROW EXECUTE FUNCTION ledger_prepare_posting();

CREATE TRIGGER postings_apply AFTER INSERT ON postings
    FOR EACH ROW EXECUTE FUNCTION ledger_apply_posting();
