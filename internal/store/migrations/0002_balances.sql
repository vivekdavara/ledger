-- I4, I5 and I7 (DESIGN.md): the database keeps every account's balance, so it can't drift from the postings,
-- and nothing else may change it.

-- A posting moves its account's balance and records the balance and version after it. The UPDATE takes the
-- account's row lock and runs the overdraft CHECK while holding it, so two concurrent payments can't both spend
-- the same money. Whatever balance_after and account_version the client sent are overwritten.
CREATE FUNCTION ledger_apply_posting() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE accounts
       SET balance = balance + NEW.amount, version = version + 1
     WHERE id = NEW.account_id AND currency = NEW.currency
    RETURNING balance, version INTO NEW.balance_after, NEW.account_version;
    IF NOT FOUND THEN
        -- Say what the foreign key would have said, before the NOT NULL on balance_after says something vaguer.
        RAISE EXCEPTION 'account % does not exist or is not in %', NEW.account_id, NEW.currency
            USING ERRCODE = 'foreign_key_violation', CONSTRAINT = 'postings_account_currency_fkey';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER postings_apply BEFORE INSERT ON postings
    FOR EACH ROW EXECUTE FUNCTION ledger_apply_posting();

-- An account starts at zero, and from then on its balance and version change only through ledger_apply_posting.
-- That function runs as a trigger, so its UPDATE reaches this guard at trigger depth 2, while a direct UPDATE
-- arrives at depth 1. An account's identity (id, type, currency, creation time) never changes.
CREATE FUNCTION ledger_guard_account() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.balance <> 0 OR NEW.version <> 0 THEN
            RAISE EXCEPTION 'a new account starts with balance 0 and version 0'
                USING ERRCODE = 'restrict_violation', CONSTRAINT = 'balance_via_postings_only';
        END IF;
        RETURN NEW;
    END IF;
    IF (NEW.id, NEW.type, NEW.currency, NEW.created_at)
       IS DISTINCT FROM (OLD.id, OLD.type, OLD.currency, OLD.created_at) THEN
        RAISE EXCEPTION 'an account''s id, type, currency and creation time never change'
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'account_identity_immutable';
    END IF;
    IF (NEW.balance, NEW.version) IS DISTINCT FROM (OLD.balance, OLD.version) AND pg_trigger_depth() < 2 THEN
        RAISE EXCEPTION 'an account''s balance changes only through postings'
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'balance_via_postings_only';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER accounts_guard BEFORE INSERT OR UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION ledger_guard_account();
