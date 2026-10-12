-- I1, I2 and I6 (DESIGN.md): an entry's postings sum to zero in each currency, there are exactly as many as it
-- declares, and history is never changed.

-- I1 and I2 are about a group of rows, so they can only be checked once all of an entry's postings exist: at
-- commit. The check runs after each entry insert (an entry with no postings can't commit) and after each posting
-- insert (postings can't be added to an entry committed earlier: even a balanced pair breaks the count).
CREATE FUNCTION ledger_check_entry(p_entry_id bigint) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    declared smallint;
    actual   bigint;
    off      text;
BEGIN
    SELECT posting_count INTO declared FROM journal_entries WHERE id = p_entry_id;
    SELECT count(*) INTO actual FROM postings WHERE entry_id = p_entry_id;
    IF actual IS DISTINCT FROM declared THEN
        RAISE EXCEPTION 'journal entry % declares % postings but has %', p_entry_id, declared, actual
            USING ERRCODE = 'check_violation', CONSTRAINT = 'entry_posting_count';
    END IF;
    -- sum() of bigint is numeric, so this sum can't overflow.
    SELECT string_agg(format('%s by %s', currency, total), ', ' ORDER BY currency) INTO off
      FROM (SELECT currency, sum(amount) AS total FROM postings WHERE entry_id = p_entry_id GROUP BY currency) sums
     WHERE total <> 0;
    IF off IS NOT NULL THEN
        RAISE EXCEPTION 'journal entry % does not balance: off in %', p_entry_id, off
            USING ERRCODE = 'check_violation', CONSTRAINT = 'entry_balanced';
    END IF;
END
$$;

CREATE FUNCTION ledger_check_entry_trigger() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'journal_entries' THEN
        PERFORM ledger_check_entry(NEW.id);
    ELSE
        PERFORM ledger_check_entry(NEW.entry_id);
    END IF;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER journal_entries_check AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_check_entry_trigger();

CREATE CONSTRAINT TRIGGER postings_check AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_check_entry_trigger();

-- I6: entries and postings are append-only. A mistake is corrected by a reversing entry, so the history shows
-- both the mistake and the correction.
CREATE FUNCTION ledger_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: post a reversing entry instead of changing history', TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation', CONSTRAINT = 'append_only';
END
$$;

CREATE TRIGGER journal_entries_append_only BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER journal_entries_no_truncate BEFORE TRUNCATE ON journal_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER postings_append_only BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER postings_no_truncate BEFORE TRUNCATE ON postings
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();
