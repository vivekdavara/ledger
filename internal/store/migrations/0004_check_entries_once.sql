-- Check each entry once at commit, not once per posting.
--
-- Under 0003 the commit-time check ran N + 1 times for an entry of N postings (after the entry insert and after
-- each posting insert), and each run read all N postings: quadratic in the entry's size. But the per-posting
-- check exists only for one case: a posting added to an entry that an earlier transaction committed. For an entry
-- created by the current transaction, the entry's own check runs at this commit anyway.
--
-- So each entry now records the transaction that created it, and a posting's check is skipped when its entry was
-- created by the current transaction. pg_current_xact_id() is the top-level transaction's 64-bit id even inside a
-- savepoint, so it never wraps around and a savepoint can't make an old entry look new. The client can't choose
-- the value (the trigger below overwrites it) or change it later (entries are append-only). Choosing it would be
-- an attack: transaction ids are sequential, so a stamp naming a later transaction would let that transaction
-- append to the entry unchecked.

ALTER TABLE journal_entries ADD COLUMN created_xact xid8;

CREATE FUNCTION ledger_stamp_entry() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.created_xact := pg_current_xact_id();
    RETURN NEW;
END
$$;

CREATE TRIGGER journal_entries_stamp BEFORE INSERT ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_stamp_entry();

CREATE OR REPLACE FUNCTION ledger_check_entry_trigger() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'journal_entries' THEN
        PERFORM ledger_check_entry(NEW.id);
    -- Entries from before this migration have no stamp, so they always get the full check.
    ELSIF NOT EXISTS (SELECT 1 FROM journal_entries
                       WHERE id = NEW.entry_id AND created_xact = pg_current_xact_id()) THEN
        PERFORM ledger_check_entry(NEW.entry_id);
    END IF;
    RETURN NULL;
END
$$;
