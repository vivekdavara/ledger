-- Undo 0004, whose skip was unsound (found in review on day 1). SET CONSTRAINTS ... IMMEDIATE runs an entry's
-- own commit-time check early, and once run it is used up. A posting appended later in the same transaction then
-- skipped its check, because its entry was the transaction's own, and the entry committed with more postings than
-- it declared and out of balance. TestAnEarlyCheckDoesNotExcuseLaterPostings reproduces it.
--
-- Every posting's check is the full check again, as in 0003: it runs after the posting exists, so the last
-- posting's check always sees the final entry, whenever the client makes the checks run.

CREATE OR REPLACE FUNCTION ledger_check_entry_trigger() RETURNS trigger
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

DROP TRIGGER journal_entries_stamp ON journal_entries;
DROP FUNCTION ledger_stamp_entry();
ALTER TABLE journal_entries DROP COLUMN created_xact;
