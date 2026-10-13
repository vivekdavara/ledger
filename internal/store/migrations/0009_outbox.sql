-- Day 3: a transactional outbox. Every journal entry has one entry_posted event, written by a trigger in the entry's
-- own transaction, so an entry and its event commit together or not at all, whichever client wrote the entry. A
-- publisher (internal/outbox) delivers the unpublished events and marks them published.
--
-- An event row holds only its entry's id. Entries and postings are append-only, so the publisher builds the event
-- from them when it delivers, and it is the same whenever that is.

CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_id     bigint NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    -- Deliveries tried, and the last failure's message, for whoever looks into an event that won't go out.
    attempts     integer NOT NULL DEFAULT 0,
    last_error   text,
    CONSTRAINT outbox_entry_fkey FOREIGN KEY (entry_id) REFERENCES journal_entries (id),
    CONSTRAINT outbox_one_per_entry UNIQUE (entry_id),
    CONSTRAINT outbox_attempts_nonnegative CHECK (attempts >= 0)
);

-- The publisher's queue: the events still to deliver, oldest first. A published event leaves the index.
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE FUNCTION ledger_enqueue_entry() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO outbox (entry_id) VALUES (NEW.id);
    RETURN NULL;
END
$$;

-- Like every ledger function, it resolves names in the ledger's own schema (0007).
DO $$
BEGIN
    EXECUTE format('ALTER FUNCTION ledger_enqueue_entry() SET search_path = %I, pg_temp', current_schema());
END
$$;

CREATE TRIGGER journal_entries_enqueue AFTER INSERT ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_enqueue_entry();

-- The entries recorded before this migration get their events too, in the order they were posted. None is missed:
-- CREATE TRIGGER above waited for every transaction inserting entries to end, and holds off new ones until this
-- migration commits, when they see the trigger.
INSERT INTO outbox (entry_id) SELECT id FROM journal_entries ORDER BY id;
