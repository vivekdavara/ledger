-- What the API (day 2) needs from the schema: idempotency keys, and balances whose shown value always fits.

-- A PostEntry request carries a key chosen by the client. The key, a SHA-256 of the request and the entry it
-- created are written in the entry's own transaction, so either the entry and its key both exist or neither does.
-- A retry with the same key and request finds the row and gets the original entry back; the same key with a
-- different request is refused. Keys are visible ASCII so they can travel in an HTTP header unchanged.
CREATE TABLE idempotency_keys (
    key          text PRIMARY KEY,
    request_hash bytea NOT NULL,
    entry_id     bigint NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT idempotency_keys_key_format CHECK (key ~ '^[!-~]{1,200}$'),
    CONSTRAINT idempotency_keys_hash_length CHECK (octet_length(request_hash) = 32),
    CONSTRAINT idempotency_keys_entry_fkey FOREIGN KEY (entry_id) REFERENCES journal_entries (id),
    CONSTRAINT idempotency_keys_one_per_entry UNIQUE (entry_id)
);

-- The API shows a credit-normal account's balance negated (DESIGN.md, "Sign convention"). Every bigint has a
-- negation except the smallest, so the range is made symmetric: a posting that would land exactly on -2^63 fails
-- here, like one that would go past it fails on bigint's range.
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_negatable CHECK (balance > -9223372036854775808);
