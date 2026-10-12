-- The ledger's tables and every rule that one row can check by itself. Amounts are bigint minor units; positive
-- is a debit, negative a credit (DESIGN.md, "Sign convention"). The rules that span rows are in 0002 and 0003.

CREATE TABLE currencies (
    code     text PRIMARY KEY CHECK (code ~ '^[A-Z]{3}$'),
    exponent smallint NOT NULL CHECK (exponent BETWEEN 0 AND 4)
);

-- Must match the registry in internal/money; TestCurrenciesMatchTheGoRegistry checks it.
INSERT INTO currencies (code, exponent) VALUES
    ('CAD', 2), ('CHF', 2), ('EUR', 2), ('GBP', 2), ('INR', 2), ('JPY', 0), ('KWD', 3), ('USD', 2);

CREATE TABLE accounts (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name           text NOT NULL,
    type           text NOT NULL,
    currency       text NOT NULL REFERENCES currencies (code),
    allow_negative boolean NOT NULL DEFAULT false,
    balance        bigint NOT NULL DEFAULT 0,
    version        bigint NOT NULL DEFAULT 0,
    created_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT accounts_name_unique UNIQUE (name),
    CONSTRAINT accounts_name_length CHECK (name <> '' AND char_length(name) <= 200),
    CONSTRAINT accounts_type_known CHECK (type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
    CONSTRAINT accounts_version_nonnegative CHECK (version >= 0),
    -- The target of the postings' composite foreign key, which makes a posting's currency its account's.
    CONSTRAINT accounts_id_currency UNIQUE (id, currency),
    -- On its normal side a protected account never goes below zero. Written without negating balance, which
    -- would overflow for the smallest bigint. An unknown type is accounts_type_known's to report, not this one's.
    CONSTRAINT accounts_no_overdraft CHECK (
        allow_negative OR CASE
            WHEN type IN ('asset', 'expense') THEN balance >= 0
            WHEN type IN ('liability', 'equity', 'revenue') THEN balance <= 0
            ELSE true
        END
    )
);

CREATE TABLE journal_entries (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    description   text NOT NULL DEFAULT '',
    posting_count smallint NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT entries_description_length CHECK (char_length(description) <= 500),
    CONSTRAINT entries_posting_count_range CHECK (posting_count BETWEEN 2 AND 64)
);

CREATE TABLE postings (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_id        bigint NOT NULL,
    account_id      bigint NOT NULL,
    currency        text NOT NULL,
    amount          bigint NOT NULL,
    balance_after   bigint NOT NULL,
    account_version bigint NOT NULL,
    CONSTRAINT postings_entry_fkey FOREIGN KEY (entry_id) REFERENCES journal_entries (id),
    CONSTRAINT postings_account_currency_fkey FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency),
    CONSTRAINT postings_amount_range CHECK (amount <> 0 AND amount BETWEEN -1000000000000000 AND 1000000000000000),
    CONSTRAINT postings_account_version_positive CHECK (account_version >= 1),
    -- One posting per account per entry. The overdraft check runs per posting, so two postings to one account
    -- (+100 then -50) could fail on an intermediate balance the entry as a whole never reaches.
    CONSTRAINT postings_one_per_account UNIQUE (entry_id, account_id),
    -- An account's postings are numbered 1, 2, 3, ...: the history's order and its pagination key.
    CONSTRAINT postings_account_version_unique UNIQUE (account_id, account_version)
);
