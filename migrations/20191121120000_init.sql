-- +goose Up
-- SQL in this section is executed when the migration is applied.

-- accounts, unlike all the other tables
-- is not tenant tainted because at account
-- is meant to be a fundmentally shared store
-- of value across tenants.
CREATE TABLE IF NOT EXISTS accounts(
  account_pk BIGSERIAL PRIMARY KEY,
  -- account_id UUID DEFAULT gen_random_uuid() -> not supported in embedded
  account_id BIGSERIAL UNIQUE,
  user_ari TEXT UNIQUE,
  last_played_sequence BIGINT DEFAULT 0,
  running_balance BIGINT DEFAULT 0,
  running_held BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS transactions(
  transaction_pk BIGSERIAL PRIMARY KEY,
  transaction_id BIGSERIAL UNIQUE,
  tenant TEXT,
  account_id BIGINT REFERENCES accounts(account_id),
  held_amount_in_cents BIGINT,
  debited_amount_in_cents BIGINT,
  credited_amount_in_cents BIGINT,
  last_played_sequence BIGINT DEFAULT 0,
  created TIMESTAMPTZ DEFAULT NOW(),
  updated TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(transaction_id, tenant)
);

CREATE TABLE IF NOT EXISTS operations(
  operation_pk BIGSERIAL PRIMARY KEY,
  operation_id BIGSERIAL UNIQUE,
  tenant TEXT,
  transaction_id BIGINT,
  operation_type TEXT,
  amount_in_cents BIGINT,
  sequence BIGINT,
  created TIMESTAMPTZ DEFAULT NOW(),
  updated TIMESTAMPTZ DEFAULT NOW(),
  FOREIGN KEY (transaction_id, tenant) REFERENCES transactions(transaction_id, tenant),
  UNIQUE(operation_id, tenant)
);

CREATE TABLE IF NOT EXISTS events(
  event_pk BIGSERIAL PRIMARY KEY,
  event_id BIGSERIAL UNIQUE,
  tenant TEXT,
  account_id BIGINT REFERENCES accounts(account_id),
  transaction_id BIGINT,
  operation_id BIGINT,
  sequence BIGINT,
  running_balance BIGINT,
  running_held BIGINT,
  FOREIGN KEY (transaction_id, tenant) REFERENCES transactions(transaction_id, tenant),
  FOREIGN KEY (operation_id, tenant) REFERENCES operations(operation_id, tenant),
  UNIQUE(event_id, tenant)
);

-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
