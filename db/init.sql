CREATE TABLE IF NOT EXISTS shipments (
    id uuid PRIMARY KEY,
    tracking_code varchar(64) NOT NULL UNIQUE,
    status varchar(20) NOT NULL CHECK (status IN ('created', 'dispatched')),
    version integer NOT NULL CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS outbox (
    id uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL REFERENCES shipments(id),
    event_type varchar(100) NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox (created_at, id) WHERE published_at IS NULL;
