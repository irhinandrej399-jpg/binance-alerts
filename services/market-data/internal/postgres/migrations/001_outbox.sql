CREATE TABLE IF NOT EXISTS outbox (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 event_id text NOT NULL UNIQUE CHECK (event_id <> ''),
 message_key text NOT NULL CHECK (message_key <> ''),
 payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS outbox_published_idx ON outbox(published_at) WHERE published_at IS NOT NULL;
