CREATE TABLE IF NOT EXISTS instrument_tracking (
 symbol text PRIMARY KEY CHECK (symbol ~ '^[A-Z0-9]+$'),
 enabled boolean NOT NULL,
 version bigint NOT NULL CHECK (version > 0),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS tracking_rejections (
 topic text NOT NULL,
 partition_id integer NOT NULL,
 offset_id bigint NOT NULL,
 reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(topic, partition_id, offset_id)
);
