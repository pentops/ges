-- +goose Up

CREATE TABLE replay_event (
  replay_id text PRIMARY KEY, -- {queue_url}/{event_id}
  event_id text REFERENCES event(id) NOT NULL,
  queue_url text NOT NULL
);

CREATE TABLE replay_upsert (
  replay_id text PRIMARY KEY, -- {queue_url}/{entity_name}/{entity_id}
  entity_name text NOT NULL,
  entity_id text NOT NULL,
  queue_url text NOT NULL
);

-- +goose Down

DROP TABLE replay_event;
DROP TABLE replay_upsert;
