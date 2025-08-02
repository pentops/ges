-- +goose Up

CREATE TABLE replay_event (
  replay_id text PRIMARY KEY,
  event_id text REFERENCES event(id) NOT NULL,
  queue_url text NOT NULL
);

CREATE TABLE replay_upsert (
  replay_id text PRIMARY KEY,
  grpc_service text NOT NULL,
  grpc_method text NOT NULL,
  entity_id text NOT NULL,
  queue_url text NOT NULL
);

CREATE TABLE replay_generic (
  replay_id text PRIMARY KEY, 
  grpc_service text NOT NULL,
  grpc_method text NOT NULL,
  message_id text NOT NULL,
  queue_url text NOT NULL
);

-- +goose Down

DROP TABLE replay_generic;
DROP TABLE replay_event;
DROP TABLE replay_upsert;
