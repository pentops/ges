-- +goose Up
CREATE TABLE event (
  grpc_service text NOT NULL,
  grpc_method text NOT NULL,

	id text PRIMARY KEY,
  timestamp timestamptz NOT NULL,
  entity_name text NOT NULL,

  data jsonb NOT NULL -- ges.v1.Event

);

CREATE TABLE upsert (
  grpc_service text NOT NULL,
  grpc_method text NOT NULL,

  entity_name text NOT NULL,
  entity_id text NOT NULL,
  last_event_id text NOT NULL,
  last_event_timestamp timestamptz NOT NULL,
  
  data jsonb NOT NULL, -- ges.v1.Upsert

  PRIMARY KEY (grpc_service, grpc_method, entity_id)
);

CREATE TABLE generic (
  grpc_service text NOT NULL,
  grpc_method text NOT NULL,
  message_id text PRIMARY KEY,
  timestamp timestamptz NOT NULL,
  data jsonb NOT NULL -- original message
);


-- +goose Down

DROP TABLE generic;
DROP TABLE event;
DROP TABLE upsert;
