-- +goose Up
CREATE TABLE event (
	id text PRIMARY KEY,
  timestamp timestamptz NOT NULL,
  entity_name text NOT NULL,

  data jsonb NOT NULL
);


-- +goose Down

DROP TABLE event;
