-- +goose Up
CREATE TABLE event (
	id text PRIMARY KEY,
  timestamp timestamptz NOT NULL,
  entity_name text NOT NULL,

  data jsonb NOT NULL -- ges.v1.Event

);

CREATE TABLE upsert (
  entity_name text NOT NULL,
  entity_id text NOT NULL,
  last_event_id text NOT NULL,
  last_event_timestamp timestamptz NOT NULL,
  
  data jsonb NOT NULL, -- ges.v1.Upsert

  PRIMARY KEY (entity_name, entity_id)
);


-- +goose Down

DROP TABLE event;
DROP TABLE upsert;
