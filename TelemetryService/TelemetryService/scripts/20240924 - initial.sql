CREATE TABLE device_item (
  device_item_id  UUID PRIMARY KEY
 ,device_type     VARCHAR(255) NOT NULL
 ,name            VARCHAR(255) NOT NULL
 ,model           VARCHAR(255) NOT NULL
 ,device_address  VARCHAR(255) NOT NULL
 ,serial_number   VARCHAR(255) NOT NULL
 ,status          VARCHAR(50) NOT NULL
 ,user_id         UUID
 ,home_id         UUID
);

CREATE TABLE telemetry (
  telemetry_id    SERIAL PRIMARY KEY
 ,collected_at    TIMESTAMP NOT NULL
 ,value           VARCHAR(10) NOT NULL
 ,value_type      VARCHAR(50) NOT NULL
 ,device_item_id  UUID NOT NULL
 ,CONSTRAINT fk_device_item FOREIGN KEY (device_item_id)
    REFERENCES device_item(device_item_id) ON DELETE CASCADE
);

CREATE INDEX idx_telemetry_device_item_id ON telemetry(device_item_id);
