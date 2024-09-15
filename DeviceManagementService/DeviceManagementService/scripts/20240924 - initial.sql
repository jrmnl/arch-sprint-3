CREATE TABLE device_item (
  device_item_id    UUID PRIMARY KEY DEFAULT gen_random_uuid()
 ,device_type       VARCHAR(255) NOT NULL
 ,name              VARCHAR(255) NOT NULL
 ,model             VARCHAR(255) NOT NULL
 ,device_address    VARCHAR(255) NOT NULL
 ,serial_number     VARCHAR(255) NOT NULL
 ,status            VARCHAR(50) NOT NULL
 ,user_id           UUID
 ,home_id           UUID
);
