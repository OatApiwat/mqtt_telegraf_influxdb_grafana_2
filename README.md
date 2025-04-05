# mqtt_telegraf_influxdb_grafana
## influx
## use iot_data
## SELECT * FROM "mqtt_consumer" WHERE "topic" = 'iot/data_1' AND time > now() - 1h;
## SELECT * FROM "mqtt_status" WHERE "topic" = 'iot/data_3' AND time > now() - 1h;

## DELETE FROM "mqtt_consumer"

## CREATE RETENTION POLICY "7_days" ON "iot_data" DURATION 7d REPLICATION 1 DEFAULT

## SHOW RETENTION POLICIES ON "iot_data"

## SHOW FIELD KEYS FROM "mqtt_consumer"
## DELETE FROM iot_data WHERE measurement IS NOT NULL;


