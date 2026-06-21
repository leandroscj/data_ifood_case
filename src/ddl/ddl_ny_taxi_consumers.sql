CREATE DATABASE IF NOT EXISTS spec;

CREATE TABLE IF NOT EXISTS spec.ny_taxi_consumers (
    VendorID                BIGINT,
    passenger_count         DOUBLE,
    tpep_pickup_datetime    TIMESTAMP_NTZ,
    tpep_dropoff_datetime   TIMESTAMP_NTZ,
    vehicle                 STRING,
    total_amount            DOUBLE
)
USING DELTA
PARTITIONED BY (competencia INTEGER)
LOCATION 's3a://ifood-case-406207085720/spec/ny_taxi_consumers/';
