CREATE DATABASE IF NOT EXISTS sot;

CREATE TABLE IF NOT EXISTS sot.ny_taxi (
    VendorID            BIGINT,
    tpep_pickup_datetime  TIMESTAMP_NTZ,
    tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count     DOUBLE,
    trip_distance       DOUBLE,
    RatecodeID          DOUBLE,
    store_and_fwd_flag  STRING,
    PULocationID        BIGINT,
    DOLocationID        BIGINT,
    payment_type        DOUBLE,
    fare_amount         DOUBLE,
    extra               DOUBLE,
    mta_tax             DOUBLE,
    tip_amount          DOUBLE,
    tolls_amount        DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount        DOUBLE,
    congestion_surcharge  DOUBLE,
    airport_fee         DOUBLE,
    ehail_fee           INT,
    trip_type           DOUBLE,
    vehicle             STRING
)
USING DELTA
PARTITIONED BY (competencia INTEGER)
LOCATION 's3a://ifood-case-406207085720/sot/ny_taxi/';
