CREATE DATABASE IF NOT EXISTS sor;

CREATE TABLE IF NOT EXISTS sor.green_taxi (
    VendorID BIGINT,
    lpep_pickup_datetime TIMESTAMP_NTZ,
    lpep_dropoff_datetime TIMESTAMP_NTZ,
    store_and_fwd_flag STRING,
    RatecodeID DOUBLE,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee INT,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type DOUBLE,
    trip_type DOUBLE,
    congestion_surcharge DOUBLE
)
USING DELTA
PARTITIONED BY (competencia INTEGER)
LOCATION 's3a://ifood-case-406207085720/sor/green_taxi/';
