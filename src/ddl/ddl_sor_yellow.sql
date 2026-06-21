CREATE DATABASE IF NOT EXISTS sor;

CREATE TABLE IF NOT EXISTS sor.yellow_taxi (
    VendorID BIGINT,
    tpep_pickup_datetime TIMESTAMP_NTZ,
    tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    store_and_fwd_flag STRING,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE
)
USING DELTA
PARTITIONED BY (competencia INTEGER)
LOCATION 's3a://ifood-case-406207085720/sor/yellow_taxi/';
