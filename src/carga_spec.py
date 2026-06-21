import json
from pyspark.sql.functions import lit, col, sum as spark_sum
from utils.functions import apply_metadata_table, write_bucket

def exec_carga_spec(spec_table, table_sot):
    schema_destino = spark.table(spec_table).limit(0).schema

    filtro = (
        (col("vehicle") == "yellow") & col("competencia").isin(VEHICLE_COMPETENCIAS["yellow"]) 
        | (col("vehicle") == "green") & col("competencia").isin(VEHICLE_COMPETENCIAS["green"]) 
    )

    df_sot = spark.table(table_sot).filter(filtro)
    df_sot = df_sot.groupBy(
            "VendorID",
            "passenger_count",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "vehicle",
            "competencia"
        ).agg(spark_sum("total_amount").alias("total_amount"))


    union_df = apply_metadata_table(schema_destino, df_sot)

    write_bucket(union_df, spec_table)

if __name__ == "__main__":
    path_param = "param.json"
    with open(path_param, "r") as f:
        params = json.load(f)

    VEHICLE_COMPETENCIAS = params["VEHICLE_COMPETENCIAS"]
    tables = params["carga_spec"]
    exec_carga_spec(tables["TABLE_SPEC"], tables["TABLE_SOT"])