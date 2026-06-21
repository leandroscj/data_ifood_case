import os
import json
from pyspark.sql.functions import lit, col
from utils.functions import apply_metadata_table, write_bucket

def exec_carga_sot(sot_table, table_yellow, table_green):
    schema_destino = spark.table(sot_table).limit(0).schema

    df_yellow = spark.table(table_yellow).filter(col("competencia").isin(VEHICLE_COMPETENCIAS["yellow"]))
    df_green = spark.table(table_green).filter(col("competencia").isin(VEHICLE_COMPETENCIAS["green"]))
    
    df_green = df_green.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime") \
        .withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
    
    df_green = df_green.withColumn("vehicle", lit("green"))
    df_yellow = df_yellow.withColumn("vehicle", lit("yellow"))

    union_df = df_yellow.unionByName(df_green, allowMissingColumns=True)
    union_df = apply_metadata_table(schema_destino, union_df)

    write_bucket(union_df, sot_table)

if __name__ == "__main__":
    path_param = os.path.join(os.path.dirname(os.path.abspath(__file__)), "param.json")
    with open(path_param, "r") as f:
        params = json.load(f)

    VEHICLE_COMPETENCIAS = params["VEHICLE_COMPETENCIAS"]
    tables = params["carga_sot"]
    exec_carga_sot(tables["TABLE_SOT"], tables["TABLE_YELLOW"], tables["TABLE_GREEN"])
