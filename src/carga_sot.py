from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from utils.functions import apply_metadata_table, write_bucket

def exec_carga_sot(sot_table):
    schema_destino = spark.table(sot_table).limit(0).schema

    df_yellow = spark.table("sor.yellow_taxi").filter(col("competencia").isin(VEHICLE_COMPETENCIAS["yellow"]))
    df_green = spark.table("sor.green_taxi").filter(col("competencia").isin(VEHICLE_COMPETENCIAS["green"]))
    
    df_green = df_green.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime") \
        .withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
    
    df_green = df_green.withColumn("vehicle", lit("green"))
    df_yellow = df_yellow.withColumn("vehicle", lit("yellow"))

    union_df = df_yellow.unionByName(df_green, allowMissingColumns=True)
    union_df = apply_metadata_table(schema_destino, union_df)

    write_bucket(union_df, sot_table)

if __name__ == "__main__":
    VEHICLE_COMPETENCIAS = {
        "yellow": ["202301", "202302","202303", "202304", "202305"],
        "green": ["202301", "202302","202303", "202304", "202305"]
    }
    TABLE = "sot.green_taxi"

    exec_carga_sot(TABLE)
