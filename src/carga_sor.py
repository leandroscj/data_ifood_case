import os
import json
from pyspark.sql.functions import lit, col
from utils.functions import apply_metadata_table, write_bucket

def exec_carga_sor(competencia, path_entrada, table, schema_destino):
    df = spark.read.parquet(path_entrada)
    df = df.withColumn("competencia", lit(competencia))
    df = apply_metadata_table(schema_destino, df)

    write_bucket(df, table)

if __name__ == "__main__":
    path_param = os.path.join(os.path.dirname(os.path.abspath(__file__)), "param.json")
    with open(path_param, "r") as f:
        params = json.load(f)

    BUCKET = params["BUCKET"]
    VEHICLE_COMPETENCIAS = params["VEHICLE_COMPETENCIAS"]
    
    for vehicle, competencias in VEHICLE_COMPETENCIAS.items():
        table = f"sor.{vehicle}_taxi"
        schema_destino = spark.table(table).limit(0).schema
        for competencia in competencias:
            path = f"s3a://{BUCKET}/landing/competencia={competencia}/vehicle={vehicle}"
            exec_carga_sor(competencia, path, table, schema_destino)