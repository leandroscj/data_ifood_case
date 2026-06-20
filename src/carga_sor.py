from pyspark.sql.functions import lit, col
from utils.functions import get_metadata_table, write_bucket

def exec_carga_sor(competencia, path_entrada, table):

    df = spark.read.parquet(path_entrada)
    df = df.withColumn("competencia", lit(competencia))
    get_metadata_table(table, df)

    write_bucket(df, table)

if __name__ == "__main__":
    BUCKET = "datalake-ifood-case"
    VEHICLE_COMPETENCIAS = {
        "yellow": ["202301", "202302","202303", "202304", "202305"],
        "green": ["202301", "202302","202303", "202304", "202305"]
    }

    for vehicle, competencias in VEHICLE_COMPETENCIAS.items():
        for competencia in competencias:
            path = f"s3a://{BUCKET}/landing/competencia={competencia}/vehicle={vehicle}"
            table = f"sor.{vehicle}_taxi"
            exec_carga_sor(competencia, path, table)