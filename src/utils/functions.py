from pyspark.sql.functions import lit, col

def get_metadata_table(target_table, df):
    schema_destino = spark.table(target_table).limit(0).schema

    colunas_formatadas = []
    for campo in schema_destino:
        nome_coluna = campo.name
        tipo_coluna = campo.dataType    
        if nome_coluna in df.columns:
            colunas_formatadas.append(col(nome_coluna).cast(tipo_coluna))  
        else:
            colunas_formatadas.append(lit(None).cast(tipo_coluna).alias(nome_coluna))

def write_bucket(df, target_table):
    df.write \
        .mode("overwrite") \
        .option("dynamicPartitionOverwrite", "true") \
        .format("parquet") \
        .insertInto(target_table)