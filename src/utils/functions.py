from pyspark.sql.functions import lit, col

def apply_metadata_table(schema_destino, df):
    colunas_formatadas = []
    for campo in schema_destino:
        nome_coluna = campo.name
        tipo_coluna = campo.dataType    
        if nome_coluna in df.columns:
            colunas_formatadas.append(col(nome_coluna).cast(tipo_coluna))  
        else:
            colunas_formatadas.append(lit(None).cast(tipo_coluna).alias(nome_coluna))
    
    df = df.select(*colunas_formatadas)
    return df

def write_bucket(df, target_table):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .saveAsTable(target_table)