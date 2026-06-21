# iFood Data Architect Case — NY Taxi

Pipeline de dados para processamento de viagens de táxi em Nova York (yellow e green), seguindo a arquitetura em camadas SOR → SOT → SPEC com armazenamento no S3 e processamento no Databricks.

## Arquitetura

O projeto segue o modelo de **medalhão** com três camadas de dados:

| Camada | Significado | Função |
|--------|-------------|--------|
| **SOR** (System of Record) | Espelho fiel dos dados de origem | Recebe os dados brutos do S3 (landing) em formato Parquet e persiste em tabelas Delta particionadas por competência. Sem transformações de negócio — os dados são mantidos o mais próximo possível da fonte original. |
| **SOT** (Source of Truth) | Fonte única da verdade | Unifica os dados de yellow e green taxi em uma tabela consolidada. Padroniza nomes de colunas, adiciona a coluna `vehicle` para identificar a frota de origem e resolve diferenças de schema entre as duas fontes. |
| **SPEC** (Specialized) | Dados prontos para consumo | Aplica regras de negócio e agregações sobre a SOT, gerando tabelas otimizadas para análise. Cada tabela SPEC responde a uma necessidade de negócio específica. |

Essa separação garante rastreabilidade (SOR), consistência (SOT) e performance nas consultas analíticas (SPEC), além de facilitar reprocessamento pontual em qualquer camada sem impactar as demais.

### Fluxo de dados

```
Landing (S3 - Parquet)
  │
  ├── yellow_tripdata_2023-01.parquet ... 05.parquet
  └── green_tripdata_2023-01.parquet ... 05.parquet
  │
  ▼
┌─────────────────────────────────────┐
│  create_tables.py (DDLs)            │  ← Cria os databases e tabelas Delta
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│  carga_sor.py                       │  ← Lê Parquet do S3 → grava nas tabelas SOR
│  sor.yellow_taxi / sor.green_taxi   │
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│  carga_sot.py                       │  ← Unifica yellow + green → sot.ny_taxi
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│  carga_spec.py                      │  ← Agrega dados SOT → spec.ny_taxi_consumers
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│  analysis/ (Queries SQL)            │  ← Consultas analíticas sobre a SPEC
└─────────────────────────────────────┘
```

## Estrutura do Repositório

```
data_ifood_case/
├── src/
│   ├── ddl/                          # DDLs de criação das tabelas Delta
│   │   ├── ddl_sor_yellow.sql        # Tabela sor.yellow_taxi
│   │   ├── ddl_sor_green.sql         # Tabela sor.green_taxi
│   │   ├── ddl_sot_taxi.sql          # Tabela sot.ny_taxi (unificada)
│   │   └── ddl_ny_taxi_consumers.sql # Tabela spec.ny_taxi_consumers
│   ├── utils/
│   │   └── functions.py              # Funções reutilizáveis (apply_metadata_table, write_bucket)
│   ├── create_tables.py              # Executa todos os DDLs automaticamente
│   ├── carga_sor.py                  # Ingestão landing → SOR
│   ├── carga_sot.py                  # Transformação SOR → SOT
│   ├── carga_spec.py                 # Agregação SOT → SPEC
│   └── param.json                    # Parâmetros de execução
├── analysis/
│   ├── media_valor.sql               # Média de total_amount por mês (yellow)
│   └── media_passageiro.sql          # Média de passageiros por hora (maio)
├── requirements.txt
└── README.md
```

## Organização no S3

O bucket segue uma estrutura de diretórios separada por camada. Cada camada armazena os dados Delta particionados por competência:

```
s3://ifood-case-406207085720/
├── landing/                              # Dados brutos de origem (Parquet)
│   ├── competencia=202301/
│   │   ├── vehicle=yellow/
│   │   └── vehicle=green/
│   ├── competencia=202302/
│   │   └── ...
│   └── ...
├── sor/                                  # System of Record (Delta)
│   ├── yellow_taxi/
│   │   ├── _delta_log/
│   │   ├── competencia=202301/
│   │   └── ...
│   └── green_taxi/
│       ├── _delta_log/
│       └── ...
├── sot/                                  # Source of Truth (Delta)
│   └── ny_taxi/
│       ├── _delta_log/
│       ├── competencia=202301/
│       └── ...
└── spec/                                 # Specialized (Delta)
    └── ny_taxi_consumers/
        ├── _delta_log/
        └── ...
```

## Parametrização

Todos os scripts leem seus parâmetros do arquivo `src/param.json`. Isso permite alterar o bucket, as competências a processar e os nomes das tabelas sem modificar o código:

```json
{
    "BUCKET": "ifood-case-406207085720",
    "VEHICLE_COMPETENCIAS": {
        "yellow": ["202301", "202302", "202303", "202304", "202305"],
        "green": ["202301", "202302", "202303", "202304", "202305"]
    },
    "carga_sot": {
        "TABLE_SOT": "sot.ny_taxi",
        "TABLE_YELLOW": "sor.yellow_taxi",
        "TABLE_GREEN": "sor.green_taxi"
    },
    "carga_spec": {
        "TABLE_SPEC": "spec.ny_taxi_consumers",
        "TABLE_SOT": "sot.ny_taxi"
    }
}
```

| Parâmetro | Descrição |
|-----------|-----------|
| `BUCKET` | Nome do bucket S3. Utilizado na carga SOR para montar o path de leitura e pelo `create_tables.py` para substituir o bucket nos DDLs. |
| `VEHICLE_COMPETENCIAS` | Dicionário que define quais competências (meses) processar para cada tipo de veículo. Para adicionar ou remover meses, basta alterar as listas. |
| `carga_sot` | Nomes das tabelas de entrada (SOR) e saída (SOT) para a carga SOT. |
| `carga_spec` | Nomes das tabelas de entrada (SOT) e saída (SPEC) para a carga SPEC. |

## Como os scripts funcionam

### `create_tables.py`

Lê todos os arquivos `.sql` da pasta `src/ddl/`, substitui o nome do bucket pelo valor em `param.json` e executa cada statement via `spark.sql()`. Isso cria os databases (`sor`, `sot`, `spec`) e as tabelas Delta com particionamento por `competencia`.

### `carga_sor.py`

Para cada veículo e competência definidos no `param.json`:

1. Lê os arquivos Parquet da landing zone no S3
2. Adiciona a coluna `competencia` com o valor do mês sendo processado
3. Aplica `apply_metadata_table` — que lê o schema da tabela de destino e reordena/casta as colunas do DataFrame para corresponder exatamente ao schema da tabela
4. Escreve na tabela SOR usando `partitionOverwriteMode=dynamic`

### `carga_sot.py`

1. Lê as tabelas `sor.yellow_taxi` e `sor.green_taxi` filtrando pelas competências do `param.json`
2. Renomeia as colunas `lpep_*` do green para `tpep_*` (padronização)
3. Adiciona a coluna `vehicle` em cada DataFrame (`"yellow"` / `"green"`)
4. Faz o `unionByName` com `allowMissingColumns=True` — colunas exclusivas de um veículo ficam como NULL no outro
5. Aplica o schema da tabela SOT e escreve

### `carga_spec.py`

1. Lê a tabela `sot.ny_taxi` filtrando pelas competências do `param.json`
2. Faz `GROUP BY` em `VendorID`, `passenger_count`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `vehicle`, `competencia`
3. Agrega com `SUM(total_amount)`
4. Aplica o schema da tabela SPEC e escreve

### `apply_metadata_table`

Função central que garante conformidade entre o DataFrame e a tabela de destino:

- Consulta o schema da tabela de destino via `spark.table(tabela).limit(0).schema`
- Para cada coluna do schema: se existe no DataFrame, aplica o cast do tipo correto; se não existe, adiciona como `NULL` com o tipo esperado
- Retorna o DataFrame com as colunas na ordem e tipos exatos da tabela

### `write_bucket`

Escreve o DataFrame usando Delta com `partitionOverwriteMode=dynamic`. O Delta detecta automaticamente quais partições (valores de `competencia`) existem no DataFrame e sobrescreve apenas elas, sem afetar as demais. Isso garante idempotência — rodar o mesmo processo duas vezes com os mesmos dados gera o mesmo resultado.

## Execução no Databricks

### Pré-requisitos

1. **Bucket S3** criado na AWS
2. **Databricks workspace** com acesso ao S3
3. **Conector S3** configurado no Databricks

### Passo 1 — Criar o bucket S3

No console da AWS:

1. Acesse **S3 → Create bucket**
2. Defina o nome do bucket (ex: `ifood-case-406207085720`)
3. Selecione a região (de preferência a mesma do Databricks)
4. Mantenha "Block all public access" ativado
5. Faça upload dos arquivos Parquet na estrutura de pastas descrita na seção [Organização no S3](#organização-no-s3)

### Passo 2 — Configurar o acesso do Databricks ao S3

O Databricks precisa de permissão para ler e escrever no bucket. Existem duas formas de configurar:

**Opção A — Instance Profile (recomendado para produção):**

Associar uma IAM Role ao cluster do Databricks com permissões de leitura/escrita no bucket S3.

Documentação: [Configure S3 access with instance profiles](https://docs.databricks.com/aws/en/connect/storage/tutorial-s3-instance-profile)

**Opção B — Unity Catalog com External Location:**

Registrar o bucket como External Location no Unity Catalog usando uma Storage Credential.

Documentação: [Create an external location to connect cloud storage to Databricks](https://docs.databricks.com/aws/en/connect/unity-catalog/external-locations)

### Passo 3 — Importar o código no Databricks

1. No Databricks, vá em **Workspace → Import**
2. Importe os arquivos da pasta `src/` para o Workspace
3. Verifique que a estrutura de pastas ficou assim no Workspace:
   ```
   /Workspace/Users/seu-email/
   ├── ddl/
   │   ├── ddl_sor_yellow.sql
   │   ├── ddl_sor_green.sql
   │   ├── ddl_sot_taxi.sql
   │   └── ddl_ny_taxi_consumers.sql
   ├── utils/
   │   └── functions.py
   ├── create_tables.py
   ├── carga_sor.py
   ├── carga_sot.py
   ├── carga_spec.py
   └── param.json
   ```

### Passo 4 — Ordem de execução

Execute os scripts na seguinte ordem, cada um em uma célula de notebook ou como arquivo Python:

| Ordem | Script | O que faz |
|-------|--------|-----------|
| 1 | `create_tables.py` | Cria os databases e tabelas Delta no catálogo |
| 2 | `carga_sor.py` | Carrega dados brutos do S3 (landing) para as tabelas SOR |
| 3 | `carga_sot.py` | Unifica yellow e green em uma tabela SOT |
| 4 | `carga_spec.py` | Agrega dados da SOT na tabela SPEC |

Para executar um arquivo `.py` via notebook, use:

```python
%run ./create_tables
%run ./carga_sor
%run ./carga_sot
%run ./carga_spec
```

### Passo 5 — Consultas analíticas

Após a execução, utilize as queries da pasta `analysis/` no SQL Editor do Databricks:

- `media_valor.sql` — Média de `total_amount` por mês para yellow táxis
- `media_passageiro.sql` — Média de passageiros por hora do dia em maio

## Implementação 100% AWS

Se o projeto fosse implementado inteiramente na AWS sem o Databricks, a arquitetura ficaria assim:

### Serviços utilizados

| Componente | Serviço AWS | Função |
|------------|-------------|--------|
| Armazenamento | **S3** | Data Lake com a mesma estrutura de pastas (landing, sor, sot, spec) |
| Catálogo | **AWS Glue Data Catalog** | Catálogo de metadados das tabelas, substituindo o Unity Catalog do Databricks |
| Processamento | **AWS Glue Jobs (PySpark)** | Execução dos scripts de carga (SOR, SOT, SPEC) usando Spark serverless |
| Orquestração | **AWS Step Functions** | Workflow que encadeia a execução: Create Tables → Carga SOR → Carga SOT → Carga SPEC |
| Trigger | **AWS Lambda + S3 Event** | Ao receber novos arquivos na landing zone, um evento do S3 dispara uma Lambda que inicia o workflow no Step Functions |
| Consultas | **Amazon Athena** | Consultas SQL diretamente sobre as tabelas Delta no S3, sem necessidade de cluster |
| Monitoramento | **CloudWatch** | Logs e métricas dos Glue Jobs e Step Functions |

### Fluxo

```
Arquivo chega no S3 (landing/)
  │
  ▼
S3 Event Notification → Lambda → Step Functions
  │
  ▼
Step Functions executa em sequência:
  ├── Glue Job: create_tables
  ├── Glue Job: carga_sor
  ├── Glue Job: carga_sot
  └── Glue Job: carga_spec
  │
  ▼
Athena consulta as tabelas SPEC
```

### Diferenças em relação à implementação com Databricks

| Aspecto | Databricks | 100% AWS |
|---------|------------|----------|
| Processamento | Cluster gerenciado ou Serverless | Glue Jobs (Spark serverless) |
| Catálogo | Unity Catalog | Glue Data Catalog |
| Orquestração | Notebooks / Workflows | Step Functions |
| SQL interativo | SQL Editor / SQL Warehouse | Athena |
| Delta Lake | Nativo | Requer configuração do Delta connector no Glue |
| Custo | Pay-per-use (DBUs) | Pay-per-use (DPUs no Glue, dados escaneados no Athena) |

### Referências

- [AWS Glue - Documentação](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
- [AWS Step Functions - Documentação](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html)
- [Amazon Athena - Documentação](https://docs.aws.amazon.com/athena/latest/ug/what-is.html)
- [Delta Lake no Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html)
- [Databricks - External Locations (S3)](https://docs.databricks.com/aws/en/connect/unity-catalog/external-locations)
- [Databricks - Instance Profile (S3)](https://docs.databricks.com/aws/en/connect/storage/tutorial-s3-instance-profile)
