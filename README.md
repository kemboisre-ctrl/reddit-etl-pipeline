# Pipeline ETL Reddit

Pipeline de engenharia de dados end-to-end que extrai dados de midia social do Reddit, orquestrado com **Apache Airflow (TaskFlow API + LocalExecutor)**, transformado com **PySpark** e carregado no **Amazon Redshift**.

![Arquitetura](docs/reddit_etl_architecture.png)

## Stack Tecnologico

| Camada | Tecnologia | Proposito |
|---|---|---|
| **Fonte** | Reddit API (PRAW) | Extracao de dados de midia social |
| **Orquestracao** | Apache Airflow (TaskFlow API + LocalExecutor) | DAGs modernos e pythonicos, execucao paralela em subprocessos |
| **Data Lake** | Amazon S3 | Armazenamento de JSONL bruto e Parquet curado |
| **Catalogo** | AWS Glue Data Catalog | Descoberta de schema e metadados de tabelas |
| **Processamento** | PySpark (modo local[*]) | Transformacao estilo distribuido em maquina unica |
| **Data Warehouse** | Amazon Redshift Serverless | Dados estruturados prontos para analise |
| **Analytics** | Jupyter + SQL | Analise de tendencias e metricas de engajamento |
| **IaC** | Terraform | Provisionamento de S3 + Glue + Redshift |

## Destaques da Arquitetura

- **TaskFlow API**: Decoradores modernos `@dag` e `@task` para codigo DAG pythonico e limpo com passagem automatica de XCom
- **LocalExecutor**: Tarefas executam como subprocessos paralelos na maquina do scheduler — sem Redis, sem Celery, sem containers de workers
- **Extracao com rate limiting**: Wrapper PRAW customizado com controle de taxa estilo token-bucket e rastreamento de paginacao
- **Particionamento S3**: `raw/subreddit/year=/month=/day=` para queries de time-travel e otimizacao de custo Athena
- **Tarefas idempotentes**: Seguro re-executar sem duplicatas (sobrescrita S3 + upsert com tabela de staging no Redshift)
- **Boas praticas de XCom**: Apenas metadados (URIs S3, contagens) passam pelo Airflow — dados brutos fluem S3 -&gt; Spark -&gt; S3 -&gt; Redshift

## Por que TaskFlow API?

A **TaskFlow API** (introduzida no Airflow 2.0+) substitui o legado `PythonOperator` por decoradores Python:

| Legado (PythonOperator) | Moderno (TaskFlow) |
|---|---|
| `task = PythonOperator(task_id="x", python_callable=func)` | `@task def func(): ...` |
| `ti.xcom_pull()` / `ti.xcom_push()` manual | Passagem automatica via argumentos de funcao |
| Definicao imperativa de DAG | Composicao declarativa e pythonica de funcoes |
| Boilerplate verboso | Codigo limpo e legivel |

**Exemplo:**
```python
@task
def extract() -&gt; list:
    return [{"s3_uri": "s3a://..."}]

@task
def transform(meta: list) -&gt; dict:
    return run_spark_transform(meta)

@dag
def pipeline():
    transform(extract())
