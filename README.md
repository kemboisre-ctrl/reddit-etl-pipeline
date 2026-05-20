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

Inicio Rapido
Pre-requisitos
Docker + Docker Compose
Credenciais da API Reddit (https://www.reddit.com/prefs/apps)
Conta AWS (free tier e suficiente)
1. Clonar e Setup
git clone <url-do-seu-repo>
cd reddit-etl-pipeline
chmod +x setup.sh && ./setup.sh

2. Configurar Credenciais
Edite .env:
REDDIT_CLIENT_ID=seu_id_aqui
REDDIT_CLIENT_SECRET=seu_secret_aqui
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=seu-bucket-unico
REDSHIFT_HOST=seu-cluster.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=dev
REDSHIFT_USER=awsuser
REDSHIFT_PASSWORD=sua_senha
REDSHIFT_IAM_ROLE=arn:aws:iam::123456789012:role/RedshiftS3Role

3. Build e Inicializacao
docker-compose build
docker-compose up airflow-init
docker-compose up -d

4. Acessar Servicos
| Servico    | URL                     | Credenciais       |
| ---------- | ----------------------- | ----------------- |
| Airflow UI | <http://localhost:8080> | admin / admin     |
| Postgres   | localhost:5432          | airflow / airflow |

5. Executar o Pipeline
Abra a UI do Airflow -> ative o DAG reddit_etl_pipeline para On
Clique no botao play para disparar uma execucao manual
Acompanhe a progressao: extract -> transform -> load
Clique em qualquer tarefa -> Log para ver output em tempo real
6. Verificar Resultados
aws s3 ls s3://seu-bucket/raw/reddit/dataengineering/
psql -h seu-host-redshift -U awsuser -d dev -c "SELECT COUNT(*) FROM reddit_posts;"

Otimizacao do Redshift
CREATE TABLE reddit_posts (
    id              VARCHAR(10) DISTKEY,
    title           VARCHAR(500),
    author          VARCHAR(50),
    score           INTEGER,
    upvote_ratio    FLOAT,
    num_comments    INTEGER,
    created_utc     FLOAT,
    subreddit       VARCHAR(50) SORTKEY,
    url             VARCHAR(500),
    selftext        VARCHAR(2000),
    is_video        BOOLEAN,
    over_18         BOOLEAN,
    stickied        BOOLEAN,
    engagement_ratio FLOAT,
    high_engagement  BOOLEAN,
    processed_date   DATE,
    extracted_at     VARCHAR(30)
);

Terraform
cd infrastructure/terraform
terraform init
terraform plan -var="s3_bucket_name=seu-bucket-unico" -var="redshift_password=SenhaSegura123!"
terraform apply

Comandos Makefile
make build    # Build das imagens Docker
make init     # Inicializar DB do Airflow
make up       # Iniciar todos os servicos
make down     # Parar todos os servicos
make logs     # Acompanhar logs do scheduler
make test     # Executar testes unitarios
make clean    # Remover containers e volumes

Licenca
MIT — Construido para aprendizado e demonstracao de portfolio.

**Como usar:**
1. Clique no botão de copiar (ícone no canto superior direito do bloco de código)
2. Cole no arquivo `README.md` do seu repositório GitHub
3. Commit e push


