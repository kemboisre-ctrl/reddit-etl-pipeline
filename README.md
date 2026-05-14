Pipeline ETL Reddit
Pipeline de engenharia de dados end-to-end que extrai dados de mídia social do Reddit, orquestrado com Apache Airflow (TaskFlow API + LocalExecutor), transformado com PySpark e carregado no Amazon Redshift.
docs/reddit_etl_architecture.png
Stack Tecnológico
Table
Camada	Tecnologia	Propósito
Fonte	Reddit API (PRAW)	Extração de dados de mídia social
Orquestração	Apache Airflow (TaskFlow API + LocalExecutor)	DAGs modernos e pythonicos, execução paralela em subprocessos
Data Lake	Amazon S3	Armazenamento de JSONL bruto e Parquet curado
Catálogo	AWS Glue Data Catalog	Descoberta de schema e metadados de tabelas
Processamento	PySpark (modo local[*])	Transformação estilo distribuído em máquina única
Data Warehouse	Amazon Redshift Serverless	Dados estruturados prontos para análise
Analytics	Jupyter + SQL	Análise de tendências e métricas de engajamento
IaC	Terraform	Provisionamento de S3 + Glue + Redshift
Destaques da Arquitetura
TaskFlow API: Decoradores modernos @dag e @task para código DAG pythonico e limpo com passagem automática de XCom
LocalExecutor: Tarefas executam como subprocessos paralelos na máquina do scheduler — sem Redis, sem Celery, sem containers de workers
Extração com rate limiting: Wrapper PRAW customizado com controle de taxa estilo token-bucket e rastreamento de paginação
Particionamento S3: raw/subreddit/year=/month=/day= para queries de time-travel e otimização de custo Athena
Tarefas idempotentes: Seguro re-executar sem duplicatas (sobrescrita S3 + upsert com tabela de staging no Redshift)
Boas práticas de XCom: Apenas metadados (URIs S3, contagens) passam pelo Airflow — dados brutos fluem S3 → Spark → S3 → Redshift
Por que TaskFlow API?
A TaskFlow API (introduzida no Airflow 2.0+) substitui o legado PythonOperator por decoradores Python:
Table
Legado (PythonOperator)	Moderno (TaskFlow)
task = PythonOperator(task_id="x", python_callable=func)	@task def func(): ...
ti.xcom_pull() / ti.xcom_push() manual	Passagem automática via argumentos de função
Definição imperativa de DAG	Composição declarativa e pythonica de funções
Boilerplate verboso	Código limpo e legível
Exemplo:
Python
Copy
@task
def extract() -> list:
    return [{"s3_uri": "s3a://..."}]

@task
def transform(meta: list) -> dict:  # meta é passado automaticamente via XCom
    return run_spark_transform(meta)

@dag
def pipeline():
    transform(extract())  # Dependências inferidas automaticamente
Por que LocalExecutor?
Table
Aspecto	CeleryExecutor	LocalExecutor
Containers	10 (Redis, workers, cluster Spark)	4 (Postgres, Airflow apenas)
Memória	~6-8 GB	~2-3 GB
Complexidade	Filas de mensagens, gerenciamento de workers	Spawn simples de subprocessos
Ideal para	Sistemas distribuídos em produção	Aprendizado, dev em máquina única
Para um projeto de portfólio/aprendizado, o LocalExecutor é perfeitamente adequado. Ainda demonstra:
Execução paralela de tarefas (subprocessos)
Isolamento de tarefas e lógica de retry
Agendamento e monitoramento completos do Airflow
Estrutura do Projeto
plain
Copy
reddit-etl-pipeline/
├── docker-compose.yml              # 4 serviços: Postgres + Airflow (web/scheduler/init)
├── Makefile                        # Comandos comuns
├── .env.example                    # Template para secrets
├── .gitignore                      # Evita commit de credenciais
├── setup.sh                        # Init de diretórios (execute uma vez)
├── README.md                       # Este arquivo
│
├── docker/
│   ├── Dockerfile.airflow          # Airflow + PySpark + PRAW + Java
│   └── requirements.txt            # Todas dependências Python fixadas
│
├── dags/
│   └── reddit_etl_dag.py           # DAG TaskFlow: decoradores @dag + @task
│
├── src/
│   ├── extract/
│   │   └── reddit_client.py        # Wrapper PRAW (rate limiting, paginação)
│   ├── transform/
│   │   └── spark_job.py            # PySpark em modo local[*]
│   └── load/
│       └── redshift_loader.py      # COPY + upsert com tabela de staging
│
├── tests/
│   └── test_reddit_client.py      # Testes unitários (mocking, rate limit, serialização)
│
├── notebooks/
│   └── analytics_demo.ipynb       # Showcase de portfólio
│
├── scripts/
│   └── init_db.sql                # DDL Redshift com DISTKEY/SORTKEY
│
├── infrastructure/
│   └── terraform/
│       ├── main.tf                # S3 + Glue + Redshift Serverless
│       ├── variables.tf           # Inputs
│       └── outputs.tf             # Outputs
│
└── docs/
    └── reddit_etl_architecture.png  # Diagrama de arquitetura
Início Rápido
Pré-requisitos
Docker + Docker Compose
Credenciais da API Reddit (obtenha aqui)
Conta AWS (free tier é suficiente)
1. Clonar & Setup
bash
Copy
git clone <url-do-seu-repo>
cd reddit-etl-pipeline
chmod +x setup.sh && ./setup.sh
2. Configurar Credenciais
Edite .env:
bash
Copy
# Reddit (https://www.reddit.com/prefs/apps → crie app "script")
REDDIT_CLIENT_ID=seu_id_aqui
REDDIT_CLIENT_SECRET=seu_secret_aqui

# AWS (IAM → Security credentials → Access keys)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=seu-bucket-unico

# Redshift (opcional — apenas para etapa de carga na nuvem)
REDSHIFT_HOST=seu-cluster.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=dev
REDSHIFT_USER=awsuser
REDSHIFT_PASSWORD=sua_senha
REDSHIFT_IAM_ROLE=arn:aws:iam::123456789012:role/RedshiftS3Role
3. Build & Inicialização
bash
Copy
# Build da imagem Airflow customizada
docker-compose build

# Inicializar banco de dados Airflow e criar usuário admin
docker-compose up airflow-init

# Iniciar todos os serviços em background
docker-compose up -d
Ou use o Makefile:
bash
Copy
make build
make init
make up
4. Acessar Serviços
Table
Serviço	URL	Credenciais
Airflow UI	http://localhost:8080	admin / admin
Postgres	localhost:5432	airflow / airflow
5. Executar o Pipeline
Abra a UI do Airflow → ative o DAG reddit_etl_pipeline para On
Clique no botão ▶️ (play) para disparar uma execução manual
Acompanhe a progressão das tarefas: extract → transform → load
Clique em qualquer tarefa → Log para ver output em tempo real
6. Verificar Resultados
bash
Copy
# Verificar S3 para dados brutos aterrados
aws s3 ls s3://seu-bucket/raw/reddit/dataengineering/

# Verificar Redshift para dados carregados
psql -h seu-host-redshift -U awsuser -d dev -c "SELECT COUNT(*) FROM reddit_posts;"
Serviços Docker
Table
Serviço	Descrição
postgres	Banco de dados de metadados do Airflow
airflow-init	Inicialização única do DB e criação de usuário admin
airflow-webserver	UI + REST API em http://localhost:8080
airflow-scheduler	Parsing de DAGs, agendamento E execução de tarefas (LocalExecutor)
Apenas 4 containers — sem Redis, sem workers Celery, sem cluster Spark. O PySpark roda em modo local[*] usando todos os cores de CPU da máquina do scheduler.
Decisões de Design
TaskFlow API vs PythonOperator
A TaskFlow API (@dag, @task decorators) é o padrão moderno para Airflow 2.x:
Python
Copy
# ANTIGO: PythonOperator com manipulação manual de XCom
t1 = PythonOperator(task_id="extract", python_callable=extract_func)
t2 = PythonOperator(task_id="transform", python_callable=transform_func)
t1 >> t2

# NOVO: TaskFlow com passagem automática de dados
@task
def extract() -> list:
    return [{"s3_uri": "..."}]

@task
def transform(meta: list) -> dict:
    return run_spark_transform(meta)

@dag
def pipeline():
    transform(extract())  # XCom acontece automaticamente
LocalExecutor vs CeleryExecutor
O LocalExecutor cria tarefas como subprocessos paralelos na máquina do scheduler. É mais simples de configurar e debugar:
Sem broker de mensagens Redis
Sem containers de workers separados
Sem configuração de filas Celery
Ainda suporta execução paralela (só não distribuída entre máquinas)
Para um projeto de portfólio, esta é a troca certa. Você sempre pode mencionar:
"LocalExecutor é usado para desenvolvimento. Em produção usaria CeleryExecutor com workers dedicados e broker Redis para escalabilidade horizontal."
Por que JSONL para bruto, Parquet para curado?
Table
Formato	Caso de Uso	Por quê
JSONL	Zona de aterragem bruta	Legível por humanos, fácil de debugar, flexível em schema
Parquet	Analytics curado	Compressão colunar, predicate pushdown, nativo do Spark
Por que particionamento S3 por data?
Custo Athena: Queries com WHERE year=2024 AND month=05 escaneiam apenas arquivos relevantes
Time travel: Fácil reprocessar um dia específico se dados upstream mudarem
Políticas de lifecycle: Mover year=2024 para Glacier após 90 dias
Otimização do Redshift
sql
Copy
-- Distribuição: Sharding uniforme para paralelismo
-- Sort key: Queries de séries temporais filtram por subreddit + created_utc
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
Infraestrutura como Código (Terraform)
Provisione recursos AWS:
bash
Copy
cd infrastructure/terraform
terraform init
terraform plan -var="s3_bucket_name=seu-bucket-unico" -var="redshift_password=SenhaSegura123!"
terraform apply
Cria:
Bucket S3 com versionamento e regras de lifecycle
Banco de dados Glue Data Catalog + crawler
Namespace e workgroup Redshift Serverless
Roles IAM para Glue e Redshift
Após o Terraform completar, inicialize a tabela no Redshift:
bash
Copy
psql -h $(terraform output -raw redshift_workgroup_endpoint) \
     -U awsuser -d dev -f ../../scripts/init_db.sql
Testes
bash
Copy
# Executar testes unitários (da raiz do projeto)
python -m pytest tests/ -v

# Ou executar o smoke test diretamente
python src/extract/reddit_client.py
Estimativa de Custo (AWS Free Tier)
Table
Serviço	Custo Mensal	Notas
S3	~$0.01	500MB de armazenamento
Athena	~$0.05	Queries ad-hoc ocasionais
Redshift Serverless	$0–$5	Escala para zero quando inativo
Glue Catalog	$0	Free tier cobre 1M de objetos
Total	<$5	Desligue Redshift quando não estiver demonstrando
Troubleshooting
Table
Problema	Causa	Solução
ModuleNotFoundError: src	PYTHONPATH não configurado	Já corrigido no docker-compose.yml — use from src.extract...
PySpark sem memória	Memória padrão muito baixa	Aumente deploy.resources.limits.memory no docker-compose.yml
Extração Reddit retorna 401	Credenciais erradas	Verifique .env contra reddit.com/prefs/apps
Tarefa falha sem erro claro	Verifique logs no Airflow UI	Clique na tarefa → aba Log
Comandos do Makefile
bash
Copy
make build    # Build das imagens Docker
make init     # Inicializar DB do Airflow (primeira vez)
make up       # Iniciar todos os serviços
make down     # Parar todos os serviços
make logs     # Acompanhar logs do scheduler
make test     # Executar testes unitários Python
make clean    # Remover containers e volumes
Estendendo para CeleryExecutor (Caminho para Produção)
Se quiser demonstrar orquestração distribuída no futuro:
Adicione serviço redis ao docker-compose.yml
Altere AIRFLOW__CORE__EXECUTOR para CeleryExecutor
Adicione AIRFLOW__CELERY__BROKER_URL apontando para Redis
Adicione serviço airflow-worker com command: celery worker
Adicione roteamento de filas aos decoradores @task: @task(queue="spark_queue")
O código core do DAG (funções @task) permanece exatamente o mesmo — apenas a camada de infraestrutura muda.
