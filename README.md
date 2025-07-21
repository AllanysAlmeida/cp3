# Pipeline de Dados - Adventure Works

Este repositório apresenta um pipeline de dados automatizado, modular e containerizado, projetado para extrair, transformar e carregar informações de múltiplas fontes (API REST e banco de dados MSSQL) em um Data Lakehouse na plataforma Databricks, utilizando as melhores práticas de engenharia de dados e governança.

## Arquitetura Geral

- **Extração:** Meltano (taps: tap-apiadventureworks, tap-mssql)
- **Formato de saída:** Parquet
- **Transformação:** dbt (Data Build Tool) com Databricks e Delta Lake
- **Containerização:** Docker & Docker Compose
- **Orquestração:** Scripts shell e integração pronta para Airflow
- **Automação:** Databricks CLI (upload e disparo de jobs)
- **Versionamento:** Git

---

## Estrutura do Projeto

```
project-root/
├── AW/                      # Projeto dbt (modelagem e transformação)
│   ├── models/
│   │   ├── staging/         # Camada de limpeza inicial
│   │   ├── intermediate/    # Camada de integração e enriquecimento
│   │   └── marts/           # Camada de fatos e dimensões
│   ├── snapshots/           # Controle de mudanças (SCD)
│   ├── tests/               # Testes de qualidade
│   └── macros/              # Macros reutilizáveis
├── checkpoint2/             # Pipelines Meltano e Docker Compose
│   ├── meltano_API/
│   ├── meltano_DB/
│   ├── docker-compose.yml
│   └── entrypoint.sh
├── parquet_to_deltatable.ipynb  # Notebook de transformação no Databricks
├── .env                     # Variáveis de ambiente e segredos
├── README.md                # Este arquivo
└── catalog.json             # Catálogo de dados gerado pelo dbt
```

---

## Configuração do Ambiente

### Pré-requisitos

- Git
- Docker e Docker Compose
- Python 3.11+
- dbt-core, dbt-databricks
- Databricks CLI

### 1. Clonar o Repositório

```
git clone https://github.com/AllanysAlmeida/ingestao-checkpoint2.git
cd ingestao-checkpoint2
```

### 2. Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as credenciais e parâmetros necessários para Databricks, API e banco de dados. Exemplo:

```
DATABRICKS_HOST="https://adb-....azuredatabricks.net"
DATABRICKS_TOKEN="dapi..."
DATABRICKS_CATALOG="seu_catalogo"
DATABRICKS_SCHEMA="seu_schema"
DATABRICKS_VOLUME_RAW="raw"
DATABRICKS_JOB_ID="SEU_JOB_ID_AQUI"
DATABRICKS_FOLDER_API="api_extraction"
DATABRICKS_FOLDER_DB="database_extraction"
TAP_APIADVENTUREWORKS_USERNAME="username"
TAP_APIADVENTUREWORKS_PASSWORD="sua_senha_da_api"
TAP_MSSQL_HOST="10.100.100.10"
TAP_MSSQL_USER="user"
TAP_MSSQL_PASSWORD="sua_senha_do_db"
TAP_MSSQL_PORT="8080"
TAP_MSSQL_DATABASE="My_DB"
```

---

## Execução dos Pipelines de Extração

### 1. Executar ambos os pipelines em paralelo

```
sudo docker compose up --build
```

### 2. Executar um pipeline específico

- API: `sudo docker compose up --build pipeline-api`
- Banco de Dados: `sudo docker compose up --build pipeline-db`

### 3. Parar e limpar

Para parar os contêineres, pressione Ctrl + C. Para remover contêineres e redes criadas:

```
sudo docker compose down
```

---

## Transformação e Modelagem com dbt

### 1. Instalar dependências do dbt

No diretório `AW`:

```
pip install -r requirements.txt
```

### 2. Configurar o perfil do dbt

No arquivo `profiles.yml` (pode ser criado em `~/.dbt/` ou configurado via variável de ambiente `DBT_PROFILES_DIR`), defina a conexão com o Databricks.

### 3. Executar as transformações

- Executar toda a modelagem:

```
dbt run --threads 4
```

- Executar apenas a camada staging:

```
dbt run --select staging --threads 4
```

- Executar apenas a camada marts:

```
dbt run --select marts --threads 4
```

- Executar snapshots (controle de mudanças):

```
dbt snapshot
```

### 4. Testar a qualidade dos dados

```
dbt test
```

### 5. Gerar e visualizar a documentação

```
dbt docs generate
```

Para servir a documentação localmente:

```
dbt docs serve
```

---

## Governança, Testes e Documentação

- **Testes dbt:** Unicidade, not null, integridade referencial, valores aceitos, freshness, volume e distribuição.
- **Documentação automática:** Todos os modelos, colunas e relacionamentos são documentados e podem ser visualizados via `dbt docs`.
- **Linhagem de dados:** Visualização gráfica da origem até o consumo.
- **Versionamento:** Todo o código versionado via Git.

---

## Observações Finais

- O pipeline foi projetado para ser modular, escalável e seguro.
- A transformação dos dados segue o padrão de camadas (staging, intermediate, marts) e melhores práticas de Data Warehouse.
- O projeto está pronto para integração com orquestradores como Airflow.
- Para dúvidas ou sugestões, consulte a documentação do dbt, Meltano e Databricks, ou abra uma issue neste repositório.
