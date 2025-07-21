# Pipeline de Ingestão de Dados - Adventure Works

Este repositório apresenta um pipeline de dados automatizado e containerizado, projetado para extrair informações de múltiplas fontes (API REST e banco de dados MSSQL) e ingeri-las em um Data Lakehouse na plataforma Databricks.

## Arquitetura e Ferramentas

- **Extração:** Meltano (`tap-apiadventureworks`, `tap-mssql`)
- **Formato de Saída:** Parquet
- **Containerização:** Docker & Docker Compose
- **Plataforma de Dados:** Databricks
- **Automação:** Databricks CLI (upload e disparo de jobs)

---

## 🚀 Começando

Siga os passos abaixo para configurar e executar o projeto localmente.

### 1. Pré-requisitos

Certifique-se de ter as seguintes ferramentas instaladas:

- [Git](https://git-scm.com/book/pt-br/v2/Primeiros-passos-Instalando-Git)
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### 2. Clonar o Repositório

```bash
git clone https://github.com/AllanysAlmeida/ingestao-checkpoint2.git
cd ingestao-checkpoint2
```

Todos os comandos a seguir devem ser executados a partir deste diretório.

---

## ⚙️ Configuração do Ambiente

A configuração é dividida em duas partes: Databricks (Notebook e Job) e ambiente local (`.env`).

### 1. Configuração do Databricks

#### a) Importar o Notebook de Transformação

1. No seu workspace Databricks, navegue até a pasta desejada.
2. Clique no menu de três pontos ao lado da pasta e selecione **Import**.
3. Escolha a opção **File** e selecione o notebook:  
  `./parquet_to_deltatable.ipynb`
4. Clique em **Import**.

#### b) Criar o Job no Databricks

1. No menu à esquerda, acesse **Workflows** e clique em **Create Job**.
2. Defina:
  - **Nome:** Ex: `Processa_Arquivos_Parquet`
  - **Task name:** `Transforma_Parquet_em_Delta`
  - **Type:** Notebook
  - **Source:** Workspace
  - **Path:** Selecione o notebook importado
  - **Cluster:** Configure um Job Cluster (ex: 14.3 LTS)
  - **Parameters:** Adicione os 5 parâmetros esperados pelo notebook:
    - `catalog`
    - `schema`
    - `folder_volume`
    - `file_parquet`
    - `table_name`
3. Clique em **Create** e anote o Job ID para uso no `.env`.

### 2. Configuração do Ambiente Local (`.env`)

Crie um arquivo `.env` na raiz do projeto com as credenciais e parâmetros:

```env
# ==================================
# CREDENCIAIS COMPARTILHADAS (DATABRICKS)
# ==================================
DATABRICKS_HOST="https://adb-....azuredatabricks.net"
DATABRICKS_TOKEN="dapi..."
DATABRICKS_CATALOG="seu_catalogo"
DATABRICKS_SCHEMA="seu_schema"
DATABRICKS_VOLUME_RAW="raw"
DATABRICKS_JOB_ID="SEU_JOB_ID_AQUI"

# ==================================
# CONFIGURAÇÕES ESPECÍFICAS DOS PIPELINES
# ==================================

# --- Para a extração da API ---
DATABRICKS_FOLDER_API="api_extraction"
TAP_APIADVENTUREWORKS_USERNAME="username"
TAP_APIADVENTUREWORKS_PASSWORD="sua_senha_da_api"

# --- Para a extração do Banco de Dados ---
DATABRICKS_FOLDER_DB="database_extraction"
TAP_MSSQL_HOST="10.100.100.10"
TAP_MSSQL_USER="user"
TAP_MSSQL_PASSWORD="sua_senha_do_db"
TAP_MSSQL_PORT="8080"
TAP_MSSQL_DATABASE="My_DB"
```

---

## 🚀 Executando os Pipelines

### 1. Executar Ambos os Pipelines em Paralelo

```bash
sudo docker compose up --build
```

### 2. Executar um Pipeline Específico

- **API:**  
  `sudo docker compose up --build pipeline-api`
- **Banco de Dados:**  
  `sudo docker compose up --build pipeline-db`

### 3. Parar e Limpar

Para parar os contêineres, pressione `Ctrl + C`.  
Para remover contêineres e redes criadas:

```bash
sudo docker compose down
```
