#!/bin/bash

# Encerra o script imediatamente se um comando falhar
set -e
set -o pipefail

# --- FUNÇÕES DE PIPELINE ---

run_api_pipeline() {
    echo "========================================================================"
    echo "       EXECUTANDO A EXTRAÇÃO E INGESTÃO DA API PARA O DATABRICKS"
    echo "========================================================================"

    # Navega para o diretório do projeto correto
    cd meltano_API

    # 1. Pipeline Meltano
    echo "Limpando diretório de saída anterior..."
    rm -rf ./output/
    echo "Iniciando extração da API para arquivos Parquet..."
    meltano run tap-apiadventureworks target-parquet
    echo "Extração da API concluída!!!"

    # 2. Ingestão para o Databricks
    local VOLUME_PATH="dbfs:/Volumes/$DATABRICKS_CATALOG/$DATABRICKS_SCHEMA/$DATABRICKS_VOLUME_RAW/$DATABRICKS_FOLDER_API/"
    echo "Fazendo a ingestão dos arquivos .parquet para: $VOLUME_PATH"
    databricks fs cp --recursive --overwrite ./output/ "$VOLUME_PATH"
    echo "Ingestão dos dados da API para o Databricks concluída!!!"

    # 3. Disparo de Jobs
    echo "Iniciando jobs de transformação dos arquivos .parquet da API para Delta Tables..."
    find ./output -type f -name '*.parquet' | while read -r FILEPATH; do
        local FILENAME=$(basename "$FILEPATH")
        local TABLENAME=$(basename "$(dirname "$FILEPATH")")
        echo "--> Disparando job para: $FILENAME (Endpoint: $TABLENAME)"
        
        local JSON_PAYLOAD=$(printf '{
            "job_id": %s, "notebook_params": { "file_parquet": "%s", "folder_volume": "%s/%s", "table_name": "%s", "catalog": "%s", "schema": "%s" }
        }' "$DATABRICKS_JOB_ID" "$FILENAME" "$DATABRICKS_FOLDER_API" "$TABLENAME" "$TABLENAME" "$DATABRICKS_CATALOG" "$DATABRICKS_SCHEMA")

        databricks jobs run-now --json "$JSON_PAYLOAD"
    done
}

run_db_pipeline() {
    echo "========================================================================"
    echo "  EXECUTANDO A EXTRAÇÃO E INGESTÃO DO BANCO DE DADOS PARA O DATABRICKS"
    echo "========================================================================"

    # Navega para o diretório do projeto correto
    cd meltano_DB
    
    # 1. Pipeline Meltano
    echo "Limpando diretório de saída anterior..."
    rm -rf ./output/
    echo "Iniciando extração do banco de dados no formato Parquet..."
    meltano run tap-mssql target-parquet
    echo "Extração do banco de dados concluída!!!"

    # 2. Ingestão para o Databricks
    local VOLUME_PATH="dbfs:/Volumes/$DATABRICKS_CATALOG/$DATABRICKS_SCHEMA/$DATABRICKS_VOLUME_RAW/$DATABRICKS_FOLDER_DB/"
    echo "Fazendo a ingestão dos arquivos .parquet para: $VOLUME_PATH"
    databricks fs cp --recursive --overwrite ./output/ "$VOLUME_PATH"
    echo "Ingestão dos dados do banco de dados para o Databricks concluída!!!"

    # 3. Disparo de Jobs
    echo "Iniciando jobs de transformação dos arquivos .parquet do banco de dados para Delta Tables..."
    find ./output -type f -name '*.parquet' | while read -r FILEPATH; do
        local FILENAME=$(basename "$FILEPATH")
        local TABLENAME=$(basename "$(dirname "$FILEPATH")")
        echo "--> Disparando job para: $FILENAME (Tabela: $TABLENAME)"
        
        local JSON_PAYLOAD=$(printf '{
            "job_id": %s, "notebook_params": { "file_parquet": "%s", "folder_volume": "%s/%s", "table_name": "%s", "catalog": "%s", "schema": "%s" }
        }' "$DATABRICKS_JOB_ID" "$FILENAME" "$DATABRICKS_FOLDER_DB" "$TABLENAME" "$TABLENAME" "$DATABRICKS_CATALOG" "$DATABRICKS_SCHEMA")

        databricks jobs run-now --json "$JSON_PAYLOAD"
    done
}

# --- PONTO DE ENTRADA PRINCIPAL ---
PROJECT_TO_RUN=$1

if [ -z "$PROJECT_TO_RUN" ]; then
    echo "Erro: Nenhum projeto especificado. Uso: docker run <imagem> [api|db]"
    exit 1
fi

case $PROJECT_TO_RUN in
    api) run_api_pipeline ;;
    db) run_db_pipeline ;;
    *) echo "Erro: Argumento inválido '$PROJECT_TO_RUN'. Use 'api' ou 'db'."; exit 1 ;;
esac

echo "========================================================================="
echo " Extração e carregamento do '$PROJECT_TO_RUN' finalizado com sucesso."
echo "========================================================================="