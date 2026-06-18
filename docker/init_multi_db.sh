#!/bin/bash
# Cria banco adicional junto ao banco padrao do Airflow
set -e

function create_database() {
    local database=$1
    echo "Criando banco extra: $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" \
        -c "CREATE DATABASE $database;" \
        -c "CREATE USER urbanflow WITH PASSWORD 'urbanflow123';" \
        -c "GRANT ALL PRIVILEGES ON DATABASE $database TO urbanflow;"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$database" \
        -c "GRANT ALL ON SCHEMA public TO urbanflow;" \
        -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO urbanflow;" \
        -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO urbanflow;"
    echo "Banco $database criado com sucesso"
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Criando bancos: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_database $db
    done
    echo "Todos os bancos criados"
fi
