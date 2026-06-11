#!/bin/bash
# Cria banco adicional urbanflow_legado junto ao banco padrão do Airflow
set -e

function create_database() {
    local database=$1
    echo "  Criando banco '$database'..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE $database;
        CREATE USER urbanflow WITH PASSWORD 'urbanflow123';
        GRANT ALL PRIVILEGES ON DATABASE $database TO urbanflow;
        \c $database
        GRANT ALL ON SCHEMA public TO urbanflow;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "📦 Criando bancos adicionais: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_database $db
    done
    echo "✅ Bancos adicionais criados"
fi
