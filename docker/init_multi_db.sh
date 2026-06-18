#!/bin/bash
# Cria banco adicional urbanflow_legado junto ao banco padrao do Airflow
set -e

function create_database() {
    local database=$1
    echo "  Criando banco '$database'..."
    # 1) Cria banco, usuario e grant de database
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE $database;
        DO \$\$ BEGIN
            CREATE USER urbanflow WITH PASSWORD 'urbanflow123';
        EXCEPTION WHEN duplicate_object THEN
            ALTER USER urbanflow WITH PASSWORD 'urbanflow123';
        END \$\$;
        GRANT ALL PRIVILEGES ON DATABASE $database TO urbanflow;
EOSQL
    # 2) Grants de schema -- conexao explicita ao banco recem-criado
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$database" <<-EOSQL
        GRANT ALL ON SCHEMA public TO urbanflow;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO urbanflow;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO urbanflow;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Criando bancos adicionais: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_database $db
    done
    echo "Bancos adicionais criados"
fi
