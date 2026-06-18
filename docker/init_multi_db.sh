#!/bin/bash
# Cria o banco urbanflow_legado para o sistema legado
# O Airflow conecta como superuser (POSTGRES_USER), nao precisa de role separado
set -e

echo "Criando banco urbanflow_legado..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -c "CREATE DATABASE urbanflow_legado;"
echo "Banco urbanflow_legado criado com sucesso"
