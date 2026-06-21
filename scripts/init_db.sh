#!/bin/bash
set -e

# Create databases
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE warehouse;
EOSQL

# Create tables in the warehouse database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "warehouse" <<-EOSQL
    CREATE TABLE IF NOT EXISTS daily_active_users (
        date DATE PRIMARY KEY,
        unique_users INTEGER NOT NULL,
        total_events INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS product_sales (
        date DATE NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        category VARCHAR(50) NOT NULL,
        units_sold INTEGER NOT NULL,
        revenue DOUBLE PRECISION NOT NULL,
        PRIMARY KEY (date, product_id)
    );

    CREATE TABLE IF NOT EXISTS funnel_metrics (
        date DATE NOT NULL,
        device VARCHAR(20) NOT NULL,
        page_views INTEGER NOT NULL,
        clicks INTEGER NOT NULL,
        adds_to_cart INTEGER NOT NULL,
        purchases INTEGER NOT NULL,
        conversion_rate DOUBLE PRECISION NOT NULL,
        PRIMARY KEY (date, device)
    );
EOSQL
