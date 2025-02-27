#!/usr/bin/env bash

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
RETRIES=10
until nc -z postgres 5432 || [ $RETRIES -eq 0 ]; do
  echo "PostgreSQL not ready yet. Retrying..."
  sleep 2
  RETRIES=$((RETRIES-1))
done

if [ $RETRIES -eq 0 ]; then
  echo "PostgreSQL is still not available. Exiting."
  exit 1
fi
echo "PostgreSQL is up!"

# Upgrade the database (apply migrations)
airflow db upgrade

# Create admin user if not exists
airflow users list | grep -q "admin" || \
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Start the scheduler in the background
airflow scheduler &

# Start the webserver
exec airflow webserver
