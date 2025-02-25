#!/bin/bash
# entrypoint.sh for Airflow container

# Ensure Airflow database is initialized and upgraded
airflow db upgrade

# Check if admin user exists, if not create it
if ! airflow users list | grep -q "admin"; then
  echo "Admin user does not exist, creating admin user"
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --role Admin \
    --password admin
else
  echo "Admin user already exists"
fi

# Start the webserver
airflow webserver
