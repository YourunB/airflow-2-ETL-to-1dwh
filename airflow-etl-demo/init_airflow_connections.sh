#!/bin/bash

airflow connections add 'pg_source1' \
  --conn-uri 'postgresql://user1:pass1@host.docker.internal:5434/source1' || echo "pg_source1 already exists, skipping"

airflow connections add 'pg_source2' \
  --conn-uri 'postgresql://user2:pass2@host.docker.internal:5435/source2' || echo "pg_source2 already exists, skipping"

airflow connections add 'pg_dwh' \
  --conn-uri 'postgresql://dwhuser:dwhpass@host.docker.internal:5436/dwh' || echo "pg_dwh already exists, skipping"

