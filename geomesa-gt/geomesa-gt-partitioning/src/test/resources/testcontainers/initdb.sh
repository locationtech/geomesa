#!/bin/sh

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

# Create the extension
echo "Loading pg_cron extensions into $POSTGRES_DB"
"${psql[@]}" --dbname="$POSTGRES_DB" <<- EOSQL
  ALTER SYSTEM SET shared_preload_libraries = pg_cron;
  ALTER SYSTEM SET cron.database_name = $POSTGRES_DB;
  CREATE EXTENSION IF NOT EXISTS pg_cron;
EOSQL

# in postgres 12 JIT was changed from default off to default on
# disable JIT optimizations, as they can cause extreme slowness with postgis queries above a certain size
echo "Configuring postgis defaults"
"${psql[@]}" --dbname="$POSTGRES_DB" <<- EOSQL
  ALTER SYSTEM SET jit_optimize_above_cost = '-1';
EOSQL
