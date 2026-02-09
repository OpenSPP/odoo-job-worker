#!/bin/bash
# Entrypoint for job_worker test container
set -euo pipefail

# Set defaults for DB connection variables
export DB_HOST="${DB_HOST:-${HOST:-localhost}}"
export DB_PORT="${DB_PORT:-5432}"
export DB_USER="${DB_USER:-${USER:-odoo}}"
export DB_PASSWORD="${DB_PASSWORD:-${PASSWORD:-odoo}}"
export DB_NAME="${DB_NAME:-odoo}"

# Generate odoo.conf from template
if [ -f /etc/odoo/odoo.conf.template ]; then
    envsubst < /etc/odoo/odoo.conf.template > /etc/odoo/odoo.conf
fi

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
TIMEOUT=60
ELAPSED=0
until PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -c '\q' 2>/dev/null; do
    if [ $ELAPSED -ge $TIMEOUT ]; then
        echo "ERROR: PostgreSQL connection timeout after ${TIMEOUT}s"
        exit 1
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done
echo "PostgreSQL is ready."

exec "$@"
