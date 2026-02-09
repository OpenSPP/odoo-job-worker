#!/bin/bash
# Run tests for job_worker modules using Docker
set -euo pipefail

cd "$(dirname "$0")"

MODULES="${1:-job_worker,job_worker_monitor}"
ADDONS_PATH="/opt/odoo/odoo/addons,/opt/odoo/odoo/odoo/addons,/mnt/extra-addons,/mnt/extra-addons/odoo-job-worker"

echo "Starting Postgres..."
docker compose up -d db

echo "Waiting for DB..."
sleep 5

IFS=',' read -ra MODULE_ARRAY <<< "$MODULES"
FAILED=0

for MODULE in "${MODULE_ARRAY[@]}"; do
    MODULE=$(echo "$MODULE" | xargs)
    echo ""
    echo "========================================"
    echo "Running tests for: ${MODULE}"
    echo "========================================"

    TEST_DB="test_${MODULE}_$(date +%s)"
    if ! docker compose run --rm odoo odoo \
        -d "${TEST_DB}" \
        --workers 0 \
        --test-enable \
        --test-tags "/${MODULE}" \
        --addons-path="${ADDONS_PATH}" \
        --stop-after-init \
        -i "${MODULE}" \
        --log-level=test 2>&1 | tee /dev/stderr | grep -q "0 failed, 0 error"; then
        echo "FAILED: ${MODULE}"
        FAILED=$((FAILED + 1))
    else
        echo "PASSED: ${MODULE}"
    fi
done

echo ""
echo "========================================"
if [ $FAILED -gt 0 ]; then
    echo "RESULT: ${FAILED} module(s) had test failures"
    exit 1
else
    echo "RESULT: All tests passed"
fi
