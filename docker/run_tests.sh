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
    # The log is captured to a file and inspected AFTER the run, rather than piped
    # straight into `grep -q`. With `set -o pipefail` (line 3), `grep -q` exits on
    # its first match, closing the pipe and SIGPIPEing the still-writing producer
    # — so the pipeline's status became that non-zero status even though the match
    # SUCCEEDED, and a fully green run printed "FAILED". It only bites on real
    # runs, where the log is long enough that Odoo is still writing when the match
    # lands, which is why it survived this long: short runs look fine.
    LOG_FILE="$(mktemp)"
    docker compose run --rm odoo odoo \
        -d "${TEST_DB}" \
        --workers 0 \
        --test-enable \
        --test-tags "/${MODULE}" \
        --addons-path="${ADDONS_PATH}" \
        --stop-after-init \
        -i "${MODULE}" \
        --log-level=test >"${LOG_FILE}" 2>&1 || true
    cat "${LOG_FILE}"

    # Require the summary line to be PRESENT and clean, rather than testing for
    # the absence of a failure line: a missing summary (Odoo crashed before
    # running tests, the image failed to build, the DB was unreachable) must count
    # as a failure, not as "no failures found".
    if grep -q "0 failed, 0 error" "${LOG_FILE}"; then
        echo "PASSED: ${MODULE}"
    else
        echo "FAILED: ${MODULE}"
        FAILED=$((FAILED + 1))
    fi
    rm -f "${LOG_FILE}"
done

echo ""
echo "========================================"
if [ $FAILED -gt 0 ]; then
    echo "RESULT: ${FAILED} module(s) had test failures"
    exit 1
else
    echo "RESULT: All tests passed"
fi
