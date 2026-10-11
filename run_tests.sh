#!/bin/bash
# run_tests.sh - Automate building and testing the pg_scheduler extension

set -e  # Exit on any error (except specially handled ones)

# Settings (override via environment variables)
PG_CONFIG="${PG_CONFIG:-$HOME/postgres-custom/bin/pg_config}"
PGCTL="${PGCTL:-$HOME/postgres-custom/bin/pg_ctl}"
PGDATA="${PGDATA:-$HOME/pgdata-custom}"
PGPORT="${PGPORT:-5432}"
JOBS="${JOBS:-4}"
SUDO="${SUDO-sudo}"

# Tools derived from pg_config
BINDIR="$(dirname "$PG_CONFIG")"
INITDB="$BINDIR/initdb"

# Function to stop the PostgreSQL server and clean up build artifacts
# (called on exit)
cleanup() {
    echo ">>> Stopping PostgreSQL server..."
    "$PGCTL" stop -D "$PGDATA" -m fast 2>/dev/null || true
    echo ">>> Cleaning up build artifacts..."
    make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" clean 2>/dev/null || true
}

# Trap EXIT to ensure cleanup is called on normal or abnormal exit
trap cleanup EXIT

# Change to the script's directory (assumed to be the extension root)
cd "$(dirname "$0")"

# 0. Create a test cluster if it does not exist yet
if [ ! -d "$PGDATA" ]; then
    echo ">>> Creating test cluster in $PGDATA"
    "$INITDB" -D "$PGDATA" -U postgres --auth=trust >/dev/null
    cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
shared_preload_libraries = 'scheduler'
scheduler.database = 'postgres'
scheduler.wake_interval = '1s'
EOF
fi

# 1. Clean
echo ">>> Cleaning previous build"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" clean

# 2. Build
echo ">>> Building with -j$JOBS"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" -j"$JOBS"

# 3. Install into the target PostgreSQL
echo ">>> Installing extension"
$SUDO make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" install

# 4. Start the server (ignore errors if already running)
echo ">>> Starting PostgreSQL server"
"$PGCTL" start -D "$PGDATA" -l "$PGDATA/server.log" || {
    echo ">>> Failed to start server, maybe it's already running. Checking status..."
    "$PGCTL" status -D "$PGDATA"
}

# Give the server a moment to fully start
sleep 2

# Determine which role to connect as (the cluster may have been created by the
# OS user or by "postgres"; do not assume either).
export PGHOST="${PGHOST:-localhost}"
export PGPORT
if [ -z "${PGUSER:-}" ]; then
    for candidate in "$USER" postgres; do
        if "$BINDIR/psql" -h "$PGHOST" -p "$PGPORT" -U "$candidate" -d postgres \
             -tAc 'SELECT 1' >/dev/null 2>&1; then
            PGUSER="$candidate"
            break
        fi
    done
fi
export PGUSER

# 5. Run regression tests (including TAP if enabled)
echo ">>> Running tests (make installcheck)"
echo ">>> Connecting as role '${PGUSER:-<default>}'"
# We don't want set -e to abort on test failure, so we handle the exit code manually
set +e
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" installcheck
TEST_RESULT=$?
set -e

if [ $TEST_RESULT -ne 0 ]; then
    echo ">>> WARNING: tests failed with exit code $TEST_RESULT"
    # Server will still be stopped via trap cleanup
else
    echo ">>> All tests passed successfully"
fi

# Server will be stopped automatically on script exit (trap cleanup)

exit $TEST_RESULT
