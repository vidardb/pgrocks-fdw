#!/bin/bash
#
# Run the test scripts in sql/ against a running PostgreSQL server and compare
# the output against expected/.
#
# The server must already have kv_fdw installed and listed in
# shared_preload_libraries.  Connection settings come from the standard libpq
# environment variables (PGHOST, PGPORT, PGUSER); the user must be a superuser,
# since the scripts create and drop databases.
#
# Usage:
#     scripts/run_tests.sh              # run and compare
#     scripts/run_tests.sh --regenerate # overwrite expected/ with what we got
#
# Output goes to $OUT_DIR (default /tmp/pgrocks_test_output), including a .diff
# for anything that did not match.

set -uo pipefail

# The scripts use paths relative to the repository root (\copy of
# sql/products.csv), so run from there regardless of where we were called.
cd "$(dirname "$0")/.."

OUT_DIR=${OUT_DIR:-/tmp/pgrocks_test_output}
PGUSER=${PGUSER:-postgres}
export PGUSER

REGENERATE=no
if [ "${1:-}" = "--regenerate" ]; then
    REGENERATE=yes
fi

mkdir -p "$OUT_DIR"

# The scripts that have a transcript in expected/ to compare against.  The
# others in sql/ are exploratory and have no recorded output.
CASES="basic test testcolumn testcopy testddl"
failures=0

fail() {
    echo "FAIL  $*"
    failures=$((failures + 1))
}

# No ON_ERROR_STOP: several scripts open with a DROP FOREIGN TABLE that is
# expected to fail on a fresh database, and testddl.sql runs "\d testddl" after
# renaming the table away.  Errors are caught by comparing the whole transcript
# against expected/ instead.
psql_script() {  # psql_script <script-name> <output-file>
    psql -a -f "sql/$1.sql" > "$2" 2>&1
}

# Drop anything a script left behind, so clear.sql's DROP SERVER is not blocked
# by a dependent foreign table.
drop_leftover_foreign_tables() {
    psql -d kvtest -q -c "DO \$\$
        DECLARE r record;
        BEGIN
            FOR r IN SELECT ftrelid::regclass AS t FROM pg_foreign_table LOOP
                EXECUTE 'DROP FOREIGN TABLE ' || r.t;
            END LOOP;
        END \$\$;" > /dev/null 2>&1
}

# testddl.sql records current_timestamp values, so its transcript can never
# match byte for byte.  Mask timestamps, then collapse whitespace and the runs
# of dashes in psql's rule lines: psql sizes a column to its widest value, and
# current_timestamp(2) prints a variable number of digits depending on how the
# fraction rounds, so the column width moves between runs.
normalize() {
    sed -E -e 's/[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?/TIMESTAMP/g' \
           -e 's/-{3,}/---/g' \
           -e 's/[[:space:]]+/ /g' \
           -e 's/ $//' "$1"
}

compare() {  # compare <script-name>
    local name=$1
    local expected="expected/${name}.output"
    local actual="${OUT_DIR}/${name}.output"

    if [ "$REGENERATE" = yes ]; then
        cp "$actual" "$expected"
        echo "regenerated  ${expected}"
        return
    fi

    if diff -u "$expected" "$actual" > "${OUT_DIR}/${name}.diff" 2>&1; then
        echo "ok    ${name} (exact match)"
        rm -f "${OUT_DIR}/${name}.diff"
        return
    fi
    if diff -u <(normalize "$expected") <(normalize "$actual") \
         > "${OUT_DIR}/${name}.diff" 2>&1; then
        echo "ok    ${name} (match with timestamps masked)"
        rm -f "${OUT_DIR}/${name}.diff"
        return
    fi
    fail "${name}: output differs from expected/${name}.output"
    head -40 "${OUT_DIR}/${name}.diff"
}

echo "Running pgrocks-fdw tests against ${PGHOST:-default host} as ${PGUSER}"

for name in $CASES; do
    # Each case gets a fresh database so it cannot be affected by the previous.
    psql_script create "${OUT_DIR}/create.output" || fail "${name}: create.sql failed"
    psql_script "$name" "${OUT_DIR}/${name}.output" || fail "${name}: ${name}.sql failed"
    drop_leftover_foreign_tables
    psql_script clear "${OUT_DIR}/clear.output" || fail "${name}: clear.sql failed"
done

compare create
compare clear
for name in $CASES; do
    compare "$name"
done

if [ "$REGENERATE" = yes ]; then
    echo
    echo "expected/ regenerated; review the diff before committing."
    exit 0
fi

if [ "$failures" -ne 0 ]; then
    echo
    echo "${failures} failure(s); diffs are in ${OUT_DIR}"
    exit 1
fi

echo
echo "All pgrocks-fdw tests passed."
