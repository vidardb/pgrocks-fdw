#!/bin/bash

set -e

if [[ -z "${1:-}" ]]; then
    PG_CONFIG=/usr/local/pgsql/bin/pg_config make
    sudo PG_CONFIG=/usr/local/pgsql/bin/pg_config make install
else
    PG_CONFIG=/usr/local/pgsql/bin/pg_config VIDARDB=true make
    sudo PG_CONFIG=/usr/local/pgsql/bin/pg_config VIDARDB=true make install
fi

sudo bash -c 'echo "shared_preload_libraries = 'kv_fdw'" >> /usr/local/pgsql/data/postgresql.conf'
PGLOG=${PGLOG:-/tmp/pgsql.log}
sudo -u postgres /usr/local/pgsql/bin/pg_ctl -U postgres -D /usr/local/pgsql/data -l "${PGLOG}" stop

# pg_ctl only says it could not start; the reason a preloaded library was
# rejected is in the server log, which is otherwise lost when the job ends.
if ! sudo -u postgres /usr/local/pgsql/bin/pg_ctl -U postgres -D /usr/local/pgsql/data -l "${PGLOG}" start; then
    echo "=== postmaster failed to start; ${PGLOG} follows ===" >&2
    sudo cat "${PGLOG}" >&2
    exit 1
fi
