#!/bin/bash

set -e

if [[ -z "$1" ]]; then
    PG_CONFIG=/usr/local/pgsql/bin/pg_config make
    sudo PG_CONFIG=/usr/local/pgsql/bin/pg_config make install
else
    PG_CONFIG=/usr/local/pgsql/bin/pg_config VIDARDB=true make
    sudo PG_CONFIG=/usr/local/pgsql/bin/pg_config VIDARDB=true make install
fi

sudo bash -c 'echo "shared_preload_libraries = 'kv_fdw'" >> /usr/local/pgsql/data/postgresql.conf'
sudo -u postgres /usr/local/pgsql/bin/pg_ctl -U postgres -D /usr/local/pgsql/data -l logfile stop
sudo -u postgres /usr/local/pgsql/bin/pg_ctl -U postgres -D /usr/local/pgsql/data -l logfile start
