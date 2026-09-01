#!/bin/bash

set -e

export LD_LIBRARY_PATH=/usr/local/pgsql/lib
export PATH=/usr/local/pgsql/bin:$PATH
export PG_VERSION=18.6
export PGLOG=${PGLOG:-/tmp/pgsql.log}

wget https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz
tar xf postgresql-${PG_VERSION}.tar.gz

(cd postgresql-${PG_VERSION} && ./configure --enable-debug --enable-cassert --with-libxml && make -j`nproc` && sudo make install)

echo "Going to execute: sudo mkdir /usr/local/pgsql/data"
sudo mkdir /usr/local/pgsql/data

echo "Going to execute: sudo chown postgres /usr/local/pgsql/data"
sudo chown postgres /usr/local/pgsql/data

echo "Going to execute: sudo -u postgres /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data"
sudo -u postgres /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data

# Absolute, and outside the workspace: pg_ctl resolves a relative -l against
# the current directory, which the postgres user cannot enter on a CI runner.
sudo touch "${PGLOG}"
sudo chmod 777 "${PGLOG}"

echo "Going to execute: sudo -u postgres /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l ${PGLOG} start"
sudo -u postgres /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l "${PGLOG}" start
