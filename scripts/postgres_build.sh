#!/bin/bash

set -e

export LD_LIBRARY_PATH=/usr/local/pgsql/lib
export PATH=/usr/local/pgsql/bin:$PATH
export PG_VERSION=13.2

wget https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz
tar xf postgresql-${PG_VERSION}.tar.gz

(cd postgresql-${PG_VERSION} && ./configure --enable-debug --enable-cassert --with-libxml && make -j`nproc` && sudo make install)

echo "Going to execute: sudo mkdir /usr/local/pgsql/data"
sudo mkdir /usr/local/pgsql/data

echo "Going to execute: sudo chown postgres /usr/local/pgsql/data"
sudo chown postgres /usr/local/pgsql/data

echo "Going to execute: sudo -u postgres /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data"
sudo -u postgres /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data

sudo touch logfile
sudo chmod 777 logfile

echo "Going to execute: sudo -u postgres /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l logfile start"
sudo -u postgres /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l logfile start
