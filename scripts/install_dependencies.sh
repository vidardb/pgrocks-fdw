#!/bin/bash

set -e

# Only the toolchain is needed here. PostgreSQL itself is built from source by
# postgres_build.sh into /usr/local/pgsql, and that is the pg_config the
# extension is built against, so no PostgreSQL packages are installed. The PGDG
# apt repository this used to add was pinned to focal, which apt.postgresql.org
# no longer serves.
#
# pkg-config and libicu-dev are needed by PostgreSQL 18's configure but were
# not by 13's: ICU became a hard requirement in 16, and it is located through
# pkg-config.
sudo apt update
sudo apt install -y gcc g++ clang flex libbison-dev libxml2-dev zlib1g-dev \
    libreadline-dev m4 cmake build-essential git wget pkg-config libicu-dev
