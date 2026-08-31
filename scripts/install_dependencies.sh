#!/bin/bash

set -e

# Only the toolchain is needed here. PostgreSQL itself is built from source by
# postgres_build.sh into /usr/local/pgsql, and that is the pg_config the
# extension is built against, so no PostgreSQL packages are installed. The PGDG
# apt repository this used to add was pinned to focal, which apt.postgresql.org
# no longer serves.
sudo apt update
sudo apt install -y gcc g++ clang flex libbison-dev libxml2-dev zlib1g-dev \
    libreadline-dev m4 cmake build-essential git wget
