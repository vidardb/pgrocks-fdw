# Mirror from https://hub.docker.com/_/postgres
#
# Built from the repository root, so that the image contains the extension as
# checked out rather than a tarball downloaded from GitHub.
FROM postgres:18-trixie

ARG apt_opts
ARG env_exts

# Install Start Entrypoint
COPY docker_image/docker-entrypoint.sh /usr/local/bin/
RUN chmod 777 /usr/local/bin/docker-entrypoint.sh

# Install RocksDB and the extension. trixie packages RocksDB 9.10, so it is
# installed rather than built from source; librocksdb-dev is kept afterwards
# because kv_fdw.so links against it.
ENV POSTGRES_VERSION=18
COPY . /tmp/pgrocks-fdw
RUN set -xe && export $env_exts && apt-get $apt_opts update && \
    apt-get $apt_opts install -y --no-install-recommends \
        build-essential librocksdb-dev postgresql-server-dev-${POSTGRES_VERSION} && \
    make -C /tmp/pgrocks-fdw && make -C /tmp/pgrocks-fdw install && \
    apt-get purge -y --auto-remove build-essential postgresql-server-dev-${POSTGRES_VERSION} && \
    rm -rf /var/lib/apt/lists/* /tmp/pgrocks-fdw
