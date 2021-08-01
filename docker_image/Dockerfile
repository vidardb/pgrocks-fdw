# Mirror from https://hub.docker.com/_/postgres
FROM vidardb/postgres:13.2

ARG apt_opts
ARG env_exts

# Install Start Entrypoint
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod 777 /usr/local/bin/docker-entrypoint.sh

# Install RocksDB Engine
ENV ROCKSDB_VERSION=6.11.4
RUN set -xe && export $env_exts && apt-get $apt_opts update && apt-get $apt_opts install -y --no-install-recommends gcc-8 g++-8 make wget && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8 && \
    wget --no-check-certificate -O rocksdb.tar.gz https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz && \
    tar zxvf rocksdb.tar.gz && cd rocksdb-${ROCKSDB_VERSION} && \
    DEBUG_LEVEL=0 PORTABLE=1 make shared_lib install-shared && cd .. && \
    apt-get purge -y --auto-remove make wget && \
    rm -rf /var/lib/apt/lists/* /tmp/* rocksdb.tar.gz rocksdb-${ROCKSDB_VERSION}

# Install PostgresForeignDataWrapper
ENV POSTGRES_VERSION=13
ENV FDW_VERSION=master
RUN set -xe && export $env_exts && apt-get $apt_opts update && \
    apt-get $apt_opts install -y --no-install-recommends make wget ca-certificates postgresql-server-dev-${POSTGRES_VERSION} && \
    wget --no-check-certificate -O pgrocks-fdw.tar.gz https://github.com/vidardb/pgrocks-fdw/archive/${FDW_VERSION}.tar.gz && \
    tar zxvf pgrocks-fdw.tar.gz && cd pgrocks-fdw-${FDW_VERSION} && make && make install && cd .. && \
    apt-get purge -y --auto-remove make wget ca-certificates postgresql-server-dev-${POSTGRES_VERSION} && \
    rm -rf /var/lib/apt/lists/* /tmp/* pgrocks-fdw.tar.gz pgrocks-fdw-${FDW_VERSION}
