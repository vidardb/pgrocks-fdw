FROM postgres:11.5

# install RocksDB
ENV ROCKSDB_VERSION=6.2.4
RUN set -xe && \
    apt-get update && apt-get install -y --no-install-recommends gcc-6 g++-6 make wget && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-6 800 --slave /usr/bin/g++ g++ /usr/bin/g++-6 && \
    wget --no-check-certificate -O rocksdb.tar.gz https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz && \
    tar zxvf rocksdb.tar.gz && cd rocksdb-${ROCKSDB_VERSION} && \
    DEBUG_LEVEL=0 PORTABLE=1 make shared_lib install-shared && cd .. && \
    apt-get purge -y --auto-remove make wget && \
    rm -rf /var/lib/apt/lists/* /tmp/* rocksdb.tar.gz rocksdb-${ROCKSDB_VERSION}

# install ForeignDataWrapper
ENV POSTGRES_VERSION=11
ENV FDW_VERSION=0.1-alpha
RUN set -xe && \
    apt-get update && \
    apt-get install -y --no-install-recommends make wget ca-certificates postgresql-server-dev-${POSTGRES_VERSION} && \
    wget --no-check-certificate -O PostgresForeignDataWrapper.tar.gz https://github.com/jsc0218/PostgresForeignDataWrapper/archive/v${FDW_VERSION}.tar.gz && \
    tar zxvf PostgresForeignDataWrapper.tar.gz && cd PostgresForeignDataWrapper-${FDW_VERSION} && make && make install && cd .. && \
    apt-get purge -y --auto-remove make wget ca-certificates postgresql-server-dev-${POSTGRES_VERSION} && \
    rm -rf /var/lib/apt/lists/* /tmp/* PostgresForeignDataWrapper.tar.gz PostgresForeignDataWrapper-${FDW_VERSION}
