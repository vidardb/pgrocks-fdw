#!/bin/bash

set -e

git clone -b v${ROCKSDB_VERSION} https://github.com/facebook/rocksdb.git

# DISABLE_WARNING_AS_ERROR: this RocksDB predates gcc 10, and its warning
# set trips -Werror on any current compiler (range-loop-construct,
# maybe-uninitialized, redundant-move).
(cd rocksdb/ && sudo DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 make shared_lib install-shared -j`nproc`)

sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"

sudo ldconfig
