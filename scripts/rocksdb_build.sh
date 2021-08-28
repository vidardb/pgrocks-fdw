#!/bin/bash

set -e

git clone -b v${ROCKSDB_VERSION} https://github.com/facebook/rocksdb.git

(cd rocksdb/ && sudo DEBUG_LEVEL=0 make shared_lib install-shared -j`nproc`)

sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"

sudo ldconfig
