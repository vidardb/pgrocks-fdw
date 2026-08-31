#!/bin/bash

set -e

if [[ -z "${VIDARDB_VERSION}" ]]; then
  git clone -b master https://"${ACCESS_TOKEN}"@github.com/vidardb/vidardb-engine.git
else
  git clone -b v"${VIDARDB_VERSION}" https://"${ACCESS_TOKEN}"@github.com/vidardb/vidardb-engine.git
fi

# DISABLE_WARNING_AS_ERROR: see the note in rocksdb_build.sh; the engine is
# a RocksDB fork of the same vintage.
(cd vidardb-engine/ && sudo DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 make shared_lib install-shared -j`nproc`)

sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"

sudo ldconfig
