#!/bin/bash

set -e

if [[ -z "${VIDARDB_VERSION}" ]]; then
  git clone -b master https://"${ACCESS_TOKEN}"@github.com/vidardb/vidardb-engine.git
else
  git clone -b v"${VIDARDB_VERSION}" https://"${ACCESS_TOKEN}"@github.com/vidardb/vidardb-engine.git
fi

(cd vidardb-engine/ && sudo DEBUG_LEVEL=0 make shared_lib install-shared -j`nproc`)

sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"

sudo ldconfig
