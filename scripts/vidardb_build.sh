#!/bin/bash

set -e

if [[ -z "${ACCESS_TOKEN:-}" ]]; then
  echo "ACCESS_TOKEN is empty; it must be a token that can read vidardb/vidardb-engine." >&2
  exit 1
fi

# The token goes in the password field. Putting it in the userinfo field on its
# own leaves the password empty, so git falls back to prompting and dies with
# "could not read Password" instead of reporting the real authentication error.
ENGINE_URL="https://x-access-token:${ACCESS_TOKEN}@github.com/vidardb/vidardb-engine.git"

if [[ -z "${VIDARDB_VERSION}" ]]; then
  git clone -b master "${ENGINE_URL}"
else
  git clone -b v"${VIDARDB_VERSION}" "${ENGINE_URL}"
fi

# DISABLE_WARNING_AS_ERROR: see the note in rocksdb_build.sh; the engine is
# a RocksDB fork of the same vintage.
(cd vidardb-engine/ && sudo DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 make shared_lib install-shared -j`nproc`)

sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"

sudo ldconfig
