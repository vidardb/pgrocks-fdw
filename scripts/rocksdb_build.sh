#!/bin/bash

set -e

git clone -b v${ROCKSDB_VERSION} https://github.com/facebook/rocksdb.git

# USE_RTTI: RocksDB's makefile builds with -fno-rtti by default, so no typeinfo
# for its polymorphic classes ends up in the library. kv_storage.cc derives a
# comparator from rocksdb::Comparator, which since RocksDB 9 inherits from
# Customizable, and the vtable it emits needs typeinfo for those bases. Without
# this the extension links but the postmaster refuses to load it with
# "undefined symbol: _ZTIN7rocksdb12CustomizableE". Distributions package
# RocksDB with RTTI on, which is why only the from-source build hits it.
#
# DISABLE_WARNING_AS_ERROR: RocksDB builds itself with -Werror, which turns any
# warning its own sources happen to trip on the host compiler into a build
# failure that has nothing to do with this extension.
(cd rocksdb/ && sudo DEBUG_LEVEL=0 USE_RTTI=1 DISABLE_WARNING_AS_ERROR=1 make shared_lib install-shared -j`nproc`)

sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"

sudo ldconfig
