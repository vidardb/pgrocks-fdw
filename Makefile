
MODULE_big     = kv_fdw

# RocksDB 9 needs C++17 for the std::string_view and std::optional in its
# public headers, and RocksDB 10 builds itself as C++20 and uses defaulted
# comparison operators in them, so C++20 is the floor for anything current.
# Kept in one variable because it has to match across the object and bitcode
# rules; if they disagree, a JIT-enabled server compiles the same source twice
# under two different standards.
CXX_STD        = -std=c++20

COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

%.bc : %.cpp
	$(COMPILE.cxx.bc) -o $@ $<
	$(LLVM_BINPATH)/opt -module-summary -f $@ -o $@

ifdef VIDARDB
PG_CPPFLAGS += -DVIDARDB
SHLIB_LINK   = -lvidardb
else
SHLIB_LINK   = -lrocksdb
endif

ifeq ($(shell uname -s),Linux)
    COMPILE.cc   = $(CXX) $(CXXFLAGS) $(CXX_STD) $(CPPFLAGS) -c
endif

ifeq ($(shell uname -s),Darwin)
COMPILE.cc   = $(CXX) $(CXXFLAGS) $(CXX_STD) $(CPPFLAGS) -c
PG_CPPFLAGS += -Wno-deprecated-declarations
SHLIB_LINK  += -lstdc++
endif

PG_CFLAGS   += -Wno-declaration-after-statement
PG_CPPFLAGS += -Isrc

OBJS         = src/kv_fdw.o src/kv_utility.o src/server/kv_storage.o src/ipc/kv_posix.o \
			   src/ipc/kv_message.o src/ipc/kv_channel.o src/ipc/kv_mq.o \
			   src/client/kv_client.o src/server/kv_worker.o src/server/kv_manager.o

EXTENSION    = kv_fdw
DATA         = kv_fdw--0.0.1.sql


# Users need to specify their own path
ifndef PG_CONFIG
PG_CONFIG    = /usr/bin/pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Users can specify their own configuration
REGISTRY ?= vidardb
TAG ?= rocksdb-9.10
IMAGE ?= postgresql
DOCKER ?= docker
NETWORK ?= default
APT_OPTS ?=
ENV_EXTS ?=

src/server/kv_storage.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) $(CXX_STD) -fPIC -c -o $@ src/server/kv_storage.cc

src/ipc/kv_posix.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) $(CXX_STD) -fPIC -c -o $@ src/ipc/kv_posix.cc

src/ipc/kv_message.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) $(CXX_STD) -fPIC -c -o $@ src/ipc/kv_message.cc

src/ipc/kv_channel.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) $(CXX_STD) -fPIC -c -o $@ src/ipc/kv_channel.cc

src/ipc/kv_mq.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) $(CXX_STD) -fPIC -c -o $@ src/ipc/kv_mq.cc
	
src/client/kv_client.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) $(CXX_STD) -fPIC -c -o $@ src/client/kv_client.cc

src/server/kv_worker.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) $(CXX_STD) -fPIC -c -o $@ src/server/kv_worker.cc

src/server/kv_manager.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) $(CXX_STD) -fPIC -c -o $@ src/server/kv_manager.cc

.PHONY: docker-image
docker-image:
	@echo "Building docker image..."
	$(DOCKER) build --no-cache --pull --network $(NETWORK) \
		--build-arg apt_opts="$(APT_OPTS)" \
		--build-arg env_exts="$(ENV_EXTS)" \
		-f docker_image/Dockerfile -t $(REGISTRY)/$(IMAGE):$(TAG) .

.PHONY: indent
indent:
	@echo "Runing pgindent for format code..."
	./src/tools/pgindent/check-indent.sh
