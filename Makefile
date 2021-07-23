
MODULE_big   = kv_fdw

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
    COMPILE.cc   = $(CXX) $(CXXFLAGS) -std=c++11 $(CPPFLAGS) -c
endif

ifeq ($(shell uname -s),Darwin)
COMPILE.cc   = $(CXX) $(CXXFLAGS) -std=c++11 $(CPPFLAGS) -c
PG_CPPFLAGS += -Wno-deprecated-declarations
SHLIB_LINK  += -lstdc++
endif

PG_CFLAGS += -Wno-declaration-after-statement
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
TAG ?= rocksdb-6.2.4
IMAGE ?= postgresql
DOCKER ?= docker
NETWORK ?= default
APT_OPTS ?=
ENV_EXTS ?=

src/server/kv_storage.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/server/kv_storage.cc

src/ipc/kv_posix.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/ipc/kv_posix.cc

src/ipc/kv_message.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/ipc/kv_message.cc

src/ipc/kv_channel.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/ipc/kv_channel.cc

src/ipc/kv_mq.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/ipc/kv_mq.cc
	
src/client/kv_client.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/client/kv_client.cc

src/server/kv_worker.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/server/kv_worker.cc

src/server/kv_manager.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/server/kv_manager.cc

.PHONY: docker-image
docker-image:
	@echo "Building docker image..."
	$(DOCKER) build --no-cache --pull --network $(NETWORK) \
		--build-arg apt_opts="$(APT_OPTS)" \
		--build-arg env_exts="$(ENV_EXTS)" \
		-t $(REGISTRY)/$(IMAGE):$(TAG) docker_image

.PHONY: indent
indent:
	@echo "Runing pgindent for format code..."
	./src/tools/pgindent/check-indent.sh
