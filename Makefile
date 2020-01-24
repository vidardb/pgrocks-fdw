
MODULE_big   = kv_fdw

ifdef VIDARDB
PG_CPPFLAGS += -Wno-declaration-after-statement -DVidarDB
SHLIB_LINK   = -lvidardb
else
PG_CPPFLAGS += -Wno-declaration-after-statement
SHLIB_LINK   = -lrocksdb
endif

OBJS         = src/kv_fdw.o src/kv_utility.o src/kv_shm.o src/kv_storage.o src/kv_posix.o

EXTENSION    = kv_fdw
DATA         = sql/kv_fdw--0.0.1.sql

# Users need to specify their own path
PG_CONFIG    = /usr/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

src/kv_storage.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/kv_storage.cc
