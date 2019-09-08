
MODULE_big   = kv_fdw

PG_CPPFLAGS += -Wno-declaration-after-statement
SHLIB_LINK   = -lrocksdb
OBJS         = src/kv_fdw.o src/kv.o

EXTENSION    = kv_fdw

PG_CONFIG    = /usr/lib/postgresql/11/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

src/kv.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/kv.cc
