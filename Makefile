
EXTENSION    = kv_fdw
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
MODULE_big   = $(EXTENSION)
OBJS         = $(patsubst %.c,%.o,$(wildcard src/*.c)) src/kv.o
PG_CONFIG    = /usr/lib/postgresql/11/bin/pg_config
SHLIB_LINK   = -lrocksdb
PG_CPPFLAGS += -Wno-declaration-after-statement

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

src/kv.bc:
	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ src/kv.cc
	