# PostgresForeignDataWrapper

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for RocksDB: https://rocksdb.org/

# Building

We test this foreign data wrapper on Ubuntu Server 18.04 using PostgreSQL-11 and RocksDB-4.9.

* Install PostgreSQL and the dev library:

```
sudo apt-get install postgresql-11

sudo apt-get install postgresql-server-dev-11
```

* Install RocksDB from source code:

```
cd rocksdb

sudo DEBUG_LEVEL=0 make shared_lib install-shared
```

   If necessary, add /usr/local/lib to LD_LIBRARY_PATH.

* Build this foreign data wrapper

```
cd PostgresForeignDataWrapper 

make

sudo make install
```

# Test

From a sudo user:

```
sudo service postgresql restart  

cd PostgresForeignDataWrapper

sudo -u postgres psql -U postgres -a -f test/sql/create.sql 

sudo -u postgres psql -U postgres -d kvtest -a -f test/sql/basic.sql 

sudo -u postgres psql -U postgres -d kvtest -a -f test/sql/clear.sql  
```

# Start PostgreSQL with debug mode

sudo service postgresql stop  

sudo -u postgres /usr/lib/postgresql/11/bin/postgres -d 0 -D /var/lib/postgresql/11/main -c config_file=/etc/postgresql/11/main/postgresql.conf  
