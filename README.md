# PostgresForeignDataWrapper

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for [RocksDB](https://rocksdb.org/)

RocksDB is a high performance key-value store based on a log-structured merge-tree (LSM tree). RocksDB can efficiently use many CPU cores and fast storage. This is the first foreign data wrapper that connects a LSM-tree-based storage engine to PostgreSQL. Because RocksDB is an embeddable key-value store, you do not need to run another server to use this extension.

This extension can also be used for other systems that have RocksDB-like APIs, but please check the compatibility before you use this extension for other systems.

This extension is developed and maintained by the VidarDB team. Please join the [google group](https://groups.google.com/forum/#!forum/postgresql-foreign-data-wrapper-for-rocksdb) to report bugs or issues.

# Building

We test this foreign data wrapper on Ubuntu Server 18.04 using PostgreSQL-11 together with RocksDB-6.2.4 ( built with GCC-8.3.0).

- Install PostgreSQL and the dev library which is required by extensions:

  ```sh
  sudo apt-get install postgresql-11

  sudo apt-get install postgresql-server-dev-11
  ```

- Install RocksDB from source code:


  ```sh
  cd rocksdb

  sudo DEBUG_LEVEL=0 make shared_lib install-shared
  
  sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"  
 
  sudo ldconfig
  ```

- Build this foreign data wrapper:
  
  ```sh
  cd PostgresForeignDataWrapper 

  make

  sudo make install
  ```

- Restart the PostgreSQL server to enable this extension:

  ```sh
  sudo service postgresql restart
  ```

- Uninstall this extension:

  ```sh
  cd PostgresForeignDataWrapper
  
  sudo make uninstall
  ```

# Limitations

- The first attribute in the table definition must be the primary key.

- Composite primary key is not supported for now.  

# Usage

Before using this foreign data wrapper, you need to add it to ```shared_preload_libraries``` in your ```postgresql.conf``` 

    shared_preload_libraries = 'kv_fdw'    # (change requires restart)

and restart Postgres:


```sh
    sudo service postgresql restart  
```

This extension does not have any parameter. After creating the extension and corresponding server, you can use RocksDB as a foreign storage engine for your PostgreSQL.

A simple example is as follows.


```
CREATE DATABASE kvtest;  
\c kvtest  

CREATE EXTENSION kv_fdw;  
CREATE SERVER kv_server FOREIGN DATA WRAPPER kv_fdw;  

CREATE FOREIGN TABLE test(key TEXT, value TEXT) SERVER kv_server;  

INSERT INTO test VALUES('YC', 'VidarDB');  
SELECT * FROM test;  

INSERT INTO test VALUES('California', 'Waterloo');  
SELECT * FROM test;  

DELETE FROM test WHERE key='California';  
SELECT * FROM test;  

UPDATE test SET value='VidarSQL';  
SELECT * FROM test;  

DROP FOREIGN TABLE test;  

DROP SERVER kv_server;  
DROP EXTENSION kv_fdw;  
  
\c postgres  
DROP DATABASE kvtest;  

``` 

# Testing

We have tested certain typical SQL statements and will add more test cases later. The test scripts are in the test/sql folder. The corresponding results can be found in the test/expected folder. You can run the tests in the following way:


```sh
sudo service postgresql restart  

cd PostgresForeignDataWrapper

sudo -u postgres psql -U postgres -a -f test/sql/create.sql 

sudo -u postgres psql -U postgres -d kvtest -a -f test/sql/test.sql 

sudo -u postgres psql -U postgres -d kvtest -a -f test/sql/clear.sql  
```

# Debug 

If you want to debug the source code, you may need to start PostgreSQL in the debug mode:


```sh
sudo service postgresql stop  

sudo -u postgres /usr/lib/postgresql/11/bin/postgres -d 0 -D /var/lib/postgresql/11/main -c config_file=/etc/postgresql/11/main/postgresql.conf
```  
