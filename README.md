<img style="width:100%;" src="/github-banner.png">

# pgrocks-fdw 

[![CI](https://github.com/vidardb/pgrocks-fdw/actions/workflows/main.yml/badge.svg)](https://github.com/vidardb/pgrocks-fdw/actions/workflows/main.yml)

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for [RocksDB](https://rocksdb.org/). This repo has been listed in PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Foreign_data_wrappers). We are also building an extension to supercharge PostgreSQL analytics. It's not product ready yet, but if you are interested, please contact us at info@vidardb.com to gain access.

RocksDB is a high performance key-value store based on a log-structured merge-tree (LSM tree). RocksDB can efficiently use many CPU cores and fast storage. This is the first foreign data wrapper that connects a LSM-tree-based storage engine to PostgreSQL. Because RocksDB is an embeddable key-value store, you do not need to run another server to use this extension.

This extension can also be used for other systems that have RocksDB-like APIs, but please check the compatibility before you use this extension for other systems.

This extension is developed and maintained by the VidarDB team. Currently only works for PG18. Feel free to report bugs or issues via Github.

# Building

CI builds this foreign data wrapper two ways: against PostgreSQL 18 with the RocksDB 9.10 that Debian trixie packages, and against PostgreSQL 18.6 with RocksDB 10.10.1 both built from source. It needs a compiler with C++20, because RocksDB 10 uses C++20 in its public headers.

- Install PostgreSQL and the dev library which is required by extensions:

  ```sh
  # get the signing key and import it
  curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | sudo gpg --dearmor -o /usr/share/keyrings/pgdg.gpg

  # add the repository, using the codename of the distribution you are on
  echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    | sudo tee /etc/apt/sources.list.d/pgdg.list

  # fetch the metadata from the new repo
  sudo apt-get update

  # install postgresql and the dev library
  sudo apt-get install postgresql-18
  sudo apt-get install postgresql-server-dev-18
  ```

- Install [RocksDB](https://github.com/facebook/rocksdb) from source code:

  ```sh
  git clone -b v10.10.1 https://github.com/facebook/rocksdb.git

  cd rocksdb

  # USE_RTTI=1 is required. RocksDB builds with -fno-rtti by default, which
  # leaves no typeinfo for its polymorphic classes in the library, and this
  # extension derives a comparator from rocksdb::Comparator. Without it the
  # extension links but PostgreSQL refuses to load it, reporting
  # "undefined symbol: _ZTIN7rocksdb12CustomizableE".
  sudo DEBUG_LEVEL=0 USE_RTTI=1 make shared_lib install-shared
  
  sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf"  
 
  sudo ldconfig
  ```

- Build this foreign data wrapper:

  ```sh
  git clone https://github.com/vidardb/pgrocks-fdw.git

  cd pgrocks-fdw 

  make

  sudo make install
  ```

  To build the foreign data wrapper for [VidarDB](https://github.com/vidardb/vidardb) [ version <= 1.0.0 ], add flag `VIDARDB=true` to the above `make` command.

- Before using this foreign data wrapper, we need to add it to `shared_preload_libraries` in the `postgresql.conf`:

  ```sh
  sudo bash -c 'echo "shared_preload_libraries = 'kv_fdw'" >> /etc/postgresql/18/main/postgresql.conf'
  ```

  and restart PostgreSQL:

  ```sh
  sudo service postgresql restart
  ```

- When uninstall this extension, first issue the following commands, and then delete the data by locating PostgreSQL data folder via `show data_directory;` in PostgreSQL terminal.

  ```sh
  cd pgrocks-fdw 
  
  sudo make uninstall
  ```

# Limitations

- The first attribute in the table definition must be the primary key. Composite primary key can be achieved via creating a new data type `CREATE TYPE ... AS`.

- Currently no rollback, abort.

- Currently once the table is created, cannot drop or add columns.

- Do not support secondary index.


# Usage

This extension does not have any parameter. After creating the extension and corresponding server, you can use RocksDB as a foreign storage engine for your PostgreSQL.

A simple example is as follows (*you can run '`sudo -u postgres psql -U postgres`' to connect the local postgresql server*):


```
    CREATE DATABASE example;  
    \c example  

    CREATE EXTENSION kv_fdw;  
    CREATE SERVER kv_server FOREIGN DATA WRAPPER kv_fdw;  

    CREATE FOREIGN TABLE student(id INTEGER, name TEXT) SERVER kv_server;  

    INSERT INTO student VALUES(20757123, 'Rafferty');  
    SELECT * FROM student;  

    INSERT INTO student VALUES(20767234, 'Jones');  
    SELECT * FROM student;  

    DELETE FROM student WHERE name='Jones';  
    SELECT * FROM student;  

    UPDATE student SET name='Tom' WHERE id=20757123;  
    SELECT * FROM student;  

    DROP FOREIGN TABLE student;  

    DROP SERVER kv_server;  
    DROP EXTENSION kv_fdw;  
  
    \c postgres  
    DROP DATABASE example;  

``` 

# Testing

We have tested certain typical SQL statements and will add more test cases later. The test scripts are in the sql folder which are recommended to be placed in a non-root directory. The corresponding results can be found in the expected folder.

Against a server that already has the extension installed and preloaded, run every script that has a recorded result and compare:

```sh
    cd pgrocks-fdw

    PGUSER=postgres scripts/run_tests.sh
```

Each script runs against a freshly created ```kvtest``` database. ```testddl.sql``` records ```current_timestamp``` values, so its transcript is compared with timestamps masked; the rest are compared byte for byte. After an intentional behaviour change, regenerate the recorded results with ```scripts/run_tests.sh --regenerate``` and review the diff.

You can also run an individual script by hand:

```sh
    sudo service postgresql restart  

    cd pgrocks-fdw 

    sudo -u postgres psql -U postgres -a -f sql/create.sql 

    sudo -u postgres psql -U postgres -d kvtest -a -f sql/test.sql 

    sudo -u postgres psql -U postgres -d kvtest -a -f sql/clear.sql  
```

# Debug 

If you want to debug the source code, you may need to start PostgreSQL in the debug mode:


```sh
    sudo service postgresql stop  

    sudo -u postgres /usr/lib/postgresql/18/bin/postgres -d 0 -D /var/lib/postgresql/18/main -c config_file=/etc/postgresql/18/main/postgresql.conf
```  

# Docker

We can also run PostgreSQL with RocksDB in Docker container and you can refer to [here](docker_image/README.md).
